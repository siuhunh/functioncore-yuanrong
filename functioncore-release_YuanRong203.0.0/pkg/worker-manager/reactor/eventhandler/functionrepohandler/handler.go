/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package functionrepohandler -
package functionrepohandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	commutils "functioncore/pkg/common/utils"
	workercache "functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/checkpoint"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
)

const (
	// DefaultTimeout define a defaultTimeout
	DefaultTimeout = 900
)

// FunctionRepoHandler define function repo handler
type FunctionRepoHandler struct {
	EtcdClient *etcd3.EtcdClient
	VpcHandler *vpcmanager.VpcHandler
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *FunctionRepoHandler {
	controller, err := vpcmanager.GetVPCController()
	if err != nil {
		fmt.Printf("failed to init vpc controller with error: %s and exit now\n", err.Error())
		log.GetLogger().Errorf("failed to init vpc controller with error: %s and exit now", err.Error())
		log.GetLogger().Sync()
		os.Exit(0)
	}
	handler := &FunctionRepoHandler{
		VpcHandler: controller,
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *FunctionRepoHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.FunctionRepoEventKind && event.Kind != utils.InstanceMetadataEventKind {
		return errors.New("failed to match")
	}
	funcName := utils.GetFuncNameByKey(event.KeyRef)
	funcSvcID := utils.CrNameByKey(event.KeyRef)
	if !workercache.CheckEventSource(event, funcSvcID) {
		log.GetLogger().Warnf("skip %s event, type %s, key %s, name: %s for func: %s from %s etcd, since receive: %v",
			event.Kind, event.EventType, event.KeyRef, funcName, funcSvcID, event.SourceType,
			time.Now().Sub(event.StartTime))
		return nil
	}

	workercache.SingleFlight.Remove(funcSvcID)
	// process FunctionRepo event or process InstanceMetadata event
	log.GetLogger().Infof("process %s event, type %s, key %s, name: %s for func: %s from %s etcd, since receive: %v",
		event.Kind, event.EventType, event.KeyRef, funcName, funcSvcID, event.SourceType,
		time.Now().Sub(event.StartTime))
	fetcher := podfetcher.GetPodFetcher()
	if fetcher == nil {
		return errors.New("pod fetcher is nil")
	}
	ctx := fetcher.CreateNewContext(funcSvcID)
	if event.Kind == utils.InstanceMetadataEventKind {
		return handler.processInstanceMetadata(fetcher, event, funcSvcID)
	} else if event.EventType == evs.Deleted && event.Kind == utils.FunctionRepoEventKind {
		return handler.processFunctionDeleted(ctx, fetcher, event, funcSvcID)
	} else {
		// function repo add event
		functionMeta, err := handler.parseFunctionMeta(event, funcSvcID)
		if err != nil {
			log.GetLogger().Errorf("failed to parse function meta for: %s with error: %s", funcSvcID, err.Error())
			return nil
		}
		crSignature := utils.GetCRSignature(functionMeta)
		actions, metadata := workercache.WorkerCache.HandleFunctionRepo(funcSvcID,
			workercache.NewFuncMeta(functionMeta, event, crSignature))
		fetcher.DoActions(funcSvcID, actions, metadata)
		return nil
	}
}

func (handler *FunctionRepoHandler) processInstanceMetadata(fetcher *podfetcher.PodFetcher,
	event evs.Event, funcSvcID string) error {
	var minScale, maxScale int64
	var idleMode bool
	var profileMeta types.ProfileMetaData
	functionInfo := utils.GetFunctionInfoByKey(event.KeyRef)
	if event.EventType == evs.Deleted {
		minScale, maxScale = utils.DefaultMinInstanceNum, utils.DefaultMaxInstanceNum
		profileMeta = types.ProfileMetaData{}
		log.GetLogger().Warnf("use default min scale: %d, max scale: %d, profile meta: %+v for: %s",
			minScale, maxScale, profileMeta, funcSvcID)
	} else {
		functionRepoInfo, err := handler.parseFunctionMetaInfo(event, funcSvcID)
		if err != nil {
			log.GetLogger().Errorf("failed to parse function meta for: %s with error: %s", funcSvcID, err.Error())
			return nil
		}
		idleMode = functionRepoInfo.InstanceMetaData.IdleMode
		minScale = functionRepoInfo.InstanceMetaData.MinInstance
		maxScale = functionRepoInfo.InstanceMetaData.MaxInstance
		profileMeta = functionRepoInfo.ProfileMetaData
		if minScale < 0 {
			minScale = utils.DefaultMinInstanceNum
			log.GetLogger().Warnf("use default min scale: %d, max scale: %d for: %s", minScale, maxScale, funcSvcID)
		}
		if maxScale < 0 {
			maxScale = utils.DefaultMaxInstanceNum
			log.GetLogger().Warnf("use default min scale: %d, max scale: %d for: %s", minScale, maxScale, funcSvcID)
		}
		log.GetLogger().Infof("update min scale: %d, max scale: %d, profile meta: %+v for: %s",
			minScale, maxScale, profileMeta, funcSvcID)
	}
	// generate actions with min scale and max scale
	actions, metadata := workercache.WorkerCache.HandleInsMetadata(funcSvcID, config.InstanceMetaData{
		MinInstance: minScale, MaxInstance: maxScale, IdleMode: idleMode}, profileMeta, functionInfo)
	fetcher.DoActions(funcSvcID, actions, metadata)
	if metadata.FuncMeta != nil && utils.IsPullTriggerFunction(metadata.FuncMeta) && minScale == 0 {
		// clean VPC PullTrigger
		functionInfo := utils.GetFunctionInfoByKey(event.KeyRef)
		handler.VpcHandler.CleanPullTrigger(functionInfo.FuncName)
	}
	log.GetLogger().Infof("succeed to process instance metadata for func: %s, cost time: %s",
		funcSvcID, time.Since(event.StartTime).String())
	return nil
}

func (handler *FunctionRepoHandler) processFunctionDeleted(ctx context.Context, fetcher *podfetcher.PodFetcher,
	event evs.Event, funcSvcID string) error {
	// parse functionMeta from previous value when function is deleted
	functionMeta, err := handler.parseFunctionMeta(event, funcSvcID)
	if err != nil {
		log.GetLogger().Errorf("failed to parse function meta for: %s with error: %s", funcSvcID, err.Error())
		// no need to retry
		return nil
	}
	if utils.IsPullTriggerFunction(functionMeta) {
		// clean VPC PullTrigger
		functionInfo := utils.GetFunctionInfoByKey(event.KeyRef)
		handler.VpcHandler.CleanPullTrigger(functionInfo.FuncName)
	}
	meta := workercache.NewFuncMeta(nil, event, utils.GetCRSignature(functionMeta))
	if functionMeta != nil && functionMeta.Spec.Configuration.FuncVpc.VpcID != "" &&
		len(functionMeta.Spec.Configuration.DNSDomainCfg) != 0 {
		meta.FuncMeta.Spec.Configuration.FuncVpc = functionMeta.Spec.Configuration.FuncVpc
		meta.FuncMeta.Spec.Configuration.DNSDomainCfg = functionMeta.Spec.Configuration.DNSDomainCfg
	}
	if functionMeta != nil && functionMeta.Spec.Configuration.EndpointTenantVpc.VpcID != "" &&
		len(functionMeta.Spec.Configuration.EndpointDomainCfg) != 0 {
		meta.FuncMeta.Spec.Configuration.EndpointTenantVpc = functionMeta.Spec.Configuration.EndpointTenantVpc
		meta.FuncMeta.Spec.Configuration.EndpointDomainCfg = functionMeta.Spec.Configuration.EndpointDomainCfg
	}
	if functionMeta != nil && functionMeta.Spec.Configuration.CustomContainerConfig.Image != "" {
		meta.FuncMeta.Spec.Configuration.CustomContainerConfig.Image =
			functionMeta.Spec.Configuration.CustomContainerConfig.Image
		meta.FuncMeta.Spec.Configuration.FunctionName = functionMeta.Spec.Configuration.FunctionName
		meta.FuncMeta.Spec.Configuration.FunctionVersion = functionMeta.Spec.Configuration.FunctionVersion
		meta.FuncMeta.Spec.Configuration.TenantID = functionMeta.Spec.Configuration.TenantID
	}

	if functionMeta != nil && functionMeta.Spec.SnapshotDeployment.ObjectID != "" {
		log.GetLogger().Infof("begin to clear snapshot data for func: %s", funcSvcID)
		// obtain the snapshot meta cache before clearing the cache.
		snapshotMeta := workercache.WorkerCache.GetSnapshotMeta(funcSvcID)
		if snapshotMeta == nil || snapshotMeta.ClusterID == "" {
			log.GetLogger().Warnf("unable to get to etcd snapshot cluster for func: %s", funcSvcID)
			snapshotMeta = &types.SnapshotMeta{ClusterID: utils.GetEnvClusterID()}
		}
		go checkpoint.GetCheckpointManager().DeleteSnapShotData(functionMeta, snapshotMeta, funcSvcID)
	}

	actions, metadata := workercache.WorkerCache.HandleFunctionRepo(funcSvcID, meta)
	fetcher.DoActions(funcSvcID, actions, metadata)

	workercache.MetricsCache.ClearByFunctionService(functionMeta)
	workercache.GetCustomRetryAlarmHelper().TryClearAlarmIfExist(funcSvcID)
	go etcdhandler.DeleteInstanceByPrefixFromETCD(context.Background(), functionMeta)
	clearClusterBridgeInstance(functionMeta)
	log.GetLogger().Infof("succeed to process function deleted for func: %s, cost time: %s",
		funcSvcID, time.Since(event.StartTime).String())

	return nil
}

func clearClusterBridgeInstance(functionMeta *faasfunction.FunctionService) {
	if functionMeta == nil {
		return
	}
	if err := utils.ClearClusterBridgeInstance(
		functionMeta.Spec.Configuration.IsBridgeFunction,
		functionMeta.Spec.Configuration.FunctionVersionURN); err != nil {
		log.GetLogger().Warnf("failed to ClearClusterBridgeInstance for %s:%s",
			functionMeta.Spec.Configuration.FunctionVersionURN, err.Error())
	}
}

func (handler *FunctionRepoHandler) parseFunctionMetaInfo(event evs.Event,
	funcSvcID string) (*config.FunctionMetaInfo, error) {
	var (
		functionRepoInfo = config.FunctionMetaInfo{}
		err              error
	)
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("functionservice value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return nil, errors.New("invalid parameter")
	}
	if event.EventType == evs.Deleted {
		err = json.Unmarshal(ev.PrevValue, &functionRepoInfo)
	} else {
		err = json.Unmarshal(ev.Value, &functionRepoInfo)
	}
	if err != nil {
		log.GetLogger().Errorf("unmarshal function repo info for func: %s with key: %s and error: %s",
			funcSvcID, ev.Key, err.Error())
		return nil, err
	}
	return &functionRepoInfo, nil
}

func (handler *FunctionRepoHandler) parseFunctionMeta(event evs.Event,
	funcSvcID string) (*faasfunction.FunctionService, error) {
	var functionMeta faasfunction.FunctionService
	functionRepoInfo, err := handler.parseFunctionMetaInfo(event, funcSvcID)
	if err != nil {
		return nil, err
	}
	commutils.ValidateTimeout(&functionRepoInfo.FuncMetaData.Timeout, DefaultTimeout)
	_, err = govalidator.ValidateStruct(functionRepoInfo)
	if err != nil {
		log.GetLogger().Errorf("param check error, err is: %s", err.Error())
		return nil, errors.New("invalid parameter")
	}
	// no code deploy package, return
	if functionRepoInfo.S3MetaData.ObjectID == "" || functionRepoInfo.S3MetaData.BucketID == "" {
		return nil, errors.New("NODEPLOY")
	}
	if err = utils.TransferRepoMetaToFunctionService(functionRepoInfo, &functionMeta); err != nil {
		log.GetLogger().Errorf("transfer repo meta to function service error: %s", err.Error())
		return nil, err
	}
	return &functionMeta, nil
}
