/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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
	"encoding/json"
	"errors"
	"time"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/instance"
	"functioncore/pkg/autoscaler/scaler"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/functiontypes"
)

const (
	// DefaultTimeout define a defaultTimeout
	DefaultTimeout = 900
)

// FunctionRepoHandler define function repo handler
type FunctionRepoHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *FunctionRepoHandler {
	handler := &FunctionRepoHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *FunctionRepoHandler) HandleEvent(event eventsource.Event) error {
	if event.Kind != utils.FunctionRepoEventKind && event.Kind != utils.InstanceMetadataEventKind {
		return errors.New("failed to match")
	}
	// process FunctionRepo event or process InstanceMetadata event
	log.GetLogger().Infof("process %s event, type %s, key %s, since receive: %v", event.Kind,
		event.EventType, event.KeyRef, time.Now().Sub(event.StartTime))
	if event.Kind == utils.InstanceMetadataEventKind {
		cluster, funcSvcID := utils.GetClusterByInstanceKey(event.KeyRef)
		if cluster == "" {
			log.GetLogger().Errorf("failed to parse cluster when processInstanceMetadata, key: %s", event.KeyRef)
			return errors.New("failed to parse cluster when processInstanceMetadata")
		}
		return handler.processInstanceMetadata(event, cluster, funcSvcID)
	}
	funcSvcID := utils.CrNameByKey(event.KeyRef)
	return handler.processFunctionMetadata(event, funcSvcID)
}

func (handler *FunctionRepoHandler) processFunctionMetadata(event eventsource.Event, funcSvcID string) error {
	// function repo add event
	functionMeta, err := handler.parseFunctionMeta(event, funcSvcID)
	if err != nil {
		log.GetLogger().Warnf("failed to parse function meta for: %s with error: %s", err.Error())
		return nil
	}
	if functionMeta.FuncMetaData.BusinessType != constants.BusinessTypeCAE {
		log.GetLogger().Debugf("skip non CAE function for: %s", funcSvcID)
		return nil
	}

	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("filed to get config when process function metadata, err: %s", err.Error())
		return nil
	}

	asCache := cache.GetInstance()
	if event.EventType == eventsource.Deleted {
		asCache.RemoveResourceMetricCacheByFunc(funcSvcID)
		asCache.RemoveCaeMetricCacheByFunc(funcSvcID)
		asCache.RemoveFunctionMeta(funcSvcID)
		asCache.RemoveFunctionInstanceCache(funcSvcID)
		scaler.GetScaleManager().RemoveScaler(funcSvcID)
		scaler.ClearLastScaleTime(funcSvcID)
		scaler.ClearLastScaleUpTime(funcSvcID)
		if conf.EnableInstanceKeyHandle {
			instance.DeleteInfo(functionMeta)
		}
		return nil
	}
	minScale := functionMeta.InstanceMetaData.MinInstance
	maxScale := functionMeta.InstanceMetaData.MaxInstance
	if minScale < 0 {
		minScale = utils.DefaultMinInstanceNum
		log.GetLogger().Warnf("use default min scale: %d, max scale: %d for: %s", minScale, maxScale, funcSvcID)
	}
	if maxScale < 0 {
		maxScale = utils.DefaultMaxInstanceNum
		log.GetLogger().Warnf("use default min scale: %d, max scale: %d for: %s", minScale, maxScale, funcSvcID)
	}
	log.GetLogger().Infof("update min scale: %d, max scale: %d, for: %s", minScale, maxScale, funcSvcID)
	cache.GetInstance().UpdateInstanceMetaCache(funcSvcID, minScale, maxScale)

	oldFunctionMeta, exists := asCache.GetFunctionMeta(funcSvcID)
	asCache.UpdateFunctionMeta(funcSvcID, functionMeta)
	asCache.UpdateFunctionInstanceCache(funcSvcID)
	asbresConfig := functionMeta.ExtendedMetaData.AsbResConfig
	scaler.GetScaleManager().UpdateScalerGroup(funcSvcID, asbresConfig)

	if conf.EnableInstanceKeyHandle && instance.CheckISNeedUpdate(exists, oldFunctionMeta, functionMeta) {
		instance.UpdateInfo(funcSvcID, functionMeta)
	}
	return nil
}

func (handler *FunctionRepoHandler) parseFunctionMetaInfo(event eventsource.Event,
	funcSvcID string) (*functiontypes.FunctionEtcdInfo, error) {
	var (
		functionRepoInfo = functiontypes.FunctionEtcdInfo{}
		err              error
	)
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("functionservice value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return nil, errors.New("invalid parameter")
	}
	if event.EventType == eventsource.Deleted {
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

func (handler *FunctionRepoHandler) parseFunctionMeta(event eventsource.Event,
	funcSvcID string) (*functiontypes.FunctionEtcdInfo, error) {
	functionMeta, err := handler.parseFunctionMetaInfo(event, funcSvcID)
	if err != nil {
		return nil, err
	}
	_, err = govalidator.ValidateStruct(functionMeta)
	if err != nil {
		log.GetLogger().Errorf("param check error, err is: %s", err.Error())
		return nil, errors.New("invalid parameter")
	}
	return functionMeta, nil
}

func (handler *FunctionRepoHandler) processInstanceMetadata(event eventsource.Event, cluster, funcSvcID string) error {
	_, err := handler.parseFunctionMetaInfo(event, funcSvcID)
	if err != nil {
		log.GetLogger().Errorf("failed to parse function meta for: %s with error: %s", funcSvcID, err.Error())
		return nil
	}

	return nil
}
