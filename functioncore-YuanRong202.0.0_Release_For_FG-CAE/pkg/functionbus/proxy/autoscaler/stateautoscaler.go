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

// Package autoscaler contains interface and implement of autoscaler
package autoscaler

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

// StateAutoScaler define scale-up and scale-down function
type StateAutoScaler interface {
	ScaleUp(req *types.DispatcherRequest) (*types.Instance, error)
	ScaleDown(workerID string, stateID string) bool
}

type stateAutoScaler struct {
	// tenantID/funcName/functionVersion/-, cpu/memory is empty for the stateful function
	functionKeyWithResource string
	scaleTimeout            time.Duration
	sync.Mutex
}

// NewStateAutoScaler -
func NewStateAutoScaler(funcKeyWithResource string, metadata servicediscovery.FunctionMetaInfo) StateAutoScaler {
	return &stateAutoScaler{
		functionKeyWithResource: funcKeyWithResource,
		scaleTimeout:            getScaleTimeout(metadata),
	}
}

// ScaleUp -
func (sas *stateAutoScaler) ScaleUp(req *types.DispatcherRequest) (*types.Instance, error) {
	sas.Lock()
	defer sas.Unlock()
	log.GetLogger().Infof("scale up function: %s, stateID: %s, traceID: %s",
		urnutils.AnonymizeTenantKey(sas.functionKeyWithResource), req.Request.StateID, req.TraceID)
	insResource := utils.GetResourceFromFuncKeyWithResource(sas.functionKeyWithResource)
	if insResource == nil {
		log.GetLogger().Warnf("failed to scale up function, can't get instance resource from "+
			"funcKeyWithResource %s", urnutils.AnonymizeTenantKey(sas.functionKeyWithResource))
		return nil, errors.New("insResource is nil")
	}
	scaleUpParam := common.ScaleUpParam{
		InsResource: insResource, TraceID: req.TraceID, NodeLabel: req.NodeLabel,
		FunctionKey: req.FunctionKey, StateID: req.Request.StateID, Timeout: sas.scaleTimeout,
	}
	instance, errCode, errMsg := action.ScaleUp(scaleUpParam)
	if errCode != 0 && errCode != snerror.InnerRuntimeInitTimeoutCode {
		log.GetLogger().Warnf("failed to scale up function: %s, errorCode: %d, error: %s traceID: %s",
			urnutils.AnonymizeTenantKey(sas.functionKeyWithResource), errCode, errMsg, req.TraceID)
		response := buildScaleResp(errCode, errMsg)
		sas.responseError(req, response)
		if snerror.IsUserErr(errCode) {
			return nil, snerror.New(int(errCode), errMsg)
		}
		return nil, errors.New(errMsg)
	}
	log.GetLogger().Infof("scale up function successfully, funcKeyWithResource: %s, deployedNode: %s, "+
		"traceID: %s, stateID: %s, instanceID: %s", urnutils.AnonymizeTenantKey(sas.functionKeyWithResource),
		instance.DeployedNode, req.TraceID, req.Request.StateID, instance.InstanceID)
	return sas.workerScaleUp(instance, req.Request.StateID)
}

func (sas *stateAutoScaler) workerScaleUp(instance *types.Instance, stateID string) (*types.Instance, error) {
	workerAddr := instance.IP + ":" + instance.Port
	if instance.DeployedIP == internal.SelfNodeIP {
		config := &workerbroker.BrokerConfig{
			FuncKeyWithResource: sas.functionKeyWithResource,
			WorkerID:            instance.InstanceID,
			WorkerAddr:          workerAddr,
			StateID:             stateID,
			IsDirectFunc:        instance.IsDirectFunc,
			HasInitializer:      instance.HasInitializer,
		}
		// invoke local worker to handle requests
		if err := workerbroker.GetInstance().CreateWorkerBroker(config); err != nil {
			log.GetLogger().Errorf("failed to create workerBroker, instance: %s", instance.InstanceID)
			// force to delete question worker
			go sas.ScaleDown(instance.InstanceID, stateID)
			return nil, fmt.Errorf("failed to create worker broker, err:%s", err.Error())
		}
		return instance, nil
	}
	// it's remote worker
	err := common.NotifyRemote(instance.DeployedIP, &common.FunctionProperty{
		StateID:    stateID,
		TenantID:   common.BusProxyTenantID,
		InstanceID: instance.InstanceID,
	}, &common.NotifyEvent{
		NotifyType:          common.ChangeState,
		Enable:              true,
		FuncKeyWithResource: sas.functionKeyWithResource,
		WorkerAddress:       workerAddr,
		WorkerOwnerIP:       instance.OwnerIP,
		IsDirectFunc:        instance.IsDirectFunc,
		HasInitializer:      instance.HasInitializer,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to notify remote, and force to scale down the instance: %s, "+
			"nodeIP: %s, funcKeyWithResource: %s", instance.InstanceID, instance.DeployedIP,
			urnutils.AnonymizeTenantKey(sas.functionKeyWithResource))
		// force to delete question worker
		go sas.ScaleDown(instance.InstanceID, stateID)
		return nil, fmt.Errorf("failed to notify remote, err:%s", err.Error())
	}
	workerbroker.GetInstance().CreateRemoteBroker(sas.functionKeyWithResource, instance.InstanceID,
		instance.DeployedIP, stateID)
	return instance, nil
}

// ScaleDown -
func (sas *stateAutoScaler) ScaleDown(workerID string, stateID string) bool {
	log.GetLogger().Infof("start to scale down instance: %s, funcKeyWithResource: %s, stateID: %s", workerID,
		urnutils.AnonymizeTenantKey(sas.functionKeyWithResource), stateID)
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(sas.functionKeyWithResource)
	_, errorCode, errorMsg := action.ScaleDown(workerID, functionKey, false)
	if errorCode == 0 {
		log.GetLogger().Debugf("succeed to scale down instance: %s, funcKeyWithResource: %s, stateID: %s",
			workerID, urnutils.AnonymizeTenantKey(sas.functionKeyWithResource), stateID)
		return true
	}
	log.GetLogger().Warnf("failed to scale down funcKeyWithResource: %s workerID: %s, error code: %d, "+
		"error message: %s, stateID: %s", urnutils.AnonymizeTenantKey(sas.functionKeyWithResource), workerID, errorCode,
		errorMsg, stateID)
	return false
}

func (sas *stateAutoScaler) responseError(req *types.DispatcherRequest, response *types.TCPResponse) {
	log.GetLogger().Warnf("failed to scale up for %s and response error, errCode: %d, errMsg: %s",
		urnutils.AnonymizeTenantKey(sas.functionKeyWithResource), response.ErrorCode, response.ErrorMessage)
	req.Response <- response
}
