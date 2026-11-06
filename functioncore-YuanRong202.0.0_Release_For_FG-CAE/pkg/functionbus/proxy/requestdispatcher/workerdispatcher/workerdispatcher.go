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

// Package workerdispatcher the dispatcher of request to worker
package workerdispatcher

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

var (
	errDispatcherType = errors.New("requestDispatcher type error in map")
	// ErrBusTerminating means that functionbus is terminating
	ErrBusTerminating = errors.New("functionbus is terminating")
	// ErrInvokeStatefulWithoutStateID -
	ErrInvokeStatefulWithoutStateID = errors.New("cannot invoke stateful function without stateID")
	minKeepaliveTime                = 15
	maxKeepaliveTime                = 60
)

// WorkerDispatcher dispatch for requests to workers
type WorkerDispatcher struct {
	workerRequestDispatchers map[string]*requestdispatcher.RequestDispatcher
	registry                 *servicediscovery.Registry
	busTerminating           bool
	sync.RWMutex
}

// CreateWorkerDispatcher -
func CreateWorkerDispatcher(registry *servicediscovery.Registry) *WorkerDispatcher {
	return &WorkerDispatcher{
		workerRequestDispatchers: make(map[string]*requestdispatcher.RequestDispatcher, 0),
		registry:                 registry,
		busTerminating:           false,
	}
}

// Request find specific worker to request
func (p *WorkerDispatcher) Request(req *types.DispatcherRequest) error {
	// if the functionbus is terminating, new requests would response ErrBusTerminating
	p.RLock()
	if p.busTerminating {
		p.RUnlock()
		return ErrBusTerminating
	}
	p.RUnlock()

	funcKeyWithResource := utils.GetFuncKeyWithResource(req.FunctionKey,
		types.InstanceResource{CPU: req.Request.CPU, Memory: req.Request.Memory})
	dispatcher, err := p.getFunctionDispatcher(funcKeyWithResource, false)
	if err != nil {
		return err
	}

	// request forward from other proxy, schedule to the worker owned by other proxy
	if req.Position.InstanceID != "" && req.Position.InstanceID != internal.HostName &&
		!strings.HasPrefix(req.Position.InstanceID, common.ProxyReqPrefix) {
		dispatcher.PreSchedule(req.Position.InstanceID)
		workerbroker.GetInstance().Process(req.Position.InstanceID, req)
		return nil
	}

	return dispatcher.Request(req)
}

// DeleteFunctionDispatcher delete function dispatcher by functionKey
func (p *WorkerDispatcher) DeleteFunctionDispatcher(functionKey string) {
	var dispatcherSlice []*requestdispatcher.RequestDispatcher
	p.Lock()
	for keyWithResource, dispatcher := range p.workerRequestDispatchers {
		getFuncKey := utils.GetFuncKeyFromFuncKeyWithResource(keyWithResource)
		if getFuncKey == functionKey {
			delete(p.workerRequestDispatchers, keyWithResource)
			dispatcherSlice = append(dispatcherSlice, dispatcher)
			log.GetLogger().Infof("delete dispatcher of funcKeyWithResource: %s",
				urnutils.AnonymizeTenantKey(keyWithResource))
		}
	}
	p.Unlock()

	log.GetLogger().Debugf("the function has been deleted, functionKey:%s", functionKey)
	for _, dispatcher := range dispatcherSlice {
		go dispatcher.Stop()
	}
}

// UpdateFunctionDispatcher delete function dispatcher by functionKey
func (p *WorkerDispatcher) UpdateFunctionDispatcher(functionKey string) {
	funcMeta, err := p.registry.GetFunctionMetadata(functionKey)
	if err != nil {
		log.GetLogger().Warnf("can't find funcMeta of functionKey: %s, error: %s",
			urnutils.AnonymizeTenantKey(functionKey), err.Error())
		return
	}

	log.GetLogger().Debugf("the function has been updated, functionKey:%s", functionKey)

	if funcMeta.FuncMetaData.IsStatefulFunction {
		p.DeleteFunctionDispatcher(functionKey)
		return
	}

	p.Lock()
	for keyWithResource, dispatcher := range p.workerRequestDispatchers {
		getFuncKey := utils.GetFuncKeyFromFuncKeyWithResource(keyWithResource)
		if getFuncKey == functionKey {
			dispatcher.SetInstanceMetadata(*funcMeta)
		}
	}
	p.Unlock()
}

// InitFunctionDispatcher -
func (p *WorkerDispatcher) InitFunctionDispatcher(funcKeyWithResource, instanceID string,
	provider *servicediscovery.Provider, isSynced bool) {
	dispatcher, err := p.getFunctionDispatcher(funcKeyWithResource, provider.BusinessType == constants.BusinessTypeCAE)
	if err != nil {
		return
	}
	if provider != nil && provider.WillDelete != provider.PreviousWillDeleteState {
		dispatcher.UpdateWorkerWillDeleteStatus(provider.InstanceID, provider.WillDelete)
		provider.PreviousWillDeleteState = provider.WillDelete
	}
}

func (p *WorkerDispatcher) getFunctionDispatcher(funcKeyWithResource string, isCAEFunc bool) (
	*requestdispatcher.RequestDispatcher, error) {
	p.RLock()
	dispatcher, ok := p.workerRequestDispatchers[funcKeyWithResource]
	if ok {
		p.RUnlock()
		return dispatcher, nil
	}
	p.RUnlock()

	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	var metadata servicediscovery.FunctionMetaInfo
	funcMeta, err := p.registry.GetFunctionMetadata(functionKey)
	if err != nil {
		log.GetLogger().Warnf("can't find funcMeta of funcKeyWithResource: %s, error: %s",
			urnutils.AnonymizeTenantKey(funcKeyWithResource), err.Error())
	} else {
		metadata = *funcMeta
		if funcMeta.FuncMetaData.IsStatefulFunction {
			return nil, ErrInvokeStatefulWithoutStateID
		}
	}

	p.Lock()
	dispatcher, ok = p.workerRequestDispatchers[funcKeyWithResource]
	if !ok {
		dispatcher = requestdispatcher.CreateRequestDispatcher(funcKeyWithResource, metadata, isCAEFunc)
		dispatcher.Start()
		p.workerRequestDispatchers[funcKeyWithResource] = dispatcher
		log.GetLogger().Debugf("create request dispatcher successfully, funcKeyWithResource: %s",
			urnutils.AnonymizeTenantKey(funcKeyWithResource))
	}
	p.Unlock()
	return dispatcher, nil
}

// StopAllDispatchers stops all request dispatchers when Functiontask starts to update
func (p *WorkerDispatcher) StopAllDispatchers() {
	p.Lock()
	p.busTerminating = true
	for name, dispatcher := range p.workerRequestDispatchers {
		dispatcher.StopWhenTerminate()
		delete(p.workerRequestDispatchers, name)
	}
	p.Unlock()
}

// GetPendingRequestsMetrics uses to get unhandled requests of each function
func (p *WorkerDispatcher) GetPendingRequestsMetrics() []workermanager.PendingRequestView {
	pendingRequestMetrics := make([]workermanager.PendingRequestView, 0)
	if p == nil {
		return pendingRequestMetrics
	}
	currentTimeStamp := time.Now().UTC().Unix()
	p.RLock()
	defer p.RUnlock()
	for key, dispatcher := range p.workerRequestDispatchers {
		funcKey := utils.GetFuncKeyFromFuncKeyWithResource(key)
		if dispatcher.GetRequestQueueLen() != 0 && funcKey != "" {
			tenantID, funcName, funcVersion := utils.ParseFuncKey(funcKey)
			pendingRequestMetrics = append(pendingRequestMetrics, workermanager.PendingRequestView{
				TenantID:        tenantID,
				FuncName:        funcName,
				FuncVersion:     funcVersion,
				BusinessID:      common.InternalScaleBusinessID,
				ReportTimestamp: currentTimeStamp,
				PendingRequests: dispatcher.GetRequestQueueLen(),
			})
		}
	}
	return pendingRequestMetrics
}

// UpdateKeepAliveTime -
func (p *WorkerDispatcher) UpdateKeepAliveTime(keepAliveMap map[string]string) {
	p.RLock()
	defer p.RUnlock()
	for k, v := range p.workerRequestDispatchers {
		funcKeyWithRes := urnutils.AnonymizeTenantKey(k)
		kaTime := 0
		if kaTimeStr, ok := keepAliveMap[funcKeyWithRes]; ok {
			kaTimeInt, err := strconv.Atoi(kaTimeStr)
			if err != nil {
				log.GetLogger().Warnf("funcKeyWithRes: %s | failed to convert keepalive time from string",
					funcKeyWithRes)
				kaTimeInt = 0
			}
			if kaTimeInt < minKeepaliveTime || kaTimeInt > maxKeepaliveTime {
				log.GetLogger().Warnf("funcKeyWithRes: %s | kaTimeInt: %d | keep alive time is invalid",
					funcKeyWithRes, kaTimeInt)
				kaTimeInt = 0
			}
			kaTime = kaTimeInt
			log.GetLogger().Infof("funcKeyWithRes: %s | keepaliveTime: %d | updated keepalive time",
				funcKeyWithRes, kaTimeInt)
		}
		v.SetSchedulerKeepaliveTime(kaTime)
	}
}
