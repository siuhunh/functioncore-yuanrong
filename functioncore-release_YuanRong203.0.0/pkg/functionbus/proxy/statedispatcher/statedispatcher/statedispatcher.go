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

// Package statedispatcher the dispatcher of request of stateful function
package statedispatcher

import (
	"errors"
	"sync"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/statedispatcher"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

// StateDispatcher dispatch for requests to workers
type StateDispatcher struct {
	stateReqDispatchers map[string]*statedispatcher.RequestDispatcher
	registry            *servicediscovery.Registry
	sync.Mutex
}

// CreateStateDispatcher -
func CreateStateDispatcher(registry *servicediscovery.Registry) *StateDispatcher {
	return &StateDispatcher{
		stateReqDispatchers: make(map[string]*statedispatcher.RequestDispatcher, 0),
		registry:            registry,
	}
}

// Request find specific worker to request
func (p *StateDispatcher) Request(req *types.DispatcherRequest) error {
	funcKeyWithResource := utils.GetFuncKeyWithResource(req.FunctionKey,
		types.InstanceResource{CPU: req.Request.CPU, Memory: req.Request.Memory})
	dispatcher := p.getFunctionDispatcher(funcKeyWithResource)
	if dispatcher == nil {
		return errors.New(proxyerrors.SendReqErr)
	}
	return dispatcher.Request(req)
}

func (p *StateDispatcher) getFunctionDispatcher(funcKeyWithResource string) *statedispatcher.RequestDispatcher {
	p.Lock()
	defer p.Unlock()
	dispatcher, ok := p.stateReqDispatchers[funcKeyWithResource]
	if ok {
		return dispatcher
	}

	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	funcMetaData, err := p.registry.GetFunctionMetadata(functionKey)
	if err != nil {
		log.GetLogger().Errorf("get func metadata err, funcKeyWithResource: %s, err: %s", funcKeyWithResource,
			err.Error())
		return nil
	}
	dispatcher = statedispatcher.CreateRequestDispatcher(funcKeyWithResource, false, *funcMetaData)
	dispatcher.Start()
	p.stateReqDispatchers[funcKeyWithResource] = dispatcher
	log.GetLogger().Infof("create request dispatcher successfully, funcKeyWithResource: %s",
		urnutils.AnonymizeTenantKey(funcKeyWithResource))
	return dispatcher
}

// DeleteStateReqDispatcher -
func (p *StateDispatcher) DeleteStateReqDispatcher(funcKey string) {
	var dispatcherSlice []*statedispatcher.RequestDispatcher
	p.Lock()
	for funcKeyWithResource, dispatcher := range p.stateReqDispatchers {
		getFuncKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
		if getFuncKey == funcKey {
			delete(p.stateReqDispatchers, funcKeyWithResource)
			dispatcherSlice = append(dispatcherSlice, dispatcher)
			log.GetLogger().Infof("delete dispatcher of funcKeyWithResource: %s",
				urnutils.AnonymizeTenantKey(funcKeyWithResource))
		}
	}
	p.Unlock()
	log.GetLogger().Debugf("the function has been deleted, functionKey:%s", funcKey)
	for _, dispatcher := range dispatcherSlice {
		go dispatcher.Stop()
	}
}

// UpdateFunctionDispatcher update state dispatcher when function updated
func (p *StateDispatcher) UpdateFunctionDispatcher(functionKey string) {
	funcMeta, err := p.registry.GetFunctionMetadata(functionKey)
	if err != nil {
		log.GetLogger().Warnf("can't find funcMeta of functionKey: %s, error: %s",
			urnutils.AnonymizeTenantKey(functionKey), err.Error())
		return
	}
	if !funcMeta.FuncMetaData.IsStatefulFunction {
		p.DeleteStateReqDispatcher(functionKey)
	}
}

// InitFunctionDispatcher init state function dispatcher
func (p *StateDispatcher) InitFunctionDispatcher(funcKeyWithResource string) {
	dispatcher := p.getFunctionDispatcher(funcKeyWithResource)
	if dispatcher == nil {
		log.GetLogger().Warnf("get StateDispatcher failed with %s", funcKeyWithResource)
	}
}

// DeleteInstanceByStateID -
func (p *StateDispatcher) DeleteInstanceByStateID(stateID string) {
	p.Lock()
	for _, reqDispatcher := range p.stateReqDispatchers {
		reqDispatcher.ReceiveInsDeleteEvent(stateID)
	}
	p.Unlock()
}

// PutStateQueueMetadataByStateID -
func (p *StateDispatcher) PutStateQueueMetadataByStateID(stateID string) {
	p.Lock()
	for _, reqDispatcher := range p.stateReqDispatchers {
		reqDispatcher.ReceiveStateQueueMetadataPutEvent(stateID)
	}
	p.Unlock()
}

// DeleteStateQueueByStateID Clear the state queue by channel
func DeleteStateQueueByStateID(stateRouter map[string]string) {
	var stateID string
	for key := range stateRouter {
		if constants.IsStateID(key) {
			stateID = key
			break
		}
	}
	tenantID := stateRouter[constants.TenantIDKey+stateID]
	serviceID := stateRouter[constants.ServiceIDKey+stateID]

	statedispatcher.GetStateQueueMetadataChannel() <- common.StatefulFuncMetaData{
		TenantID:  tenantID,
		ServiceID: serviceID,
		StateID:   stateID,
	}
}
