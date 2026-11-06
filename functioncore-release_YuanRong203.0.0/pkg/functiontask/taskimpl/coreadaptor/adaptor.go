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

// Package coreadaptor defines adaptor for function core.
package coreadaptor

import (
	"strings"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/adaptor"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/efs"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
	"functioncore/pkg/functiontask/taskimpl/corecontext"
	"functioncore/pkg/functiontask/taskimpl/coreinvoker"
	"functioncore/pkg/functiontask/taskimpl/corestateclient"
	"functioncore/pkg/functiontask/taskimpl/corestorage"
)

var (
	ttlCheckInterval = 1 * time.Minute
)

// CoreAdaptor represents the interface of the adaptor for function core.
type CoreAdaptor struct {
	adaptor.Adaptor
}

// Init the adaptor for function core.
func Init(taskConfig *coreconfig.Config) *CoreAdaptor {
	invoker := new(coreinvoker.Invoker)
	var extFutureStore efs.ExtFutureStore
	if taskConfig.PersistentFuture {
		extFutureStore = kvstorage.New(corestorage.NewStorage())
	} else {
		extFutureStore = new(efs.EmptyExtFutureStore)
	}
	a := adaptor.Adaptor{
		FutureStore:       futurestore.New(),
		CallerFutureStore: futurestore.New(),
		StateQueueMap:     statequeue.NewStateQueueMap(invoker, corestateclient.New(), statequeue.DefaultConfig()),
		ExtFutureStore:    extFutureStore,
	}
	a.InitMethods()
	coreAdaptor := &CoreAdaptor{Adaptor: a}
	stopCh := signals.WaitForSignal()
	coreAdaptor.clearTTLFuture(stopCh)
	return coreAdaptor
}

func (a *CoreAdaptor) clearTTLFuture(stopCh <-chan struct{}) {
	go a.clearFutureStoreTTLFuture(stopCh)
	go a.clearCallerFutureStoreTTLFuture(stopCh)
}

func (a *CoreAdaptor) clearFutureStoreTTLFuture(stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	ticker := time.NewTicker(ttlCheckInterval)
	for {
		select {
		case <-ticker.C:
			futureIDs := a.FutureStore.GetTTLFuture()
			a.FutureStore.ClearFuturesByID(futureIDs)
		case <-stopCh:
			ticker.Stop()
			return
		}
	}
}

func (a *CoreAdaptor) clearCallerFutureStoreTTLFuture(stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	ticker := time.NewTicker(ttlCheckInterval)
	for {
		select {
		case <-ticker.C:
			futureIDs := a.CallerFutureStore.GetTTLFuture()
			a.CallerFutureStore.ClearFuturesByID(futureIDs)
		case <-stopCh:
			ticker.Stop()
			return
		}
	}
}

// MsgHandler handles the message.
func (a *CoreAdaptor) MsgHandler(funcName string, funcCallID uint64, hookArgs *functioncore.HookArgs) error {
	taskCtx := corecontext.New(funcCallID, hookArgs)
	taskCtx.SetExtFutureStore(a.ExtFutureStore)
	taskCtx.SetStateQueue(a.StateQueueMap)
	taskCtx.SetFutureStore(a.CallerFutureStore, a.FutureStore)
	taskCtx.SetFilter(a.CallerFilter, a.Filter)
	taskCtx.SetIsGetInstanceScenario(false)
	if hookArgs.ExternalRequest {
		reqIDList := common.ReqIDList{hookArgs.TraceID}
		taskCtx.SetReqIDList(reqIDList)
		taskCtx.SetSource(common.SourceRuntimeToLocalFunctionTask)
		a.handleExternalRequest(taskCtx, funcName, hookArgs)
		return nil
	}
	err := a.CommonAdaptor(taskCtx, funcName, hookArgs.FuncArgs, common.SourceRuntimeToLocalFunctionTask)
	return err
}

// ExpiredCb will be called when the state is expired.
func (a *CoreAdaptor) ExpiredCb(key string) {
	if !strings.Contains(key, "stateID") {
		return
	}
	log.GetLogger().Debugf("expired callback, key: %s", key)
	success := a.StateQueueMap.DelStateIfIdle(key)
	if !success {
		// 1. the state id is not in the function task.
		// 2. there are requests in the state queue.
		return
	}
	tenantID, serviceID, stateID := metadata.ExtractMetaKey(key)
	err := functioncore.DeleteStateMapping(stateID,
		tenantID, coreconfig.NameOfFunctionTask)
	if err != nil {
		log.GetLogger().Warnf("deleteStateMapping, err: %s", err.Error())
	}
	_, err = functioncore.HDel(metadata.GeneStateHashKey(tenantID, serviceID, ""), stateID)
	if err != nil {
		log.GetLogger().Warnf("delete stateID from StateIDHash, err: %s, key: %s", err.Error(), key)
	}
}
