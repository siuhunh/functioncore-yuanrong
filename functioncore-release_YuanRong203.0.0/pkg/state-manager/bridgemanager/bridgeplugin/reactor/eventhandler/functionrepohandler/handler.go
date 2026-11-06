/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

	"github.com/pkg/errors"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/scalehandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
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
func (handler *FunctionRepoHandler) HandleEvent(event evs.Event) error {
	traceID := uuid.New().String()
	if event.Kind != utils.FunctionRepoEventKind {
		return errors.Errorf("[bridgeplugin] event: %s failed to match event kind: %s, traceID:%s",
			event.Kind, utils.FunctionRepoEventKind, traceID)
	}
	functionMeta, err := handler.parseFunctionMetaInfo(event)
	if err != nil {
		log.GetLogger().Errorf("failed to parse function meta for: %s with error: %s,traceID:%s",
			event.KeyRef, err.Error(), traceID)
		return err
	}
	if !functionMeta.FuncMetaData.IsBridgeFunction {
		log.GetLogger().Debugf("%s is not bridge function type, traceID:%s",
			functionMeta.FuncMetaData.FunctionURN, traceID)
		return nil
	}
	bridgeCache := cache.Instance()
	if event.EventType == evs.Deleted {
		tenantID, _, funcName := utils.GetFunctionInfoByURN(functionMeta.FuncMetaData.FunctionURN)
		scalehandler.ClearLastScaleTime(urnutils.CrName(tenantID, funcName, utils.BridgeVersionLatest))

		log.GetLogger().Debugf("HandleEvent delete FuncNameInfoCache, "+
			"key:%s, val:%+v, traceID:%s", functionMeta.FuncMetaData.FunctionURN, nil, traceID)
		releaseFuncRelatedBridgeRef(bridgeCache, functionMeta, traceID)
		bridgeCache.FuncNameInfoCache.Delete(functionMeta.FuncMetaData.FunctionURN)
		cache.DeleteBridgeInstanceTimestamp(functionMeta.FuncMetaData.FunctionURN)
		return nil
	}
	return bridgeCache.UpdateBridgeConfigByMeta(functionMeta)
}

func releaseFuncRelatedBridgeRef(bridgeCache *cache.BridgeCache, funcMeta *config.FunctionMetaInfo, traceID string) {
	relatedStateID := make([]string, 0)
	bridgeCache.StateIDInfoCache.Range(func(stateIDIf, stateIDInfoIf any) bool {
		stateID, ok := stateIDIf.(string)
		if !ok {
			log.GetLogger().Warnf("failed to assert stateIDIf:%+v, traceID:%s", stateIDIf, traceID)
			return true
		}
		stateInfo, ok := stateIDInfoIf.(*cache.StateIDInfo)
		if !ok {
			log.GetLogger().Warnf("failed to assert stateIDInfoIf:%+v, traceID:%s", stateIDInfoIf, traceID)
			return true
		}
		if stateInfo.BridgeFuncURN == funcMeta.FuncMetaData.FunctionURN {
			relatedStateID = append(relatedStateID, stateID)
		}
		return true
	})
	for _, stateID := range relatedStateID {
		log.GetLogger().Debugf("begin to release bridge ref of stateid:%s, traceID:%s", stateID, traceID)
		err := action.ReleaseBridgeWhenSuicide(&bgtypes.BridgeInstance{}, stateID, traceID)
		if err != nil {
			log.GetLogger().Warnf("release bridge of stateid:%s failed:%s, "+
				"traceID:%s", stateID, err.Error(), traceID)
		}
	}
}

func (handler *FunctionRepoHandler) parseFunctionMetaInfo(event evs.Event) (*config.FunctionMetaInfo, error) {
	var (
		functionRepoInfo = config.FunctionMetaInfo{}
		err              error
	)
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return nil, errors.New("[bridgeplugin] FunctionRepoHandler invalid etcd event")
	}
	if event.EventType == evs.Deleted {
		err = json.Unmarshal(ev.PrevValue, &functionRepoInfo)
	} else {
		err = json.Unmarshal(ev.Value, &functionRepoInfo)
	}
	if err != nil {
		return nil, err
	}
	return &functionRepoInfo, nil
}
