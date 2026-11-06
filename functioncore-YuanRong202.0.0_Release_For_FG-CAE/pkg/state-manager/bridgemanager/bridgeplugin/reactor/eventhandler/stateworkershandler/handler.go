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

// Package stateworkershandler -
package stateworkershandler

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

// StateWorkerHandler define state information handler
type StateWorkerHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new an event handler
func NewEventHandler(etcdCli *etcd3.EtcdClient) *StateWorkerHandler {
	handler := &StateWorkerHandler{
		EtcdClient: etcdCli,
	}
	return handler
}

// HandleEvent handle event
func (handler *StateWorkerHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.StateWorkersEventKind {
		return fmt.Errorf("event type not match, can matched type %s, real type %s",
			utils.StateWorkersEventKind, event.Kind)
	}
	stateID, err := utils.ParseStateIDFromStateWorkersKey(event.KeyRef)
	if err != nil {
		return err
	}
	traceID := uuid.New().String()
	log.GetLogger().Debugf("received stateworkers event, type:%s, key:%s, traceID:%s",
		event.EventType, event.KeyRef, traceID)
	bridgeCache := cache.Instance()
	if event.EventType == evs.Deleted {
		handler.deleteStateWorker(bridgeCache, stateID)
		bridgeIns := &bgtypes.BridgeInstance{}
		return action.ReleaseBridgeWhenSuicide(bridgeIns, stateID, traceID)
	}
	handler.addStateWorker(bridgeCache, stateID)
	stateInfoCache := &bridgeCache.StateIDInfoCache
	stateInfoIf, ok := stateInfoCache.Load(stateID)
	if !ok {
		log.GetLogger().Warnf("can't find stateInfoCache of stateid %s, ignore it", stateID)
		return nil
	}
	stateInfo, ok := stateInfoIf.(*cache.StateIDInfo)
	if !ok {
		return fmt.Errorf("stateInfo of stateID %s loaded from stateInfoCache assert err", stateID)
	}
	etcdEvent, ok := event.Object.(*etcd3.Event)
	if !ok {
		return fmt.Errorf("event object assert etcd3.Event failed")
	}
	stateWorkerInfo := make(map[string]string)
	err = json.Unmarshal(etcdEvent.Value, &stateWorkerInfo)
	if err != nil {
		return err
	}
	newCluster := stateWorkerInfo["cluster"]
	oldCluster := stateInfo.BelongCluster
	if len(newCluster) != 0 && newCluster != oldCluster {
		stateInfo.BelongCluster = newCluster
		err := bridgeCache.StoreSpecificStateIDInfoToEtcd(stateID, traceID)
		if err != nil {
			stateInfo.BelongCluster = oldCluster
			return err
		}
	}
	return nil
}

func (handler *StateWorkerHandler) addStateWorker(bridgeCache *cache.BridgeCache, stateID string) {
	bridgeCache.StateWorkersLock.Lock()
	bridgeCache.StateWorkersCache[stateID] = true
	bridgeCache.StateWorkersLock.Unlock()
}

func (handler *StateWorkerHandler) deleteStateWorker(bridgeCache *cache.BridgeCache, stateID string) {
	bridgeCache.StateWorkersLock.Lock()
	delete(bridgeCache.StateWorkersCache, stateID)
	bridgeCache.StateWorkersLock.Unlock()
}
