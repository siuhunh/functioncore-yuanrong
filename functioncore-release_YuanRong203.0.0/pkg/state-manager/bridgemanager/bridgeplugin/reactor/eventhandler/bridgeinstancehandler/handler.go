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

// Package bridgeinstancehandler -
package bridgeinstancehandler

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/pkg/errors"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

// BridgeInstanceHandler define bridge instance meta handler
type BridgeInstanceHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *BridgeInstanceHandler {
	handler := &BridgeInstanceHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent implements HandleEvent interface in BridgeInstanceHandler
func (handler *BridgeInstanceHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.BridgeInstanceEventKind {
		return errors.Errorf("[bridgeplugin] event: %s failed to match event kind: %s",
			event.Kind, utils.BridgeInstanceEventKind)
	}

	if event.Object != nil {
		ev, off := event.Object.(*etcd3.Event)
		if !off {
			log.GetLogger().Errorf("[bridgeplugin] bridge instance error")
			return errors.Errorf("[bridgeplugin] bridge instance error")
		}
		// the event of bridge instance is suicide, need to process etcd and cache info
		if ev != nil && ev.Value == nil && ev.PrevValue != nil && event.EventType == evs.Deleted {
			return processBridgeSuicideEvent(event, handler)
		}
	}

	bridgeInstanceMeta, bridgeInstance, err := handler.parseBridgeInstMetaInfo(event)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to parse bridge instance meta for: %s with error: %s",
			event.KeyRef, err.Error())
		return err
	}
	cache.UpdateBridgeInstanceTimestamp(bridgeInstance.BridgeFuncUrn,
		bridgeInstanceMeta.Applier, int(time.Now().Unix()))
	// handler event when stateID exist in bridgeCache
	_, ok := cache.Instance().StateIDInfoCache.Load(bridgeInstanceMeta.StateID)
	if ok {
		traceID := uuid.New().String()
		log.GetLogger().Debugf("[bridgeplugin] perceive bridge instance scale out,"+
			"call NewBridge, traceID: %s", traceID)
		return action.NewBridge(bridgeInstance, bridgeInstanceMeta.StateID, traceID)
	}
	return nil
}

func processBridgeSuicideEvent(event evs.Event, handler *BridgeInstanceHandler) error {
	traceID := uuid.New().String()
	log.GetLogger().Debugf("[bridgeplugin] need to release related info when monitoring bridge suicide event,"+
		"event info %v, traceId:%s", event, traceID)
	preBridgeInstanceMeta, preBridgeInstance, err := handler.parsePreDeleteBridgeInstMetaInfo(event)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to parse preDeleteBridgeInstMetaInfo when bridge "+
			"suicide, keyRef: %s with error: %s, traceID:%s", event.KeyRef, err.Error(), traceID)
		return err
	}
	bridgeCache := cache.Instance()
	_, ok := bridgeCache.FuncNameInfoCache.Load(preBridgeInstance.BridgeFuncUrn)
	if ok {
		log.GetLogger().Infof("bridge func %s meta founded, ignore bridge func suicide event",
			preBridgeInstance.BridgeFuncUrn)
		return nil
	}
	if preBridgeInstanceMeta.StateID == "" {
		return nil
	}
	log.GetLogger().Infof("[bridgeplugin] processBridgeSuicideEvent, preBridgeInstanceMeta : %v, "+
		"preBridgeInstance: %v, traceID: %s", preBridgeInstanceMeta, preBridgeInstance, traceID)
	stateLock, _ := bridgeCache.StateIDLockMap.LoadOrStore(preBridgeInstanceMeta.StateID, &sync.Mutex{})
	lock, ok := stateLock.(*sync.Mutex)
	if !ok || lock == nil {
		return errors.Errorf("[bridgeplugin] failed to load StateId info, stateID: %s, traceID: %s",
			preBridgeInstanceMeta.StateID, traceID)
	}
	lock.Lock()
	defer lock.Unlock()
	if v, ok := bridgeCache.StateIDInfoCache.Load(preBridgeInstanceMeta.StateID); ok {
		stateIDInfo, ok := v.(*cache.StateIDInfo)
		if !ok {
			log.GetLogger().Errorf("[bridgeplugin] failed to get type StateIDInfo, key: %s, traceID: %s",
				preBridgeInstanceMeta.StateID, traceID)
			return errors.Errorf("[bridgeplugin] failed to get type StateIDInfo. key: %s",
				preBridgeInstanceMeta.StateID)
		}
		log.GetLogger().Infof("[bridgeplugin] get StateIDInfo success, event info %+v, "+
			"stateKey: %s, stateID: %s, traceID:%s", event, stateIDInfo.StateKey, preBridgeInstanceMeta.StateID, traceID)
		err = action.ReleaseBridgeWhenSuicide(preBridgeInstance, preBridgeInstanceMeta.StateID, traceID)
		return err
	}
	log.GetLogger().Errorf("[bridgeplugin] Load from StateIDInfoCache nok, stateKey: %s, traceID: %s",
		preBridgeInstanceMeta.StateID, traceID)
	return nil
}

func (handler *BridgeInstanceHandler) parseBridgeInstMetaInfo(event evs.Event) (*config.WorkerInstance,
	*bgtypes.BridgeInstance, error) {
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return nil, nil, errors.New("[bridgeplugin] parse BridgeInstanceHandler invalid etcd event")
	}
	instance := config.WorkerInstance{}
	err := json.Unmarshal(ev.Value, &instance)
	if err != nil {
		return nil, nil, err
	}
	path := ev.Key
	bridgeFuncURN := utils.GetFuncURNByWorkersPath(path)
	bridgePodName := utils.GetPodNameByPath(path)
	bridgeInstance := &bgtypes.BridgeInstance{
		PodName:       bridgePodName,
		BridgeFuncUrn: bridgeFuncURN,
	}
	return &instance, bridgeInstance, nil
}

func (handler *BridgeInstanceHandler) parsePreDeleteBridgeInstMetaInfo(event evs.Event) (*config.WorkerInstance,
	*bgtypes.BridgeInstance, error) {
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return nil, nil, errors.New("[bridgeplugin] pre delete BridgeInstanceHandler invalid etcd event")
	}
	instance := config.WorkerInstance{}
	err := json.Unmarshal(ev.PrevValue, &instance)
	if err != nil {
		return nil, nil, err
	}
	path := ev.Key
	bridgePodName := utils.GetPodNameByPath(path)
	bridgeFuncURN := utils.GetFuncURNByWorkersPath(path)
	bridgeInstance := &bgtypes.BridgeInstance{
		PodName:       bridgePodName,
		BridgeFuncUrn: bridgeFuncURN,
	}
	return &instance, bridgeInstance, nil
}
