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

// Package clientinstancehandler -
package clientinstancehandler

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

// ClientInstanceHandler define client instance meta handler
type ClientInstanceHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *ClientInstanceHandler {
	handler := &ClientInstanceHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

func (handler *ClientInstanceHandler) processReleaseInfoByClientCited(podInfo bgtypes.PodInfo, event evs.Event) error {
	v, ok := cache.Instance().ClientCiteBridgeFuncMap.Load(podInfo.ClientPodName)
	traceID := uuid.New().String()
	if !ok {
		log.GetLogger().Debugf("[bridgeplugin] no match cite bridge Func: podName: %s, traceID: %s",
			podInfo.ClientPodName, traceID)
	} else {
		citeBridgeInfo, ok := v.([]string)
		if !ok {
			log.GetLogger().Errorf("[bridgeplugin] failed to convert slice failed, client podName:%s, "+
				"traceID: %s", podInfo.ClientPodName, traceID)
			return nil
		}
		for _, val := range citeBridgeInfo {
			clientInstance, err := handler.getClientInstance(podInfo.ClientPodName, val, podInfo.TenantID)
			if err != nil {
				return err
			}
			log.GetLogger().Debugf("[bridgeplugin] process release info client cited clientPodName is :%s, "+
				"bridgeFuncName:%s, traceID: %s", podInfo.ClientPodName, val, traceID)
			if clientInstance == nil {
				log.GetLogger().Infof("[bridgeplugin] process release info client cited clientPodName "+
					"clientPodName is :%s, clientInstance is nil, traceID: %s", podInfo.ClientPodName, traceID)
				continue
			}
			for stateID := range clientInstance.FuncUseInfo {
				clientInstance.AssignStateID = stateID
				log.GetLogger().Debugf("[bridgeplugin] clientInstance has been delete, client: %s, stateID: "+
					"%s, client instance meta: %s, traceID: %s", clientInstance.PodName, clientInstance.AssignStateID,
					event.KeyRef, traceID)
				if _, err = action.ProcessRelatedInfo(clientInstance, true, traceID); err != nil {
					log.GetLogger().Errorf("[bridgeplugin] clientInstance delete failed, client: %s, stateID: "+
						"%s, client instance meta: %s, err: %s, traceID: %s", clientInstance.PodName,
						clientInstance.AssignStateID, event.KeyRef, err.Error(), traceID)
					continue
				}
			}
		}
		log.GetLogger().Infof("[bridgeplugin] need to delete ClientCiteBridgeFunc, client podName: %s, "+
			"traceID: %s,", podInfo.ClientPodName, traceID)
		cache.Instance().ClientCiteBridgeFuncMap.Delete(podInfo.ClientPodName)
	}
	return nil
}

// HandleEvent handle event
func (handler *ClientInstanceHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.ClientInstanceEventKind {
		return errors.Errorf("[bridgeplugin] event: %s failed to match event kind: %s",
			event.Kind, utils.ClientInstanceEventKind)
	}
	if event.EventType != evs.Deleted {
		return nil
	}
	// client instance delete event
	podInfo, err := handler.parseInstanceMetaInfo(event)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to parse client instance meta for: %s with error: %s",
			event.KeyRef, err.Error())
		return err
	}

	err = handler.processReleaseInfoByClientCited(podInfo, event)
	if err != nil {
		return err
	}
	return nil
}

func (handler *ClientInstanceHandler) parseInstanceMetaInfo(event evs.Event) (bgtypes.PodInfo, error) {
	var podInfo bgtypes.PodInfo
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return podInfo, errors.New("[bridgeplugin] ClientInstanceHandler invalid etcd event")
	}
	podInfo.ClientPodName = utils.GetPodNameByPath(ev.Key)
	podInfo.BridgeFuncName = utils.GetFuncNameByWorkersPath(ev.Key)
	podInfo.TenantID = utils.GetTenantIDByWorkersPath(ev.Key)
	return podInfo, nil
}

func (handler *ClientInstanceHandler) getClientInstance(podName string, funcName string, tenantID string) (
	*bgtypes.ClientInstance, error) {
	clientInstanceKey := fmt.Sprintf("%s_%s_%s", podName, tenantID, funcName)
	v, ok := cache.Instance().ClientPodClientInstCache.Load(clientInstanceKey)
	if !ok {
		log.GetLogger().Infof("[bridgeplugin] can't get clientInstance for clientInstanceKey: %s, "+
			"cache cleared or pod isn't userfunc type.", clientInstanceKey)
		return nil, nil
	}
	clientInstance, ok := v.(*bgtypes.ClientInstance)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] failed to assert type *ClientInstance. value: %T", v)
		return nil, errors.New("[bridgeplugin] failed to assert type *ClientInstance in getClientInstance")
	}
	return clientInstance, nil
}
