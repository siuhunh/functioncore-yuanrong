/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package clientinfohandler -
package clientinfohandler

import (
	"encoding/json"

	"github.com/pkg/errors"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

// ClientInfoHandler define client information handler
type ClientInfoHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *ClientInfoHandler {
	handler := &ClientInfoHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *ClientInfoHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.ClientInfoEventKind {
		return errors.Errorf("[bridgeplugin] event: %s failed to match event kind: %s",
			event.Kind, utils.ClientInfoEventKind)
	}

	podInfo, err := handler.parsePodInfoByEvent(event)
	if err != nil {
		log.GetLogger().Errorf("failed to parse client information for: %s with error: %s",
			event.KeyRef, err.Error())
		return err
	}

	bridgeCache := cache.Instance()
	if event.EventType != evs.Deleted {
		clientInstance, err := handler.parseClientInfo(event)
		if err != nil {
			return err
		}
		if clientInstance.LastModifyClusterID == utils.ClusterID {
			log.GetLogger().Infof("[bridgeplugin] Last record client info modify by %s,"+
				"match current cluster,skip it, stateId:%s", clientInstance.LastModifyClusterID,
				clientInstance.AssignStateID)
			return nil
		}
		log.GetLogger().Infof("call refresh cache when received update event"+
			"LastModifyClusterID:%s, currentClusterID:%s, stateId %s", clientInstance.LastModifyClusterID,
			utils.ClusterID, clientInstance.AssignStateID)
		// call refresh cache when received update event
		return bridgeCache.RefreshClientCache(podInfo.ClientPodName, clientInstance)
	}
	if event.EventType == evs.Deleted {
		// call clear cache when received deleted event
		log.GetLogger().Infof("call clear cache when received deleted event, "+
			"client podName:%s, bridgeFuncName %s", podInfo.ClientPodName, podInfo.BridgeFuncName)
		return bridgeCache.ClearClientCache(podInfo.ClientPodName, podInfo.BridgeFuncName, podInfo.TenantID)
	}
	return nil
}

func (handler *ClientInfoHandler) parsePodInfoByEvent(event evs.Event) (bridgetypes.PodInfo, error) {
	var podInfo bridgetypes.PodInfo
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return podInfo, errors.New("[bridgeplugin] ClientInfoHandler parsePodInfoByEvent invalid" +
			" etcd event")
	}
	podInfo.ClientPodName = utils.GetPodNameByPath(ev.Key)
	podInfo.BridgeFuncName = utils.GetFuncNameByWorkersPath(ev.Key)
	podInfo.TenantID = utils.GetTenantIDByWorkersPath(ev.Key)
	return podInfo, nil
}

func (handler *ClientInfoHandler) parseClientInfo(event evs.Event) (*bridgetypes.ClientInstance, error) {
	var (
		clientInstance = bridgetypes.ClientInstance{}
		err            error
	)
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return nil, errors.New("[bridgeplugin] ClientInfoHandler parseClientInfo invalid etcd event")
	}

	err = json.Unmarshal(ev.Value, &clientInstance)
	if err != nil {
		return nil, err
	}
	return &clientInstance, nil
}
