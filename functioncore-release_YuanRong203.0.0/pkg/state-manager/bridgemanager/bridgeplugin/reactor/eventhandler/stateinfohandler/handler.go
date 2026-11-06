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

// Package stateinfohandler -
package stateinfohandler

import (
	"encoding/json"

	"github.com/pkg/errors"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

// StateInfoHandler define state information handler
type StateInfoHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *StateInfoHandler {
	handler := &StateInfoHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *StateInfoHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.StateInfoEventKind {
		return errors.Errorf("[bridgeplugin] event: %s failed to match event kind: %s",
			event.Kind, utils.StateInfoEventKind)
	}
	stateID, stateIDInfo, err := handler.parseStateIDInfo(event)
	if err != nil {
		log.GetLogger().Errorf("failed to parse stateID information for: %s with error: %s", event.KeyRef, err.Error())
		return err
	}
	bridgeCache := cache.Instance()
	if event.EventType == evs.Deleted {
		// call clear cache when received deleted event
		return bridgeCache.ClearStateInfoCache(stateID)
	}
	if stateIDInfo.LastModifyCluster == utils.ClusterID {
		log.GetLogger().Infof("[bridgeplugin] Last record modify by %s, match current cluster,skip it, "+
			"stateID:%s", stateIDInfo.LastModifyCluster, stateID)
		return nil
	}
	// call refresh cache when received update event
	return bridgeCache.RefreshStateInfoCache(stateID, stateIDInfo)
}

func (handler *StateInfoHandler) parseStateIDInfo(event evs.Event) (string, *cache.StateIDInfo, error) {
	var (
		stateIDInfo = cache.StateIDInfo{}
		err         error
		stateID     string
	)
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		return "", nil, errors.New("[bridgeplugin] StateInfoHandler invalid etcd event")
	}
	stateID = utils.GetStateIDByPath(ev.Key)
	if event.EventType != evs.Deleted {
		err = json.Unmarshal(ev.Value, &stateIDInfo)
	}
	if err != nil {
		return "", nil, err
	}
	return stateID, &stateIDInfo, nil
}
