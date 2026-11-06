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

// Package networkhandler -
package networkhandler

import (
	"encoding/json"
	"errors"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

// PublicNetworkHandler define public network bandwidth handler
type PublicNetworkHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *PublicNetworkHandler {
	handler := &PublicNetworkHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

func (handler *PublicNetworkHandler) parsePublicNetworkMetaInfo(event evs.Event) (cache.PublicNetworkConfig, error) {
	publicNetworkConf := cache.PublicNetworkConfig{}
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("public network value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return cache.PublicNetworkConfig{}, errors.New("invalid parameter")
	}
	err := json.Unmarshal(ev.Value, &publicNetworkConf)
	if err != nil {
		log.GetLogger().Errorf("unmarshal meta info with key: %s, error: %s", ev.Key, err.Error())
		return cache.PublicNetworkConfig{}, err
	}
	log.GetLogger().Infof("public network key %s, keyType %s, value %v in etcd", ev.Key, event.EventType,
		publicNetworkConf)
	return publicNetworkConf, nil
}

// HandleEvent handle event
func (handler *PublicNetworkHandler) HandleEvent(event evs.Event) error {
	log.GetLogger().Infof("process %s event, type %s, key %s from %s etcd, since receive: %v",
		event.Kind, event.EventType, event.KeyRef, event.SourceType, time.Now().Sub(event.StartTime))
	if event.Kind != utils.PublicNetworkEventKind {
		return errors.New("failed to match")
	}
	projectID, err := utils.ProjectIDByKey(event.KeyRef)
	if err != nil {
		log.GetLogger().Errorf("failed to get projectID by key, err:%s", err.Error())
		return errors.New("invalid public network key")
	}
	switch event.EventType {
	case evs.Added, evs.Updated:
		publicNetworkConf, err := handler.parsePublicNetworkMetaInfo(event)
		if err != nil {
			log.GetLogger().Errorf("failed to parse public network meta info, err:%s", err.Error())
			return errors.New("invalid public network event value")
		}
		log.GetLogger().Infof("public network keyType %s, value %v in etcd", event.EventType, publicNetworkConf)
		cache.WorkerCache.UpdatePublicNetworkConf(projectID, publicNetworkConf)
	case evs.Deleted:
		cache.WorkerCache.DeletePublicNetworkConf(projectID)
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
		return errors.New("unsupported event type")
	}
	return nil
}
