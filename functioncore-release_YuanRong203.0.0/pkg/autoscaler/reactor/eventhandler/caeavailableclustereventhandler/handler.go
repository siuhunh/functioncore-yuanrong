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

// Package caeavailableclustereventhandler -
package caeavailableclustereventhandler

import (
	"encoding/json"
	"errors"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
)

// CaeAvailableClusterHandler define cae available cluster handler
type CaeAvailableClusterHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *CaeAvailableClusterHandler {
	handler := &CaeAvailableClusterHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *CaeAvailableClusterHandler) HandleEvent(event eventsource.Event) error {
	if event.Kind != utils.CaeAvailableClusterEventKind {
		return errors.New("failed to match event kind CaeAvailableCluster")
	}
	// process FunctionRepo event or process InstanceMetadata event
	log.GetLogger().Infof("process %s event, type %s, key %s, since receive: %v", event.Kind,
		event.EventType, event.KeyRef, time.Now().Sub(event.StartTime))
	funcSvcID := utils.GetFuncSvcIDByCaeAvailableClusterKey(event.KeyRef)

	if funcSvcID == "" {
		return errors.New("invalid KeyRef of cae available cluster event")
	}

	return handler.processAvailableClusterEvent(event, funcSvcID)
}

func (handler *CaeAvailableClusterHandler) processAvailableClusterEvent(event eventsource.Event,
	funcSvcID string) error {

	asCache := cache.GetInstance()
	if event.EventType == eventsource.Deleted {
		asCache.RemoveCaeAvailableCluster(funcSvcID)
		return nil
	}

	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("AvailableClusterEvent value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return errors.New("invalid parameter")
	}
	var clusters []string
	err := json.Unmarshal(ev.Value, &clusters)
	if err != nil {
		log.GetLogger().Errorf("unmarshal available clusters for func: %s with key: %s and error: %s",
			funcSvcID, ev.Key, err.Error())
		return err
	}
	log.GetLogger().Infof("update available clusters to cache, funcSvcID: %s, clusters: %v.",
		funcSvcID, clusters)
	asCache.UpdateCaeAvailableCluster(funcSvcID, clusters)
	return nil
}
