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

// Package clusterhandler -
package clusterhandler

import (
	"encoding/json"
	"errors"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/utils"
)

// ClusterMetadataHandler define cluster status metadata handler
type ClusterMetadataHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new event handler
func NewEventHandler(etcdClient *etcd3.EtcdClient) *ClusterMetadataHandler {
	handler := &ClusterMetadataHandler{
		EtcdClient: etcdClient,
	}
	return handler
}

// HandleEvent handle event
func (handler *ClusterMetadataHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.ClusterEventKind {
		return errors.New("failed to match")
	}
	clusterStatusMap, err := handler.parseClusterStatusInfo(event)
	if err != nil {
		log.GetLogger().Errorf("failed to parse cluster status meta info, err:%s", err.Error())
		return errors.New("invalid cluster status event value")
	}
	log.GetLogger().Infof("cluster status keyType %s, value %v in etcd", event.EventType, clusterStatusMap)
	switch event.EventType {
	case evs.Added:
		log.GetLogger().Infof("update events with keyType %s, value %v in etcd", event.EventType, clusterStatusMap)
		clusterstatusutil.ClusterStatusCache.UpdateClusterStatusMap(clusterStatusMap)
		cache.WorkerCache.UpdateFuncMaxScale()
		podAction := cache.ClusterStatusStatefulFlowOut()
		podfetcher.StatefulInstanceFlowOutDelete(podAction)
	case evs.Deleted:
		log.GetLogger().Errorf("delete event type %s", event.EventType)
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
		return errors.New("unsupported event type")
	}
	return nil
}

func (handler *ClusterMetadataHandler) parseClusterStatusInfo(event evs.Event) (
	map[string]clusterstatusutil.ClusterFlowState, error) {
	var clusterStatusMap map[string]clusterstatusutil.ClusterFlowState
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("cluster status info value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return clusterStatusMap, errors.New("invalid parameter")
	}
	err := json.Unmarshal(ev.Value, &clusterStatusMap)
	if err != nil {
		log.GetLogger().Errorf("unmarshal cluster status info with key: %s, error: %s", ev.Key, err.Error())
		return clusterStatusMap, err
	}
	log.GetLogger().Infof("cluster status event key %s, keyType %s, value %v in etcd",
		ev.Key, event.EventType, clusterStatusMap)
	return clusterStatusMap, nil
}
