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

// Package workermanagereventhandler -
package workermanagereventhandler

import (
	"errors"
	"time"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

// WorkerManagerHandler define worker-manager handler
type WorkerManagerHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new a event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *WorkerManagerHandler {
	handler := &WorkerManagerHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *WorkerManagerHandler) HandleEvent(event eventsource.Event) error {
	if event.Kind != utils.WorkManagerEventKind {
		return errors.New("failed to match")
	}
	// process FunctionRepo event or process InstanceMetadata event
	log.GetLogger().Infof("process %s event, type %s, key %s, since receive: %v", event.Kind,
		event.EventType, event.KeyRef, time.Now().Sub(event.StartTime))
	workerMgrInstance, err := utils.GetWorkerMgrInsByKey(event.KeyRef)
	if err != nil {
		log.GetLogger().Errorf("failed to get worker-manager ")
		return err
	}
	return handler.processWorkerManagerInstanceEvent(event, workerMgrInstance)
}

func (handler *WorkerManagerHandler) processWorkerManagerInstanceEvent(event eventsource.Event,
	workerMgrInstance *utils.WorkerMgrInstance) error {
	if event.EventType == eventsource.Deleted {
		clustermgr.GetClusterMgr().RemoveCluster(workerMgrInstance.ClusterID)
		return nil
	}
	clusterInfo := clustermgr.Cluster{
		ClusterID: workerMgrInstance.ClusterID,
		ActiveNodes: clustermgr.NodeStatus{
			IP:              workerMgrInstance.NodeIP,
			LastSuccessTime: time.Now(),
		},
	}
	clustermgr.GetClusterMgr().AddCluster(workerMgrInstance.ClusterID, clusterInfo)
	return nil
}
