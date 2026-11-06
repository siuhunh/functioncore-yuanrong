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

// Package workermanagerinstancehandler is handler of worker-manager
package workermanagerinstancehandler

import (
	"fmt"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	statemanagerutils "functioncore/pkg/state-manager/utils"
)

// Handler define worker-manager handler
type Handler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new an event handler
func NewEventHandler(etcdCli *etcd3.EtcdClient) *Handler {
	handler := &Handler{
		EtcdClient: etcdCli,
	}
	return handler
}

// HandleEvent handle event
func (handler *Handler) HandleEvent(event eventsource.Event) error {
	if event.Kind != utils.WorkManagerEventKind {
		return fmt.Errorf("event type not match, can matched type %s, real type %s",
			utils.WorkManagerEventKind, event.Kind)
	}
	// process worker-manager instance event
	log.GetLogger().Infof("process %s event, type %s, key %s, since receive: %v", event.Kind,
		event.EventType, event.KeyRef, time.Now().Sub(event.StartTime))
	clusterID, nodeIP, err := utils.ParseWorkerManagerClusterInfoFromKey(event.KeyRef)
	if err != nil {
		log.GetLogger().Errorf("failed to get worker-manager cluster info, err: %s", err.Error())
		return err
	}
	processWorkerManagerInstanceEvent(event, clusterID, nodeIP)
	return nil
}

func processWorkerManagerInstanceEvent(event eventsource.Event, clusterID string, nodeIP string) {
	if statemanagerutils.GetClusterID() != clusterID {
		return
	}
	if event.EventType == eventsource.Deleted {
		nodemgr.Instance().RemoveNode()
	} else {
		nodemgr.Instance().AddNode(nodeIP)
	}
}
