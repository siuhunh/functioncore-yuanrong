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

// Package staterouterhandler defines handler of state router
package staterouterhandler

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/utils"
)

// StateHandler handle etcd event of state router
type StateHandler struct {
}

var clusterID = os.Getenv("CLUSTER_ID")

const (
	// /sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/xxxx
	defaultStateRouterLen = 13
	stateIDIndex          = 12
	base                  = 10
	bitSize               = 64
)

// NewEventHandler create new Handler
func NewEventHandler() *StateHandler {
	return &StateHandler{}
}

// HandleEvent handle tenant vpc router event
func (handler *StateHandler) HandleEvent(event eventsource.Event) error {
	log.GetLogger().Infof("process %s event, type %s, key %s from %s etcd, since receive: %v",
		event.Kind, event.EventType, event.KeyRef, event.SourceType, time.Now().Sub(event.StartTime))
	stateRouter, ok := parseStateRouter(event)
	if !ok {
		return nil
	}
	if stateRouter.Cluster == clusterID {
		// only store the stateID in this cluster, it will be delete if event type is DELETE
		cache.StateWorkersList.Store(stateRouter.StateID, stateRouter)
	} else {
		// delete previously stored stateID
		cache.StateWorkersList.Delete(stateRouter.StateID)
	}
	log.GetLogger().Infof("prepare to handle state: %s router event from cluster: %s, funcName: %s, "+
		"businessID: %s, tenantID: %s, version: %s", stateRouter.StateID, stateRouter.Cluster, stateRouter.FuncName,
		stateRouter.BusinessID, stateRouter.TenantID, stateRouter.Version)
	switch event.EventType {
	case eventsource.Added, eventsource.Updated:
		if stateRouter.Applier != constants.StateManagerUpdate {
			log.GetLogger().Warnf("state router event %s not from state-manager update, ignore: %s",
				event.KeyRef, stateRouter.Applier)
			return nil
		}
		funcSvcID := utils.CrName(stateRouter.BusinessID, stateRouter.TenantID,
			stateRouter.ServiceID+"@"+stateRouter.FuncName, stateRouter.Version)
		log.GetLogger().Infof("update stateRouter for %s, funcSvcID is %s", stateRouter.StateID, funcSvcID)
		if stateRouter.Cluster != clusterID {
			// state router update and move node id to another cluster
			if err := handlePodWithoutReload(funcSvcID, stateRouter); err != nil {
				return err
			}
			return nil
		}
		// state router update and move node id to this cluster
		podName, ok := cache.WorkerCache.GetPodInMap(funcSvcID, stateRouter.StateID)
		if !ok {
			// state pod is not in this cluster, need pull up
			log.GetLogger().Infof("Need to pull up pod, funcSvcID is %s, stateID %s", funcSvcID, stateRouter.StateID)
			if err := handlePodWithPullUp(funcSvcID, stateRouter); err != nil {
				return err
			}
			return nil
		}
		// state pod is in this cluster, need re pull up
		log.GetLogger().Infof("success get podName: %s, stateID: %s", podName, stateRouter.StateID)
		if err := handlePodWithReduce(funcSvcID, stateRouter); err != nil {
			return err
		}
		return nil
	case eventsource.Deleted:
		cache.StateWorkersList.Delete(stateRouter.StateID)
		log.GetLogger().Infof("skip handle add or delete stateRouter event for %s", stateRouter.StateID)
	default:
		log.GetLogger().Errorf("unknown event type %s", event.EventType)
	}
	return nil
}

// parseStateRouter -
func parseStateRouter(event eventsource.Event) (cache.StateMetadata, bool) {
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Warnf("invalid event object %s", event.KeyRef)
		return cache.StateMetadata{}, false
	}
	result := strings.Split(event.KeyRef, "/")
	if len(result) != defaultStateRouterLen {
		log.GetLogger().Warnf("invalid event key %s, length: %d", event.KeyRef, len(result))
		return cache.StateMetadata{}, false
	}
	stateID := result[stateIDIndex]
	valueMap := map[string]string{}
	err := json.Unmarshal(ev.Value, &valueMap)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal state router %s, type: %s, err %s",
			event.KeyRef, event.EventType, err.Error())
		return cache.StateMetadata{}, false
	}
	stateMetaData := cache.StateMetadata{
		StateID:    stateID,
		FuncName:   valueMap[constants.FuncNameKey+stateID],
		TenantID:   valueMap[constants.TenantIDKey+stateID],
		BusinessID: valueMap[constants.BusinessIDKey+stateID],
		Version:    valueMap[constants.VersionKey+stateID],
		ServiceID:  valueMap[constants.ServiceIDKey+stateID],
		NodeID:     valueMap[stateID],
		Creator:    valueMap[constants.CreatorKey+stateID],
		Cluster:    valueMap[constants.ClusterKey],
		Applier:    valueMap[constants.ApplierKey],
	}
	dynamicCPU, err := strconv.ParseInt(valueMap[constants.CPUKey+stateID], base, bitSize)
	if err != nil {
		dynamicCPU = 0
	}
	dynamicMem, err := strconv.ParseInt(valueMap[constants.MemoryKey+stateID], base, bitSize)
	if err != nil {
		dynamicMem = 0
	}
	stateMetaData.CPU = dynamicCPU
	stateMetaData.Memory = dynamicMem
	return stateMetaData, true
}

// handlePodWithoutReload -
func handlePodWithoutReload(funcSvcID string, stateRouter cache.StateMetadata) error {
	log.GetLogger().Infof("state router update and move node id to another cluster, stateID: %s, funcSvcID: %s",
		stateRouter.StateID, funcSvcID)
	podName, ok := cache.WorkerCache.GetPodInMap(funcSvcID, stateRouter.StateID)
	if !ok {
		return nil
	}
	log.GetLogger().Infof("success get podName: %s, prepare to delete it, stateID: %s, funcSvcID: %s",
		podName, stateRouter.StateID, funcSvcID)
	podAction, err := cache.WorkerCache.GetStatePodActionWithoutReload(funcSvcID,
		stateRouter.Creator, stateRouter.StateID)
	if err != nil {
		return err
	}
	podfetcher.StatefulInstanceFlowOutDelete(podAction)
	return nil
}

// handlePodWithPullUp -
func handlePodWithPullUp(funcSvcID string, stateRouter cache.StateMetadata) error {
	podActions, err := cache.WorkerCache.GetStatePodActionPullUp(funcSvcID, stateRouter.Creator,
		stateRouter.StateID, stateRouter.CPU, stateRouter.Memory)
	if err != nil {
		return err
	}
	for _, podAction := range podActions {
		podfetcher.GetPodFetcher().DoActions(podAction.FuncSvc, podAction.StatefulAction, podAction.StatefulMeta)
	}
	return nil
}

// handlePodWithReduce -
func handlePodWithReduce(funcSvcID string, stateRouter cache.StateMetadata) error {
	podActions, err := cache.WorkerCache.GetStatePodActionReduce(funcSvcID, stateRouter.Creator, stateRouter.StateID)
	if err != nil {
		return err
	}
	for _, podAction := range podActions {
		podfetcher.GetPodFetcher().DoActions(podAction.FuncSvc, podAction.StatefulAction, podAction.StatefulMeta)
	}
	return nil
}
