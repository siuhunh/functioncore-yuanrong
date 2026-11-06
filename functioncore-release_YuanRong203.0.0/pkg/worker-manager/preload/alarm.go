/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package preload alarm
package preload

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
)

var (
	requestFailedNodeIPMap sync.Map
	requestFailedNodeCount atomic.Int64
	existsFailedNode       = false
	// map[nodeIP]map[tenantID]string{}
	exceedTenantPreloadDiskLimitNodeMap  = map[string]map[string]string{}
	exceedTenantPreloadDiskLimitNodeLock sync.Mutex
	exceedPreloadDiskLimitTenant         = "PreloadDiskLimit"

	otherProblemNodeMap  = map[string]map[string]string{}
	otherProblemNodeLock sync.Mutex
)

func periodCheckIsExistsFailedNode(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	ticker := time.NewTicker(checkIsExistsRequestFailedNodeInterval)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debug("check preload request failed node start")
			checkIsExistsFailedNode()
		case <-stopCh:
			log.GetLogger().Errorf("module process exit")
			ticker.Stop()
			return
		}
	}
}

func checkIsExistsFailedNode() bool {
	failedNodeCount := requestFailedNodeCount.Load()
	if failedNodeCount > 0 {
		reachTenantPreloadLimitNodes, reachPreloadLimitNodes := getExceedTenantPreloadDiskLimitNodes()
		otherProblemNodes := getOtherProblemNodes()
		existsFailedNode = true
		if len(reachPreloadLimitNodes) > 0 {
			log.GetLogger().Warnf("preload disk space usage exceeds the limit nodes: %v", reachPreloadLimitNodes)
		}
		if len(reachTenantPreloadLimitNodes) > 0 {
			log.GetLogger().Warnf("preload tenant disk space usage exceeds the limit nodes: %v", reachTenantPreloadLimitNodes)
		}
		if len(otherProblemNodes) > 0 {
			log.GetLogger().Warnf("preload other problem nodes: %v", otherProblemNodes)
		}
		alarm.GetClient().ReportAlarm(alarm.PreloadRequest,
			fmt.Sprintf("error msg=%d nodes where the preload disk space usage exceeds the limit. "+
				"%d nodes where the tenant preload disk space usage exceeds the limit."+
				"%d nodes with other problem.",
				len(reachPreloadLimitNodes), len(reachTenantPreloadLimitNodes), len(otherProblemNodes)))
		return existsFailedNode
	}
	if existsFailedNode && failedNodeCount == 0 {
		existsFailedNode = false
		alarm.GetClient().ClearAlarm(alarm.PreloadRequest)
		log.GetLogger().Infof("preload alarm is cleared when all node preload requests are ok.")
	}
	return existsFailedNode
}

func addRequestFailedNode(nodeIP string) {
	_, exists := requestFailedNodeIPMap.LoadOrStore(nodeIP, "")
	if !exists {
		// not exists in requestFailedNodeIPMap, is new request failed node
		requestFailedNodeCount.Add(1)
	}
}

func removeRequestFailedNodeIfExists(nodeIP string) {
	_, exists := requestFailedNodeIPMap.LoadAndDelete(nodeIP)
	if exists && requestFailedNodeCount.Load() > int64(0) {
		// exists in requestFailedNodeIPMap, request failed node is healthy again
		requestFailedNodeCount.Add(-1)
	}
}

func removeRequestFailedNodeWhenOK(nodeIP string, tenantID string) {
	// the tenant not reach node preload disk limit
	removeExceedTenantPreloadDiskLimitIfExists(tenantID, nodeIP)
	// not reach preload disk limit
	removeExceedTenantPreloadDiskLimitIfExists(exceedPreloadDiskLimitTenant, nodeIP)
	// other problems are eliminated
	removeOtherProblemExists(tenantID, nodeIP)
	if !checkNodeExistsPreloadProblem(nodeIP) {
		// when no exists preload problem (all tenant request the node ok)
		removeRequestFailedNodeIfExists(nodeIP)
	}
}

func recordExceedTenantPreloadDiskLimit(tenantID, nodeIP string) {
	exceedTenantPreloadDiskLimitNodeLock.Lock()
	defer exceedTenantPreloadDiskLimitNodeLock.Unlock()
	tenantIDSet, exists := exceedTenantPreloadDiskLimitNodeMap[nodeIP]
	if !exists {
		tenantIDSet = make(map[string]string)
	}
	tenantIDSet[tenantID] = ""
	exceedTenantPreloadDiskLimitNodeMap[nodeIP] = tenantIDSet
}

func removeExceedTenantPreloadDiskLimitIfExists(tenantID, nodeIP string) {
	exceedTenantPreloadDiskLimitNodeLock.Lock()
	defer exceedTenantPreloadDiskLimitNodeLock.Unlock()
	tenantIDSet, ok := exceedTenantPreloadDiskLimitNodeMap[nodeIP]
	if !ok {
		return
	}
	delete(tenantIDSet, tenantID)
	if len(tenantIDSet) == 0 {
		delete(exceedTenantPreloadDiskLimitNodeMap, nodeIP)
	} else {
		exceedTenantPreloadDiskLimitNodeMap[nodeIP] = tenantIDSet
	}
}

func getExceedTenantPreloadDiskLimitNodes() ([]string, []string) {
	exceedTenantPreloadDiskLimitNodeLock.Lock()
	defer exceedTenantPreloadDiskLimitNodeLock.Unlock()
	var reachTenantPreloadLimitNodes []string
	var reachPreloadLimitNodes []string
	for nodeIP, tenantIDSet := range exceedTenantPreloadDiskLimitNodeMap {
		_, exists := tenantIDSet[exceedPreloadDiskLimitTenant]
		if exists {
			reachPreloadLimitNodes = append(reachPreloadLimitNodes, nodeIP)
		}
		if len(tenantIDSet) == 1 && exists {
			// only reach preload disk limit not append in reachTenantPreloadLimitNodes
			continue
		}
		reachTenantPreloadLimitNodes = append(reachTenantPreloadLimitNodes, nodeIP)
	}
	return reachTenantPreloadLimitNodes, reachPreloadLimitNodes
}

func checkNodeExistsPreloadProblem(nodeIP string) bool {
	exceedTenantPreloadDiskLimitNodeLock.Lock()
	_, existsDiskProblem := exceedTenantPreloadDiskLimitNodeMap[nodeIP]
	exceedTenantPreloadDiskLimitNodeLock.Unlock()

	otherProblemNodeLock.Lock()
	_, existsOtherProblem := otherProblemNodeMap[nodeIP]
	otherProblemNodeLock.Unlock()

	return existsDiskProblem || existsOtherProblem
}

func recordOtherProblem(tenantID, nodeIP string) {
	otherProblemNodeLock.Lock()
	defer otherProblemNodeLock.Unlock()
	tenantIDSet, exists := otherProblemNodeMap[nodeIP]
	if !exists {
		tenantIDSet = make(map[string]string)
	}
	tenantIDSet[tenantID] = ""
	otherProblemNodeMap[nodeIP] = tenantIDSet
}

func removeOtherProblemExists(tenantID, nodeIP string) {
	otherProblemNodeLock.Lock()
	defer otherProblemNodeLock.Unlock()
	tenantIDSet, ok := otherProblemNodeMap[nodeIP]
	if !ok {
		return
	}
	delete(tenantIDSet, tenantID)
	if len(tenantIDSet) == 0 {
		delete(otherProblemNodeMap, nodeIP)
	} else {
		otherProblemNodeMap[nodeIP] = tenantIDSet
	}
}

func getOtherProblemNodes() []string {
	otherProblemNodeLock.Lock()
	defer otherProblemNodeLock.Unlock()
	var nodes []string
	for nodeIP := range otherProblemNodeMap {
		nodes = append(nodes, nodeIP)
	}
	return nodes
}
