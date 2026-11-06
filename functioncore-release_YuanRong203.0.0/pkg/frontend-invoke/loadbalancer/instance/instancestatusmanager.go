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

package instance

import "sync"

var (
	manager = &WorkerInstanceManager{
		nodeStatusMap: make(map[string]bool),
	}
)

// WorkerInstanceManager worker instance Manager
type WorkerInstanceManager struct {
	nodeStatusMap   map[string]bool
	nodeStatusMutex sync.RWMutex
}

// GetWorkerInstanceManager -
func GetWorkerInstanceManager() *WorkerInstanceManager {
	return manager
}

// AddWorkerInstanceStatus -
func (manager *WorkerInstanceManager) AddWorkerInstanceStatus(nodeIP string, status bool) {
	manager.nodeStatusMutex.Lock()
	defer manager.nodeStatusMutex.Unlock()
	manager.nodeStatusMap[nodeIP] = status
}

// DeleteWorkerInstanceStatus -
func (manager *WorkerInstanceManager) DeleteWorkerInstanceStatus(nodeIP string) {
	manager.nodeStatusMutex.Lock()
	defer manager.nodeStatusMutex.Unlock()
	delete(manager.nodeStatusMap, nodeIP)
}

// GetWorkerInstanceStatus return status of worker instance
func (manager *WorkerInstanceManager) GetWorkerInstanceStatus(nodeIP string) (bool, bool) {
	manager.nodeStatusMutex.RLock()
	defer manager.nodeStatusMutex.RUnlock()
	status, ok := manager.nodeStatusMap[nodeIP]
	return status, ok
}
