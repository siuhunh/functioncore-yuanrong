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

package cache

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
)

var (
	checkpointCache = &CheckpointCache{
		checkpointTaskList: make(map[string]checkpointTaskInfo),
	}
)

// CheckpointCache CheckpointCache struct
type CheckpointCache struct {
	// key: taskID   value: *checkpointTaskInfo
	checkpointTaskList map[string]checkpointTaskInfo
	mutex              sync.RWMutex
}

type checkpointTaskInfo struct {
	canceled        bool
	podName         string
	cancelStartTime time.Time
}

// AddCheckpointTaskInfo -
func (cc *CheckpointCache) AddCheckpointTaskInfo(taskID string) {
	if taskID == "" {
		log.GetLogger().Warnf("cache task info failed, taskID can not be empty.")
		return
	}
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	cc.checkpointTaskList[taskID] = checkpointTaskInfo{
		canceled:        false,
		podName:         "",
		cancelStartTime: time.Time{},
	}
	log.GetLogger().Infof("checkpoint task added, taskID: %s, cached task num:%d.", taskID, len(cc.checkpointTaskList))
}

// DelCheckpointTaskInfo -
func (cc *CheckpointCache) DelCheckpointTaskInfo(taskID string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	info, exist := cc.checkpointTaskList[taskID]
	if exist && info.canceled {
		log.GetLogger().Infof("taskID cancel cost time: %s, taskID: %s.",
			time.Since(info.cancelStartTime).String(), taskID)
	}
	delete(cc.checkpointTaskList, taskID)
}

// SetTaskInfoCancel -
func (cc *CheckpointCache) SetTaskInfoCancel(taskID string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	info, exist := cc.checkpointTaskList[taskID]
	if !exist {
		log.GetLogger().Errorf("taskID not found when cancel! taskID: %s.", taskID)
		return
	}
	info.canceled = true
	info.cancelStartTime = time.Now()
	cc.checkpointTaskList[taskID] = info
}

// SetTaskInfoPodName -
func (cc *CheckpointCache) SetTaskInfoPodName(taskID, podName string) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	info, exist := cc.checkpointTaskList[taskID]
	if !exist {
		log.GetLogger().Errorf("taskID not found when cache podName! taskID: %s.", taskID)
		return
	}
	info.podName = podName
	cc.checkpointTaskList[taskID] = info
}

// GetTaskInfoCancel -
func (cc *CheckpointCache) GetTaskInfoCancel(taskID string) bool {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	info, exist := cc.checkpointTaskList[taskID]
	if !exist {
		log.GetLogger().Errorf("taskID not found! taskID: %s.", taskID)
		return false
	}
	return info.canceled
}

// GetTaskInfoPodName -
func (cc *CheckpointCache) GetTaskInfoPodName(taskID string) string {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	info, exist := cc.checkpointTaskList[taskID]
	if !exist {
		log.GetLogger().Errorf("taskID not found when query podName! taskID: %s.", taskID)
		return ""
	}
	return info.podName
}

// IsTaskExist -
func (cc *CheckpointCache) IsTaskExist(taskID string) bool {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	_, exist := cc.checkpointTaskList[taskID]
	return exist
}

// IsTaskCanceled -
func (cc *CheckpointCache) IsTaskCanceled(taskID string) bool {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()
	info, exist := cc.checkpointTaskList[taskID]
	if !exist {
		log.GetLogger().Errorf("taskID not found when query cancel! taskID: %s.", taskID)
		return false
	}
	return info.canceled
}

// GetCheckpointCache get cache of checkpoint task
func GetCheckpointCache() *CheckpointCache {
	return checkpointCache
}
