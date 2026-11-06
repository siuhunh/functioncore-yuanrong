/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

// Package status include status of worker
package status

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Status is worker status
type Status int32

// Status codes
const (
	Waiting Status = iota
	Initializing
	Ready
	Checkpointing
	CheckpointReady
	CheckpointFailed
)

var (
	workerStatus = struct {
		sync.RWMutex
		status Status
	}{}

	exiting uint32 = 0
)

// SetExitState -
func SetExitState() {
	atomic.StoreUint32(&exiting, 1)
}

// GetExitState -
func GetExitState() uint32 {
	return atomic.LoadUint32(&exiting)
}

// Set sets worker status, this method is concurrent security.
func Set(s Status) {
	workerStatus.Lock()
	workerStatus.status = s
	workerStatus.Unlock()
}

// Get returns worker status, this method is concurrent security.
func Get() Status {
	workerStatus.RLock()
	status := workerStatus.status
	workerStatus.RUnlock()
	return status
}

// CompareAndSet returns true if compare ok, and set the status, it's atomic method.
func CompareAndSet(old, new Status) bool {
	workerStatus.Lock()
	defer workerStatus.Unlock()

	if workerStatus.status == old {
		workerStatus.status = new
		return true
	}
	return false
}

// String return string according case
func (s Status) String() string {
	switch s {
	case Waiting:
		return "waiting"
	case Initializing:
		return "initializing"
	case Ready:
		return "ready"
	case Checkpointing:
		return "checkpointing"
	case CheckpointReady:
		return "checkpointready"
	case CheckpointFailed:
		return "checkpointfailed"
	default:
		return fmt.Sprintf("Unknown status - %d", s)
	}
}
