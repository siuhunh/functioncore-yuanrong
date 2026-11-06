/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package etcd3

import (
	"sync"
	"time"
)

// LeaseMonitor -
type LeaseMonitor struct {
	mutex          sync.Mutex
	lastUpdateTime time.Time
}

// CreateLeaseMonitor -
func CreateLeaseMonitor() *LeaseMonitor {
	return &LeaseMonitor{
		mutex: sync.Mutex{},
	}
}

// Update -
func (m *LeaseMonitor) Update() {
	m.mutex.Lock()
	m.lastUpdateTime = time.Now()
	m.mutex.Unlock()
}

// CheckLeaseTimeout -
func (m *LeaseMonitor) CheckLeaseTimeout(timeout time.Duration) (bool, time.Duration) {
	m.mutex.Lock()
	isTimeout := false
	leaseFailedDuration := time.Since(m.lastUpdateTime)
	if leaseFailedDuration > timeout {
		isTimeout = true
	}
	m.mutex.Unlock()
	return isTimeout, leaseFailedDuration
}
