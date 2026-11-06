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

package cache

import (
	"context"
	"runtime"
	"sync/atomic"

	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
)

// GetOrCreateAutoScaler -
func GetOrCreateAutoScaler(funcSvcID string, newer func() *autoscaler.AutoScaler) (*autoscaler.AutoScaler, bool) {
	fc, ok := WorkerCache.getFunctionCache(funcSvcID)
	if !ok {
		return nil, false
	}

	fc.Lock()
	if fc.autoScaler == nil {
		fc.autoScaler = newer()
		fc.deleteHook = append(fc.deleteHook, func(fc *functionCache) {
			fc.autoScaler.Close()
			fc.autoScaler = nil
		})
		fc.deleteInstanceHook = append(fc.deleteInstanceHook,
			func(fc *functionCache, instance v1alpha1.WorkerInstance) {
				fc.autoScaler.RemoveWorkerInstanceMetrics(context.Background(), instance.InstanceID)
			})
	}
	as := fc.autoScaler
	fc.Unlock()
	return as, true
}

// SpinMutex -
type SpinMutex int32

// Lock implements sync.Locker
func (m *SpinMutex) Lock() {
	for !m.TryLock() {
		runtime.Gosched()
	}
}

// Unlock implements sync.Locker
func (m *SpinMutex) Unlock() {
	atomic.StoreInt32((*int32)(m), 0)
}

// TryLock tries to lock the mutex. Returns true if the mutex locks successfully.
func (m *SpinMutex) TryLock() bool {
	return atomic.CompareAndSwapInt32((*int32)(m), 0, 1)
}
