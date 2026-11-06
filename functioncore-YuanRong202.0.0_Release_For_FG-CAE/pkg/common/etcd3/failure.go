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

// Package etcd3 is the cache for failed etcd operations
package etcd3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
)

// ETCDOperation -
type ETCDOperation struct {
	Ctx     context.Context
	Value   string
	Operate string
}

var (
	isDoing int32
	// consecutiveFailuresNums counts the number of consecutive failures
	consecutiveFailuresNums int32
	// failedOperations key is etcd path, value is etcd operation
	failedOperation sync.Map
)

const (
	// intervalFailuresNums an alarm is generated after 10 consecutive failures
	intervalFailuresNums = 10
)

// GetDoingStatus -
func GetDoingStatus() int32 {
	return atomic.LoadInt32(&isDoing)
}

// StoreDoingStatus -
func StoreDoingStatus(status int32) {
	atomic.StoreInt32(&isDoing, status)
}

// SaveFailedOperation -
func SaveFailedOperation(ctx context.Context, path, value, operate string) {
	failedOperation.Store(path, &ETCDOperation{
		Ctx:     ctx,
		Value:   value,
		Operate: operate,
	})
}

// RemoveFailedOperation -
func RemoveFailedOperation(path string) {
	failedOperation.Delete(path)
}

// RedoFailedOperations -
func RedoFailedOperations(redoFunc func(ctx context.Context, path, value, operation string) error,
	stopCh <-chan struct{}) {
	// status is doing and it will not range and redo
	if GetDoingStatus() == 1 {
		return
	}
	if stopCh == nil {
		return
	}
	select {
	case _, ok := <-stopCh:
		if !ok {
			// stop channel is closed
			return
		}
	default:
		StoreDoingStatus(1)
		etcdRetryOperate(redoFunc)
		StoreDoingStatus(0)
	}
}

func etcdRetryOperate(redoFunc func(ctx context.Context, path, value, operation string) error) {
	failedOperation.Range(func(key, value interface{}) bool {
		path, ok := key.(string)
		if !ok {
			return false
		}
		operation, ok := value.(*ETCDOperation)
		if !ok {
			return false
		}
		select {
		case _, ok = <-operation.Ctx.Done():
			if !ok {
				log.GetLogger().Warnf("context is canceled for path: %s and operate: %s", path, operation.Operate)
			}
			// ignore context canceled because function changed or deleted will not influence redo operate etcd
			operation.Ctx = context.Background()
		default: // no block
		}
		if err := redoFunc(operation.Ctx, path, operation.Value, operation.Operate); err != nil {
			atomic.AddInt32(&consecutiveFailuresNums, 1)
			if failureNums := atomic.LoadInt32(&consecutiveFailuresNums); failureNums%intervalFailuresNums == 0 {
				log.GetLogger().Errorf("failed to operate etcd, failures num: %d", failureNums)
				alarm.GetClient().ReportAlarm(alarm.EtcdOperationFailedContinuously,
					fmt.Sprintf("error msg=%s,", fmt.Sprintf("failed to operate etcd, failures num: %d", failureNums)))
			}
			log.GetLogger().Warnf("failed to redo operating etcd with %s and operate: %s, error: %s",
				path, operation.Operate, err.Error())
			return true
		}
		atomic.StoreInt32(&consecutiveFailuresNums, 0)
		alarm.GetClient().ClearAlarm(alarm.EtcdOperationFailedContinuously)
		log.GetLogger().Infof("succeed to redo operating etcd with %s and operate: %s", path, operation.Operate)
		failedOperation.Delete(key)
		return true
	})
}
