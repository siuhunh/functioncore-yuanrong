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

// Package future implements the future.
package future

import (
	"sync"
	"time"

	"functioncore/pkg/functiontask/task/common"
)

type valueType = *common.Response

// Future represents a promise which will be resolved in the future.
type Future struct {
	ch  chan struct{}
	val valueType
	sync.RWMutex
}

// New returns a new future store.
func New() *Future {
	return &Future{
		ch:  make(chan struct{}, 1),
		val: nil,
	}
}

// GetUntil return the value of the future if it is obtained within `t`, otherwise return nil.
func (f *Future) GetUntil(t time.Duration) valueType {
	select {
	case <-f.ch:
		f.ch <- struct{}{}
		f.RLock()
		val := f.val
		f.RUnlock()
		return val
	case <-time.After(t):
		return nil
	}
}

// Get blocking get the value of the future.
func (f *Future) Get() valueType {
	f.RLock()
	val := f.val
	f.RUnlock()
	if val == nil {
		<-f.ch
		f.ch <- struct{}{}
	}
	f.RLock()
	val = f.val
	f.RUnlock()
	return val
}

// Peek peeks the value of the future.
func (f *Future) Peek() valueType {
	f.RLock()
	defer f.RUnlock()
	return f.val
}

// Set sets the value of the future.
func (f *Future) Set(result valueType) {
	f.Lock()
	if f.val != nil {
		f.Unlock()
		return
	}
	f.val = result
	f.Unlock()
	f.ch <- struct{}{}
}

// Clear clears the value of the future.
func (f *Future) Clear() {
	f.Lock()
	if f.val != nil {
		f.Unlock()
		<-f.ch
		f.Lock()
		f.val = nil
	}
	f.Unlock()
}
