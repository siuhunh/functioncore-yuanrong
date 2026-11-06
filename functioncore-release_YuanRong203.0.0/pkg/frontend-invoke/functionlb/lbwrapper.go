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

package functionlb

import (
	"sync"
	"time"

	"functioncore/pkg/common/loadbalance"
)

type lbWrapper struct {
	m  *sync.Mutex
	lb loadbalance.LoadBalance
}

func createLBWrapper(lb loadbalance.LoadBalance) *lbWrapper {
	return &lbWrapper{
		m:  &sync.Mutex{},
		lb: lb,
	}
}

// Start -
func (w *lbWrapper) Start() {
	w.lb.Start()
}

// Stop -
func (w *lbWrapper) Stop() {
	w.lb.Stop()
}

// Add -
func (w *lbWrapper) Add(workerInstance string) {
	if w.lb.NoLock() {
		w.lb.Add(workerInstance, 1)
		return
	}

	w.m.Lock()
	w.lb.Add(workerInstance, 1)
	w.m.Unlock()
}

// Remove -
func (w *lbWrapper) Remove(workerInstance string) {
	if w.lb.NoLock() {
		w.lb.Remove(workerInstance)
		return
	}

	w.m.Lock()
	w.lb.Remove(workerInstance)
	w.m.Unlock()
}

// NextWithRequest -
func (w *lbWrapper) NextWithRequest(req *loadbalance.Request, move bool) interface{} {
	if w.lb.NoLock() {
		res := w.lb.NextWithRequest(req, move)
		return res
	}

	w.m.Lock()
	res := w.lb.NextWithRequest(req, move)
	w.m.Unlock()
	return res
}

// Done -
func (w *lbWrapper) Done(workerInstance string, cost time.Duration) {
	if w.lb.NoLock() {
		w.lb.Done(workerInstance, cost)
		return
	}

	w.m.Lock()
	w.lb.Done(workerInstance, cost)
	w.m.Unlock()
}

// SetConcurrency -
func (w *lbWrapper) SetConcurrency(concurrency int) {
	w.lb.SetConcurrency(concurrency)
}

// SetIsCaeFunc -
func (w *lbWrapper) SetIsCaeFunc(isCaeFunc bool) {
	w.lb.SetIsCaeFunc(isCaeFunc)
}
