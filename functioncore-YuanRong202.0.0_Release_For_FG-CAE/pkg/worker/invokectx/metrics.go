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

package invokectx

import (
	"sync"
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
)

// DefaultMetrics is a global Metrics
var DefaultMetrics = NewMetrics()

// Metrics is a one-stop solution to invoke related contextes
type Metrics struct {
	pool        *sync.Pool
	notifiers   []chan<- bool
	m           sync.Map // uint64, *innerCtx
	size        int64    // atomic, len(m)
	idGenerator uint64   // atomic
	idx         int
}

// NewMetrics -
func NewMetrics() *Metrics {
	return &Metrics{
		pool: &sync.Pool{New: func() interface{} {
			return &innerCtx{}
		}},
	}
}

// AcquireInvokeCtx retries a InvokeCtx from the cached pool
func (m *Metrics) AcquireInvokeCtx() InvokeCtx {
	id := atomic.AddUint64(&m.idGenerator, 1)
	innerCtxIf := m.pool.Get()
	innerCtx, ok := innerCtxIf.(*innerCtx)
	if !ok {
		log.GetLogger().Errorf("failed to assert innerCtx")
		return InvokeCtx{}
	}

	ic := InvokeCtx{
		ID:    id,
		inner: innerCtx,
	}

	m.m.Store(id, innerCtx)
	if atomic.AddInt64(&m.size, 1) == 1 {
		m.notify(true)
	}

	return ic
}

// GetInvokeCtx finds an acquired InvokeCtx using id. The bool result reports whether the InvokeCtx was present.
func (m *Metrics) GetInvokeCtx(id uint64) (InvokeCtx, bool) {
	innerCtxIf, ok := m.m.Load(id)
	if !ok {
		return InvokeCtx{}, false
	}
	innerCtx, ok := innerCtxIf.(*innerCtx)
	if !ok {
		log.GetLogger().Errorf("failed to assert innerCtx")
		return InvokeCtx{}, false
	}

	return InvokeCtx{
		ID:    id,
		inner: innerCtx,
	}, true
}

// ReleaseInvokeCtx puts the InvokeCtx to the cached pool. After release, the InvokeCtx is no longer acquired.
func (m *Metrics) ReleaseInvokeCtx(ic InvokeCtx) {
	ic.inner.mtx.Lock()
	innerCtxIf, ok := m.m.LoadAndDelete(ic.ID)
	if !ok {
		ic.inner.mtx.Unlock()
		return
	}
	innerCtx, ok := innerCtxIf.(*innerCtx)
	if !ok {
		log.GetLogger().Errorf("failed to assert innerCtx")
		ic.inner.mtx.Unlock()
		return
	}
	for _, poolable := range innerCtx.values {
		if poolable != nil {
			poolable.Reset()
		}
	}

	size := atomic.AddInt64(&m.size, -1)
	ic.inner.mtx.Unlock()
	if size == 0 {
		m.notify(false)
	}

	m.pool.Put(innerCtx)
}

// Len -
func (m *Metrics) Len() int {
	return int(atomic.LoadInt64(&m.size))
}

func (m *Metrics) notify(hasSome bool) {
	for _, notifier := range m.notifiers {
		if notifier != nil {
			select {
			case notifier <- hasSome:
			default:
			}
		}
	}
}

// RegisterNotifier -
func (m *Metrics) RegisterNotifier() <-chan bool {
	ch := make(chan bool, 1)
	m.notifiers = append(m.notifiers, ch)
	return ch
}
