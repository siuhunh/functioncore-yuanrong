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

// Package invokectx -
package invokectx

import (
	"sync"

	"functioncore/pkg/common/logger/log"
)

// Poolable defines a type which can be cached by a sync.Pool
type Poolable interface {
	Reset()
}

type innerCtx struct {
	values []Poolable
	mtx    sync.Mutex
}

// InvokeCtx -
type InvokeCtx struct {
	inner       *innerCtx
	ID          uint64
	IsExtension bool
}

// IsInnerCtxEmpty Is InnerCtx Empty
func (ic *InvokeCtx) IsInnerCtxEmpty() bool {
	return ic.inner == nil
}

// SetIsExtension -
func (ic *InvokeCtx) SetIsExtension(isExtension bool) {
	ic.IsExtension = isExtension
}

func (m *Metrics) register(newFn func() Poolable) (int, func(InvokeCtx) Poolable) {
	id := m.idx
	m.idx++
	m.pool.New = func() interface{} {
		return &innerCtx{values: make([]Poolable, id+1)}
	}
	return id, func(ic InvokeCtx) Poolable {
		if id < 0 || id >= len(ic.inner.values) {
			log.GetLogger().Errorf("invalid id %d", id)
			return nil
		}

		poolable := ic.inner.values[id]
		if poolable == nil {
			poolable = newFn()
			ic.inner.values[id] = poolable
		}
		return poolable
	}
}

// Register should be called at the initialization stage. It should not be called after any 'AcquireInvokeCtx' or
// 'ReleaseInvokeCtx'. Register is not go-routine safe.
func (m *Metrics) Register(newFn func() Poolable) func(InvokeCtx) Poolable {
	_, poolable := m.register(newFn)
	return poolable
}
