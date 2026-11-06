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
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
)

type rangeElem struct {
	inner    Poolable
	occupied uint32 // atomic boolean
}

// Reset implements invokectx.Poolable
func (e *rangeElem) Reset() {
	atomic.StoreUint32(&e.occupied, 0)
	e.inner.Reset()
}

func (e *rangeElem) acquire() bool {
	return atomic.SwapUint32(&e.occupied, 1) == 0
}

func (e *rangeElem) release() bool {
	return atomic.SwapUint32(&e.occupied, 0) == 1
}

func (e *rangeElem) isAcquired() bool {
	return atomic.LoadUint32(&e.occupied) == 1
}

// Range -
type Range struct {
	getter  func(InvokeCtx) Poolable
	rangeFn func(func(InvokeCtx, Poolable) bool) bool
	cnt     int64 // atomic
}

// Range calls f sequentially for each invokeCtx and its poolable presents (previously marked)
// If f returns false, range stops the iteration.
func (r *Range) Range(f func(InvokeCtx, Poolable) bool) bool {
	exist := r.rangeFn(func(ic InvokeCtx, rangeElemIf Poolable) bool {
		rangeElem, ok := rangeElemIf.(*rangeElem)
		if !ok {
			log.GetLogger().Errorf("failed to assert")
			return true
		}

		if rangeElem.isAcquired() {
			if !f(ic, rangeElem.inner) {
				return false
			}
		}

		return true
	})
	return exist
}

// Mark stores the invokeCtx and gets the poolable from it. 'ReleaseInvokeCtx' automatically 'Unmark's the invokeCtx
func (r *Range) Mark(ic InvokeCtx) Poolable {
	rangeElem := r.getRangeElem(ic)
	if rangeElem.acquire() {
		if !ic.IsExtension {
			atomic.AddInt64(&r.cnt, 1)
		}
	}
	return rangeElem.inner
}

// Unmark delete the invokeCtx and gets the poolable from it
func (r *Range) Unmark(ic InvokeCtx) Poolable {
	rangeElem := r.getRangeElem(ic)
	if rangeElem.release() {
		if !ic.IsExtension {
			atomic.AddInt64(&r.cnt, -1)
		}
	}
	return rangeElem.inner
}

// Len returns the number of elements presents (previously marked)
func (r *Range) Len() int {
	return int(atomic.LoadInt64(&r.cnt))
}

func (r *Range) getRangeElem(ic InvokeCtx) *rangeElem {
	rangeElemIf := r.getter(ic)
	rangeElem, ok := rangeElemIf.(*rangeElem)
	if !ok {
		log.GetLogger().Errorf("failed to assert")
		return nil
	}
	return rangeElem
}

func (m *Metrics) rangePoolable(idx int, f func(InvokeCtx, Poolable) bool) bool {
	var tmpExist bool
	m.m.Range(func(idIf, innerCtxIf interface{}) bool {
		innerCtx, ok := innerCtxIf.(*innerCtx)
		if !ok {
			log.GetLogger().Errorf("failed to assert")
			return false
		}
		if idx < 0 || idx >= len(innerCtx.values) {
			log.GetLogger().Errorf("invalid idx %d", idx)
			return false
		}

		poolable := innerCtx.values[idx]
		if poolable == nil {
			return true
		}

		id, ok := idIf.(uint64)
		if !ok {
			log.GetLogger().Errorf("failed to assert id")
			return false
		}
		innerCtx.mtx.Lock()
		_, flag := m.m.Load(id)
		if !flag {
			log.GetLogger().Errorf("this innerCtx has been released")
			innerCtx.mtx.Unlock()
			return true
		}
		ic := InvokeCtx{
			ID:    id,
			inner: innerCtx,
		}
		exist := f(ic, poolable)
		if !exist {
			tmpExist = true
		}
		innerCtx.mtx.Unlock()
		return exist
	})
	return tmpExist
}

// RegisterRange should be called at the initialization stage. It should not be called after any 'AcquireInvokeCtx' or
// 'ReleaseInvokeCtx'. RegisterRange is not go-routine safe.
func (m *Metrics) RegisterRange(newFn func() Poolable) *Range {
	idx, getter := m.register(func() Poolable {
		return &rangeElem{inner: newFn()}
	})
	rangeFn := func(f func(InvokeCtx, Poolable) bool) bool {
		return m.rangePoolable(idx, f)
	}
	return &Range{
		getter:  getter,
		rangeFn: rangeFn,
	}
}
