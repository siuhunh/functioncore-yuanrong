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

package autoscaler

import (
	"context"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/logger/log"
)

type slideWindow struct {
	executor *Executor
	ring     *ringBuffer
	interval time.Duration
	cancel   context.CancelFunc
}

type slideWindowValue map[string][]workermanager.ReportMetrics

type slideWindowVisitor interface {
	visit(*slideWindow)
}

// interval must be greater than zero.
func newSlideWindow(executor *Executor, interval time.Duration, slotNum int) *slideWindow {
	sw := &slideWindow{
		executor: executor,
		ring:     newRingBuffer(slotNum),
		interval: interval,
	}
	ctx, cancel := context.WithCancel(context.Background())
	executor.Every(ctx, interval, func(ctx context.Context) {
		elem := sw.ring.advance()
		// Value inside this element becomes outdated. Clear it.
		elem.val = nil
	})
	sw.cancel = cancel

	return sw
}

func (sw *slideWindow) async(ctx context.Context, visitors ...slideWindowVisitor) error {
	return sw.executor.Async(ctx, func(ctx context.Context) {
		for _, visitor := range visitors {
			visitor.visit(sw)
		}
	})
}

func (sw *slideWindow) do(ctx context.Context, visitors ...slideWindowVisitor) error {
	return sw.executor.Do(ctx, func(ctx context.Context) {
		for _, visitor := range visitors {
			visitor.visit(sw)
		}
	})
}

func (sw *slideWindow) close() {
	sw.cancel()
}

type slideWindowAdd struct {
	currentTimestamp int64
	recordTimestamp  int64
	instanceID       string
	val              workermanager.ReportMetrics
}

func (v *slideWindowAdd) visit(sw *slideWindow) {
	diff := v.currentTimestamp - v.recordTimestamp
	if diff < 0 {
		log.GetLogger().Warnf("record %d occurs later than current %d for instance %s, ignore", v.recordTimestamp,
			v.currentTimestamp, v.instanceID)
		return
	}
	offset := sw.ring.len() - int(float64(diff)/sw.interval.Seconds()) - 1
	if offset < 0 {
		log.GetLogger().Warnf("outdated record %d, current %d, instance %s, ignore", v.recordTimestamp,
			v.currentTimestamp, v.instanceID)
		return
	}
	elem := sw.ring.get(offset)
	if elem.val == nil {
		elem.val = make(slideWindowValue)
	}
	val, ok := elem.val.(slideWindowValue)
	if !ok {
		log.GetLogger().Errorf("failed to assert")
		return
	}
	val[v.instanceID] = append(val[v.instanceID], v.val)
}

type slideWindowDelete struct {
	key string
}

func (v *slideWindowDelete) visit(sw *slideWindow) {
	sw.ring.rangeAll(func(_ int, elem *ringElem) {
		if elem.val == nil {
			return
		}
		val, ok := elem.val.(slideWindowValue)
		if !ok {
			log.GetLogger().Errorf("failed to assert")
			return
		}
		delete(val, v.key)
	})
}

// SlideWindowRangeFn is used as a callback when ranging the slide window. 0 <= offset < SlotNum. Newer metrics have
// larger offsets. Old metrics are removed when their offsets become smaller than 0.
type SlideWindowRangeFn func(offset int, instanceID string, list []workermanager.ReportMetrics)

type slideWindowRange struct {
	rangeFn SlideWindowRangeFn
}

func (v *slideWindowRange) visit(sw *slideWindow) {
	sw.ring.rangeAll(func(offset int, elem *ringElem) {
		if elem.val == nil {
			return
		}
		val, ok := elem.val.(slideWindowValue)
		if !ok {
			log.GetLogger().Errorf("failed to assert")
			return
		}
		for k, list := range val {
			v.rangeFn(offset, k, list)
		}
	})
}

const defaultRingBufferSlotNum = 16

type ringBuffer struct {
	slots      []*ringElem
	currentPos int
}

type ringElem struct {
	val interface{}
}

func newRingBuffer(slotNum int) *ringBuffer {
	if slotNum <= 0 {
		slotNum = defaultRingBufferSlotNum
	}
	slots := make([]*ringElem, slotNum)
	for i := range slots {
		slots[i] = &ringElem{}
	}
	return &ringBuffer{
		slots: slots,
	}
}

func (rb *ringBuffer) get(offset int) *ringElem {
	idx := rb.wrap(rb.currentPos + offset)
	elem := rb.slots[idx]
	return elem
}

func (rb *ringBuffer) rangeAll(fn func(offset int, elem *ringElem)) {
	for i, elem := range rb.slots {
		offset := rb.wrap(rb.len() + i - rb.currentPos)
		fn(offset, elem)
	}
}

func (rb *ringBuffer) advance() *ringElem {
	elem := rb.slots[rb.currentPos]
	rb.currentPos = rb.wrap(rb.currentPos + 1)
	return elem
}

func (rb *ringBuffer) wrap(i int) int {
	return i % len(rb.slots)
}

func (rb *ringBuffer) len() int {
	return len(rb.slots)
}
