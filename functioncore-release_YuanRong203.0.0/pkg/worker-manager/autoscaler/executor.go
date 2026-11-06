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
	"container/list"
	"context"
	"errors"
	"time"

	"functioncore/pkg/common/logger/log"
)

const (
	defaultExecutorInterval = 1 * time.Second
	slotNum                 = 60
)

// Executor -
type Executor struct {
	ring     *ringBuffer
	eventCh  chan *executorEvent
	stopCh   chan struct{}
	interval time.Duration
	key      executorContextKey
}

// DefaultExecutor -
var DefaultExecutor = NewExecutor(defaultExecutorInterval, slotNum)

// ExecutorFn -
type ExecutorFn func(context.Context)

type executorContextKey struct{}

type executorEvent struct {
	ctx    context.Context
	fn     ExecutorFn
	waitCh chan struct{}
}

type executorTask struct {
	ctx    context.Context
	fn     ExecutorFn
	circle int
}

// NewExecutor -
func NewExecutor(interval time.Duration, slotNum int) *Executor {
	if interval <= 0 {
		interval = defaultExecutorInterval
	}
	ring := newRingBuffer(slotNum)
	ring.rangeAll(func(_ int, elem *ringElem) {
		elem.val = list.New()
	})
	e := &Executor{
		ring:     ring,
		interval: interval,
		stopCh:   make(chan struct{}),
		eventCh:  make(chan *executorEvent),
	}

	go e.loop()

	return e
}

// Async runs an ExecutorFn asynchronously
func (e *Executor) Async(ctx context.Context, fn ExecutorFn) error {
	return e.execute(ctx, true, fn)
}

// Do runs an ExecutorFn and waits util complete
func (e *Executor) Do(ctx context.Context, fn ExecutorFn) error {
	return e.execute(ctx, false, fn)
}

func (e *Executor) execute(ctx context.Context, async bool, fn ExecutorFn) error {
	if v := ctx.Value(e.key); v != nil {
		// Inside the executor loop. Execute fn directly.
		fn(ctx)
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	event := &executorEvent{ctx: ctx, fn: fn}
	if !async {
		event.waitCh = make(chan struct{})
	}
	e.eventCh <- event
	if !async {
		select {
		case <-e.stopCh:
			return errors.New("executor finished")
		case <-event.waitCh:
			return nil
		}
	}
	return nil
}

// Every runs an ExecutorFn every interval duration. Use context to cancel.
func (e *Executor) Every(ctx context.Context, interval time.Duration, fn ExecutorFn) {
	var wrap func(context.Context)
	wrap = func(ctx context.Context) {
		fn(ctx)

		select {
		case <-ctx.Done():
			// Task has been canceled
			return
		default:
			e.insertTask(ctx, wrap, interval)
		}
	}

	err := e.Do(ctx, func(ctx context.Context) {
		e.insertTask(ctx, wrap, interval)
	})
	if err != nil {
		log.GetLogger().Warnf("failed to insert task: %s", err.Error())
	}
}

// Close terminates an executor
func (e *Executor) Close() {
	close(e.stopCh)
}

func (e *Executor) loop() {
	ticker := time.NewTicker(e.interval)
	for {
		select {
		case _, ok := <-ticker.C:
			if !ok {
				log.GetLogger().Errorf("executor ticker chan is closed")
				return
			}
			e.processTick()
		case event, ok := <-e.eventCh:
			if !ok {
				log.GetLogger().Errorf("executor event chan is closed")
				return
			}
			// Mark that we are inside the executor loop.
			ctx := context.WithValue(event.ctx, e.key, struct{}{})
			event.fn(ctx)
			if event.waitCh != nil {
				close(event.waitCh)
			}
		case <-e.stopCh:
			log.GetLogger().Infof("executor finished")
			return
		}
	}
}

func (e *Executor) processTick() {
	ringElem := e.ring.advance()
	ll, ok := ringElem.val.(*list.List)
	if !ok {
		log.GetLogger().Errorf("failed to assert")
		return
	}

	var elements []*list.Element
	for elem := ll.Front(); elem != nil; elem = elem.Next() {
		task, ok := elem.Value.(*executorTask)
		if !ok {
			log.GetLogger().Errorf("failed to assert")
			continue
		}

		if task.circle > 0 {
			task.circle--
			continue
		}

		elements = append(elements, elem)

		select {
		case <-task.ctx.Done():
			// Task has been canceled
			continue
		default:
			task.fn(task.ctx)
		}
	}
	for _, elem := range elements {
		ll.Remove(elem)
	}
}

func (e *Executor) getOffsetAndCircle(delay time.Duration) (int, int) {
	delaySeconds := delay.Seconds()
	intervalSeconds := e.interval.Seconds()
	offset := int(delaySeconds/intervalSeconds) % e.ring.len()
	circle := int(delaySeconds / intervalSeconds / float64(e.ring.len()))
	return offset, circle
}

func (e *Executor) insertTask(ctx context.Context, fn ExecutorFn, delay time.Duration) {
	offset, circle := e.getOffsetAndCircle(delay)

	elem := e.ring.get(offset)
	ll, ok := elem.val.(*list.List)
	if !ok {
		log.GetLogger().Errorf("failed to assert")
		return
	}
	ll.PushBack(&executorTask{
		ctx:    ctx,
		fn:     fn,
		circle: circle,
	})
}
