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

// Package opqueue provide operation queue that parallelize operations by key
package opqueue

import (
	"container/list"
	"sync"

	"github.com/panjf2000/ants/v2"

	"functioncore/pkg/common/uchan"
)

// Operation operations to parallelize
type Operation struct {
	Key string
	Run func()
}

// OperationQueue -
type OperationQueue interface {
	Push(Operation)
}

type innerQueue interface {
	Push(Operation)
	Pop() (Operation, bool)
	Len() int
}

func newFifo() *fifo {
	return &fifo{
		l: list.New(),
	}
}

type fifo struct {
	l *list.List
}

// Push pushes Operation to inner queue
func (f *fifo) Push(op Operation) {
	f.l.PushFront(op)
}

// Pop pops Operations from inner queue
func (f *fifo) Pop() (Operation, bool) {
	if f.l.Len() == 0 {
		return Operation{}, false
	}
	ele := f.l.Front()
	obj := ele.Value
	f.l.Remove(ele)
	op, ok := obj.(Operation)
	if !ok {
		return Operation{}, false
	}
	return op, true
}

// Len returns length of inner queue
func (f *fifo) Len() int {
	return f.l.Len()
}

// NewOperationQueue create operation queue with init size
func NewOperationQueue(size int) OperationQueue {
	p, _ := ants.NewPool(size, ants.WithPreAlloc(true))
	que := &opQueue{
		queues:   make(map[string]innerQueue),
		pool:     p,
		pushChan: uchan.NewUnboundedChan(size),
		complete: uchan.NewUnboundedChan(size),
	}
	go que.run()
	return que
}

type opQueue struct {
	pool     *ants.Pool
	pushChan *uchan.UnboundedChan
	complete *uchan.UnboundedChan
	queues   map[string]innerQueue
	sq       sync.Map
	stopCh   <-chan struct{}
}

// Push pushes Operation to pushChan
func (o *opQueue) Push(op Operation) {
	o.pushChan.In <- op
}

func (o *opQueue) runOp(op Operation) {
	o.pool.Submit(func() {
		op.Run()
		o.complete.In <- op.Key
	})
}

func (o *opQueue) run() {
	for {
		select {
		case item, ok := <-o.pushChan.Out:
			if !ok {
				continue
			}
			op, ok := item.(Operation)
			if !ok {
				continue
			}
			if que, ok := o.queues[op.Key]; ok {
				que.Push(op)
			} else {
				o.queues[op.Key] = newFifo()
				o.runOp(op)
			}
		case item, ok := <-o.complete.Out:
			if !ok {
				continue
			}
			key, ok := item.(string)
			if !ok {
				continue
			}
			que := o.queues[key]
			if que.Len() == 0 {
				delete(o.queues, key)
				continue
			}
			op, ok := que.Pop()
			if ok {
				o.runOp(op)
			}
		case <-o.stopCh:
			return
		}
	}
}
