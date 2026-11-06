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

// Package schedulequeue provides queue for schedule task
package schedulequeue

import (
	"container/heap"
	"sync"
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
)

type priorityScheduleQueue struct {
	mux       *sync.Mutex
	cond      *sync.Cond
	name      string
	signal    chan struct{}
	queue     priorityQueue
	stopCh    <-chan struct{}
	isStopped bool
}

// PriorityFunc is type of function to compare priority of items
type PriorityFunc func(x, y interface{}) bool

// NewPriorityScheduleQueue creates PriorityScheduleQueue with given priorityFunc
func NewPriorityScheduleQueue(name string, priorityFunc PriorityFunc, stopCh <-chan struct{}) Interface {
	m := &sync.Mutex{}
	return &priorityScheduleQueue{
		name:   name,
		mux:    m,
		cond:   sync.NewCond(m),
		signal: make(chan struct{}, 1),
		queue: priorityQueue{
			queue:        make([]interface{}, 0, 0),
			priorityFunc: priorityFunc,
		},
		stopCh: stopCh,
	}
}

// Add adds val to schedule queue
func (psq *priorityScheduleQueue) Add(val interface{}) *Item {
	item := &Item{
		Val:       val,
		scheduled: make(chan struct{}),
		waitDone:  make(chan struct{}),
	}
	psq.mux.Lock()
	defer psq.mux.Unlock()
	if psq.isStopped {
		item.signal()
		return item
	}
	heap.Push(&psq.queue, item)
	psq.cond.Signal()
	return item
}

// Done notifies queue that item is processed; see Item.Wait() for usage example
func (psq *priorityScheduleQueue) Done(item *Item) {
	psq.notify()
}

// Start starts to schedule items in queue
func (psq *priorityScheduleQueue) Start() {
	psq.notify()
	go psq.schedule()
}

func (psq *priorityScheduleQueue) schedule() {
	for {
		select {
		case _, ok := <-psq.signal:
			if !ok {
				return
			}
			item, ok := psq.pop().(*Item)
			if !ok {
				continue
			}
			item.signal()
			// NOTE: here must wait waitDone to make sure queue can load correct value of isTimeout
			<-item.waitDone
			if atomic.LoadUint32(&item.isTimeout) == 1 {
				psq.notify()
			}
		case <-psq.stopCh:
			log.GetLogger().Warnf("priority schedule queue %s stopped", psq.name)
			psq.mux.Lock()
			psq.isStopped = true
			psq.mux.Unlock()
			psq.clear()
			return
		}
	}
}

func (psq *priorityScheduleQueue) clear() {
	psq.mux.Lock()
	for psq.queue.Len() != 0 {
		item, ok := heap.Pop(&psq.queue).(*Item)
		if !ok {
			continue
		}
		item.signal()
	}
	psq.mux.Unlock()
	return
}

func (psq *priorityScheduleQueue) notify() {
	select {
	case psq.signal <- struct{}{}:
	default:
	}
}

func (psq *priorityScheduleQueue) pop() interface{} {
	psq.mux.Lock()

	for psq.queue.Len() == 0 {
		psq.cond.Wait()
	}

	item := heap.Pop(&psq.queue)

	psq.mux.Unlock()
	return item
}

type priorityQueue struct {
	queue        []interface{}
	priorityFunc func(x, y interface{}) bool
}

// Len implements sort.Interface
func (pq priorityQueue) Len() int {
	return len(pq.queue)
}

// Less implements sort.Interface
func (pq priorityQueue) Less(i, j int) bool {
	if i < 0 || i >= len(pq.queue) || j < 0 || j >= len(pq.queue) {
		return false
	}
	x, ok := pq.queue[i].(*Item)
	if !ok {
		return false
	}
	y, ok := pq.queue[j].(*Item)
	if !ok {
		return false
	}
	return pq.priorityFunc(x.Val, y.Val)
}

// Swap implements sort.Interface
func (pq priorityQueue) Swap(i, j int) {
	if i < 0 || i >= len(pq.queue) || j < 0 || j >= len(pq.queue) {
		return
	}
	pq.queue[i], pq.queue[j] = pq.queue[j], pq.queue[i]
}

// Push implements heap.Interface
func (pq *priorityQueue) Push(x interface{}) {
	pq.queue = append(pq.queue, x)
}

// Pop implements heap.Interface
func (pq *priorityQueue) Pop() interface{} {
	n := len(pq.queue)
	if n == 0 {
		return nil
	}
	item := pq.queue[n-1]
	pq.queue[n-1] = nil
	pq.queue = pq.queue[0 : n-1]
	return item
}
