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

// Package queue imply multiple queues
package queue

import (
	"container/heap"
)

const (
	defaultQueueLength = 20
)

var _ Queue = (*PriorityQueue)(nil)

// PriorityQueue implements a scheduling queue.
// The head of PriorityQueue is the highest priority pending request.
type PriorityQueue struct {
	items        *Heap
	priorityFunc PriorityFunc
}

// NewPriorityQueue return priority queue.
func NewPriorityQueue(priorityFunc PriorityFunc) Queue {
	h := make(Heap, 0, defaultQueueLength)
	heap.Init(&h)
	return &PriorityQueue{
		items:        &h,
		priorityFunc: priorityFunc,
	}
}

// PriorityFunc is a function type to get the priority from an object.
type PriorityFunc func(obj interface{}) (int, error)

// Push an item into queue.
func (p *PriorityQueue) Push(obj interface{}) error {
	priority, err := p.priorityFunc(obj)
	if err != nil {
		return err
	}
	heap.Push(p.items, &item{obj: obj, priority: priority})
	return nil
}

// Front returns the head of the heap
func (p *PriorityQueue) Front() (interface{}, error) {
	if p.Len() > 0 {
		return (*p.items)[0].obj, nil
	}
	return nil, ErrQueueIsEmpty
}

// Pop returns the head of the heap and removes it.
func (p *PriorityQueue) Pop() (interface{}, error) {
	if p.items.Len() > 0 {
		item, ok := heap.Pop(p.items).(*item)
		if !ok || item == nil {
			return nil, errAssertionFailed
		}
		if item.obj != nil {
			return item.obj, nil
		}
	}
	return nil, ErrQueueIsEmpty
}

// Len return the length of queue.
func (p *PriorityQueue) Len() int {
	return p.items.Len()
}

type item struct {
	// The object which is stored in the heap.
	obj      interface{}
	priority int
	// The index of the object's key in the Heap.queue.
	index int
}

var _ heap.Interface = (*Heap)(nil)

// Heap implements heap.Interface and keeps the order of elements
// according to the heap invariant.
type Heap []*item

// Len returns the number of items in the Heap.
func (h Heap) Len() int { return len(h) }

// Less is used to compare two objects in the heap.
func (h Heap) Less(i, j int) bool {
	if i >= len(h) || j >= len(h) {
		return false
	}
	return h[i].priority > h[j].priority
}

// Swap implements swapping of two elements in the heap.
func (h Heap) Swap(i, j int) {
	if i >= len(h) || j >= len(h) {
		return
	}
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

// Push push an item to heap
func (h *Heap) Push(x interface{}) {
	n := len(*h)
	item, ok := x.(*item)
	if !ok {
		return
	}
	item.index = n
	*h = append(*h, item)
}

// Pop pop an item from heap
func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.index = -1
	*h = old[0 : n-1]
	return item
}
