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

import "container/list"

// FifoQueue implements a fifo scheduling queue.
type FifoQueue struct {
	queue *list.List
}

// NewFifoQueue return fifo queue.
// According benchmark when queue length larger than 1000, based on list may better.
func NewFifoQueue() Queue {
	return &FifoQueue{queue: list.New()}
}

// Push an item into queue.
func (f *FifoQueue) Push(obj interface{}) error {
	f.queue.PushBack(obj)
	return nil
}

// Front return front item of queue
func (f *FifoQueue) Front() (interface{}, error) {
	if f.queue.Len() == 0 {
		return nil, ErrQueueIsEmpty
	}
	return f.queue.Front().Value, nil
}

// Pop pop an item from queue.
func (f *FifoQueue) Pop() (interface{}, error) {
	if f.queue.Len() == 0 {
		return nil, ErrQueueIsEmpty
	}

	element := f.queue.Front()
	obj := element.Value
	f.queue.Remove(element)
	return obj, nil
}

// Len return the length of queue.
func (f *FifoQueue) Len() int {
	return f.queue.Len()
}
