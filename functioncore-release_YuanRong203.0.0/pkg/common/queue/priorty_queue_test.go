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
	"fmt"
	"testing"
)

// PriorityCallRequest is used to priority queue
type PriorityCallRequest struct {
	Priority    int
	CallRequest CallRequest
}

// CallRequest is call request
type CallRequest struct {
	callRequest string
}

func Test_Heap(t *testing.T) {
	items := map[string]int{
		"banana": 3, "apple": 2, "pear": 4,
	}
	pq := make(Heap, len(items))
	i := 0
	for obj, priority := range items {
		pq[i] = &item{
			obj:      obj,
			priority: priority,
			index:    i,
		}
		i++
	}
	heap.Init(&pq)

	it := &item{
		obj:      "orange",
		priority: 1,
	}
	heap.Push(&pq, it)
	for i := 4; i >= 1; i-- {
		if item, ok := heap.Pop(&pq).(*item); ok {
			if item.priority != i {
				t.Fatalf("heap pop value is not %d", i)
			}
		}
	}
}

func Test_PriorityQueue(t *testing.T) {
	priorityFunc := func(obj interface{}) (int, error) {
		if priorityCallRequest, ok := obj.(*PriorityCallRequest); ok {
			return priorityCallRequest.Priority, nil
		}
		return -1, fmt.Errorf("failed to get priority from CallRequest")
	}
	pq := NewPriorityQueue(priorityFunc)

	num := 100
	for i := 0; i < num; i++ {
		_ = pq.Push(&PriorityCallRequest{
			Priority: i,
			CallRequest: CallRequest{
				fmt.Sprintf("callReques-%d", i),
			},
		})
	}
	value, err := pq.Front()
	if p, ok := value.(*PriorityCallRequest); ok && err == nil {
		str := fmt.Sprintf("callReques-%d", 99)
		if p.CallRequest.callRequest != str {
			t.Fatalf("got callRequest is %s, expected is %s", p.CallRequest.callRequest, str)
		}
		if pq.Len() != num {
			t.Fatalf("pq.Front() is invalid")
		}
	}
	for i := num - 1; i > 0; i-- {
		value, err := pq.Pop()
		if err != nil {
			t.Error(err.Error())
		}
		if p, ok := value.(*PriorityCallRequest); ok {
			str := fmt.Sprintf("callReques-%d", i)
			if p.CallRequest.callRequest != str {
				t.Fatalf("got callRequest is %s, expected is %s", p.CallRequest.callRequest, str)
			}
		}
	}
}

func Test_Same_Priority(t *testing.T) {
	priorityFunc := func(obj interface{}) (int, error) {
		if priorityCallRequest, ok := obj.(*PriorityCallRequest); ok {
			return priorityCallRequest.Priority, nil
		}
		return -1, fmt.Errorf("failed to get priority from CallRequest")
	}
	pq := NewPriorityQueue(priorityFunc)

	num := 100
	for i := 0; i < num; i++ {
		_ = pq.Push(&PriorityCallRequest{
			Priority: i % 3,
			CallRequest: CallRequest{
				fmt.Sprintf("callReques-%d", i),
			},
		})
	}
	latestPriority := 2
	for i := num - 1; i > 0; i-- {
		value, err := pq.Pop()
		if err != nil {
			t.Error(err.Error())
		}
		if p, ok := value.(*PriorityCallRequest); ok {
			str := fmt.Sprintf("callReques-%d", i)
			if latestPriority < p.Priority {
				t.Fatalf("got callRequest is %s, expected is %s", p.CallRequest.callRequest, str)
			}
			latestPriority = p.Priority
		}
	}
}

func TestPriorityQueue_Front(t *testing.T) {
	priorityFunc := func(obj interface{}) (int, error) {
		if priorityCallRequest, ok := obj.(*PriorityCallRequest); ok {
			return priorityCallRequest.Priority, nil
		}
		return -1, fmt.Errorf("failed to get priority from CallRequest")
	}
	pq := NewPriorityQueue(priorityFunc)
	if _, err := pq.Front(); err == nil {
		t.Error("failed to test front of empty queue")
	}
	if _, err := pq.Pop(); err == nil {
		t.Error("failed to test pop of empty queue")
	}
}
