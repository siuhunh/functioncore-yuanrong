/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package queue

import "container/list"

// MonotonicQueue implement monotonic queue with max value of windows length k
// NOTE: MonotonicQueue is not thread safe
type MonotonicQueue struct {
	maxVal  *list.List
	sliding *list.List
	k       int
}

// NewMonotonicQueue creates monotonic queue of windows length k
func NewMonotonicQueue(k int) *MonotonicQueue {
	return &MonotonicQueue{
		maxVal:  list.New(),
		sliding: list.New(),
		k:       k,
	}
}

// Push pushes element
func (mq *MonotonicQueue) Push(n int) {
	for mq.maxVal.Len() > 0 && mq.maxVal.Back().Value.(int) < n {
		mq.maxVal.Remove(mq.maxVal.Back())
	}
	mq.maxVal.PushBack(n)
	mq.sliding.PushBack(n)

	if mq.sliding.Len() > mq.k {
		mq.pop()
	}
}

// GetMax get max value in windows
func (mq *MonotonicQueue) GetMax() int {
	if mq.maxVal.Len() == 0 {
		return 0
	}
	return mq.maxVal.Front().Value.(int)
}

// Resize resize the windows size
func (mq *MonotonicQueue) Resize(k int) {
	if k >= mq.k {
		mq.k = k
		return
	}
	for mq.sliding.Len() > k {
		mq.pop()
	}
	mq.k = k
}

// Len returns current window length
func (mq *MonotonicQueue) Len() int {
	return mq.sliding.Len()
}

func (mq *MonotonicQueue) pop() {
	if mq.maxVal.Len() > 0 && mq.maxVal.Front().Value.(int) == mq.sliding.Front().Value.(int) {
		mq.maxVal.Remove(mq.maxVal.Front())
	}
	mq.sliding.Remove(mq.sliding.Front())
}
