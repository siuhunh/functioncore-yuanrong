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

package dataaggregation

import "sync/atomic"

type avgCounter struct {
	val   int64
	sum   int64
	num   int64
	valid bool
}

// NewAtomicAverage creates AtomicAverage with given capacity
func NewAtomicAverage(capacity int64) AtomicAverage {
	if capacity <= 0 {
		capacity = maxWindowCapacity
	}
	return AtomicAverage{
		capacity: capacity,
	}
}

// AtomicAverage average data aggregation atomically
// NOTE: the data aggregation is not accurate and should only be used in performance case
type AtomicAverage struct {
	val      atomic.Value
	capacity int64
}

// AddSample add sample
func (aa *AtomicAverage) AddSample(sample int64) {
	v, ok := aa.val.Load().(avgCounter)
	if !ok {
		v = avgCounter{}
	}
	v.valid = true
	v.sum += sample
	v.num++
	v.val = v.sum / v.num
	if v.num > aa.capacity {
		v.num = 1
		v.sum = sample
	}
	aa.val.Store(v)
}

// AddSamples add samples
func (aa *AtomicAverage) AddSamples(samples []int64) {
	v, ok := aa.val.Load().(avgCounter)
	if !ok {
		v = avgCounter{}
	}
	v.valid = true
	if v.num+int64(len(samples)) > aa.capacity {
		v.num = 0
		v.sum = 0
	}
	for _, s := range samples {
		v.sum += s
		v.num++
	}
	v.val = v.sum / v.num
	aa.val.Store(v)
}

// GetValue get val; 0 means there is no sample; otherwise it returns at least 1
func (aa *AtomicAverage) GetValue() int64 {
	v, ok := aa.val.Load().(avgCounter)
	if !ok || !v.valid {
		return 0
	}
	if v.val == 0 {
		return 1
	}
	return v.val
}

// Reset resets all samples
func (aa *AtomicAverage) Reset() {
	aa.val.Store(avgCounter{})
}
