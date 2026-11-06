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

package ratelimit

import "sync/atomic"

var (
	traceIDBucketSize atomic.Value
	traceIDFrequency  atomic.Value
)

const (
	defaultTraceIDBucketSize = 1000
	defaultTraceIDFrequency  = 1.0

	minTraceIDFrequency = 0.00001
)

// UpdateRateLimitConfig update rate limit config
func UpdateRateLimitConfig(newTraceIDBucketSize int, newTraceIDFrequency float64) {
	if newTraceIDBucketSize > 0 {
		traceIDBucketSize.Store(newTraceIDBucketSize)
	}
	if newTraceIDFrequency > minTraceIDFrequency {
		traceIDFrequency.Store(newTraceIDFrequency)
	}
}

// GetTraceIDBucketSize get traceIDBucketSize
func GetTraceIDBucketSize() int {
	valIf := traceIDBucketSize.Load()
	val, ok := valIf.(int)
	if !ok {
		return defaultTraceIDBucketSize
	}
	return val
}

// GetTraceIDFrequency get traceIDFrequency
func GetTraceIDFrequency() float64 {
	valIf := traceIDFrequency.Load()
	val, ok := valIf.(float64)
	if !ok {
		return defaultTraceIDFrequency
	}
	return val
}
