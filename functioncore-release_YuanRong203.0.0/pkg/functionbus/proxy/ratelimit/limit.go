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

// Package ratelimit provides request rate limiter
package ratelimit

import "golang.org/x/time/rate"

// Limit define the interface of token bucket algorithm
// Take returns whether request is allowed with no block
// UpdateLimiterConfig update limiter config
type Limit interface {
	Take() bool
	UpdateLimiterConfig()
}

// SimpleLimiter implement a simple rate limiter
type SimpleLimiter struct {
	*rate.Limiter
}

// Take returns whether you can take a token from the bucket in token bucket algorithm.
func (sl *SimpleLimiter) Take() bool {
	return sl.Limiter.Allow()
}

// UpdateLimiterConfig update limiter config
func (sl *SimpleLimiter) UpdateLimiterConfig() {
	sl.Limiter.SetLimit(rate.Limit(GetTraceIDFrequency()))
	sl.Limiter.SetBurst(GetTraceIDBucketSize())
}

// NewSimpleLimiter return a simple limiter with given rate and bucket size
func NewSimpleLimiter(r float64, b int) *SimpleLimiter {
	return &SimpleLimiter{
		Limiter: rate.NewLimiter(rate.Limit(r), b),
	}
}

// CallBackLimiter implement a rate limiter with callback.
type CallBackLimiter struct {
	*rate.Limiter
	cb func()
}

// Take always calls callback
func (cl *CallBackLimiter) Take() bool {
	if cl.cb != nil {
		cl.cb()
	}
	return cl.Limiter.Allow()
}

// UpdateLimiterConfig update limiter config
func (cl *CallBackLimiter) UpdateLimiterConfig() {
	cl.Limiter.SetLimit(rate.Limit(GetTraceIDFrequency()))
	cl.Limiter.SetBurst(GetTraceIDBucketSize())
}

// NewCallBackLimiter return a callback limiter with given rate and bucket size
func NewCallBackLimiter(r float64, b int, cb func()) *CallBackLimiter {
	return &CallBackLimiter{
		Limiter: rate.NewLimiter(rate.Limit(r), b),
		cb:      cb,
	}
}
