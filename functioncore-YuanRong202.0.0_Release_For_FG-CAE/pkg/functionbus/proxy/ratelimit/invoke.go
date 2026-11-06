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

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
)

var (
	invokeManager     *InvokeManager
	invokeManagerOnce sync.Once
)

const (
	defaultInvokeRemoveTime = 1 * time.Minute
)

// InvokeManager limits invocations between functions
type InvokeManager struct {
	trace *sync.Map // limiter map for traceID, key:string -> value:Limit
}

// GetInvokeLimiter returns global instance of InvokeManager
func GetInvokeLimiter() *InvokeManager {
	invokeManagerOnce.Do(func() {
		InitLimitConfig()
		invokeManager = &InvokeManager{
			trace: &sync.Map{},
		}
	})
	return invokeManager
}

func (im *InvokeManager) deleteTraceID(traceID string, reset chan struct{}) {
	if reset == nil {
		log.GetLogger().Errorf("reset is nil")
		return
	}
	timer := time.NewTimer(defaultInvokeRemoveTime)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			im.trace.Delete(traceID)
			return
		case _, ok := <-reset:
			if !ok {
				im.trace.Delete(traceID)
				return
			}
			timer.Reset(defaultInvokeRemoveTime)
		}
	}
}

func (im *InvokeManager) getTraceLimiter(traceID string) Limit {
	reset := make(chan struct{}, 1)
	callback := func() {
		reset <- struct{}{}
	}
	limiter, loaded := im.trace.LoadOrStore(traceID,
		NewCallBackLimiter(traceIDFrequency, traceIDBucketSize, callback))
	if loaded {
		limit, ok := limiter.(Limit)
		if !ok {
			log.GetLogger().Errorf("failed to convert limiter to Limit")
			return nil
		}
		return limit
	}
	go im.deleteTraceID(traceID, reset)
	limit, ok := limiter.(Limit)
	if !ok {
		log.GetLogger().Errorf("failed to convert limiter to Limit")
		return nil
	}
	return limit
}

// Take returns whether function invoke with traceID is allowed
func (im *InvokeManager) Take(traceID string) bool {
	traceLimiter := im.getTraceLimiter(traceID)
	return traceLimiter.Take()
}
