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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInvokeLimiter_Take(t *testing.T) {
	limiter := GetInvokeLimiter()
	traceID := "testID1"
	wg := sync.WaitGroup{}
	wg.Add(defaultInvokeBucketSize)
	for i := 0; i < defaultInvokeBucketSize; i++ {
		go func() {
			defer wg.Done()
			assert.Equal(t, true, limiter.Take(traceID))
		}()
	}
	wg.Wait()
	for i := 0; i < defaultInvokeBucketSize; i++ {
		assert.Equal(t, false, limiter.Take(traceID))
	}

	for i := 0; i < 3; i++ {
		time.Sleep((1/defaultInvokeRate + 1) * time.Second)
		assert.Equal(t, true, limiter.Take(traceID))
	}
}

func TestSimpleLimiter(t *testing.T) {
	limiter := NewSimpleLimiter(defaultInvokeRate, defaultInvokeBucketSize)
	wg := sync.WaitGroup{}
	wg.Add(defaultInvokeBucketSize)
	for i := 0; i < defaultInvokeBucketSize; i++ {
		go func() {
			defer wg.Done()
			assert.Equal(t, true, limiter.Take())
		}()
	}
	wg.Wait()
	for i := 0; i < defaultInvokeBucketSize; i++ {
		assert.Equal(t, false, limiter.Take())
	}

	for i := 0; i < 3; i++ {
		time.Sleep((1/defaultInvokeRate + 1) * time.Second)
		assert.Equal(t, true, limiter.Take())
	}
}

func Test_deleteTraceID(t *testing.T) {
	manager := GetInvokeLimiter()
	manager.deleteTraceID("202206250111", nil)

	manager.trace.Store("202206250111", 202206250111)
	resetChan := make(chan struct{}, 1)

	go func() {
		manager.deleteTraceID("202206250111", resetChan)
	}()
	close(resetChan)
	time.Sleep(100 * time.Millisecond)
	if _, ok := manager.trace.Load("202206250111"); ok {
		t.Error("not delete key 202206250111")
	}
}
