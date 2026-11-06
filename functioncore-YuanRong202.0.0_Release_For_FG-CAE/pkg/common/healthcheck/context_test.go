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

package healthcheck

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	counter := uint64(0)
	healthFunc := func(ctx context.Context) error {
		count := atomic.AddUint64(&counter, 1)
		if count >= 2 && count <= 5 {
			return errors.New("")
		}
		return nil
	}

	healthCtx := NewContext(Config{
		Timeout:   100 * time.Millisecond,
		Interval:  300 * time.Millisecond,
		Threshold: 2,
	}, healthFunc)

	stopCh := make(chan struct{})
	go healthCtx.Run(stopCh)

	time.Sleep(400 * time.Millisecond)

	assert.Equal(t, 0, healthCtx.failedCount, "health check should not fail")
	select {
	case <-healthCtx.GetContext().Done():
		assert.Fail(t, "health check ok and context should not done")
	default:
	}

	time.Sleep(600 * time.Millisecond)
	assert.Equal(t, 2, healthCtx.failedCount, "health check should failed twice")
	tmpTimer := time.NewTimer(50 * time.Millisecond)
	select {
	case <-healthCtx.GetContext().Done():
		tmpTimer.Stop()
	case <-tmpTimer.C:
		assert.Fail(t, "health check not ok and context should be done")
	}

	time.Sleep(time.Second)
	assert.Equal(t, 0, healthCtx.failedCount, "health check should be reset")
	select {
	case <-healthCtx.GetContext().Done():
		assert.Fail(t, "health check ok and context should not done")
	default:
	}
	close(stopCh)
}

func TestContext_UpdateConfig(t *testing.T) {
	counter := uint64(0)
	healthFunc := func(ctx context.Context) error {
		count := atomic.AddUint64(&counter, 1)
		if count >= 2 && count <= 5 {
			return errors.New("")
		}
		return nil
	}

	healthCtx := NewContext(Config{
		Timeout:   100 * time.Millisecond,
		Interval:  300 * time.Millisecond,
		Threshold: 2,
	}, healthFunc)

	stopCh := make(chan struct{})
	go healthCtx.Run(stopCh)

	healthCtx.UpdateConfig(Config{
		Timeout:   200 * time.Millisecond,
		Interval:  100 * time.Millisecond,
		Threshold: 5,
	})
	time.Sleep(450 * time.Millisecond)
	assert.Equal(t, 3, healthCtx.failedCount)
	select {
	case <-healthCtx.GetContext().Done():
		assert.Fail(t, "health check ok and context should not done")
	default:
	}
}

func TestContext_UpdateDisable(t *testing.T) {
	healthFunc := func(ctx context.Context) error {
		return errors.New("")
	}

	healthCtx := NewContext(Config{
		Timeout:   100 * time.Millisecond,
		Interval:  100 * time.Millisecond,
		Threshold: 2,
	}, healthFunc)
	stopCh := make(chan struct{})
	go healthCtx.Run(stopCh)
	time.Sleep(300 * time.Millisecond)
	select {
	case <-healthCtx.GetContext().Done():
	default:
		assert.Fail(t, "health check not ok and context should be done")
	}

	healthCtx.UpdateConfig(Config{
		Timeout:   10 * time.Millisecond,
		Interval:  10 * time.Millisecond,
		Threshold: 1,
		Disable:   true,
	})
	time.Sleep(100 * time.Millisecond)
	select {
	case <-healthCtx.GetContext().Done():
		assert.Fail(t, "health check disabled and context won't done")
	default:
	}
}
