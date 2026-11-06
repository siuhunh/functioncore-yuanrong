/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package checkpoint

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"gotest.tools/assert"

	"functioncore/pkg/common/alarm"
)

func TestConcurrencyLimiter_Apply(t *testing.T) {
	limiter := CreateConcurrencyLimiter(1)
	// test no blocking
	{
		limiter.Apply()
		limiter.Release()
	}
	{
		limiter.Apply()
		limiter.Release()
	}
}

func TestConcurrencyLimiter_Reset(t *testing.T) {
	limiter := CreateConcurrencyLimiter(1)
	// test no blocking
	{
		limiter.Apply()
		limiter.Release()
	}
	limiter.Reset(2)
	{
		limiter.Apply()
		limiter.Release()
	}
}

func TestConcurrencyLimiter(t *testing.T) {
	total := 10
	limiter := CreateConcurrencyLimiter(2)
	wg := &sync.WaitGroup{}
	wg.Add(total)
	count := int64(0)
	currency := int64(0)
	maxCurrency := int64(0)

	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()
			defer limiter.Release()
			limiter.Apply()
			atomic.AddInt64(&count, 1)
			atomic.AddInt64(&currency, 1)
			current := atomic.LoadInt64(&currency)
			max := atomic.LoadInt64(&maxCurrency)
			for current > max {
				if atomic.CompareAndSwapInt64(&maxCurrency, max, current) {
					break
				}
				current = atomic.LoadInt64(&currency)
				max = atomic.LoadInt64(&maxCurrency)
			}

			time.Sleep(100 * time.Millisecond)
			atomic.AddInt64(&currency, -1)
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(total), count)
	assert.Equal(t, int64(2), maxCurrency)
}

func TestConcurrencyLimiter_Reset2(t *testing.T) {
	limiter := CreateConcurrencyLimiter(2)
	ch := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			limiter.Apply()
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-ch:
		t.Errorf("limiter should block")
	default:
	}

	limiter.Reset(3)
	select {
	case <-ch:
	}
}

func TestConcurrencyLimiterWithAlarm1(t *testing.T) {
	isReportAlarm := false
	isClearAlarm := false
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
			isReportAlarm = true
		}),
		gomonkey.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			isClearAlarm = true
		}),
	}
	defer func() {
		for _, patch := range patches {
			patch.Reset()
		}
	}()
	limiter := CreateConcurrencyLimiter(2)
	ch := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			limiter.Apply()
		}()
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case <-ch:
		t.Errorf("limiter should block")
	default:
	}
	assert.Equal(t, isReportAlarm, true)
	assert.Equal(t, isClearAlarm, false) // blocking num is not less 0, it will not clear alarm
	limiter.Reset(3)
	select {
	case <-ch:
	}
	assert.Equal(t, isReportAlarm, true)
	assert.Equal(t, isClearAlarm, true) // blocking num is 0, it will clear alarm
}

func TestConcurrencyLimiterWithAlarm2(t *testing.T) {
	isReportAlarm := false
	isClearAlarm := false
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
			isReportAlarm = true
		}),
		gomonkey.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			isClearAlarm = true
		}),
	}
	defer func() {
		for _, patch := range patches {
			patch.Reset()
		}
	}()
	requestNum := 3
	limiter := CreateConcurrencyLimiter(2)
	wg := &sync.WaitGroup{}
	wg.Add(requestNum)
	for i := 0; i < requestNum; i++ {
		go func() {
			defer wg.Done()
			defer limiter.Release()
			limiter.Apply()
			time.Sleep(50 * time.Millisecond)
		}()
	}
	wg.Wait()

	assert.Equal(t, isReportAlarm, true)
	assert.Equal(t, isClearAlarm, true)
}

func TestConcurrencyLimiter_AlarmForWaitingQueueTimeout(t *testing.T) {
	var (
		patch      = gomonkey.NewPatches()
		mockLogger = &fakeLogger{}
	)

	convey.Convey("test AlarmForWaitingQueue", t, func() {
		convey.Convey("test alarm for waiting queue timeout", func() {
			isReportAlarm := false
			isClearAlarm := false
			requestNum := 3
			limiter := CreateConcurrencyLimiter(2)
			wg := &sync.WaitGroup{}
			mockLogGetLogger(patch, mockLogger)
			timer := time.NewTimer(25 * time.Millisecond)
			patch.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			})

			patch.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
				if almType == alarm.CheckpointQueueLongTime {
					isReportAlarm = true
				}
			})
			patch.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
				if almType == alarm.CheckpointQueueLongTime {
					isClearAlarm = true
				}
			})

			wg.Add(requestNum)
			for i := 0; i < requestNum; i++ {
				go func() {
					defer wg.Done()
					defer limiter.Release()
					limiter.Apply()
					time.Sleep(50 * time.Millisecond)
				}()
			}
			wg.Wait()

			convey.So(mockLogger.WarnMsg, convey.ShouldContainSubstring, "snapshot queue waiting time exceeds threshold:")
			convey.So(isReportAlarm, convey.ShouldBeTrue)
			convey.So(isClearAlarm, convey.ShouldBeTrue)
		})

		convey.Convey("test alarm for waiting queue without timeout", func() {
			requestNum := 3
			limiter := CreateConcurrencyLimiter(2)
			wg := &sync.WaitGroup{}
			mockLogGetLogger(patch, mockLogger)
			timer := time.NewTimer(75 * time.Millisecond)
			patch.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			})
			patch.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
			})
			patch.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {})

			wg.Add(requestNum)
			for i := 0; i < requestNum; i++ {
				go func() {
					defer wg.Done()
					defer limiter.Release()
					limiter.Apply()
					time.Sleep(50 * time.Millisecond)
				}()
			}
			wg.Wait()

			convey.So(mockLogger.WarnMsg, convey.ShouldNotContainSubstring, "snapshot queue waiting time exceeds threshold:")
		})
		convey.Reset(func() {
			patch.Reset()
			mockLogger = &fakeLogger{}
		})
	})
}
