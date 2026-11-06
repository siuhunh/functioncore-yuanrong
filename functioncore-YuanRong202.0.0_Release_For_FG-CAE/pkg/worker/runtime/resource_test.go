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

package runtime

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/timeutil"
	"functioncore/test/utils"
)

func TestConcurrentResource_RemoveReportedTimeWindows(t *testing.T) {
	resource := ConcurrentResource{}
	testCases := []struct {
		name                    string
		usedResourceTimeWindows []TimeWindow
		reportedNums            int
		expectLength            int
	}{
		{
			name:                    "remove all",
			usedResourceTimeWindows: []TimeWindow{{}, {}, {}},
			reportedNums:            3,
			expectLength:            0,
		},
		{
			name:                    "remove part",
			usedResourceTimeWindows: []TimeWindow{{}, {}, {}},
			reportedNums:            1,
			expectLength:            2,
		},
		{
			name:                    "remove zero",
			usedResourceTimeWindows: []TimeWindow{{}, {}, {}},
			reportedNums:            0,
			expectLength:            3,
		},
		{
			name:                    "remove over",
			usedResourceTimeWindows: []TimeWindow{{}, {}, {}},
			reportedNums:            4,
			expectLength:            0,
		},
	}

	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			resource.timeWindows = tt.usedResourceTimeWindows
			resource.RemoveReportedTimeWindows(tt.reportedNums)
			convey.So(len(resource.timeWindows), convey.ShouldEqual, tt.expectLength)
		})
	}
}

func TestConcurrentResource_GetTimeWindows(t *testing.T) {
	resource := ConcurrentResource{}
	resource.SetMaxBillingDuration(300000)
	now := time.Now()
	testCases := []struct {
		name              string
		startTime         time.Time
		timeWindows       []TimeWindow
		periodStartTime   time.Time
		periodEndTime     time.Time
		expectTimeWindows []TimeWindow
	}{
		{
			name:              "not being invoked and timeWindows is empty",
			startTime:         zeroTime,
			timeWindows:       nil,
			periodStartTime:   zeroTime,
			periodEndTime:     zeroTime,
			expectTimeWindows: nil,
		},
		{
			name:              "not being invoked and timeWindows is not empty",
			startTime:         zeroTime,
			timeWindows:       []TimeWindow{{}},
			periodStartTime:   zeroTime,
			periodEndTime:     zeroTime,
			expectTimeWindows: []TimeWindow{{}},
		},
		{
			name:            "is being invoked and startTime before periodStartTime",
			startTime:       now,
			timeWindows:     nil,
			periodStartTime: now.Add(1 * time.Millisecond),
			periodEndTime:   now.Add(2 * time.Millisecond),
			expectTimeWindows: []TimeWindow{{
				StartTime:       timeutil.UnixMillisecond(now.Add(1 * time.Millisecond)),
				EndTime:         timeutil.UnixMillisecond(now.Add(2 * time.Millisecond)),
				BillingDuration: 1,
				BusyDuration:    1,
				IdleDuration:    0,
				Stage:           constants.InvokeStage,
			}},
		},
		{
			name:            "is being invoked and startTime not before periodStartTime",
			startTime:       now,
			timeWindows:     nil,
			periodStartTime: now.Add(-1 * time.Millisecond),
			periodEndTime:   now.Add(1 * time.Millisecond),
			expectTimeWindows: []TimeWindow{{
				StartTime:       timeutil.UnixMillisecond(now),
				EndTime:         timeutil.UnixMillisecond(now.Add(1 * time.Millisecond)),
				BillingDuration: 1,
				BusyDuration:    1,
				IdleDuration:    0,
				Stage:           constants.InvokeStage,
			}},
		},
	}
	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			resource.timeWindows = tt.timeWindows
			resource.startTime = tt.startTime
			actualTimeWindows := resource.GetTimeWindows(tt.periodStartTime, tt.periodEndTime)
			convey.So(actualTimeWindows, convey.ShouldResemble, tt.expectTimeWindows)
		})
	}
}

func TestConcurrentResource_StartTime(t *testing.T) {
	resource := ConcurrentResource{}
	now := time.Now().UTC()
	resource.SetInitSummary(&Summary{
		startTime: now,
		endTime:   now.Add(1 * time.Millisecond),
	})

	patch := gomonkey.ApplyFunc(GetReservedInstanceModeInCurrentTimeWindow, func() int {
		return constants.NonReservedInstanceMode
	})
	defer patch.Reset()

	requestConcurrency := uint32(1)
	resource.StartOccupation(requestConcurrency, time.Now(), false, true)
	if !resource.startTime.IsZero() {
		t.Errorf("resource.startTime should equal zeroTime")
	}
	if resource.requestCount != int32(0) {
		t.Errorf("resource.requestCount should equal 0")
	}
	returnUsingResourceStartTime := resource.StopOccupation(requestConcurrency, false, true)
	if !returnUsingResourceStartTime.IsZero() {
		t.Errorf("return startTime should equal zero time")
	}
	if resource.requestCount != int32(0) {
		t.Errorf("resource.requestCount should equal 0")
	}
	timeWindows := resource.GetTimeWindows(now, time.Now())
	if len(timeWindows) != 0 {
		t.Errorf("length of timeWindows should equal 0")
	}

	// resource.startTime is operated only when the concurrency is greater than 1.
	requestConcurrency = uint32(2)
	// requestCount 0 -> 1 -> 2 -> 1 ->0
	// The request count is incremented to 1, so resource.startTime should equal firstRequestStartTime
	firstRequestStartTime := time.Now()
	resource.StartOccupation(requestConcurrency, firstRequestStartTime, false, true)
	if !resource.startTime.Equal(firstRequestStartTime) {
		t.Errorf("resource.startTime should equal firstRequestStartTime")
	}
	// The request count is incremented to 2, so metrics.startTime should not change
	secondRequestStartTime := time.Now()
	resource.StartOccupation(requestConcurrency, secondRequestStartTime, false, true)
	if !resource.startTime.Equal(firstRequestStartTime) {
		t.Errorf("resource.startTime should equal firstRequestStartTime")
	}

	// The request count is decremented to 1, so only zero time is returned.
	returnUsingResourceStartTime = resource.StopOccupation(requestConcurrency, false, true)
	if !returnUsingResourceStartTime.IsZero() {
		t.Errorf("return startTime should equal zero time")
	}
	if !resource.startTime.Equal(firstRequestStartTime) {
		t.Errorf("metrics.startTime should equal firstRequestStartTime")
	}

	// The request count is decremented to 0,
	// so firstRequestStartTime is returned and set to zero time.
	returnUsingResourceStartTime = resource.StopOccupation(requestConcurrency, false, true)
	if !returnUsingResourceStartTime.Equal(firstRequestStartTime) {
		t.Errorf("return startTime should equal firstRequestStartTime")
	}
	if !resource.startTime.IsZero() {
		t.Errorf("resource.startTime should equal zero time")
	}

	// The first request is a retry request.
	requestConcurrency = uint32(2)
	firstRequestStartTime = time.Now()
	resource.StartOccupation(requestConcurrency, firstRequestStartTime, true, true)
	if !resource.startTime.Equal(zeroTime) {
		t.Errorf("resource.startTime should equal zeroTime")
	}
	if resource.requestCount != 0 {
		t.Errorf("resource.requestCount should be 0")
	}
	// The first request is not a retry request.
	secondRequestStartTime = time.Now()
	resource.StartOccupation(requestConcurrency, secondRequestStartTime, false, true)
	if !resource.startTime.Equal(secondRequestStartTime) {
		t.Errorf("resource.startTime should equal secondRequestStartTime")
	}
	if resource.requestCount != 1 {
		t.Errorf("resource.requestCount should be 1")
	}

	// The first request response
	returnUsingResourceStartTime = resource.StopOccupation(requestConcurrency, true, true)
	if !returnUsingResourceStartTime.IsZero() {
		t.Errorf("return startTime should equal zero time")
	}
	if resource.requestCount != 1 {
		t.Errorf("resource.requestCount should be 1")
	}
	if !resource.startTime.Equal(secondRequestStartTime) {
		t.Errorf("metrics.startTime should equal secondRequestStartTime")
	}

	// The request count is decremented to 0,
	// so firstRequestStartTime is returned and set to zero time.
	returnUsingResourceStartTime = resource.StopOccupation(requestConcurrency, false, true)
	if !returnUsingResourceStartTime.Equal(secondRequestStartTime) {
		t.Errorf("return startTime should equal secondRequestStartTime")
	}
	if !resource.startTime.IsZero() {
		t.Errorf("resource.startTime should equal zero time")
	}
	if resource.requestCount != 0 {
		t.Errorf("resource.requestCount should be 0")
	}

	testResource := ConcurrentResource{}
	now = time.Now().UTC()
	testResource.SetPreStopSummary(&Summary{
		startTime: now,
		endTime:   now.Add(1 * time.Millisecond),
	})
	// The first request is a retry request.
	requestConcurrency = uint32(2)
	firstRequestStartTime = time.Now()
	resource.StartOccupation(requestConcurrency, firstRequestStartTime, true, false)
	if !resource.startTime.Equal(zeroTime) {
		t.Errorf("resource.startTime should equal zeroTime")
	}
	if resource.requestCount != 0 {
		t.Errorf("resource.requestCount should be 0")
	}
}

func TestConcurrentResource_StartOccupationInNonIdleMode(t *testing.T) {
	resource := ConcurrentResource{}
	now := time.Now().UTC()
	resource.SetInitSummary(&Summary{
		startTime: now,
		endTime:   now.Add(1 * time.Millisecond),
	})

	patch := gomonkey.ApplyFunc(GetReservedInstanceModeInCurrentTimeWindow, func() int {
		return constants.ReservedInstanceNonIdleMode
	})
	defer patch.Reset()

	resource.StartOccupation(200, time.Now(), false, true)
	if resource.startTime == now {
		t.Errorf("resource.startTime should equal time.Now()")
	}
}

func TestConcurrentResource_StopOccupationInNonIdleMode(t *testing.T) {
	resource := ConcurrentResource{}
	now := time.Now().UTC()
	resource.SetInitSummary(&Summary{
		startTime: now,
		endTime:   now.Add(1 * time.Millisecond),
	})

	patch := gomonkey.ApplyFunc(GetReservedInstanceModeInCurrentTimeWindow, func() int {
		return constants.ReservedInstanceNonIdleMode
	})
	defer patch.Reset()

	returnUsingResourceStartTime := resource.StopOccupation(200, false, true)
	if !returnUsingResourceStartTime.IsZero() {
		t.Errorf("return startTime should equal zero time")
	}
}

func TestConcurrentResource_AppendTimeWindows(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name              string
		startTime         time.Time
		endTime           time.Time
		stage             string
		expectTimeWindows []TimeWindow
	}{
		{
			name:              "startTime is zero time",
			startTime:         zeroTime,
			endTime:           now,
			expectTimeWindows: nil,
		},
		{
			name:              "endTime is zero time",
			startTime:         now,
			endTime:           zeroTime,
			expectTimeWindows: nil,
		},
		{
			name:              "endTime before startTime",
			startTime:         now,
			endTime:           now.Add(-1 * time.Millisecond),
			expectTimeWindows: nil,
		},
		{
			name:      "no problem",
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
			stage:     constants.InvokeStage,
			expectTimeWindows: []TimeWindow{{
				StartTime:       timeutil.UnixMillisecond(now),
				EndTime:         timeutil.UnixMillisecond(now.Add(1 * time.Millisecond)),
				BillingDuration: 1,
				BusyDuration:    1,
				Stage:           constants.InvokeStage,
			}},
		},
		{
			name:      "endTime - startTime > 300000ms",
			startTime: now,
			endTime:   now.Add(300001 * time.Millisecond),
			stage:     constants.InvokeStage,
			expectTimeWindows: []TimeWindow{{
				StartTime:       timeutil.UnixMillisecond(now),
				EndTime:         timeutil.UnixMillisecond(now.Add(300001 * time.Millisecond)),
				BillingDuration: 300000,
				BusyDuration:    300000,
				Stage:           constants.InvokeStage,
			}},
		},
		{
			name:      "append InitializeStage time window",
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
			stage:     constants.InitializeStage,
			expectTimeWindows: []TimeWindow{{
				StartTime:       timeutil.UnixMillisecond(now),
				EndTime:         timeutil.UnixMillisecond(now.Add(1 * time.Millisecond)),
				BillingDuration: 1,
				BusyDuration:    1,
				Stage:           constants.InitializeStage,
			}},
		},
	}
	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			resource := ConcurrentResource{}
			// 300000ms
			resource.SetMaxBillingDuration(300000)
			resource.AppendTimeWindows(tt.startTime, tt.endTime, tt.stage)
			convey.So(resource.timeWindows, convey.ShouldResemble, tt.expectTimeWindows)
		})
	}
}

func BenchmarkConcurrentResource(b *testing.B) {
	b.SetParallelism(200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ConResource.StartOccupation(200, time.Time{}, false, true)
			ConResource.StopOccupation(200, false, true)
		}
	})

}

func TestConcurrentResource_AppendResInsLoadAndInitTimeWindows(t *testing.T) {
	convey.Convey("success case", t, func() {
		resource := ConcurrentResource{}
		now := time.Now().UTC()
		patch := gomonkey.ApplyGlobalVar(&PodApplier, constants.WorkerManagerApplier)
		defer patch.Reset()
		resource.SetLoadSummary(&Summary{
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
		})
		resource.SetInitSummary(&Summary{
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
		})
		resource.SetExtensionSummary(&Summary{
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
		})
		resource.appendResInsLoadAndInitTimeWindows()
		convey.So(len(resource.timeWindows), convey.ShouldEqual, 2)
	})
}

func TestConcurrentResource_SetPreStopSummary(t *testing.T) {
	convey.Convey("success case", t, func() {
		resource := ConcurrentResource{}
		now := time.Now().UTC()
		patch := gomonkey.ApplyGlobalVar(&PodApplier, constants.WorkerManagerApplier)
		defer patch.Reset()
		resource.SetPreStopSummary(&Summary{
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
		})
		convey.So(resource.preStopSummary.startTime, convey.ShouldEqual, now)
	})
}

func TestAppendLoadAndInitTimeWindows(t *testing.T) {
	convey.Convey("appendLoadAndInitTimeWindows case: PodApplier==worker-manager", t, func() {

		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyGlobalVar(&PodApplier, constants.WorkerManagerApplier),
			gomonkey.ApplyFunc(GetIdleMode, func() bool {
				return true
			}),
			gomonkey.ApplyFunc((*ConcurrentResource).appendResInsLoadAndInitTimeWindows, func(c *ConcurrentResource) {}),
		})
		defer patches.ResetAll()
		isIdleMode = uint32(1)
		resource := ConcurrentResource{}
		now := time.Now().UTC()
		resource.SetPreStopSummary(&Summary{
			startTime: now,
			endTime:   now.Add(1 * time.Millisecond),
		})
		resource.appendLoadAndInitTimeWindows(uint32(1), true)
	})
}

func TestCheckIsOrNotInvokeCharging(t *testing.T) {
	t.Run("requestConcurrency > 1 true", func(t *testing.T) {
		assert.True(t, CheckIsOrNotInvokeCharging(2, true))
		assert.True(t, CheckIsOrNotInvokeCharging(2, false))
	})
	t.Run("PodApplier is worker-manager  true", func(t *testing.T) {
		PodApplier = constants.WorkerManagerApplier
		defer func() { PodApplier = "" }()
		assert.True(t, CheckIsOrNotInvokeCharging(1, true))
		assert.True(t, CheckIsOrNotInvokeCharging(1, false))
	})
	t.Run("businessType websocket is worker-manager true", func(t *testing.T) {
		businessType = constants.BusinessTypeWebSocket
		defer func() { businessType = "" }()
		assert.True(t, CheckIsOrNotInvokeCharging(1, true))
		assert.True(t, CheckIsOrNotInvokeCharging(1, false))
		assert.True(t, CheckIsOrNotInvokeCharging(2, true))
		assert.True(t, CheckIsOrNotInvokeCharging(2, false))
	})
	t.Run("requestConcurrency = 1 AND directRequest = true false", func(t *testing.T) {
		assert.False(t, CheckIsOrNotInvokeCharging(1, true))
	})
	t.Run("requestConcurrency = 1 AND directRequest = false true", func(t *testing.T) {
		assert.True(t, CheckIsOrNotInvokeCharging(1, false))
	})
}
