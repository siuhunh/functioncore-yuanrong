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
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/types/workertypes"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/utils"
)

var (
	zeroTime = time.Time{}
	// ConResource concurrency resource
	ConResource = &ConcurrentResource{startTime: zeroTime}

	initDurationOnce      sync.Once
	loadDurationOnce      sync.Once
	restoreDurationOnce   sync.Once
	extensionDurationOnce sync.Once
	preStopDurationOnce   sync.Once
)

// ConcurrentResource concurrent resource
type ConcurrentResource struct {
	initSummary           *Summary
	loadSummary           *Summary
	restoreSummary        *Summary
	extensionSummary      *Summary
	preStopSummary        *Summary
	requestCount          int32
	startTime             time.Time
	startTimeMutex        sync.Mutex
	timeWindows           []workertypes.TimeWindow
	timeWindowsMutex      sync.Mutex
	maxBillingDuration    int
	initSummaryMutex      sync.Mutex
	preStopSummaryMutex   sync.Mutex
	loadSummaryMutex      sync.Mutex
	restoreSummaryMutex   sync.Mutex
	extensionSummaryMutex sync.Mutex
}

// CheckIsOrNotInvokeCharging -
func CheckIsOrNotInvokeCharging(requestConcurrency uint32, directRequest bool) bool {
	if requestConcurrency == 1 && PodApplier != constants.WorkerManagerApplier && directRequest &&
		!commonUtils.IsWebSocketFunc(businessType, utils.GetEnableRemoteDebug(), enableWebSocket) {
		return false
	}
	return true
}

// StartOccupation Start to occupy ConcurrentResource
func (c *ConcurrentResource) StartOccupation(requestConcurrency uint32, startTime time.Time,
	retryFlag, directRequest bool) {
	c.AppendLoadAndInitTimeWindows(requestConcurrency, directRequest)
	if CheckIsOrNotInvokeCharging(requestConcurrency, directRequest) && !retryFlag {
		c.startTimeMutex.Lock()
		c.requestCount++
		if c.requestCount == 1 {
			c.startTime = startTime
		}
		c.startTimeMutex.Unlock()
	}
}

// StopOccupation stop to occupy ConcurrentResource
func (c *ConcurrentResource) StopOccupation(requestConcurrency uint32, retryFlag, directRequest bool) time.Time {
	if CheckIsOrNotInvokeCharging(requestConcurrency, directRequest) && !retryFlag {
		c.startTimeMutex.Lock()
		newConcurrentRequestCount := atomic.AddInt32(&c.requestCount, -1)
		if newConcurrentRequestCount == 0 {
			usingResourceStartTime := c.startTime
			c.startTime = zeroTime
			c.startTimeMutex.Unlock()
			return usingResourceStartTime
		}
		c.startTimeMutex.Unlock()
	}
	return zeroTime
}

func (c *ConcurrentResource) generateTimeWindow(startTime, endTime time.Time, stage string) workertypes.TimeWindow {
	subDuration := endTime.Sub(startTime)
	billingDuration := int(CalculateBillingDuration(subDuration) / time.Millisecond)
	if stage == constants.InvokeStage && billingDuration > c.maxBillingDuration {
		billingDuration = c.maxBillingDuration
	}
	return workertypes.TimeWindow{
		StartTime:       timeutil.UnixMillisecond(startTime),
		EndTime:         timeutil.UnixMillisecond(endTime),
		BillingDuration: billingDuration,
		BusyDuration:    billingDuration,
		Stage:           stage,
	}
}

// GetTimeWindows -
func (c *ConcurrentResource) GetTimeWindows(periodStartTime, periodEndTime time.Time) []workertypes.TimeWindow {
	c.startTimeMutex.Lock()
	defer c.startTimeMutex.Unlock()
	// in non-idle mode, the function invoke charging is deleted to prevent repeated charging.
	if PodApplier == constants.WorkerManagerApplier {
		if GetReservedInstanceModeInCurrentTimeWindow() == constants.ReservedInstanceNonIdleMode {
			ConResource.ClearReportedTimeWindows()
			c.appendResInsLoadAndInitTimeWindows()
			c.AppendTimeWindows(periodStartTime, periodEndTime, constants.InvokeStage)
			return c.timeWindows
		}
		c.appendResInsLoadAndInitTimeWindows()
	}
	startTime := c.startTime
	// idle mode and no incoming request len(c.timeWindows) = 0
	if startTime.IsZero() {
		// This indicates that the function instance is not being invoked
		if len(c.timeWindows) == 0 {
			return nil
		}
		// all elements in timeWindows need to be reported.
		return c.timeWindows
	}

	if !startTime.IsZero() {
		// This indicates that the function instance is being invoked.
		// Update startTime to periodEndTime.
		c.startTime = periodEndTime
		if startTime.Before(periodStartTime) {
			// The startTime is earlier than periodStartTime.
			// Therefore, append time window [periodStartTime, periodEndTime] to timeWindows.
			c.AppendTimeWindows(periodStartTime, periodEndTime, constants.InvokeStage)
			return c.timeWindows
		}
		// The startTime is equal or later than periodStartTime.
		// Therefore, append time window [startTime, periodEndTime] to timeWindows.
		c.AppendTimeWindows(startTime, periodEndTime, constants.InvokeStage)
		return c.timeWindows
	}

	return nil
}

// RemoveReportedTimeWindows -
func (c *ConcurrentResource) RemoveReportedTimeWindows(reportedNums int) {
	c.timeWindowsMutex.Lock()
	if reportedNums < len(c.timeWindows) {
		c.timeWindows = c.timeWindows[reportedNums:]
		c.timeWindowsMutex.Unlock()
		return
	}
	c.timeWindows = nil
	c.timeWindowsMutex.Unlock()
}

// ClearReportedTimeWindows -
func (c *ConcurrentResource) ClearReportedTimeWindows() {
	c.timeWindowsMutex.Lock()
	c.timeWindows = nil
	c.timeWindowsMutex.Unlock()
}

// AppendTimeWindows -
func (c *ConcurrentResource) AppendTimeWindows(startTime, endTime time.Time, stage string) {
	if startTime.IsZero() || endTime.IsZero() || endTime.Before(startTime) {
		return
	}
	timeWindow := c.generateTimeWindow(startTime, endTime, stage)
	c.timeWindowsMutex.Lock()
	c.timeWindows = append(c.timeWindows, timeWindow)
	c.timeWindowsMutex.Unlock()
}

func (c *ConcurrentResource) appendResInsLoadAndInitTimeWindows() {
	c.loadSummaryMutex.Lock()
	if c.loadSummary != nil {
		loadDurationOnce.Do(func() {
			c.AppendTimeWindows(c.loadSummary.StartTime(), c.loadSummary.EndTime(), constants.LoadStage)
		})
		c.loadSummary = nil
	}
	c.loadSummaryMutex.Unlock()
	c.initSummaryMutex.Lock()
	if c.initSummary != nil {
		initDurationOnce.Do(func() {
			c.AppendTimeWindows(c.initSummary.StartTime(), c.initSummary.EndTime(), constants.InitializeStage)
		})
		c.initSummary = nil
	}
	c.initSummaryMutex.Unlock()
	c.restoreSummaryMutex.Lock()
	if c.restoreSummary != nil {
		restoreDurationOnce.Do(func() {
			c.AppendTimeWindows(c.restoreSummary.StartTime(), c.restoreSummary.EndTime(), constants.RestoreStage)
		})
		c.restoreSummary = nil
	}
	c.restoreSummaryMutex.Unlock()
	c.extensionSummaryMutex.Lock()
	if c.extensionSummary != nil {
		extensionDurationOnce.Do(func() {
			c.AppendTimeWindows(c.extensionSummary.StartTime(), c.extensionSummary.EndTime(),
				constants.ExtensionStage)
		})
		c.extensionSummary = nil
	}
	c.extensionSummaryMutex.Unlock()
}

// AppendLoadAndInitTimeWindows -
func (c *ConcurrentResource) AppendLoadAndInitTimeWindows(concurrency uint32, directRequest bool) {
	if PodApplier != constants.WorkerManagerApplier {
		c.loadSummaryMutex.Lock()
		if (concurrency > 1 || !directRequest) && c.loadSummary != nil {
			loadDurationOnce.Do(func() {
				c.AppendTimeWindows(c.loadSummary.StartTime(), c.loadSummary.EndTime(), constants.LoadStage)
			})
		}
		c.loadSummary = nil
		c.loadSummaryMutex.Unlock()

		c.initSummaryMutex.Lock()
		if (concurrency > 1 || !directRequest) && c.initSummary != nil {
			initDurationOnce.Do(func() {
				c.AppendTimeWindows(c.initSummary.StartTime(), c.initSummary.EndTime(), constants.InitializeStage)
			})
		}
		c.initSummary = nil
		c.initSummaryMutex.Unlock()

		c.restoreSummaryMutex.Lock()
		if (concurrency > 1 || !directRequest) && c.restoreSummary != nil {
			restoreDurationOnce.Do(func() {
				c.AppendTimeWindows(c.restoreSummary.StartTime(), c.restoreSummary.EndTime(), constants.RestoreStage)
			})
		}
		c.restoreSummary = nil
		c.restoreSummaryMutex.Unlock()

		c.extensionSummaryMutex.Lock()
		if (concurrency > 1 || !directRequest) && c.extensionSummary != nil {
			extensionDurationOnce.Do(func() {
				c.AppendTimeWindows(c.extensionSummary.StartTime(), c.extensionSummary.EndTime(),
					constants.ExtensionStage)
			})
		}
		c.extensionSummary = nil
		c.extensionSummaryMutex.Unlock()

		c.preStopSummaryMutex.Lock()
		if (concurrency > 1 || !directRequest) && c.preStopSummary != nil {
			preStopDurationOnce.Do(func() {
				c.AppendTimeWindows(c.preStopSummary.StartTime(), c.preStopSummary.EndTime(),
					constants.PreStopStage)
			})
		}
		c.preStopSummary = nil
		c.preStopSummaryMutex.Unlock()
	} else {
		if GetIdleMode() {
			c.appendResInsLoadAndInitTimeWindows()
		}
	}
}

// SetMaxBillingDuration -
func (c *ConcurrentResource) SetMaxBillingDuration(maxBillingDuration int) {
	c.maxBillingDuration = maxBillingDuration
}

// SetInitSummary -
func (c *ConcurrentResource) SetInitSummary(initSummary *Summary) {
	c.initSummaryMutex.Lock()
	c.initSummary = initSummary
	c.initSummaryMutex.Unlock()
}

// SetLoadSummary -
func (c *ConcurrentResource) SetLoadSummary(loadSummary *Summary) {
	c.loadSummaryMutex.Lock()
	c.loadSummary = loadSummary
	c.loadSummaryMutex.Unlock()
}

// SetRestoreSummary -
func (c *ConcurrentResource) SetRestoreSummary(restoreSummary *Summary) {
	c.restoreSummaryMutex.Lock()
	c.restoreSummary = restoreSummary
	c.restoreSummaryMutex.Unlock()
}

// SetExtensionSummary -
func (c *ConcurrentResource) SetExtensionSummary(extensionSummary *Summary) {
	c.extensionSummaryMutex.Lock()
	c.extensionSummary = extensionSummary
	c.extensionSummaryMutex.Unlock()
}

// SetPreStopSummary -
func (c *ConcurrentResource) SetPreStopSummary(preStopSummary *Summary) {
	c.preStopSummaryMutex.Lock()
	c.preStopSummary = preStopSummary
	c.preStopSummaryMutex.Unlock()
}
