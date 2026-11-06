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

package workerevaluator

import (
	"sync"
	"time"

	"functioncore/pkg/common/api/workermanager"
)

const secondsToNanoseconds = 1000000000

type timeWindowView struct {
	sync.RWMutex
	// If start time is in time window, it represents request start time. Else time window start time.
	unCompletedRequestStartTime time.Time
	maxConcurrentNum            int
	concurrentNum               int
	requestDurationNanoSeconds  int64
	metrics                     []workermanager.ReportMetrics
}

func (v *timeWindowView) requestStarted() {
	v.Lock()
	// when no request exists: record the used start time
	if v.concurrentNum == 0 {
		v.unCompletedRequestStartTime = time.Now()
	}
	v.concurrentNum++
	// attempt to update max concurrent number
	if v.concurrentNum > v.maxConcurrentNum {
		v.maxConcurrentNum = v.concurrentNum
	}
	v.Unlock()
}

func (v *timeWindowView) requestEnded() {
	v.Lock()
	v.concurrentNum--

	// when no request exists: calculate the used duration
	if v.concurrentNum == 0 {
		durationNs := time.Now().Sub(v.unCompletedRequestStartTime).Nanoseconds()
		if durationNs > 0 {
			v.requestDurationNanoSeconds += durationNs
		}
	}
	v.Unlock()
}

func divideCeil(divisor, dividend int64) int {
	if dividend == 0 || divisor == 0 {
		return 0
	}
	const ceilValue = 1
	return int(divisor/dividend) + ceilValue
}

func (v *timeWindowView) collectSingleTimeWindow(timestamp int64) {
	v.Lock()
	if v.concurrentNum != 0 {
		timeDiffSeconds := timestamp - v.unCompletedRequestStartTime.Unix()
		// calculate used time slice for request with inaccuracy of +- second
		if timeDiffSeconds > 0 {
			v.unCompletedRequestStartTime = time.Unix(timestamp, 0)
			v.requestDurationNanoSeconds += timeDiffSeconds * secondsToNanoseconds
		}
	}

	// ceil value
	usedTime := divideCeil(v.requestDurationNanoSeconds, secondsToNanoseconds)
	if usedTime > int(timeWindowsLength/time.Second) {
		usedTime = int(timeWindowsLength / time.Second)
	}

	v.metrics = append(v.metrics, workermanager.ReportMetrics{
		MaxCurrentRequestNums: v.maxConcurrentNum,
		UsedTimeSlice:         usedTime,
		SecondsTimestamp:      timestamp,
	})
	v.maxConcurrentNum = v.concurrentNum
	v.requestDurationNanoSeconds = 0
	v.Unlock()
}

func (v *timeWindowView) getTimeWindows() (metrics []workermanager.ReportMetrics) {
	v.Lock()
	metrics = v.metrics
	v.metrics = v.metrics[:0]
	v.Unlock()
	return
}
