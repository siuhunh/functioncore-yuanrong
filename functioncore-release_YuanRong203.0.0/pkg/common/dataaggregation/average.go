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

// Package dataaggregation data aggregation by time windows
package dataaggregation

import "time"

const maxWindowCapacity = 1000

// Average average data aggregation
type Average struct {
	windowLength            time.Duration
	windowCapacityThreshold int

	value int64

	lastWindowTime time.Time
	windowValueSum int64
	windowValueNum int
}

// AddSample -
func (a *Average) AddSample(sampleValue int64, t time.Time) {
	truncTime := t.Truncate(a.windowLength)
	if truncTime == a.lastWindowTime {
		if a.windowValueNum >= maxWindowCapacity {
			return
		}
		a.windowValueSum += sampleValue
		a.windowValueNum++
		if a.windowValueNum == maxWindowCapacity {
			a.value = a.windowValueSum/int64(a.windowValueNum) + 1
		}
	} else {
		// record last average value
		if a.windowValueNum >= a.windowCapacityThreshold && a.windowValueNum < maxWindowCapacity {
			a.value = a.windowValueSum/int64(a.windowValueNum) + 1
		}
		a.lastWindowTime = truncTime
		a.windowValueNum = 1
		a.windowValueSum = sampleValue
	}
}

// AddSamples -
func (a *Average) AddSamples(sampleValue []int64, t time.Time) {
	truncTime := t.Truncate(a.windowLength)
	if truncTime == a.lastWindowTime {
		if a.windowValueNum >= maxWindowCapacity {
			return
		}
		for _, value := range sampleValue {
			a.windowValueSum += value
		}
		a.windowValueNum += len(sampleValue)
		if a.windowValueNum >= maxWindowCapacity {
			a.value = a.windowValueSum/int64(a.windowValueNum) + 1
		}
	} else {
		// record last average value
		if a.windowValueNum >= a.windowCapacityThreshold && a.windowValueNum < maxWindowCapacity {
			a.value = a.windowValueSum/int64(a.windowValueNum) + 1
		}
		a.lastWindowTime = truncTime
		a.windowValueSum = 0

		for _, value := range sampleValue {
			a.windowValueSum += value
		}
		a.windowValueNum = len(sampleValue)
	}
}

// GetValue get latest average value
func (a *Average) GetValue(t time.Time) int64 {
	if a.value != 0 {
		return a.value
	}

	if a.windowValueNum >= a.windowCapacityThreshold {
		a.value = a.windowValueSum/int64(a.windowValueNum) + 1
		return a.value
	}

	return 0
}

func newAverageAggregation(windowLength time.Duration, _, windowCapacity int) DataAggregation {
	return &Average{
		value:                   0,
		lastWindowTime:          time.Now().Truncate(windowLength),
		windowLength:            windowLength,
		windowValueSum:          0,
		windowCapacityThreshold: windowCapacity,
	}
}
