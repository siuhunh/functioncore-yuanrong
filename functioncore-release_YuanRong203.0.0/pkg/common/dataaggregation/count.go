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

// Count count data aggregation
type Count struct {
	windowLength time.Duration

	value int64

	lastWindowTime time.Time
	windowValue    int64
}

// AddSample -
func (c *Count) AddSample(_ int64, t time.Time) {
	truncTime := t.Truncate(c.windowLength)
	diff := truncTime.Sub(c.lastWindowTime)
	if diff == 0 {
		c.windowValue++
		return
	}

	if diff > c.windowLength {
		c.value = 0
	}
	c.lastWindowTime = truncTime
	c.value = c.windowValue
	c.windowValue = 1
}

// AddSamples -
func (c *Count) AddSamples(_ []int64, _ time.Time) {}

// GetValue get latest count value
func (c *Count) GetValue(t time.Time) int64 {
	diff := t.Sub(c.lastWindowTime)
	if diff < c.windowLength {
		// time is in current window
		if c.value != 0 {
			return c.value
		} else {
			return c.windowValue
		}
	} else if diff-c.windowLength < c.windowLength {
		// time is in last window
		c.value = c.windowValue
		c.windowValue = 0
		c.lastWindowTime = c.lastWindowTime.Add(c.windowLength)
		return c.value
	} else {
		// time is before last window
		c.value = 0
		c.windowValue = 0
		c.lastWindowTime = t.Truncate(c.windowLength)
		return 0
	}
}

func newCountAggregation(windowLength time.Duration, _, _ int) DataAggregation {
	return &Count{
		value:          0,
		lastWindowTime: time.Now().Truncate(windowLength),
		windowLength:   windowLength,
		windowValue:    0,
	}
}
