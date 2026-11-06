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

import (
	"time"
)

// AggregationType -
type AggregationType int

const (
	// COUNT used to count data aggregation
	COUNT AggregationType = iota
	// AVERAGE used to count data aggregation
	AVERAGE
)

// DataAggregation -
type DataAggregation interface {
	AddSamples(sample []int64, t time.Time)
	AddSample(sample int64, t time.Time)
	GetValue(t time.Time) int64
}

// NewDataAggregation support sliding-windows later
func NewDataAggregation(t AggregationType, windowLength time.Duration, windowNum, windowCapacity int) DataAggregation {
	switch t {
	case COUNT:
		return newCountAggregation(windowLength, windowNum, windowCapacity)
	case AVERAGE:
		return newAverageAggregation(windowLength, windowNum, windowCapacity)
	default:
		return newAverageAggregation(windowLength, windowNum, windowCapacity)
	}
}
