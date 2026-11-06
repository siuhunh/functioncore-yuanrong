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

// Package algorithm contains auto-scale algorithm
package algorithm

import "functioncore/pkg/functionbus/common"

const (
	msToS = 1000

	invalidScalingNum = 0xff
)

// Type type of algorithm
type Type int

const (
	// PerInstanceConcurrency -
	PerInstanceConcurrency Type = iota
	// RPS -
	RPS
	// TypeNumber as capacity
	TypeNumber
)

// ScaleInfo scale information, the input of algorithm
type ScaleInfo struct {
	// updated from scheduler periodically as calculating scale down number
	CurrentRequests  int
	WorkerTPS        int
	RequestLatencyMs int

	// updated from scheduler when worker metrics updated
	UsedWorkerNum int

	// updated when function instance metadata changed
	CurrentConcurrencyPerWorker int

	// updated from request queue when queue length changed
	PendingRequests int
	RPS             int

	// updated from self when scaling request finished
	ScalingLatencyMs int
}

var algorithmCollection = make([]Algorithm, TypeNumber)

// Algorithm scale algorithm interface
type Algorithm interface {
	CalculateScaleUp(config *common.AutoScalerConfig, info ScaleInfo) int
	CalculateScaleDown(config *common.AutoScalerConfig, info ScaleInfo) int
}

func init() {
	registerAlgorithm(PerInstanceConcurrency, perInstanceConcurrency{})
	registerAlgorithm(RPS, rps{})
}

func registerAlgorithm(algorithmType Type, algorithm Algorithm) {
	algorithmCollection[algorithmType] = algorithm
}

// CalculateScaleUp scale up interface
func CalculateScaleUp(config *common.AutoScalerConfig, info ScaleInfo) int {
	values := make([]int, len(algorithmCollection))
	for index := 0; index < len(algorithmCollection); index++ {
		values[index] = algorithmCollection[index].CalculateScaleUp(config, info)
	}
	return min(values...)
}

// CalculateScaleDown scale down interface
func CalculateScaleDown(config *common.AutoScalerConfig, info ScaleInfo) int {
	values := make([]int, len(algorithmCollection))
	for index := 0; index < len(algorithmCollection); index++ {
		values[index] = algorithmCollection[index].CalculateScaleDown(config, info)
	}
	return min(values...)
}

func min(values ...int) int {
	minValue := values[0]
	for _, v := range values {
		if v < minValue {
			minValue = v
		}
	}
	return minValue
}
