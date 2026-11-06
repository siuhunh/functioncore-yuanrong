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

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/functionbus/common"
)

func TestRpsQPS_CalculateScaleUp(t *testing.T) {

	// requests can be finished in user tolerate delay
	{
		config := &common.AutoScalerConfig{UserTolerateDelayRatio: 2, MaxUserTolerateDelayMs: 100}
		info := ScaleInfo{
			PendingRequests:             0,
			RequestLatencyMs:            10,
			CurrentConcurrencyPerWorker: 1,
			WorkerTPS:                   100,
			UsedWorkerNum:               1,
		}
		num := CalculateScaleUp(config, info)
		assert.Equal(t, 0, num)
	}
	// input scale info is invalid
	{
		config := &common.AutoScalerConfig{}
		info := ScaleInfo{
			PendingRequests:             0,
			CurrentConcurrencyPerWorker: 1,
			UsedWorkerNum:               1,
		}
		num := CalculateScaleUp(config, info)
		assert.Equal(t, 0, num)
	}
	// ignore pending requests
	{
		alg := rps{}
		config := &common.AutoScalerConfig{
			BurstScaleLimitPerFunction:  5,
			IgnorePendingRequests:       true,
			ScaleEvaluateWindowLengthMs: 1000,
			MaxEvaluateScaleLatencyMs:   5000,
		}
		info := ScaleInfo{
			PendingRequests:             1,
			CurrentConcurrencyPerWorker: 1,
			UsedWorkerNum:               1,
			RequestLatencyMs:            100,
			WorkerTPS:                   10,
			RPS:                         10,
		}
		num := alg.CalculateScaleUp(config, info)
		assert.Equal(t, 0, num)
	}
	// consider pending requests
	{
		alg := rps{}
		config := &common.AutoScalerConfig{BurstScaleLimitPerFunction: 5, ScaleEvaluateWindowLengthMs: 1000,
			MaxEvaluateScaleLatencyMs: 5000}
		info := ScaleInfo{
			PendingRequests:             1,
			CurrentConcurrencyPerWorker: 1,
			RequestLatencyMs:            100,
			UsedWorkerNum:               1,
			WorkerTPS:                   10,
			RPS:                         10,
			ScalingLatencyMs:            500,
		}
		num := alg.CalculateScaleUp(config, info)
		assert.Equal(t, 1, num)
	}
}

func TestRpsQPS_CalculateScaleDown(t *testing.T) {
	// invalid input
	{
		config := &common.AutoScalerConfig{}
		info := ScaleInfo{
			CurrentRequests:             1,
			CurrentConcurrencyPerWorker: 1,
			UsedWorkerNum:               1,
			RPS:                         10,
		}
		num := CalculateScaleDown(config, info)
		assert.Equal(t, 0, num)
	}
	{
		config := &common.AutoScalerConfig{BurstScaleLimitPerFunction: 5, ScaleEvaluateWindowLengthMs: 1000,
			MaxEvaluateScaleLatencyMs: 5000}
		info := ScaleInfo{
			PendingRequests:             1,
			CurrentConcurrencyPerWorker: 1,
			RequestLatencyMs:            100,
			UsedWorkerNum:               2,
			WorkerTPS:                   10,
			RPS:                         10,
		}
		num := CalculateScaleDown(config, info)
		assert.Equal(t, 1, num)
	}
}
