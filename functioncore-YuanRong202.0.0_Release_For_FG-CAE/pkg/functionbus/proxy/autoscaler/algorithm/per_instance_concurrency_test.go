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

func TestPerInstanceConcurrency_CalculateScaleUp(t *testing.T) {
	alg := perInstanceConcurrency{}
	// invalid input
	{
		config := &common.AutoScalerConfig{StableScaleLimitPerFunction: 1}
		info := ScaleInfo{
			PendingRequests: 1,
		}
		num := alg.CalculateScaleUp(config, info)
		assert.Equal(t, 0, num)
	}
	// scale 1
	{
		config := &common.AutoScalerConfig{StableScaleLimitPerFunction: 1}
		info := ScaleInfo{
			PendingRequests:             1,
			CurrentConcurrencyPerWorker: 10,
		}
		num := alg.CalculateScaleUp(config, info)
		assert.Equal(t, 1, num)
	}
	// more than scale limit
	{
		config := &common.AutoScalerConfig{StableScaleLimitPerFunction: 1}
		info := ScaleInfo{
			PendingRequests:             11,
			CurrentConcurrencyPerWorker: 10,
		}
		num := alg.CalculateScaleUp(config, info)
		assert.Equal(t, 1, num)
	}
}

func TestPerInstanceConcurrency_CalculateScaleDown(t *testing.T) {
	alg := perInstanceConcurrency{}
	// invalid input
	{
		config := &common.AutoScalerConfig{}
		info := ScaleInfo{
			PendingRequests: 1,
		}
		num := alg.CalculateScaleDown(config, info)
		assert.Equal(t, 0, num)
	}
	// no need scale down
	{
		config := &common.AutoScalerConfig{}
		info := ScaleInfo{
			CurrentRequests:             1,
			CurrentConcurrencyPerWorker: 10,
			UsedWorkerNum:               1,
		}
		num := alg.CalculateScaleDown(config, info)
		assert.Equal(t, 0, num)
	}
	// need scale down
	{
		config := &common.AutoScalerConfig{}
		info := ScaleInfo{
			CurrentRequests:             1,
			CurrentConcurrencyPerWorker: 10,
			UsedWorkerNum:               2,
		}
		num := alg.CalculateScaleDown(config, info)
		assert.Equal(t, 1, num)
	}
}
