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
	"math"

	"functioncore/pkg/functionbus/common"
)

// default scale algorithm
type perInstanceConcurrency struct{}

// CalculateScaleUp -
func (p perInstanceConcurrency) CalculateScaleUp(config *common.AutoScalerConfig, info ScaleInfo) int {
	if info.CurrentConcurrencyPerWorker == 0 {
		return 0
	}
	needWorkerNum := int(math.Ceil(float64(info.PendingRequests) / float64(info.CurrentConcurrencyPerWorker)))
	return min(needWorkerNum, config.StableScaleLimitPerFunction)
}

// CalculateScaleDown -
func (p perInstanceConcurrency) CalculateScaleDown(_ *common.AutoScalerConfig, info ScaleInfo) int {
	if info.CurrentConcurrencyPerWorker == 0 {
		return 0
	}
	needWorkerNum := int(math.Ceil(float64(info.CurrentRequests) / float64(info.CurrentConcurrencyPerWorker)))
	var scaleNum int
	if needWorkerNum >= info.UsedWorkerNum {
		scaleNum = 0
	} else {
		scaleNum = info.UsedWorkerNum - needWorkerNum
	}
	return scaleNum
}
