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

type rps struct{}

func (r rps) reachCondition(config *common.AutoScalerConfig, info ScaleInfo) bool {
	return info.WorkerTPS > 0 && info.RequestLatencyMs <= config.MaxEvaluateScaleLatencyMs &&
		config.ScaleEvaluateWindowLengthMs != 0
}

// CalculateScaleUp -
func (r rps) CalculateScaleUp(config *common.AutoScalerConfig, info ScaleInfo) int {
	if !r.reachCondition(config, info) {
		return invalidScalingNum
	}
	userTolerateTimeMs := min(int(float32(info.RequestLatencyMs)*config.UserTolerateDelayRatio),
		config.MaxUserTolerateDelayMs)
	// request can be scheduled in user tolerate time
	if reservedReqNum := info.PendingRequests - info.UsedWorkerNum*info.WorkerTPS*userTolerateTimeMs/
		msToS; reservedReqNum < 0 {
		return 0
	}

	// scale workers to ensure outComing faster than inComing for requests
	if config.IgnorePendingRequests {
		return min(int(math.Ceil(float64(info.RPS)/
			float64(info.WorkerTPS)))-info.UsedWorkerNum, config.BurstScaleLimitPerFunction)
	}

	// after ScaleEvaluateWindowLengthMs and scaleLatencyMs, outComingRequests > pendingRequestNum + InComingRequests
	// and use availableWorkerTime * workerTPS as outComingRequests
	// and availableWorkerTime contains currentWorkersTime and scalingWorkersTime except for scaling latency
	sumEvaluateWindowLengthMs := config.ScaleEvaluateWindowLengthMs + info.ScalingLatencyMs
	inComingRequests := float64(info.RPS*sumEvaluateWindowLengthMs) / msToS
	availableWorkerTimeMsForCurrentWorker := float64(info.UsedWorkerNum * sumEvaluateWindowLengthMs)
	needWorkerTimeMs := (float64(info.PendingRequests) + inComingRequests) * msToS / float64(info.WorkerTPS)
	if availableWorkerTimeMsForScalingWorker := needWorkerTimeMs -
		availableWorkerTimeMsForCurrentWorker; availableWorkerTimeMsForScalingWorker > 0 {
		return min(int(math.Ceil(availableWorkerTimeMsForScalingWorker/
			float64(config.ScaleEvaluateWindowLengthMs))), config.BurstScaleLimitPerFunction)
	}
	return 0
}

// CalculateScaleDown -
func (r rps) CalculateScaleDown(config *common.AutoScalerConfig, info ScaleInfo) int {
	if !r.reachCondition(config, info) {
		return invalidScalingNum
	}
	needWorkerNumByRPS := int(math.Ceil(float64(info.RPS) / float64(info.WorkerTPS)))
	return info.UsedWorkerNum - needWorkerNumByRPS
}
