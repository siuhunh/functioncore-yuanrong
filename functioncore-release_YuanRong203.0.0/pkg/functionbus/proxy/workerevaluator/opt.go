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

// WorkerMetricsOpt -
type WorkerMetricsOpt func(*WorkerMetrics)

// WithFunctionKeyWithResource -
func WithFunctionKeyWithResource(funcKeyWithResource string) WorkerMetricsOpt {
	return func(wm *WorkerMetrics) {
		wm.FuncKeyWithResource = funcKeyWithResource
	}
}

// WithStateID -
func WithStateID(stateID string) WorkerMetricsOpt {
	return func(wm *WorkerMetrics) {
		wm.StateID = stateID
	}
}

// WithNodeIP -
func WithNodeIP(nodeIP string) WorkerMetricsOpt {
	return func(wm *WorkerMetrics) {
		wm.NodeIP = nodeIP
	}
}

// WithLocalWorker -
func WithLocalWorker(isMinInstance bool) WorkerMetricsOpt {
	return func(wm *WorkerMetrics) {
		wm.IsLocalWorker = true
		wm.IsMinInstance = isMinInstance
	}
}

// WithRemoteWorker -
func WithRemoteWorker() WorkerMetricsOpt {
	return func(wm *WorkerMetrics) {
		wm.IsLocalWorker = false
	}
}

// WithRecoveredRequestNum -
func WithRecoveredRequestNum(n int) WorkerMetricsOpt {
	return func(wm *WorkerMetrics) {
		wm.RecoveredRequestNum = n
	}
}
