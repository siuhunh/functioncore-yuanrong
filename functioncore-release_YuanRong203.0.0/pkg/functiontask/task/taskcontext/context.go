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

// Package taskcontext describes context interface for function task.
package taskcontext

import (
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/functiontask/task/common"
)

// Context contains some common fields used in APIs.
type Context interface {
	LoggerContext
	ReqIDListContext
	StateQueueMapContext
	FutureStoreContext
	ExtFutureStoreContext
	ServiceIDContext

	TraceID() string
	TenantID() string

	IsNewStateKeyScenario() bool

	CallTask(method int32, req RequestWithStateID, resp Serializable)
	CreateState(funcName string, stateID string) (string, string, common.TaskError)
	InvokeConfig(version string, callKernel bool, futureID string, requestAttributes map[string]int32) interface{}

	SetResult(resultStruct Serializable)
	SetResultWithLog(resultStruct Serializable, log string)
	SetResultWithCode(code int32, resultStruct interface{}, log string)

	SetIsGetInstanceScenario(flag bool)
	IsGetInstanceScenario() bool

	IsSameNode(stateID string) bool

	Clone() Context
	GetFuncStateKey(funcName string, clientInstance bridgetypes.ClientInstance) (string, common.TaskError)
	Release(funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError)
}

// NewState creates a new state id.
const NewState = "new-state"
