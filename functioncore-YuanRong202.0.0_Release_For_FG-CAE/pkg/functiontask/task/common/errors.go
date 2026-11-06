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

// Package common defines some common struct for function task.
package common

// EOK represents no error.
const EOK = 0

// Errors for mock unit test.
const (
	Error = iota + 1
	ENoSuchFunction
	ETimeout
	EUnknown
)

// Errors for function task.
const (
	EFunctionTask = iota + 110000
	EFieldDefinition
	EUnsupported
	TaskPanicErr = 110500
	TaskRetryErr = 110600
)

// Errors for state client.
const (
	EStateClient = iota + 111000
	ESetState
	EGetState
)

// Errors for future store.
const (
	EFutureStore = iota + 112000
	ESetMetadata
	ESetValue
)

// Errors for finite state machine.
const (
	EFiniteStateMachine = iota + 113000
)

// Errors for function core.
const (
	EFunctionCore = iota + 118000
	ECreateState
)

// Errors for unacceptable uncertainty user code.
const (
	EUserCode = iota + 119000
	EPanic
	EUnacceptableUncertainty
)

// Errors for user code.
const (
	EInvalidParam          = 4040 // EInvalidParam represents invalid parameter.
	EStateIDNotFound       = 4041 // EStateIDNotFound represents no such state.
	EUnknownFutureID       = 4042 // EUnknownFutureID represents unknown future id.
	ETooManyStateInService = 4043 // ETooManyStateInService represents too many state in an service.
	// ECallStackDepthExceedsLimit represents depth of call stack exceeds maximum limit.
	ECallStackDepthExceedsLimit = 4045
)

// ScheduleInsErr represents schedule instance failed.
const ScheduleInsErr = 211400

// InitStateErr represents init state failed.
const InitStateErr = 151010

const (
	// FunctionWithNoState error code of deployment project without state
	FunctionWithNoState = 4008
)

// CodeText key is error code and value is error message
var CodeText = map[int]string{
	FunctionWithNoState: "status setting is false, cannot call with status",
}

// StateIDNotFoundMsg represents msg for state id not found error
const (
	StateIDNotFoundMsg      = "instanceID recycled or not exists"
	TaskGetFutureForwardMsg = "taskGetFuture forward to another task"
)
