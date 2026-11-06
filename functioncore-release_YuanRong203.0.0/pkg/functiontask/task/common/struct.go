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

package common

import (
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Method for common runtime use.
const (
	MethodInvoke = iota
	MethodGetFuture
	MethodTerminate
	MethodWait
	MethodOnComplete
	MethodSetTimeout
	MethodFilter
	MethodReadState
	MethodSaveState
	MethodGetInstance
)

// Method for task.
// start from 20, to reserve [0,20) for common runtime methods
const (
	MethodTaskInvoke = 20 + iota
	MethodTaskGetFuture
	MethodTaskStartCheckAndResolveDeadlock
	MethodTaskEndCheckAndResolveDeadlock
	MethodTaskCreateStateQueue
)

// Method for ByKey.
// start from 30, to reserve [20,30) for task methods
const (
	MethodTriggerByKeyCLI = 30 + iota
	MethodTriggerByKey
)

// Source for requests.
const (
	// SourceRuntimeToCentralFunctionTask represents request from runtime to Central functionTask
	SourceRuntimeToCentralFunctionTask = iota
	// SourceRuntimeToLocalFunctionTask represents request from runtime to a functionTask
	SourceRuntimeToLocalFunctionTask
	// SourceFunctionTaskToFunctionTask represents request from a functionTask to another functionTask
	SourceFunctionTaskToFunctionTask
)

// TypeNames represents method names for requests.
var TypeNames = map[int32]string{
	MethodInvoke:     "invoke",
	MethodGetFuture:  "get",
	MethodTerminate:  "terminate",
	MethodWait:       "wait",
	MethodOnComplete: "onComplete",
	MethodSetTimeout: "setTimeout",
	MethodFilter:     "filter",
	MethodReadState:  "readState",
	MethodSaveState:  "saveState",

	MethodTaskInvoke:                       "taskInvoke",
	MethodTaskGetFuture:                    "taskGetFuture",
	MethodTaskStartCheckAndResolveDeadlock: "startResolveDeadlock",
	MethodTaskEndCheckAndResolveDeadlock:   "endResolveDeadlock",
	MethodTaskCreateStateQueue:             "taskCrateStateQueue",

	MethodTriggerByKeyCLI: "triggerByKeyCLI",
	MethodTriggerByKey:    "triggerByKey",
}

// TypeSources represents source names for requests.
var TypeSources = []string{"runtimeToCentralFunctionTask", "runtimeToItsFunctionTask", "functionTaskToFunctionTask"}

// ErrorResponse defines error response.
type ErrorResponse = ErrorResponsePb

// Marshal marshal ErrorResponse
func (resp *ErrorResponse) Marshal() ([]byte, error) {
	return proto.Marshal(resp)
}

// Unmarshal unmarshal ErrorResponse
func (resp *ErrorResponse) Unmarshal(data []byte) error {
	return proto.Unmarshal(data, resp)
}

// SetCode sets `Code` field of `ErrorResponse`.
func (resp *ErrorResponse) SetCode(code int32) {
	resp.Code = code
}

// SetMessage sets `Message` field of `ErrorResponse`.
func (resp *ErrorResponse) SetMessage(msg string) {
	resp.Message = msg
}

// Fields returns log fields.
func (resp *ErrorResponse) Fields() []zap.Field {
	return nil
}
