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

// Package rpc call rpc method in server
package rpc

// LoadRequest worker ===> runtime
type LoadRequest struct {
	// file name of Entry function
	Entry string `json:"entry"`
	// Entry function name
	FuncName string `json:"funcName"`
	// env of user function
	FuncEnvs []string `json:"funcEnvs"`
	// env of runtime
	RuntimeEnvs []string `json:"runtimeEnvs"`
	// info of user storage, used in financial scenarios
	StorageInfo []string `json:"storageInfo"`
	// instance load function name
	InstanceFuncName string `json:"instanceFuncName"`
	TraceID          string `json:"traceID"`
}

// LoadResponse runtime ===> worker
type LoadResponse struct {
	Error *Error `json:"error"`
}

// Error using in rpc
type Error struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

// InitializerRequest is request to invoke initialize function in runtime
// worker ===> runtime
type InitializerRequest struct {
	// Unique ID of invocation
	TraceID string            `json:"traceID"`
	Extra   map[string]string `json:"extra"`
}

// InitializerResponse is response of function initializer in runtime
// runtime ===> worker
type InitializerResponse struct {
	Error *Error `json:"error"`
}

// InvokeRequest is request to invoke function in runtime
// worker ===> runtime
type InvokeRequest struct {
	// Unique ID of invocation
	InvokeID string `json:"invokeID"`
	// Unique ID of trace chain
	TraceID string `json:"traceID"`
	// extra info of invocation
	Extra map[string]string `json:"extra"`
	// futureID of this invoke request
	FutureID string `json:"futureID"`
}

// InvokeResponse is response of function invocation in runtime
// runtime ===> worker
type InvokeResponse struct {
	Error *Error `json:"error"`
}

// HealthResponse runtime ===> worker
type HealthResponse struct {
	// "OK" if health
	Status string `json:"status"`
}

// FunctionLog runtime ===> worker
type FunctionLog struct {
	Level     string            `json:"level"`
	Timestamp string            `json:"timestamp"`
	Content   string            `json:"content"`
	InvokeID  string            `json:"invokeID"`
	TraceID   string            `json:"traceID"`
	With      map[string]string `json:"with"`
}

// PreStopRequest is request to invoke preStop function in runtime
// worker ===> runtime
type PreStopRequest struct {
	// Unique ID of invocation
	TraceID string            `json:"traceID"`
	Extra   map[string]string `json:"extra"`
}

// PreStopResponse is response of function preStop in runtime
// runtime ===> worker
type PreStopResponse struct {
	Error *Error `json:"error"`
}
