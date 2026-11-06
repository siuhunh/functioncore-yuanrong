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

// Package common implements common constant
package common

import "regexp"

// Error code and message of Call() or CallOnDemand()
const (
	FuncNameInvalidErrCode    = 220001
	FuncNameInvalidErrMsg     = "invalid function name"
	ArgsLengthInvalidErrCode  = 220002
	ArgsLengthInvalidErrMsg   = "the length of args is too large"
	TimeoutInvalidErrCode     = 220003
	TimeoutInvalidErrMsg      = "timeout is invalid"
	DeployNodeFunctionErrCode = 220005
	DeployNodeFunctionErrMsg  = "deploy function error, system internal error"
	InternalErrCode           = 220500
	InternalErrMsg            = "internal error"
	StateIDEmptyErrCode       = 220006
	StateIDEmptyErrMsg        = "stateID is empty"
	StateClientErrCode        = 220007
	StateClientErrMsg         = "state client is nil"
	HTTPClientErrMsg          = "http client is nil"
	DeployWorkerRetryErrMsg   = "callInstance retry times exceed maxRetryTimes"
	InvokeRateLimitErrCode    = 220008
	InvokeRateLimitErrMsg     = "frequency of function invocation exceeded limit"
	BridgeFuncParamErrCode    = 220009
	BridgeFuncParamErrMsg     = "param clientInstance.FuncInfo is nil"

	Tail = "Tail"

	funcNameReg      = "^[a-zA-Z][a-zA-Z0-9._-]{0,128}[a-zA-Z0-9]$|^[a-zA-Z]$"
	funcNameAliasReg = "^([a-zA-Z][a-zA-Z0-9_-]{0,128}[a-zA-Z0-9]|[a-zA-Z])[:][a-zA-Z0-9-$]{1,32}$"

	RequestAttributesLayer  = "requestLayer"
	RequestAttributesMemory = "memory"
)

var (
	// FuncNameMatch define funcName regex
	FuncNameMatch = regexp.MustCompile(funcNameReg)
	// FuncNameAliasMatch define funcNameAlias regex
	FuncNameAliasMatch = regexp.MustCompile(funcNameAliasReg)
)
