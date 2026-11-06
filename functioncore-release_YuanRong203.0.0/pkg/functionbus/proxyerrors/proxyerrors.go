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

// Package proxyerrors is used for messages error
package proxyerrors

import (
	"functioncore/pkg/common/snerror"
)

// errCode of functionbus module start with "21****"
// errcodes that cannot be retried  start with "210**"
const (
	// AuthErrCode auth check for call request error
	AuthErrCode = 210401
	// AuthErr -
	AuthErr = "functionbus authentication error"

	// InternalErrCode functionbus has a unexpection error
	InternalErrCode = 210500
	// InternalErr -
	InternalErr = "functionbus internal error"

	// UnknownErrCode functionbus has a unknown error
	UnknownErrCode = 210501
	// UnknownErr -
	UnknownErr = "unknown message"
)

// errcodes that can be retried start with "211**"
const (
	// TimeoutErrCode call request timeout error
	TimeoutErrCode = 211408
	// TimeoutErr -
	TimeoutErr = "call request timeout error"

	// SendReqErrCode call request sending error
	SendReqErrCode = 211406
	// SendReqErr -
	SendReqErr = "call request sending error"

	// SendHTTPErrCode send http request error
	SendHTTPErrCode = 211402
	// SendHTTPErr -
	SendHTTPErr = "send http request error"

	// ScheduleErrCode state call request routing error
	ScheduleErrCode = 211400

	// ScheduleErr -
	ScheduleErr = "state call request routing error"

	// ClientExitErrCode function instance  is exiting （proxy side)
	ClientExitErrCode = 211503
	// ClientExitErr -
	ClientExitErr = "peer exited error"

	// ServerExitErrCode function instance is exiting (worker side)
	WorkerExitErrCode = 211504
	// ServerExitErrCode -
	WorkerExitErr = "worker exited error"

	// RecoverRuntimeErrCode recover runtime failed (worker side)
	RecoverRuntimeErrCode = 211505
	// RecoverRuntimeErr -
	RecoverRuntimeErr = "failed to recover runtime"

	// IsUpgradingCode -
	IsUpgradingCode = 211410
	// IsUpgradingErr functiontask is updating
	IsUpgradingErr = "functiontask is updating error"

	// UserFuncIsUpdatedCode -
	UserFuncIsUpdatedCode = 211411
	// UserFuncIsUpdatedErr -
	UserFuncIsUpdatedErr = "user function metadata is updated or deleted"

	RetryContextTimeoutErr = "retry context timeout"

	// WaitDirectRequestCode -
	WaitDirectRequestCode = 211100
	// WaitDirectRequestMsg -
	WaitDirectRequestMsg = "wait request finished"
)

// errorcodes that used by functionbus internal and not visible to users start with "212***"
const (
	// ForwardCode forward call request
	ForwardCode = 212303

	// UnRegisterCallBackCode build connection without callback function
	UnRegisterCallBackCode = 212501
	// UnRegisterCallBack -
	UnRegisterCallBack = "connection callback unRegister"

	// ProcessRegErr process registry message error
	ProcessRegErr = 212421
)

// errorcodes that caused by proxy
const (
	// OverloadRequestsCode proxy requests reach limit
	OverloadRequestsCode = 213429
	// OverloadRequestsErr -
	OverloadRequestsErr = "the number of requests reaches the limit of proxy"
	// TrafficLimitEffective is the error code for traffic limitation
	TrafficLimitEffective = 4021
)

// errorcodes that caused by bus client start with "214**"
const (
	// WorkerBrokerCreateErrCode failed to create worker broker
	WorkerBrokerCreateErrCode = 214500
	// WorkerBrokerCreateErrMsg -
	WorkerBrokerCreateErrMsg = "create worker broker error"
	// ScaleUpRequestErrCode failed to send scale up request to worker-manager
	ScaleUpRequestErrCode = 214501
	// ScaleUpRequestErrMsg -
	ScaleUpRequestErrMsg = "send scale up request to worker-manager error"
	// ScaleDownRequestErrCode failed to send scale up request to worker-manager
	ScaleDownRequestErrCode = 214502
	// CAEScheduleRequestErrCode failed to schedule CAE request
	CAEScheduleRequestErrCode = 214503
	// VPARegisterFunctionInstanceErrCode failed to register function instance in bus or in SLO agent
	VPARegisterFunctionInstanceErrCode = 214504
	// VPARecoverFunctionInstanceCacheErrCode failed to recover function instance status after SLO agent restarts
	VPARecoverFunctionInstanceCacheErrCode = 214505
	// VPASyncResourceViewErrCode failed to sync resource view from SLO agent to worker manager
	VPASyncResourceViewErrCode = 214506
	// VPAFuncInsNotReadyErrCode not finished to sync function instance status from ETCD
	VPAFuncInsNotReadyErrCode = 214507
	// CAEScheduleRequestErrMsg -
	CAEScheduleRequestErrMsg = "scheduler CAE request failed"
	// ScaleDownRequestErrMsg -
	ScaleDownRequestErrMsg = "send scale down request to worker-manager error"
	// HeavyLoadCode indicate the server's memory usage reaches threshold
	HeavyLoadCode = 200500
	// HeavyLoadErr is the error message of HeavyLoadCode
	HeavyLoadErr = "http server is under heavy load"
	// MicroserviceCircuitBreakErrCode microservice instance is circuit break
	MicroserviceCircuitBreakErrCode = 211506
	// MicroserviceCircuitBreakErrMsg -
	MicroserviceCircuitBreakErrMsg = "enable Circuit Breaker, No Available Instances"
)

// error code and message for cancel
var (
	RequestCanceledCode  = uint32(snerror.RequestCanceled)
	RequestCanceledError = snerror.ErrText(snerror.RequestCanceled)
)

const (
	// StateOKCode -
	StateOKCode = 0
	// StateAcceptedCode -
	StateAcceptedCode = 2
)

// ErrorInfo -
type ErrorInfo struct {
	ErrorCode uint32
	ErrorInfo string
}
