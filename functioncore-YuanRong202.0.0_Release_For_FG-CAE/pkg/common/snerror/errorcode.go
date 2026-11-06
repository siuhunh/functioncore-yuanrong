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

// Package snerror is basic information contained in the SN error.
package snerror

import "functioncore/pkg/common/api/schedulerapi"

// The value of commonError must be a 6-digit integer starting with 33.
const (
	// InternalError if the value is 331404, try again.
	InternalError      = 330404
	InternalErrorRetry = 331404
	InternalErrorMsg   = "internal system error"
)

// user error code
const (
	// FunctionEntryNotFound function entry not found
	FunctionEntryNotFound = 4001
	// FunctionExceptionCode code of function exception
	FunctionExceptionCode = 4002
	// FunctionNotStateful -
	FunctionNotStateful = 4006
	// IllegalArgumentErrorCode code of illegal argument
	IllegalArgumentErrorCode = 4008
	// StreamExceedLimitErrorCode code of illegal argument
	StreamExceedLimitErrorCode = 4009
	// RuntimeInvokeTimeoutCode code of invoking runtime timed out
	RuntimeInvokeTimeoutCode = 4010
	// RuntimeLoadTimeoutCode code of load function timed out
	RuntimeLoadTimeoutCode = 4012
	// RequestAuthCheckFail -
	RequestAuthCheckFail = 4013
	// NoSuchFunction error code of function is not found
	NoSuchFunction = 4024
	// CreateStateByStatelessFunction error code of creating state using stateless functions
	CreateStateByStatelessFunction = 4025
	// NoSuchInstanceName error code of querying instanceID by using instanceName is not exist
	NoSuchInstanceName = 4026
	// InstanceNameExist error code of create instanceID by using instanceName but the instanceName is already used
	InstanceNameExist = 4027
	// TooManyDependencyFiles function
	TooManyDependencyFiles = 4028
	// InvokeStatefulFunctionWithoutStateID error code when user invoke a stateful function without stateID
	InvokeStatefulFunctionWithoutStateID = 4029

	// MaxRequestBodySizeErr -
	MaxRequestBodySizeErr = 4140
	// TenantStateNumExceedLimit -
	TenantStateNumExceedLimit = 4143

	// FunctionInitFailCode code of function init fail
	FunctionInitFailCode = 4201
	// RuntimeBootstrapNotFound code of runtime bootstrap not found
	RuntimeBootstrapNotFound = 4202
	// RuntimeProcessExit code of runtime exiting
	RuntimeProcessExit = 4204
	// RuntimeMemoryExceedLimit runtime has consumed too much memory
	RuntimeMemoryExceedLimit = 4205
	// RuntimeMountErrorCode error code of mount failure
	RuntimeMountErrorCode = 4206
	// DiskUsageExceed disk usage exceed code
	DiskUsageExceed = 4207
	// RequestCanceled -
	RequestCanceled = 4208
	// RuntimeUnHealthy Runtime is UnHealthy
	RuntimeUnHealthy = 4209
	// RuntimeInitTimeoutCode code of initialing runtime timed out
	RuntimeInitTimeoutCode = 4211
	// VpcNoOperationalPermissions vpc has no operational permissions
	VpcNoOperationalPermissions = 4212
	// FunctionRestoreHookFailCode -
	FunctionRestoreHookFailCode = 4213
	// RuntimeRestoreHookTimeoutCode -
	RuntimeRestoreHookTimeoutCode = 4214
	// BridgeManagerFailCode error code of bridge manager plugin failure
	BridgeManagerFailCode = 4215
	// ExtensionShellNotFound error code of extension shell not found
	ExtensionShellNotFound = 4216
	// ExtensionExecFailed error code of extension exec failed
	ExtensionExecFailed = 4217
	// TooManySpecializedFails marked that there are too many specialized fails
	TooManySpecializedFails = 4218
	// VPCNotFound error code of VPC not found
	VPCNotFound = 4219
	// RuntimeDirForbidden runtime dir is forbidden
	RuntimeDirForbidden = 4220
	// INodeUsageExceed - inode usage exceed code
	INodeUsageExceed = 4221
	// VPCXRoleNotFound vcp xrole not func
	VPCXRoleNotFound = 4222
	// StatefulFunctionReloading - stateful function instance is reloading
	StatefulFunctionReloading = 4223
	// WebSocketDownStreamConnError - cannot connect websocket server of user
	WebSocketDownStreamConnError = 4224
	// ZipFormatInvalid zip code package format invalid
	ZipFormatInvalid = 4225
	// UnsupportedRuntimeType unsupported runtime type
	UnsupportedRuntimeType = 4226
	// WebSocketNotEnableError websocket is not enable
	WebSocketNotEnableError = 4227
	// MCPSessionInstanceInvalid -
	MCPSessionInstanceInvalid = 4228
	// KmsCipherAbnormalCode -
	KmsCipherAbnormalCode = 4229
)

// instance manager error code
const (
	// ClusterOverloadCode cluster is overload and unavailable now
	ClusterOverloadCode = 150430
	// GettingPodErrorCode getting pod error code
	GettingPodErrorCode = 150431
	// VIPClusterOverloadCode cluster has no available resource
	VIPClusterOverloadCode = 150510
	// FuncMetaNotFound function meta not found, this error occurs only when the internal service is abnormal.
	FuncMetaNotFound = 150424
	// ReachMaxInstancesCode reach function max instances
	ReachMaxInstancesCode = 150429
	// ReachMaxOnDemandInstancesPerTenant reach tenant max on-demand instances
	ReachMaxOnDemandInstancesPerTenant = 150432
	// ReachMaxReversedInstancesPerTenant reach tenant max reversed instances
	ReachMaxReversedInstancesPerTenant = 150433
	// FunctionIsDisabled function is disabled
	FunctionIsDisabled = 150434
	// RefreshSilentFunc waiting for silent function to refresh, retry required
	RefreshSilentFunc = 150435
	// NotEnoughNIC marked that there were not enough network cards
	NotEnoughNIC = 150436
	// InternalVPCError internal vpc error
	InternalVPCError = 150437
	// InsufficientEphemeralStorage marked that ephemeral storage is insufficient
	InsufficientEphemeralStorage = 150438
	// CancelGeneralizePod user update function metadata to cancel generalize pod while generalizing is not finished
	CancelGeneralizePod = 150439
	// CancelCheckpoint cancel checkpoint when the format of snapshot data  is abnormal
	CancelCheckpoint = 150440
	// StreamConnException stream connection exception
	StreamConnException = 150450
)

// worker error code
const (
	// WorkerInternalErrorCode code of unexpected error in worker
	WorkerInternalErrorCode = 161900
	// ReadingCodeTimeoutCode reading code package timed out
	ReadingCodeTimeoutCode = 161901
	// CallFunctionErrorCode code of calling other function error
	CallFunctionErrorCode = 161902
	// FuncInsExceptionCode function instance exception
	FuncInsExceptionCode = 161903
	// CheckSumErrorCode code of check sum error
	CheckSumErrorCode = 161904
	// DownLoadCodeErrorCode code of download code error
	DownLoadCodeErrorCode = 161905
	// RPCClientEmptyErrorCode code of when rpc client is nil
	RPCClientEmptyErrorCode = 161906
	// RuntimeManagerProcessExited runtime-manager process exited code
	RuntimeManagerProcessExited = 161907
	// WorkerPingVpcGatewayError code of worker ping vpc gateway error
	WorkerPingVpcGatewayError = 161908
	// UploadSnapshotError code of worker upload snapshot error
	UploadSnapshotErrorCode = 161909
	// RestoreDeadErrorCode code of restore is dead
	RestoreDeadErrorCode = 161910
	// ContentInconsistentErrorCode code of worker content inconsistent error
	ContentInconsistentErrorCode = 161911
	// WebSocketUpStreamConnError cannot connect websocket server of control plane
	WebSocketUpStreamConnError = 161912
	// WebSocketRequestInternalError internal error when processing websocket related request
	WebSocketRequestInternalError = 161913
)

const (
	// RequestAcceptedCode code of request accepted
	RequestAcceptedCode = 2
)

// proxy internal error codes which suggests to retry in cluster
const (
	// ClientExitErrCode function instance  is exiting (proxy side)
	ClientExitErrCode = 211503

	// WorkerExitErrCode function instance is exiting (worker side)
	WorkerExitErrCode = 211504

	// UserFuncIsUpdatedCode -
	UserFuncIsUpdatedCode = 211411
)

// proxy internal response success code
const (
	InnerResponseSuccessCode = 0
)

const (
	// MinUserCode min user code
	MinUserCode = 4000
	// MinSysCode min system code
	MinSysCode = 10000
)

var (
	userErrorMsg = map[int]string{
		FunctionEntryNotFound:          "function entry exception",                                        // 4001
		FunctionExceptionCode:          "function invocation exception",                                   // 4002
		FunctionNotStateful:            "function name has no service ID",                                 // 4006
		IllegalArgumentErrorCode:       "response body size %d exceeds the limit of %d",                   // 4008
		StreamExceedLimitErrorCode:     "send stream exceed limit",                                        // 4009
		RuntimeInvokeTimeoutCode:       "call invoke timeout %s",                                          // 4010
		RuntimeInitTimeoutCode:         "runtime initialization timed out after %s",                       // 4011
		RuntimeLoadTimeoutCode:         "load function timed out after %s",                                // 4012
		RequestAuthCheckFail:           "failed to check auth of the request",                             // 4013
		NoSuchFunction:                 "function metadata is not found",                                  // 4024
		CreateStateByStatelessFunction: "can not create a state using the stateless function",             // 4025
		NoSuchInstanceName:             "the instance name (%s) or instance id (%s) does not exist",       // 4026
		InstanceNameExist:              "instanceID cannot be created repeatedly",                         // 4027
		TooManyDependencyFiles:         "amount of files in package is over %d ,exceeding the limit",      // 4028
		FunctionInitFailCode:           "function initialization failed",                                  // 4201
		RuntimeBootstrapNotFound:       "runtime bootstrap file is not found",                             // 4202
		RuntimeProcessExit:             "runtime process is exited",                                       // 4204
		RuntimeMemoryExceedLimit:       "runtime memory limit exceeded",                                   // 4205
		RuntimeMountErrorCode:          "failed to mount volumes for function",                            // 4206
		DiskUsageExceed:                "disk usage exceed limit",                                         // 4207
		RequestCanceled:                "function invocation canceled",                                    // 4208
		RuntimeUnHealthy:               "runtime is unHealthy",                                            // 4209
		MaxRequestBodySizeErr:          "the size of request body is beyond maximum:%d, body size:%d",     // 4140
		TenantStateNumExceedLimit:      "the number of states exceeds the limit: %d",                      // 4143
		FunctionRestoreHookFailCode:    "function restore failed",                                         // 4213
		RuntimeRestoreHookTimeoutCode:  "runtime restore timed out after %s",                              // 4214
		BridgeManagerFailCode:          "bridge manager internal error: %s",                               // 4215
		ExtensionShellNotFound:         "extension shell file is not found",                               // 4216
		ExtensionExecFailed:            "extension exec failed, error: %s",                                // 4217
		TooManySpecializedFails:        "the function fails to be started for multiple times",             // 4218
		VPCNotFound:                    "VPC item not found",                                              // 4219
		RuntimeDirForbidden:            "runtime dir /opt/function/runtime and /home/snuser is forbidden", // 4220
		INodeUsageExceed:               "Inode usage exceed limit",                                        // 4221
		VPCXRoleNotFound:               "VPC can't find xrole",                                            // 4222
		StatefulFunctionReloading:      "Stateful function instance is reloading when scaleUp",            // 4223
		WebSocketDownStreamConnError:   "cannot connect user's websocket server",                          // 4224
		UnsupportedRuntimeType:         "unsupported runtime type %s",                                     // 4226
		WebSocketNotEnableError:        "function not enable websocket",                                   // 4227
		MCPSessionInstanceInvalid:      "mcp session instance is invalid",                                 // 4228
		KmsCipherAbnormalCode:          "kms cipher is not useful, please check",                          // 4229
	}
	systemErrorMsg = map[int]string{
		FuncMetaNotFound:              "function metadata not found",                                          // 150424
		ClusterOverloadCode:           "the cluster is overload and unavailable now",                          // 150430
		GettingPodErrorCode:           "getting pod from pool error",                                          // 150431
		RefreshSilentFunc:             "waiting for refreshing the silent function",                           // 150434
		NotEnoughNIC:                  "not enough network cards",                                             // 150436
		InsufficientEphemeralStorage:  "insufficient ephemeral storage",                                       // 150438
		VIPClusterOverloadCode:        "the VIP node is overload and unavailable now",                         // 150510
		StreamConnException:           "streaming data exception",                                             // 150450
		WorkerInternalErrorCode:       "worker internal error: %s",                                            // 161900
		ReadingCodeTimeoutCode:        "reading the function code package timed out",                          // 161901
		CallFunctionErrorCode:         "call other function error",                                            // 161902
		FuncInsExceptionCode:          "function instance exception",                                          // 161903
		CheckSumErrorCode:             "check file sum error: %s",                                             // 161904
		DownLoadCodeErrorCode:         "download code from obs error: %s, bucket: %s, object: %s, layer: %s",  // 161905
		RPCClientEmptyErrorCode:       "rpc client is nil",                                                    // 161906
		RuntimeManagerProcessExited:   "runtime-manager process exited",                                       // 161907
		WorkerPingVpcGatewayError:     "ping vpc gateway error",                                               // 161908
		UploadSnapshotErrorCode:       "upload snapshot to obs err: %s, bucket: %s, object: %s, fileName: %s", // 161909
		RestoreDeadErrorCode:          "function snapshot restore is dead",                                    // 161910
		WebSocketUpStreamConnError:    "cannot connect websocket server of rdispatcher",                       // 161912
		WebSocketRequestInternalError: "websocket processing internal error",                                  // 161913
	}
)

// ErrText error text
func ErrText(code int) string {
	if code > MinUserCode && code < MinSysCode {
		return userErrorMsg[code]
	}
	return systemErrorMsg[code]
}

// IsUserErr true if a user error occurs
func IsUserErr(code uint32) bool {
	return code > MinUserCode && code < MinSysCode
}

const (
	// CreateSchedulerErrorCode code of create scheduler error
	CreateSchedulerErrorCode = 170001
	// SchedulerInternalErrorCode code of scheduler internal error
	SchedulerInternalErrorCode = 170002
	// NoIdleInstanceErrorCode code of no idle instance error
	NoIdleInstanceErrorCode = 170003
	// InstanceNotExistErrorCode code of instance not exist error
	InstanceNotExistErrorCode = 170004
	// InstanceInternalError code of instance internal error
	InstanceInternalError = 170005
	// GetPlaceHolderInstanceError code of waiting for scaling out instance error
	GetPlaceHolderInstanceError = 170006
	// ScalingOutInstanceError code of waiting for scaling out instance time out
	ScalingOutInstanceError = 170007
	// ScaleDownRequestErrCode failed to send scale up request to worker-manager
	ScaleDownRequestErrCode = 170008
	// WebsocketConnectionBuildFailed worker can not build connection with rdispatcher
	WebsocketConnectionBuildFailed = 170009
	// MoreThanOneInstanceErrorCode -
	MoreThanOneInstanceErrorCode = 170010
	// RemoteDebugInsNotReadyErrorCode -
	RemoteDebugInsNotReadyErrorCode = 170011
)

// enum of InnerUserError; In some case, we need handle user error in different way, add it by InnerUserErrBase to
// distinguish it with origin user error code
const (
	InnerUserErrBase            = 50_0000
	InnerRuntimeInitTimeoutCode = InnerUserErrBase + RuntimeInitTimeoutCode
)

const (
	// FunctionInitFailErrPrefix prefix of function init fail
	FunctionInitFailErrPrefix = "function initialization exception, error: "
	// FunctionRestoreFailErrPrefix -
	FunctionRestoreFailErrPrefix = "function restore exception, error: "
)

var (
	// SchedulerInternalError faas scheduler internal error
	SchedulerInternalError = schedulerapi.Error{
		ErrorCode:    SchedulerInternalErrorCode,
		ErrorMessage: "faas scheduler internal error",
	}
	// NoSchedulerError function scheduler not found in faas scheduler
	NoSchedulerError = schedulerapi.Error{
		ErrorCode:    CreateSchedulerErrorCode,
		ErrorMessage: "function scheduler not found in faas scheduler",
	}
	// ScalingOutFunctionInstanceError failed to scale out function instance
	ScalingOutFunctionInstanceError = schedulerapi.Error{
		ErrorCode:    ScalingOutInstanceError,
		ErrorMessage: "failed to scale out function instance",
	}
	// WebsocketConnectionBuildError failed to build websocket connection between worker and control plane
	WebsocketConnectionBuildError = schedulerapi.Error{
		ErrorCode:    WebsocketConnectionBuildFailed,
		ErrorMessage: "failed to build websocket connection between worker and control plane",
	}
)
