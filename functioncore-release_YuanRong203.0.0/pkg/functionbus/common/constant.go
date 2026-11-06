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

// Package common define const
package common

import "time"

const (
	// WatchPathPrefixWithWorkers is a workers path prefix
	WatchPathPrefixWithWorkers = "/sn/workers/business/yrk/tenant"
	// WatchPathSchedulerPrefix faas-scheduler registry path prefix
	WatchPathSchedulerPrefix = "/sn/faas-scheduler/instances"
	// WatchPathPrefixWithStateWorkers is a stateworkers path prefix
	WatchPathPrefixWithStateWorkers = "/sn/stateworkers/business/yrk/tenant"
	// WatchPathPrefixWithAliase is a aliase path prefix
	WatchPathPrefixWithAliase = "/sn/aliases/business/yrk/tenant"
	// WatchPathPrefixWithFunction is a function path perfix
	WatchPathPrefixWithFunction = "/sn/functions/business/yrk/tenant"
	// WatchPathPrefixWithFunctionCluster function available cluster key prefix
	WatchPathPrefixWithFunctionCluster = "/sn/function/available/clusters"
	// WatchPathPrefixWithFrontendInstance cluster frontend instance key prefix
	WatchPathPrefixWithFrontendInstance = "/sn/frontend/instances"
	// WatchPathPrefixWithCancel -
	WatchPathPrefixWithCancel = "/sn/cancel"
	// QOSEtcdPrefix is the etcd prefix for tenant qos limit
	QOSEtcdPrefix = "/sn/qos/business/yrk"
	// ConfigPath -
	ConfigPath = "/home/sn/config/conf.json"
	// TaskTenantID task tenant ID
	TaskTenantID = "0"
	// BusProxyTenantID bus proxy tenant ID
	BusProxyTenantID = "1"
	// MinPort is the lower limit of port
	MinPort = 0
	// MaxPort is the upper limit of port
	MaxPort = 65535
	// URNLen "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:Function:1"
	URNLen = 7
	// URNTenantPosition i1fe539427b24702acc11fbb4e134e17
	URNTenantPosition = 3
	// URNFuncNamePosition Function
	URNFuncNamePosition = 5
	// URNFuncVersionPosition 1
	URNFuncVersionPosition = 6
	// VersionLatest is the default version
	VersionLatest = "$latest"
	// InternalScaleBusinessID is business ID used to scale up/down worker
	InternalScaleBusinessID = "yrk"

	// ProxyAppID -
	ProxyAppID = "busproxy"
	// ProxyReqPrefix proxy request prefix
	ProxyReqPrefix = "proxy-"
	// ProxyBackPressurePrefix prefix of backpressure request forwarded by proxy
	ProxyBackPressurePrefix = ProxyReqPrefix + "from-"
	// DelInstanceEvent del instance
	DelInstanceEvent = "Instance-Del"
	// DelStateIDEvent del state state id
	DelStateIDEvent = "StateID-Del"
	// DelStateIDEventStateQueue del state state id to delete state queue
	DelStateIDEventStateQueue = "StateID-Del-StateQueue"
	// DelFunctionEvent del function
	DelFunctionEvent = "Function-Del"
	// UpdFunctionEvent update function
	UpdFunctionEvent = "Function-Upd"

	// DefaultFunctionName default function name
	DefaultFunctionName = "bus-proxy"
	// DefaultRegisterTimeout default register timeout
	DefaultRegisterTimeout = time.Duration(5) * time.Second
	// DefaultRetryTimes default retry times
	DefaultRetryTimes = 3
	// DefaultRetryInterval default retry interval
	DefaultRetryInterval = time.Duration(100) * time.Millisecond
	// DefaultConcurrency pending calls limit
	DefaultConcurrency = 20000
	// DefaultDialTimeout default dial timeout
	DefaultDialTimeout = 5 * time.Second
	// DefaultNotifyRemoteTimeout default notify remote timeout
	DefaultNotifyRemoteTimeout = 5 * time.Second
	// DefaultRecoverTimeout default recover timeout
	DefaultRecoverTimeout = 5 * time.Second

	// RemoteDisable represent remote worker status
	RemoteDisable = "remoteDisable"
	// RemoteEnable represent remote worker status
	RemoteEnable = "remoteEnable"
	// RemoteNotify represent remote worker status
	RemoteNotify = "remoteNotify"

	defaultIdleWorkerTimeout = 60

	defaultLowerMemoryPercent      = 0.6
	defaultHighMemoryPercent       = 0.8
	defaultStatefulHighMemPercent  = 0.85
	defaultMemoryRefreshInterval   = 50
	defaultBodyThreshold           = 50000
	defaultRequestMemoryEvaluator  = 2.5
	defaultResponseMemoryEvaluator = 3.0

	// for request latency more than defaultEvaluatedMaxLatencyMs, no need to use optimized scale algorithm
	defaultEvaluatedMaxLatencyMs = 2000
	// max user tolerate delay to schedule, default with no delay
	defaultMaxUserTolerateDelayMs = 1000
	// default max scale latency value for user function cold start time, use aggregation data after enough sample size
	defaultMaxEvaluatedScaleLatencyMs = 500
	// evaluate window length in scale algorithm
	defaultScaleEvaluateWindowLengthMs = 1000
	// function with no metrics, scale step limit
	defaultStableScaleLimitPerFunction = 5
	// function with metrics, scale step limit
	defaultBurstScaleLimitPerFunction = 10

	// request queue timeout with instance
	defaultWithInstanceQueueTimeout = 20
	// request queue timeout with max instance
	defaultMaxInstanceQueueTimeout = 10

	defaultScaleCalculationIntervalMs = 1000

	defaultRouterEtcdLeaseTimeoutSec = 600
	defaultLeaseDetectInterval       = 60

	defaultBreakBalanceQueueLengthLimit = 20

	defaultMaxRetryTime = 5

	defaultMaxWorkerDelay = 10

	// DynamicKeepAliveTimeRedisKey redis key for adjusting keep-alive time dynamically
	DynamicKeepAliveTimeRedisKey = "FunctionGraph:DynamicKeepAliveTime"
	// LastUpdateTimeKey time of updating keepalive metrics
	LastUpdateTimeKey = "lastUpdateTime"
	// DynamicKeepAliveTimeValidTimeWindow if the downloaded data's update time is generated one hour ago
	// it will be seen as outdated and default keep alive time will be used for all worker
	DynamicKeepAliveTimeValidTimeWindow = 60 * 60
	// DynamicKeepAliveTimeDefaultStartTime used for cron expression to retrieve data from redis
	DynamicKeepAliveTimeDefaultStartTime = 15
	// DynamicKeepAliveTimeCronSuffix used for generating cron expression
	DynamicKeepAliveTimeCronSuffix = " * * * *"
	// DynamicKeepAliveTimeDefaultRetry retry times for read metrics from redis
	DynamicKeepAliveTimeDefaultRetry = 20
	// DynamicKeepAliveTimeHour -
	DynamicKeepAliveTimeHour = 60
)

const (
	// RuntimeStageInitialized is runtime stage of initialized
	RuntimeStageInitialized = "initialized"
)
