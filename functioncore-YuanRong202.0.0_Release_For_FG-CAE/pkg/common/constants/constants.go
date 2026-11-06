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

// Package constants implements vars of all
package constants

import (
	"os"
	"strconv"
	"strings"
)

const (
	// ZoneKey zone key
	ZoneKey = "KUBERNETES_IO_AVAILABLEZONE"
	// ZoneNameLen define zone length
	ZoneNameLen = 255
	// DefaultAZ default az
	DefaultAZ = "defaultaz"

	// PodIPEnvKey define pod ip env key
	PodIPEnvKey = "POD_IP"

	// HostNameEnvKey defines the hostname env key
	HostNameEnvKey = "HOSTNAME"

	// HostIPEnvKey defines the host ip env key
	HostIPEnvKey = "HOST_IP"

	// PodNamespaceEnvKey define pod namespace env key
	PodNamespaceEnvKey = "POD_NAMESPACE"

	// FunctionLoadTimeoutEnvKey load Function timeout time
	FunctionLoadTimeoutEnvKey = "LOAD_FUNCTION_TIMEOUT"

	// ResourceLimitsMemory Memory limit, in bytes
	ResourceLimitsMemory = "MEMORY_LIMIT_BYTES"

	// FuncBranchEnvKey is branch env key
	FuncBranchEnvKey = "FUNC_BRANCH"

	// DataSystemBranchEnvKey is branch env key
	DataSystemBranchEnvKey = "DATASYSTEM_CAPABILITY"

	// HTTPort busproxy httpserver listen port
	HTTPort = "22423"
	// TCPort busproxy tcpserver listen port
	TCPort = "32568"
	// BusWorkerServerTCPort bus worker server listen port
	BusWorkerServerTCPort = "32569"
	// BusRuntimeServerPort bus listen port for
	BusRuntimeServerPort = "32570"
	// StreamTransPort stream-handler trans port
	StreamTransPort = "28939"
	// DefaultCachePort indicates the default port of a cache-manager server
	DefaultCachePort = "9993"

	// PlatformTenantID is tenant ID of platform function
	PlatformTenantID = "0"

	// RuntimeLogOptTail -
	RuntimeLogOptTail = "Tail"
	// RuntimeLayerDirName -
	RuntimeLayerDirName = "layer"
	// RuntimeFuncDirName -
	RuntimeFuncDirName = "func"

	// FunctionTaskAppID -
	FunctionTaskAppID = "function-task"

	// BackpressureCode indicate that frontend should choose another proxy/worker and retry
	BackpressureCode = 211429
	// HeaderBackpressure indicate that proxy can backpressure this request
	HeaderBackpressure = "X-Backpressure"
	// HeaderBackpressureNums Backpressure numbers counter
	HeaderBackpressureNums = "X-Backpressure-Nums"
	// MonitorFileName monitor file name
	MonitorFileName = "monitor-disk"

	// DefaultFuncLogIndex default function log's index
	DefaultFuncLogIndex = -2

	// IsClusterUpgrading indicate that the cluster is in upgrading phase
	IsClusterUpgrading = "FAAS_CLUSTER_IS_UPGRADING"
)

// functionBus userData key flag
const (
	// SNServiceIDFlag -
	SNServiceIDFlag = "SNServiceID"
	// FrontendCallFlag invoke from task
	FrontendCallFlag = "FrontendCallFlag"
	// SrcTenantFlag -
	SrcTenantFlag = "SrcTenantID"
	// StateKeyFlag -
	StateKeyFlag = "StateKey"
	// StateIDFlag -
	StateIDFlag = "StateID"

	// ExtraFlag -
	ExtraFlag = "Extra"
)

// HTTP invoke request header key
const (
	// HeaderExecutedDuration -
	HeaderExecutedDuration = "X-Executed-Duration"
	// HeaderStartDuration -
	HeaderStartDuration = "X-Start-Duration"
	// HeaderTraceID -
	HeaderTraceID = "X-Trace-ID"
	// HeaderIdleMode -
	HeaderIdleMode = "X-Idle-Mode"
	// HeaderBillingSwitch -
	HeaderBillingSwitch = "X-Billing-Switch"
	// HeaderEventSourceID -
	HeaderEventSourceID = "X-Event-Source-Id"
	// HeaderBusinessID -
	HeaderBusinessID = "X-Business-ID"
	// HeaderLogType -
	HeaderLogType = "X-Log-Type"
	// HeaderLogResult -
	HeaderLogResult = "x-log-result"
	// HeaderTriggerFlag -
	HeaderTriggerFlag = "X-Trigger-Flag"
	// HeaderInnerCode -
	HeaderInnerCode = "X-Inner-Code"
	// HeaderInvokeURN  -
	HeaderInvokeURN = "X-Tag-VersionUrn"
	// HeaderStateKey -
	HeaderStateKey = "X-Instance-Name"
	// HeaderStateID -
	HeaderStateID = "X-Instance-ID"
	// HeaderInstanceID -
	HeaderInstanceID = "x-instance-id"
	// HeaderBackPressureInstanceID -
	HeaderBackPressureInstanceID = "x-backpressure-instance-id"
	// HeaderBackPressureNodeID -
	HeaderBackPressureNodeID = "x-backpressure-node-id"
	// HeaderCallType is the request type
	HeaderCallType = "X-Call-Type"
	// HeaderLoadDuration duration of loading function
	HeaderLoadDuration      = "X-Load-Duration"
	HeaderMetricsTick       = "X-Metrics-Tick"
	HeaderResourceUsageTick = "X-Resource-Usage-Tick"
	HeaderCaeMetricsTick    = "X-Resource-CaeMetrics-Tick"
	HeaderPodName           = "X-Pod-Name"
	// HeaderNodeLabel is node label
	HeaderNodeLabel = "X-Node-Label"
	// HeaderForceDeploy is Force Deploy
	HeaderForceDeploy = "X-Force-Deploy"
	// HeaderAuthTimestamp is the timestamp for authorization
	HeaderAuthTimestamp = "X-Timestamp-Auth"
	// HeaderAuthorization is authorization
	HeaderAuthorization = "authorization"
	// HeaderContentSignature is content signature
	HeaderContentSignature = "x-content-signature"
	// HeaderAsync indicate whether it is an async request
	HeaderAsync = "X-ASYNC"
	// HeaderPodApplier mark who fetched the worker pod
	HeaderPodApplier = "X-Pod-Applier"
	// HeaderCPUSize is cpu size specified by invoke
	HeaderCPUSize = "X-Instance-CPU"
	// HeaderMemorySize is cpu memory specified by invoke
	HeaderMemorySize = "X-Instance-Memory"
	// HeaderRetryFlag -
	HeaderRetryFlag = "X-Retry-Flag"
	// HeaderDomainName -
	HeaderDomainName = "X-CFF-Domain-Names"
	// HeaderSubMetaDigest -
	HeaderSubMetaDigest = "X-CFF-SubMetaDigest"
	// HeaderCffHTTPResponse -
	HeaderCffHTTPResponse = "X-CFF-HTTP-Response"
	// HeaderStreamEnable
	HeaderStreamEnable = "X-Stream-Enable"
	// HeaderStreamAddr
	HeaderStreamAddr = "X-Stream-Addr"
	// HeaderStreamRunID
	HeaderStreamRunID = "X-Stream-Run-Id"
	// HeaderStreamToken
	HeaderStreamToken = "X-Stream-Token"
	// HeaderSSESessionID -
	HeaderSSESessionID = "X-SSE-Session-ID"
	// HeaderSSEPodName -
	HeaderSSEPodName = "X-SSE-Pod-Name"
	// HeaderTraceParent -
	HeaderTraceParent = "Traceparent"
	// ApplicationJSON -
	ApplicationJSON = "application/json"
	// ContentType -
	ContentType = "Content-Type"
	// Authorization -
	Authorization = "authorization"
	// AuthCheck -
	AuthCheck = "authentication"
	// AuthCheckValue -
	AuthCheckValue = "check"
	// SecretEncryptKey secret encrypt Key
	SecretEncryptKey = "secretEncryptKey"
	// WorkKeyTask work key task
	WorkKeyTask = "workerProxyKey"
	// PriorityHeader -
	PriorityHeader = "priority"

	// ErrorDuration duration when error happened,
	// used with key $LoadDuration
	ErrorDuration = -1
	// StreamHandlerHostCidr -
	StreamHandlerHostCidr = "STREAM_HANDLER_HOST_CIDR"
	// StreamHandlerServicePort -
	StreamHandlerServicePort = "STREAM_HANDLER_SERVICE_PORT"
	// DisableWaitBeforeChangeNetwork -
	DisableWaitBeforeChangeNetwork = "DISABLE_WAIT_BEFORE_CHANGE_NETWORK"
	// HeaderWorkerCost -
	HeaderWorkerCost = "X-Worker-Cost"
	// HeaderCallInstance -
	HeaderCallInstance = "X-Call-Instance"
	// HeaderCallNode -
	HeaderCallNode = "X-Call-Node"

	// HeaderErrorNode -
	HeaderErrorNode = "X-Error-Node"
	// HeaderErrorPod -
	HeaderErrorPod = "X-Error-Pod"
	// HeaderErrorCode -
	HeaderErrorCode = "X-Error-Code"

	// NATConfigures -
	NATConfigures = "NAT_CONFIGURES"
	// EndpointVpcNATConfigure -
	EndpointVpcNATConfigure = "ENDPOINT_VPC_NAT_CONFIGURES"
	// PublicNetworkVpcNATConfigure -
	PublicNetworkVpcNATConfigure = "PUBLIC_NETWORK_VPC_NAT_CONFIGURES"
	// RouteInfo -
	RouteInfo = "ROUTE_INFO"
	// SvcCidr -
	SvcCidr = "SVC_CIDR"
	// FuncMountConfig -
	FuncMountConfig = "FUNC_MOUNT_CONFIG"
	// StateID state id
	StateID = "X-POD-STATE-ID"
	// FunctionLifecycleType life cycle type of function
	FunctionLifecycleType = "X_FUNCTION_LIFECYCLE_TYPE"
	// SuicideTimeoutConfig config for worker suicide time
	SuicideTimeoutConfig = "Suicide_Timeout_Config"
	// XSuicideTimeoutConfig suicide config in header
	XSuicideTimeoutConfig = "X-Suicide-Timeout-Config"
	// HeaderDemandWssInstance demand wss instance in header
	HeaderDemandWssInstance = "X-Demand-Wss-Instance"
	// HeaderClusterID cluster id in header
	HeaderClusterID = "X-CLUSTER-ID"
)

// Extra Request Header
const (
	// HeaderRequestID -
	HeaderRequestID = "x-request-id"
	// HeaderAccessKey -
	HeaderAccessKey = "x-access-key"
	// HeaderSecretKey -
	HeaderSecretKey = "x-secret-key"
	// HeaderSecurityAccessKey -
	HeaderSecurityAccessKey = "x-security-access-key"
	// HeaderSecuritySecretKey -
	HeaderSecuritySecretKey = "x-security-secret-key"
	// HeaderWorkflowID workflowID of request head
	HeaderWorkflowID = "x-workflow-id"
	// HeaderWorkflowRunID workflowRunID of request head
	HeaderWorkflowRunID = "x-workflow-run-id"
	// HeaderWorkflowStateID stateID of request head
	HeaderWorkflowStateID = "x-workflow-state-id"
	// HeaderLiveDataTraceID -
	HeaderLiveDataTraceID = "x-livedata-trace-id"
	// HeaderAuthToken -
	HeaderAuthToken = "x-auth-token"
	// HeaderSecurityToken -
	HeaderSecurityToken = "x-security-token"
	// HeaderInvokeSource like trigger type
	HeaderInvokeSource = "x-invoke-source"
	// HeaderPersistResponse true/false
	HeaderPersistResponse = "x-persist-response"
	// HeaderFuncURN -
	HeaderFuncURN = "x-function-urn"
	// HeaderInvokeSummary -
	HeaderInvokeSummary = "x-invoke-summary"
	// HeaderBillingDuration -
	HeaderBillingDuration = "x-billing-duration"
	// HeaderInvokeAlias indicates alias of current invocation
	HeaderInvokeAlias = "x-invoke-alias"
	// HeaderOriginVersionTag indicates alias of current invocation
	HeaderOriginVersionTag = "origin_version_tag"
	// HeaderServiceID indicates the header of package caches
	HeaderServiceID = "X-service-ID"
	// HeaderSha512 indicates the header of SHA512 values
	HeaderSha512 = "sha512"
	// HeaderIsLayer indicates the header of layer flag
	HeaderIsLayer = "layer-flag"
	// ExtraHeaderMapSize extra header
	// x-request-id/x-access-key/x-secret-key/x-auth-token/x-security-token/x-invoke-source/domain-names...
	ExtraHeaderMapSize = 16
	// HeaderRouterEtcdState -
	HeaderRouterEtcdState = "X-Router-Etcd-State"
	// HeaderClusterUpgrading indicates the cluster is in upgrading phase
	HeaderClusterUpgrading = "X-Cluster-Upgrading"
	// CacheManagerClusterID if vpc_info is is_safety, use config dns instead of coredns, so use service cluster ip
	// request cache-manager server.
	CacheManagerClusterID = "Cache-Manager-ClusterID"
	// DNSResolveAddrKey dns resolve addr
	DNSResolveAddrKey = "DNS_RESOLV_ADDR"
	// apm application performance management trace headers
	HeaderLuBanNTraceID = "lubanops-ntrace-id"
	HeaderLuBanGTraceID = "lubanops-gtrace-id"
	HeaderLuBanSpanID   = "lubanops-nspan-id"
	HeaderLuBanEvnID    = "lubanops-nenv-id"
	HeaderLuBanEventID  = "lubanops-sevent-id"
	HeaderLuBanDomainID = "lubanops-ndomain-id"
)

const (
	// WorkerKey is used by worker deployer to decrypt
	WorkerKey = "config-key"

	// OndemandKey is used in ondemand scenario
	OndemandKey = "ondemand"
)

// stage
const (
	InitializeStage     = "initialize"
	RestoreStage        = "restore"
	InvokeStage         = "invoke"
	LoadStage           = "load"
	ExtensionStage      = "extension"
	ColdStartStage      = "coldstart"
	CheckpointLoadStage = "checkpoint-load"
	CheckpointInitStage = "checkpoint-init"
	PreStopStage        = "preStop"
)

// default UIDs and GIDs
const (
	DefaultWorkerUID    = 1000
	DefaultWorkerGID    = 1000
	DefaultRuntimeUID   = 1003
	DefaultRuntimeUName = "snuser"
	DefaultRuntimeGID   = 1003
	DefaultRuntimeGName = "snuser"
)

// config path
const (
	// DefaultHomePath default env home path
	DefaultHomePath = "/home/sn"
	// RuntimeManagerConfigPath runtime manager config path
	RuntimeManagerConfigPath = "/home/sn/config/runtime-manager-config.json"
)

const (
	// WorkerManagerApplier mark the instance is created by minInstance
	WorkerManagerApplier = "worker-manager"
	// ASBResApplier mark the instance is created by ASBRes
	ASBResApplier = "ASBRes"
	// PredictionApplier mark the instance is created by smart warmer predict
	PredictionApplier = "prediction"
	// FaasSchedulerApplier the instance is created by faas scheduler
	FaasSchedulerApplier = "faas-scheduler"
	// PoolInfoPrefix pool info prefix in redis
	PoolInfoPrefix = "ClusterState_Pool"
	// PoolInfoSep pool info separator in redis
	PoolInfoSep = "_"
	// ClusterIDKey cluster id key in system env
	ClusterIDKey = "CLUSTER_ID"
	// DefaultRecordingInterval default pool info recording interval, unit is second
	DefaultRecordingInterval = 5
	// DefaultRecordExpiredTime default pool info record expired time, unit is second
	DefaultRecordExpiredTime = 900
)

const (
	// FusionLanguage fusion language
	FusionLanguage = "fusion"
	// VIPUserType vip user type
	VIPUserType = "vip"
	// TenantIDLabelKey tenant id label key
	TenantIDLabelKey = "tenant_id"
	// NodeRoleLabelKey node role label key
	NodeRoleLabelKey = "node-role"
	// NodeRoleLabelValue node role label value
	NodeRoleLabelValue = "data-plane"
	// NodeRoleCALabelValue node role label value scaled by ca plugins
	NodeRoleCALabelValue = "data-plane-scaled"
	// NodeRoleGPULabelValue node role label value for GPU node
	NodeRoleGPULabelValue = "data-plane-gpu"
	// Magnification for cal cpu mem usage
	Magnification = 100
)

// GetValidNodeRoleValues returns valid values of node-role label (exclude GPU node)
func GetValidNodeRoleValues() []string {
	return []string{NodeRoleLabelValue, NodeRoleCALabelValue}
}

// GetValidNodeRoleValuesWithGPU returns valid values of node-role label (include GPU node)
func GetValidNodeRoleValuesWithGPU() []string {
	return []string{NodeRoleLabelValue, NodeRoleGPULabelValue, NodeRoleCALabelValue}
}

const (
	// LoadFunctionPath -
	LoadFunctionPath = "/load"
	// NotifyFunctionPath -
	NotifyFunctionPath = "/notify"
	// ChangePatPodIPPath -
	ChangePatPodIPPath = "/change_pat_pod_ip"
	// ShutdownPath is used to receive a worker shutdown request from the worker-manager.
	ShutdownPath = "/shutdown"
	// CheckpointPath -
	CheckpointPath = "/checkpoint"
	// SnapshotImagePath is to put SnapshotImgae
	SnapshotImagePath = "/var/tmp/snapshot"
	// TelemetryPath is to start telemetry
	TelemetryPath = "/telemetry"
	// UpdateAgencyInfo is used to send update agency info request to worker from the worker-manager.
	UpdateAgencyInfo = "/update_agency_info"
	// WssConnectPath faas-scheduler send building websocket connection request to worker
	WssConnectPath = "/wss_connect"
	// WssCountPath faas-scheduler request worker for request counts of websocket and http
	WssCountPath = "/wss_count"
	// PreStopPath path of worker preStop handler
	PreStopPath = "/prestop"
)

const (
	// FunctionAccessor - defines the microservice component name.
	FunctionAccessor = "FunctionAccessor"
	// FunctionTask -
	FunctionTask = "FunctionTask"
	// InstanceManager -
	InstanceManager = "FunctionInstanceManager"
	// StateManager -
	StateManager = "StateManager"
	// CacheManager -
	CacheManager = "CacheManager"
	// CacheServiceName indicates the header of the cache service
	CacheServiceName = "cache-manager.%s.svc.cluster.local"
	// SystemDefaultNameSpace default
	SystemDefaultNameSpace = "default"
	// SystemNameSpaceEnvKey when use system node need get system component nameSpace by env
	SystemNameSpaceEnvKey = "SYSTEM_NAMESPACE"
	// TrustworthinessNameSpace when enable system node will use this ns
	TrustworthinessNameSpace = "yuanrong-system"
	// FaaSScheduler -
	FaaSScheduler = "faas-scheduler"
	// SnapshotManager -
	SnapshotManager = "SnapshotManager"
	// Autoscaler define the alarm type
	Autoscaler = "Autoscaler"
)

const (
	// MetricsUsedResourceTimeWindow -
	MetricsUsedResourceTimeWindow = "UsedResourceTimeWindow"
	// ReservedInstanceMetricsTimeWindow -
	ReservedInstanceMetricsTimeWindow = "ResInsTimeWindow"
)

const (
	// NonReservedInstanceMode -
	NonReservedInstanceMode = iota
	// ReservedInstanceIdleMode -
	ReservedInstanceIdleMode
	// ReservedInstanceNonIdleMode -
	ReservedInstanceNonIdleMode
)

const (
	// SilentFuncKey key used to match silent functions within ETCD
	SilentFuncKey = "/silent/sn/functions/business/yrk/tenant/%s/function/%s/version/%s"
	// SilentFuncKeyPrefix -
	SilentFuncKeyPrefix = "/silent"
	// ClusterID clusterId
	ClusterID = "CLUSTER_ID"
	// SnapshotKey key used to match snapshot within ETCD
	SnapshotKey = "/snapshot/business/yrk/cluster/%s/tenant/%s/function/%s/version/%s/id/%s"
)

const (
	// RuntimeLogPath runtime log file base path
	RuntimeLogPath = "RUNTIME_LOG_PATH"
	// RuntimeLogLevel runtime log level, default info
	RuntimeLogLevel = "RUNTIME_LOG_LEVEL"
	// MaxRequestBodySize runtime max request body size
	MaxRequestBodySize = "MAX_REQUEST_BODY_SIZE"
	// PodName environ variable
	PodName = "PodName"
	// CheckpointRestoreOpts for criu
	CheckpointRestoreOpts = "CRIU_OPTS"

	// DataSystemPort data system port
	DataSystemPort = "DATASYSTEM_PORT"
	// DataSystemHost data system host
	DataSystemHost = "DATASYSTEM_HOST"
	// DataSystemJniPath data system jni path
	DataSystemJniPath = "DATASYSTEM_JNI_PATH"
	// DefaultDataSystemPort default data system port
	DefaultDataSystemPort = "31501"
	// PersistenceFunction means that the function instance will never be terminated by yuanrong
	PersistenceFunction = "PersistenceFunction"

	// AGCCredentialEnv environment key for AGC client credential config
	AGCCredentialEnv = "AGCClientCredentialConfig"
)

const (
	// FastFreezeCheckpointScriptPath -
	FastFreezeCheckpointScriptPath = "/home/snuser/bin/fastfreeze/fastfreeze_checkpoint.sh"
	// FastFreezeRunScriptPath -
	FastFreezeRunScriptPath = "/home/snuser/bin/fastfreeze/fastfreeze_run.sh"
	// FastFreezeRestoreScriptPath -
	FastFreezeRestoreScriptPath = "/home/snuser/bin/fastfreeze/fastfreeze_restore.sh"
	// RunningModeCheckpoint -
	RunningModeCheckpoint = "CHECKPOINT"
	// RunningModeRestore -
	RunningModeRestore = "RESTORE"
	// RunningModeNormal -
	RunningModeNormal = "NORMAL"
	// NotExistVersion not exist version
	NotExistVersion = "NotExistVersion"
	// FastFreezeRuntimePid -
	FastFreezeRuntimePid = 1000
	// CheckpointModeUpdate - update existing snapshots by re doing checkpoint
	CheckpointModeUpdate = "update"
	// CheckpointModeCreate - create snapshots by doing checkpoint for the first time
	CheckpointModeCreate = "create"
	// M2SFunctionPrefix -
	M2SFunctionPrefix = "com.huawei.yuanrong.m2s"
	// RestoreFailInfo -
	RestoreFailInfo = "restoreFailInfo"
)

const (
	// SnapshotPackScriptPath -
	SnapshotPackScriptPath = "/home/sn/bin/script/snapshot_pack.sh"
	// SnapshotUnpackScriptPath -
	SnapshotUnpackScriptPath = "/home/sn/bin/script/snapshot_unpack.sh"
	// SnapshotImagePathForWorker -
	SnapshotImagePathForWorker = "/runtime/var-tmp/snapshot"
	// FastFreezePreservePath - colon seperated
	FastFreezePreservePath = "/tmp:/opt/function/code:/home/snuser/log"
	// FastFreezeCAEPreservePath - colon seperated
	FastFreezeCAEPreservePath = "/tmp:/home/snuser/log"
	// FastFreezeRestoreNotifyPath -
	FastFreezeRestoreNotifyPath = "/var/tmp/notify"
	// GCPath -
	GCPath = "/home/sn/log/service/runtime"
	// GCFilePrefix -
	GCFilePrefix = "gc*"
	// FastFreezeLogPath -
	FastFreezeLogPath = "/var/tmp/fastfreeze/logs"
)

const (
	// TelemetryPlatformType -
	TelemetryPlatformType = "platform"
	// TelemetryFunctionType -
	TelemetryFunctionType = "function"
	// TelemetryExtensionType -
	TelemetryExtensionType = "extension"
	// TelemetryInitStart -
	TelemetryInitStart = "initStart"
	// TelemetryInitRuntimeDone -
	TelemetryInitRuntimeDone = "initRuntimeDone"
	// TelemetryInitReport -
	TelemetryInitReport = "initReport"
	// TelemetryStart -
	TelemetryStart = "start"
	// TelemetryRuntimeDone -
	TelemetryRuntimeDone = "runtimeDone"
	// TelemetryReport -
	TelemetryReport = "report"
	// TelemetryRestoreStart -
	TelemetryRestoreStart = "restoreStart"
	// TelemetryRestoreRuntimeDone -
	TelemetryRestoreRuntimeDone = "restoreRuntimeDone"
	// TelemetryRestoreReport -
	TelemetryRestoreReport = "restoreReport"
)

// enums of cluster's network model, current default is tunnel
const (
	NetworkModelTunnel = "tunnel"
	NetworkModelVPC    = "vpc"
)

// GetNetworkModel get cluster's network model from env; default NetworkModelTunnel
func GetNetworkModel() string {
	return networkModel
}

var networkModel = parseNetworkModel()

func parseNetworkModel() string {
	val := os.Getenv("NETWORK_MODEL")
	if val == "" {
		return NetworkModelTunnel
	}
	return val
}

const (
	// CommonLdLibraryPath -
	CommonLdLibraryPath = "/lib64:/usr/lib64:$DEPLOY_DIR:$DEPLOY_DIR/lib:$LD_LIBRARY_PATH"
	// CangJie10LdLibraryPath -
	CangJie10LdLibraryPath = "/opt/function/runtime/cangjie/runtime/lib/linux_x86_64_llvm:" +
		"/opt/function/runtime/cangjie/runtime/lib/linux_aarch64_llvm:" +
		"/home/snuser/runtime/cangjie1.0:"
	// CangJie0523LdLibraryPath -
	CangJie0523LdLibraryPath = "/opt/function/runtime/cangjie0523/runtime/lib/linux_x86_64_llvm:" +
		"/opt/function/runtime/cangjie0523/runtime/lib/linux_aarch64_llvm:" +
		"/home/snuser/runtime/cangjie0.52.3:"
	// CangJie060xLdLibraryPath -
	CangJie060xLdLibraryPath = "/opt/function/runtime/cangjie060x/runtime/lib/linux_x86_64_llvm:" +
		"/opt/function/runtime/cangjie060x/runtime/lib/linux_aarch64_llvm:" +
		"/opt/function/runtime/cangjie060x/runtime/lib/stdx_linux_x86_64_llvm/dynamic/stdx:" +
		"/opt/function/runtime/cangjie060x/runtime/lib/stdx_linux_aarch64_llvm/dynamic/stdx:" +
		"/home/snuser/runtime/cangjie0.60.x:"
)

const defaultSyncRequestTimeout = 900

// GetSyncRequestTimeout returns sync request timeout
func GetSyncRequestTimeout() int64 {
	return syncRequestTimeout
}

var syncRequestTimeout = parseDefaultSyncRequestTimeout()

func parseDefaultSyncRequestTimeout() int64 {
	env := os.Getenv("SYNC_REQUEST_TIMEOUT")
	if env == "" {
		return defaultSyncRequestTimeout
	}
	val, err := strconv.Atoi(env)
	if err != nil || val <= 0 {
		return defaultSyncRequestTimeout
	}
	return int64(val)
}

const (
	// BusinessTypeWebSocket websocket business type
	BusinessTypeWebSocket = "WEBSOCKET"
	// BusinessTypeCAE cae business type
	BusinessTypeCAE = "CAE"
	// BusinessTypeFaaS FaaS business type
	BusinessTypeFaaS = "FaaS"
	// BusinessType business type key
	BusinessType = "BUSINESS_TYPE"
	// LanguageJava8 language java8
	LanguageJava8 = "java8"
	// CustomJavaArgs customer defined args
	CustomJavaArgs = "CUSTOM_JAVA_ARGS"
	// CustomJavaArgsWhiteList customer args define whitelist
	CustomJavaArgsWhiteList = "CUSTOM_JAVA_ARGS_WHITELIST"
)

// ASBRes const
const (
	// FuncSvcID -
	FuncSvcID = "funcSvcID"
)

const (
	// EnableLogPath enable user defined log path
	EnableLogPath = "ENABLE_LOG_PATH"
	// LogPaths log paths
	LogPaths = "LOG_PATHS"
)

const (
	// CreatorKey creator key
	CreatorKey = "creator"
	// TenantIDKey tenantID key
	TenantIDKey = "tenantID"
	// BusinessIDKey businessID key
	BusinessIDKey = "businessID"
	// ServiceIDKey serviceID key
	ServiceIDKey = "serviceID"
	// VersionKey version key
	VersionKey = "version"
	// FuncNameKey funcName key
	FuncNameKey = "funcName"
	// CPUKey cpu key
	CPUKey = "cpu"
	// MemoryKey memory key
	MemoryKey = "memory"
	// ClusterKey cluster key
	ClusterKey = "cluster"
	// ApplierKey applier key
	ApplierKey = "applier"
	// StateManagerAdd add state router
	StateManagerAdd = "StateManagerAdd"
	// StateManagerUpdate update state router
	StateManagerUpdate = "StateManagerUpdate"
)

const (
	// CAEPropertiesEnvName is cae properties environment name
	CAEPropertiesEnvName = "environment_name"
	// CAEPropertiesEnvID is cae properties environment id
	CAEPropertiesEnvID = "environment_id"
	// CAEPropertiesComponentName is cae properties component name
	CAEPropertiesComponentName = "component_name"
	// CAEPropertiesComponentID is cae properties component id
	CAEPropertiesComponentID = "component_id"
	// CAEPropertiesAppName is cae properties application name
	CAEPropertiesAppName = "application_name"
	// CAEPropertiesAppID is cae properties application id
	CAEPropertiesAppID = "application_id"
)

// IsStateID if the key in the state router map is a state ID
func IsStateID(value string) bool {
	if strings.HasPrefix(value, CreatorKey) || strings.HasPrefix(value, TenantIDKey) ||
		strings.HasPrefix(value, BusinessIDKey) || strings.HasPrefix(value, ServiceIDKey) ||
		strings.HasPrefix(value, VersionKey) || strings.HasPrefix(value, FuncNameKey) ||
		strings.HasPrefix(value, CPUKey) || strings.HasPrefix(value, MemoryKey) ||
		strings.HasPrefix(value, ClusterKey) || strings.HasPrefix(value, ApplierKey) {
		return false
	}
	return true
}

const (
	// DefaultUserDataMaxSize -
	DefaultUserDataMaxSize = 512 << 20 // 512Mb
	// ExpansionUserDataMaxSize -
	ExpansionUserDataMaxSize = 10240 << 20 // 10Gb
	// DefaultUserInodesMaxSize -
	DefaultUserInodesMaxSize = 33000
	// ExpansionUserInodesMaxSize -
	ExpansionUserInodesMaxSize = 655000
)

// KmsAbnormalCipher when kms cipher abnormal, will set error to cipher plaintext
// final process in the worker to return user error
const KmsAbnormalCipher = "kmsCipherIsNotUsefulError"
