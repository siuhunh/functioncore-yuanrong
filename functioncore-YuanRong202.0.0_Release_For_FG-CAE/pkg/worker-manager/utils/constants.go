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

// Package utils gets function services from URNs
package utils

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"time"
)

const (
	// DefaultMaxInstanceNum define the default value for maximum instance
	DefaultMaxInstanceNum = 1000
	// DefaultMinInstanceNum define the default value for minimum instance
	DefaultMinInstanceNum = 0
	// DefaultMaxOnDemandInstanceNumPerTenant define the default value for maximum on-demand instance per tenant
	DefaultMaxOnDemandInstanceNumPerTenant = 1000
	// DefaultMaxReversedInstanceNumPerTenant define the default value for maximum reversed instance per tenant
	DefaultMaxReversedInstanceNumPerTenant = 1000
	// DefaultUnlimitedInstanceNumPerTenant define the default unlimited value per tenant
	DefaultUnlimitedInstanceNumPerTenant = -1
	// MaxLabelValueLength max label value length of k8s
	MaxLabelValueLength = 63

	// LabelApplier label applier
	LabelApplier = "applier"
	// LabelMigratedFrom label migrated from
	LabelMigratedFrom = "migratedFrom"
	// LabelTMPExpansion label tmp expansion
	LabelTMPExpansion = "enableTMPExpansion"
	// LabelBusinessID label business id
	LabelBusinessID = "businessID"
	// LabelTenantID label tenant id
	LabelTenantID = "tenantID"
	// LabelFunctionName label function name
	LabelFunctionName = "functionName"
	// LabelFuncNamePrefix label function name prefix
	LabelFuncNamePrefix = "funcNamePrefix"
	// LabelServiceID label service id
	LabelServiceID = "serviceID"
	// LabelShortFuncName label short function name
	LabelShortFuncName = "funcName"
	// LabelFunctionVersion label function version
	LabelFunctionVersion = "version"
	// LabelStateID label state id
	LabelStateID = "stateID"
	// LabelCreatorFuncName label creator's function name
	LabelCreatorFuncName = "creatorFuncName"
	// LabelCreatorFunctionVersion label creator's function version
	LabelCreatorFunctionVersion = "creatorFuncVersion"
	// LabelCreatorStateID label creator's state id
	LabelCreatorStateID = "creatorStateID"
	// LabelNodeType repr node type, value is highend or lowend
	LabelNodeType = "node-type"
	// LabelIsBridgeFunction label isBridgeFunction
	LabelIsBridgeFunction = "isBridgeFunction"
	// DefaultNodeType default value of LabelNodeType
	DefaultNodeType = "highend"
	// HighNodeType one value of LabelNodeType
	HighNodeType = "highend"
	// LowNodeType the other value of LabelNodeType
	LowNodeType = "lowend"
	// LabelEnableTMPExpansion -
	LabelEnableTMPExpansion = "enableTMPExpansion"
	LabelLongRun            = "longRun"
	// LabelDirect labels runtime pod that connect to bus direct
	LabelDirect = "direct"
	// LabelHasInitializer labels weather function has initializer
	LabelHasInitializer = "hasInitializer"
	// LabelCreationTime labels pod creation time
	LabelCreationTime = "creationTime"
	// LabelBusinessType businessType of function
	LabelBusinessType = "businessType"
	// LabelMCPFunc is mcp func instance
	LabelMCPFunc = "mcpFunc"
	// AppID current service name
	AppID = "worker-manager"
	// AnnotationFullFuncName annotation of full function name, 0@default@hello
	AnnotationFullFuncName = "funcName"

	// FaasAPIVersion define functionservice's api version
	FaasAPIVersion = "faas.huawei.com/v1alpha1"
	// FaasAPIName define functionservice cr name
	FaasAPIName = "FunctionService"

	// NodeEventKind define a node event kind name
	NodeEventKind = "Node"
	// PodEventKind define a pod event kind name
	PodEventKind = "FunctionInstance"
	// FunctionRepoEventKind define a function repo etcd event kind name
	FunctionRepoEventKind = "FunctionRepo"
	// InstanceMetadataEventKind define a etcd event kind name about instance metadata
	InstanceMetadataEventKind = "InstanceMetadata"
	// APIGEventKind define a apig route info event kind name
	APIGEventKind = "APIG"
	// PublicNetworkEventKind define a public network bandwidth event kind name
	PublicNetworkEventKind = "PublicNetwork"
	// TenantMetadataEventKind define a etcd event kind name about tenant metadata
	TenantMetadataEventKind = "TenantMetadata"
	// ClusterEventKind define a cluster status event kind name
	ClusterEventKind = "Cluster"
	// PredictionKind define an etcd event kind name about instance prediction
	PredictionKind = "Prediction"
	// TenantVPCRouterKind vpc router config for tenant
	TenantVPCRouterKind = "TenantVPCRouter"
	// StateRouterKind state router
	StateRouterKind = "StateRouter"
	// OwnerIPKey is the annotation key of owner ip
	OwnerIPKey = "ownerIP"
	// SafeToEvict is false will not scale down the node with pod
	SafeToEvict    = "cluster-autoscaler.kubernetes.io/safe-to-evict"
	CRSignatureKey = "crSignature"
)

const (
	// InstanceMetadataPrefix define the key prefix of etcd for instance metadata
	InstanceMetadataPrefix = "/instances/business/yrk/cluster"

	// PredictionPrefix define the key prefix of instance prediction
	PredictionPrefix = "/prediction/cluster"

	// FunctionMetadataPrefix define the key prefix of etcd for func metadata
	FunctionMetadataPrefix = "/sn/functions/business"

	// TenantMetadataPrefix define the key prefix of etcd for tenant metadata
	TenantMetadataPrefix = "/sn/quota/cluster"

	// TenantVPCRouterPrefix define the key prefix of tenant vpc router
	TenantVPCRouterPrefix = "/sn/business/yrk/tenant"

	// IsPoolPodLabelKey tells if pod is pool pod
	IsPoolPodLabelKey = "isPoolPod"
	// AppLabelKey tells the type of pod
	AppLabelKey = "app"
	// FunctionTask app type of FunctionTask
	FunctionTask = "functiontask"
	// TrueString is the literal string of true
	TrueString = "true"
	// FalseString is the literal string of false
	FalseString = "false"
	// StandardLabelKey -
	StandardLabelKey = "standard"
	// CheckpointPodStandard -
	CheckpointPodStandard = "checkpoint-pod"

	// APIGMetadataPrefix define apig ips
	APIGMetadataPrefix = "/sn/apig"
	// ClusterMetadataKey cluster status info
	ClusterMetadataKey = "fss_cluster_flow_state"
	// StateWorkerKey -
	StateWorkerKey = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/"
	// FuncPodStatus -
	FuncPodStatus = "/sn/status/workers/business/"
	// FuncCustomImgSizePrefix -
	FuncCustomImgSizePrefix = "/sn/custom/image/size/function"

	// PublicNetworkPrefix define public network bandwidth
	PublicNetworkPrefix = "/sn/publicnetwork/"
)

const (
	// K8sPatchTimeout defines the Kubernetes patch timeout interval
	K8sPatchTimeout = 10 * time.Second
)

const (
	// StatusOk status ok
	StatusOk = 150200
	// StatusBadRequest status bad request
	StatusBadRequest = 150400
	// StatusUnauthorized status unauthorized
	StatusUnauthorized = 150401
	// StatusNotFound status not found
	StatusNotFound = 150404
	// StatusInternalServerError status internal server error
	StatusInternalServerError = 150500
	// StatusQueryETCDError status query etcd error
	StatusQueryETCDError = 150501
	// StatusReachMaxInstances reach function max instances
	StatusReachMaxInstances = 150429
	// ReachMaxOnDemandInstancesPerTenant reach tenant max on-demand instances
	ReachMaxOnDemandInstancesPerTenant = 150432
	// ReachMaxReversedInstancesPerTenant reach tenant max reversed instances
	ReachMaxReversedInstancesPerTenant = 150433
	// StatusClusterOverload cluster is overload and unavailable now
	StatusClusterOverload = 150430
	// VIPClusterOverload cluster has no available resource
	VIPClusterOverload = 150510
	// FunctionIsDisabled function is disabled
	FunctionIsDisabled = 150434
	// JSONParseError json parse error
	JSONParseError = 150599
)

const (
	// P2pPort define p2p port
	P2pPort = "22668"
	// NodePort define node port
	NodePort = "22423"
	// DefaultMapSize is the default map size
	DefaultMapSize = 8
	// DefaultSliceSize is the default slice size
	DefaultSliceSize = 8
)

var (
	// ErrFunctionNameNotFound error function name not found
	ErrFunctionNameNotFound = errors.New("function name not found")
	// ErrCallType error callType is not active
	ErrCallType = errors.New("callType is not active")
	// ErrReachMaxInstanceNum reach max instance num
	ErrReachMaxInstanceNum = errors.New("reach max instance num error")
	// ErrReachMaxOnDemandInstanceNumPerTenant reach max instance num
	ErrReachMaxOnDemandInstanceNumPerTenant = errors.New("reach max on-demand instance num per tenant error")
	// ErrReachMaxReversedInstanceNumPerTenant reach max instance num
	ErrReachMaxReversedInstanceNumPerTenant = errors.New("reach max reversed instance num per tenant error")
	// ErrNoAvailableVpcPatInstance no available vpc pat instance
	ErrNoAvailableVpcPatInstance = errors.New("no available vpc pat instance")
	// ErrNoOperationalPermissionsVpc no operational permissions vpc
	ErrNoOperationalPermissionsVpc = errors.New("no operational permissions vpc, check the func xrole permissions")
	// ErrCancelGeneralizePod cancel generalize pod
	ErrCancelGeneralizePod = errors.New("cancel generalize pod")
	// ErrRouterETCDOperation RouterETCD operation error
	ErrRouterETCDOperation = errors.New("failed to operate RouterETCD")
	// ErrVIPOverload VIP node overload error
	ErrVIPOverload = errors.New("the VIP node is overload and unavailable now")
	// ErrVPCNotFound VPC item not found error
	ErrVPCNotFound = errors.New("vpc item not found")
	// ErrVPCXRoleNotFound VPC xrole not found error
	ErrVPCXRoleNotFound = errors.New("can't find xrole")
	// ErrNoNodesCanScheduler failed to pull up node because no nodes can schedule
	ErrNoNodesCanScheduler = errors.New("failed to pull up instance because no nodes can schedule")
	// ErrTrans error trans
	ErrTrans = map[int]int{
		StatusOk:                  http.StatusOK,
		StatusBadRequest:          http.StatusBadRequest,
		StatusUnauthorized:        http.StatusUnauthorized,
		StatusNotFound:            http.StatusNotFound,
		StatusInternalServerError: http.StatusInternalServerError,
	}
)

// FunctionServiceIDKey is a functionServiceID key in context
type FunctionServiceIDKey struct{}

const (
	// MaxEtcdRetryAttempts define the max etcd operation times
	MaxEtcdRetryAttempts = 3
	// RetryInterval refer to 3 * 8ms: time required each time
	RetryInterval = 24 * time.Millisecond
)

const (
	// MaxPutToRouterETCDRetryTimes max put to router etcd retry times
	MaxPutToRouterETCDRetryTimes = 10
	// PutToRouterETCDInterval put to router etcd interval
	PutToRouterETCDInterval = 20 * time.Millisecond
)

const (
	// TypeRequestNum add request num to ratio monitor
	TypeRequestNum = "TypeRequestNum"
	// TypePodNum add pod num to ratio monitor
	TypePodNum = "TypePodNum"
	// FuncNameSeparator -
	FuncNameSeparator = "@"
	// SplitFuncNameLen -
	SplitFuncNameLen = 3
	// FuncNamePrefixIndex -
	FuncNamePrefixIndex = 0
	// ServiceIDIndex -
	ServiceIDIndex = 1
	// ShortFuncNameIndex -
	ShortFuncNameIndex = 2
	// WorkerManagerDeployName -
	WorkerManagerDeployName = "worker-manager"
	// WorkerContainerName is the container name of worker
	WorkerContainerName = "worker"
	// RuntimeContainerName is the container name of runtime-manager
	RuntimeContainerName = "runtime"
	// AgentContainerName is the container name of debug-agent
	AgentContainerName = "agent"
	// WorkerInitContainerName is the container name of worker-init
	WorkerInitContainerName = "init"
)

const (
	// BaseDelayForReservedIns base delay when failed to pull up reserved instance
	BaseDelayForReservedIns = 200 * time.Millisecond
	// MaxDelayForReservedIns max delay when failed to pull up reserved instance
	MaxDelayForReservedIns = 30 * time.Second
	// SendIdleModeTimeout set timeout when send idle mode request to worker
	// the value must be greater than workerClientDialTimeout
	SendIdleModeTimeout = 13 * time.Second
	// MaxRetryNumAlarm set the maximum number of retry failure alarms
	MaxRetryNumAlarm = 10
	// IdleRetryInterval set the retry interval
	IdleRetryInterval = 5 * time.Second
	// SendPatIPInterval set send pat ip interval
	SendPatIPInterval = 5 * time.Second
	// SendPatIPTimeout set send pat ip timeout
	SendPatIPTimeout = 13 * time.Second
	// SendTerminationTimeout set timeout when send termination request to worker
	SendTerminationTimeout = 5 * time.Second
)

const (
	// HeaderAuthToken tenant user token
	HeaderAuthToken = "X-Auth-Token"
	// HeaderAlarmAuthTimestamp is the timestamp for alarm authorization
	HeaderAlarmAuthTimestamp = "X-CFF-Timestamp-Auth"
	// HeaderAlarmAuthorization is authorization
	HeaderAlarmAuthorization = "X-CFF-Authorization"
	// UpdateDomainNamesInterval -
	UpdateDomainNamesInterval = 10 * time.Minute
	// UpdateDomainNamesTimeout -
	UpdateDomainNamesTimeout = 10 * time.Second
	// UpdateTenantCacheInterval -
	UpdateTenantCacheInterval = 10 * time.Minute
	// UpdateAgencyInterval -
	UpdateAgencyInterval = 4 * time.Hour
)

const (
	// GpuNodeAffinityValue -
	GpuNodeAffinityValue = "data-plane-gpu"
	// NvidiaGPU  -
	NvidiaGPU = "nvidia.com/gpu"
	// GpuTypeAffinityKey -
	GpuTypeAffinityKey = "accelerator"
	// PodTypeCAEReserve -
	PodTypeCAEReserve = "Reserved-CAE"
	// PodTypeFaaSReserve -
	PodTypeFaaSReserve = "Reserved-FaaS"
	// PodTypeFaaS -
	PodTypeFaaS = "FaaS"
	// PodTypeCAE -
	PodTypeCAE = "CAE"
	// PodTypeDefault -
	PodTypeDefault = "default"
	// DefaultGpuType -
	DefaultGpuType = "all"
	// DefaultGPUMemoryCapacity  -
	DefaultGPUMemoryCapacity = 24
	// MbToGb -
	MbToGb = 1024
	// GpuBase -
	GpuBase = 1000

	minPodSplitLen = 4
	// custom-300-128-[1024]-xxx
	xpuResourceIndex = 3
)

// constants for GPU Isolation
const (
	GpuIsolationMemKey   = "volcano.sh/gpu-mem.128Mi"
	GpuIsolationCoreKey  = "volcano.sh/gpu-core.percentage"
	GpuIsolationMemStep  = 128
	GpuIsolationCoreStep = 5
)

// constants for CAE function node affinity config
const (
	// PoolNodeLabelKey - key of node label implies fixed node or elastic node
	PoolNodeLabelKey = "node-type"
	// CAENodeAffinityValue - CAE function will be affinity to the node with node-type label equals 'fixed'
	CAENodeAffinityValue = "fixed"
	// CAENodeAntiAffinityValue - CAE function will be anti-affinity to the node with node-type label equals 'elastic'
	CAENodeAntiAffinityValue = "elastic"
)

// MigrateTaintKey migrate taint key
const MigrateTaintKey = "DeletionCandidateOfClusterAutoscaler"

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

// LanguageCustomContainer custom-container
const LanguageCustomContainer = "custom-container"
