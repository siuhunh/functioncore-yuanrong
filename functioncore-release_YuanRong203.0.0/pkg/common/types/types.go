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

// Package types -
package types

import (
	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/trace"
	"functioncore/pkg/common/vpa"
)

// HttpResponse is general httpResponse
type HttpResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type (
	// InstanceResponse is response to query function instance
	InstanceResponse struct {
		Code    int              `json:"code"`
		Message string           `json:"message"`
		Result  []InstanceResult `json:"result"`
	}

	// InstanceResult is the result of query function instance info
	InstanceResult struct {
		Instance   InnerWorkerInstance
		InstanceID string `json:"instanceID"`
	}
	// InnerWorkerInstance is the function instance info
	InnerWorkerInstance struct {
		IP       string `json:"ip"`
		Port     string `json:"port"`
		Status   string `json:"status"`
		P2pPort  string `json:"p2pPort"`
		NodeIP   string `json:"nodeIP,omitempty"`
		NodePort string `json:"nodePort,omitempty"`
		NodeName string `json:"nodeName,omitempty"`
		StateID  string `json:"stateID,omitempty"`
		Applier  string `json:"applier,omitempty"`
		OwnerIP  string `json:"ownerIP,omitempty"`
		CPU      int    `json:"cpu,omitempty"`
		Memory   int    `json:"memory,omitempty"`
	}
)

// FunctionSpec define function spec
type FunctionSpec struct {
	FunctionType               string                    `json:"function_type"`
	URN                        string                    `json:"urn"`
	Handler                    string                    `json:"handler"`
	EntryFile                  string                    `json:"entry_file"`
	Timeout                    int64                     `json:"timeout"`
	EnvKey                     string                    `json:"env_key"`
	Environment                string                    `json:"env"`
	EncryptedUserData          string                    `json:"encrypted_user_data"`
	EnvEncryptKmsCipher        string                    `json:"user_data_encrypt_kms_key_cipher" valid:",optional"`
	EnvPlaintextKmsCipher      string                    `json:"env_plaintext_kms_cipher" valid:",optional"`
	Language                   string                    `json:"language"`
	CodeSha256                 string                    `json:"codeSha256" valid:",optional"`
	CodeSha512                 string                    `json:"codeSha512" valid:",optional"`
	ResourceMetaData           ResourceMetaData          `json:"resourceMetaData"`
	Initializer                Initializer               `json:"initializer"`
	Role                       Role                      `json:"role" valid:",optional"`
	DomainID                   string                    `json:"domain_id" valid:",optional"`
	LogTankService             logservice.LogTankService `json:"logTankService"`
	TraceService               trace.TraceService        `json:"traceService"`
	FuncMountConfig            FuncMountConfig           `json:"mount_config" valid:",optional"`
	RuntimeContainerID         string                    `json:"runtimeContainerID"`
	MaxStateSize               string                    `json:"maxStateSize"`
	EnableAuthInHeader         bool                      `json:"enable_auth_in_header" valid:"optional"`
	Heartbeat                  Heartbeat                 `json:"heartbeat" valid:",optional"`
	StartupHeartbeat           Heartbeat                 `json:"startupHeartbeat" valid:",optional"`
	UsePoolPod                 bool                      `json:"usePoolPod" valid:"optional"`
	FileMountConfig            FileMountConfig           `json:"file_mount_config" valid:",optional"`
	CustomJavaArgs             string                    `json:"customJavaArgs" valid:",optional"`
	DNSDomainCfg               []DNSDomainInfo           `json:"dns_domain_cfg" valid:",optional"`
	EndpointDomainCfg          []DNSDomainInfo           `json:"endpoint_domain_cfg" valid:",optional"`
	DNSConfig                  DNSConfig                 `json:"dns_config"`
	DNSDomainNames             map[string][]string       `json:"dns_domain_names" valid:",optional"`
	IsStreamEnable             bool                      `json:"isStreamEnable" valid:"optional"`
	StreamResponse             bool                      `json:"stream_response" valid:"optional"`
	RunningMode                string                    `json:"loadMode" valid:",optional"`
	RestoreHook                RestoreHook               `json:"restore_hook,omitempty" valid:",optional"`
	NetworkController          NetworkController         `json:"network_controller" valid:",optional"`
	EnableClassIsolation       bool                      `json:"enable_class_isolation" valid:"optional"`
	EnableGCLog                bool                      `json:"enable_gc_log" valid:"optional"`
	EnableSnapshot             string                    `json:"enable_snapshot" valid:"optional"`
	RounteInfo                 RouteInfoData             `json:"routeInfoData" valid:",optional"`
	PreStop                    PreStop                   `json:"pre_stop" valid:",optional"`
	CustomMetricsReport        CustomMetricsReport       `json:"customMetricsReport" valid:",optional"`
	MaxRequestBodySize         string                    `json:"max_request_body_size" valid:",optional"`
	DisableInsecureAkSk        bool                      `json:"disable_insecure_ak_sk" valid:",optional"`
	BusinessType               string                    `json:"businessType" valid:"optional"`
	LogPolicy                  LogPolicy                 `json:"logPolicy" valid:"optional"`
	Properties                 map[string]string         `json:"properties" valid:"optional"`
	CustomContainerPort        int                       `json:"custom_container_port" valid:"optional"`
	StreamContentType          string                    `json:"streamContentType" valid:",optional"`
	IsSafety                   bool                      `json:"is_safety" valid:",optional"`
	IsBridgeFunction           bool                      `json:"isBridgeFunction" valid:"optional"`
	LtsCustomTag               map[string]string         `json:"lts_custom_tag" valid:"optional"`
	RuntimeTermTimeout         int                       `json:"runtimeTermTimeout" valid:"optional"`
	FuncSignature              string                    `json:"funcSignature" valid:"optional"`
	CAECustomS3ConfigInfo      CAECustomS3ConfigInfo     `json:"cae_custom_s3_config_info" valid:"optional"`
	CustomUploadConfig         CustomUploadConfig        `json:"custom_upload_config" valid:"optional"`
	IsStatefulFunction         bool                      `json:"isStatefulFunction" valid:"optional"`
	EnableWebsocket            bool                      `json:"enable_websocket" valid:"optional"`
	IsMcpFunction              bool                      `json:"is_mcp_function" valid:"optional"`
	NpmConfigRegistry          string                    `json:"npm_config_registry" valid:"optional"`
	UVIndexURL                 string                    `json:"uv_index_url" valid:"optional"`
	DisableWaitBeforeChangeNet bool                      `json:"disableWaitBeforeChangeNet,omitempty" valid:"optional"`
}

// LogPolicy log policy
type LogPolicy struct {
	LogPaths           []string `json:"logPaths" valid:"optional"`
	HostPath           string   `json:"hostPath" valid:"optional"`
	EnableMultiCollect bool     `json:"enableMultiCollect" valid:"optional"`
	MultiPattern       string   `json:"multiPattern" valid:"optional"`
	LogGroupID         string   `json:"logGroupId" valid:"optional"`
	LogStreamID        string   `json:"logStreamId" valid:"optional"`
}

// RouteInfoData new route info
type RouteInfoData struct {
	ApigRouteEnable bool     `json:"apigRouteEnable" valid:"optional"`
	ApigRoutes      []string `json:"apigRoutes" valid:"optional"`
}

// ResourceMetaData include resource data such as cpu and memory
type ResourceMetaData struct {
	CPU                int64 `json:"cpu" valid:"optional"`
	Mem                int64 `json:"mem" valid:"optional"`
	GpuMemory          int64 `json:"gpu_memory" valid:"optional"`
	EphemeralStorage   int64 `json:"ephemeral_storage" valid:"optional"`
	EnableTmpExpansion bool  `json:"enable_tmp_expansion" valid:"optional"`
}

// Initializer include initializer handler and timeout
type Initializer struct {
	Handler string `json:"initializer_handler" valid:",optional"`
	Timeout int64  `json:"initializer_timeout" valid:",optional"`
}

// Heartbeat define user custom heartbeat function config
type Heartbeat struct {
	// Handler define heartbeat function entry
	Handler string `json:"heartbeat_handler" valid:",optional"`
	// Path define custom health check request path
	Path string `json:"path" valid:",optional"`
	// HealthEnable enable or disable http health check
	HealthEnable bool `json:"health_enable" valid:",optional"`
	// FailureThreshold maximum number of health check failures
	FailureThreshold int `json:"failure_threshold" valid:",optional"`
	// PeriodSeconds health check period
	PeriodSeconds int `json:"period_seconds" valid:",optional"`
	// Timeout health check request timeout interval
	Timeout int `json:"timeout" valid:",optional"`
	// FirstCheckDelay first health check delay time
	FirstCheckDelay int `json:"first_check_delay" valid:",optional"`
	// SuccessThreshold health check success threshold
	SuccessThreshold int `json:"success_threshold" valid:",optional"`
}

// RestoreHook include restorehook handler and timeout
type RestoreHook struct {
	Handler string `json:"restore_hook_handler,omitempty" valid:",optional"`
	Timeout int64  `json:"restore_hook_timeout,omitempty" valid:",optional"`
}

// NetworkController contains some special network settings
type NetworkController struct {
	DisablePublicNetwork       bool      `json:"disable_public_network" valid:",optional"`
	TriggerAccessVpcs          []VpcInfo `json:"trigger_access_vpcs" valid:",optional"`
	PublicBandWidth            string    `json:"public_bandwidth" valid:",optional"`
	DisableWaitBeforeChangeNet bool      `json:"disableWaitBeforeChangeNet,omitempty" valid:"optional"`
}

// VpcInfo contains the information of VPC access restriction
type VpcInfo struct {
	VpcName string `json:"vpc_name,omitempty"`
	VpcID   string `json:"vpc_id,omitempty"`
}

// FuncMountConfig function mount config
type FuncMountConfig struct {
	FuncMountUser FuncMountUser `json:"mount_user" valid:",optional"`
	FuncMounts    []FuncMount   `json:"func_mounts" valid:",optional"`
}

// FuncMountUser the info about mount user
type FuncMountUser struct {
	UserID  int `json:"user_id" valid:",optional"`
	GroupID int `json:"user_group_id" valid:",optional"`
}

// FuncMount the info about mounting files to pod
type FuncMount struct {
	MountType      string `json:"mount_type" valid:",optional"`
	MountResource  string `json:"mount_resource" valid:",optional"`
	MountSharePath string `json:"mount_share_path" valid:",optional"`
	LocalMountPath string `json:"local_mount_path" valid:",optional"`
	Status         string `json:"status" valid:",optional"`
	ExtendedInfo   string `json:"extended_info" valid:",optional"`
}

// Role include x_role and app_x_role
type Role struct {
	XRole    string `json:"xrole" valid:",optional"`
	AppXRole string `json:"app_xrole" valid:",optional"`
}

// FunctionDeploymentSpec define function deployment spec
type FunctionDeploymentSpec struct {
	BucketID               string `json:"bucket_id"`
	ObjectID               string `json:"object_id"`
	Access                 string `json:"access"`
	Secret                 string `json:"secret"`
	SecurityToken          string `json:"securityToken"`
	Layers                 string `json:"layers"`
	DeployDir              string `json:"deploydir"`
	CodeSize               int64  `json:"code_size"`
	CodePlaintextKmsCipher string `json:"code_plaintext_kms_cipher" valid:",optional"`
}

// SnapshotVersion define snapshot version
type SnapshotVersion struct {
	ObjectID          string            `json:"objectId,omitempty"`
	SnapshotSha256    string            `json:"sha256,omitempty"`
	SnapshotSize      int64             `json:"size,omitempty"`
	SnapshotCheckInfo SnapshotCheckInfo `json:"snapshotCheckInfo,omitempty"`
	SnapshotImageTag  string            `json:"imageTag,omitempty"`
	Timestamp         int64             `json:"timestamp,omitempty"`
	ErrCode           int               `json:"code,omitempty"`
	ErrMessage        string            `json:"message,omitempty"`
}

// SnapshotDeploymentSpec define snapshot deployment spec
type SnapshotDeploymentSpec struct {
	SnapshotCheckInfo *SnapshotCheckInfo `json:"snapshotCheckInfo,omitempty"`
	AppID             string             `json:"appId"`
	BucketID          string             `json:"bucketId"`
	ObjectID          string             `json:"objectId"`
	BucketURL         string             `json:"bucketUrl"`
	SnapshotSha256    string             `json:"sha256,omitempty"`
	SnapshotSize      int64              `json:"size,omitempty"`
	IsSnapshotExpire  bool               `json:"expire,omitempty"`
	SnapshotState     string             `json:"snapshotState,omitempty"`
	SnapshotVersions  []SnapshotVersion  `json:"snapshotVersions,omitempty"`
	SnapshotCriuOpts  string             `json:"criuOpts,omitempty"`
	// key is cluster id
	UploadedDataSystem  map[string]bool `json:"uploadedDataSystem,omitempty"`
	NeedDeleteObjectIDs []string        `json:"needDeleteObjectIDs,omitempty"`
	FastStartEnable     bool            `json:"fastStartEnable,omitempty"`
	Event               string          `json:"event,omitempty"`
	TaskID              string          `json:"taskId,omitempty"`
}

// TenantCommandDeploymentSpec define tenant command deployment spec
type TenantCommandDeploymentSpec struct {
	TaskID     string   `json:"taskId"`
	CreateTime int64    `json:"createTime"`
	Mode       string   `json:"mode"`
	FuncUrn    string   `json:"funcUrn"`
	PodName    []string `json:"podName"`
	Path       string   `json:"path"`
	Command    []string `json:"command"`
	Timeout    int64    `json:"timeout"`
}

// TenantCommandStruct define tenant command struct set to worker
type TenantCommandStruct struct {
	TaskID                 string            `json:"taskId"`
	CreateTime             int64             `json:"createTime"`
	Path                   string            `json:"path"`
	Command                []string          `json:"command"`
	Timeout                int64             `json:"timeout"`
	TenantCommandValuesMap map[string]string `json:"tenantCommandValuesMap"`
}

// SnapshotCheckInfo - snapshot check info
type SnapshotCheckInfo struct {
	JavaRuntimeSize  int64  `json:"javaRuntimeSize,omitempty"`
	JavaRuntimeHash  string `json:"javaRuntimeHash,omitempty"`
	RunTimeSize      int64  `json:"runTimeSize,omitempty"`
	ApmJavaAgentSize int64  `json:"apmJavaAgentSize,omitempty"`
	ImageOS          string `json:"imagesOS,omitempty"`
}

// SnapshotMeta define snapshot meta info
type SnapshotMeta struct {
	ClusterID      string
	ETCDKey        string
	SnapshotResult *SnapshotDeploymentSpec
}

// NATConfigure include nat configure info for worker
type NATConfigure struct {
	ContainerCidr  string              `json:"containerCidr"`
	HostVMCidr     string              `json:"hostVmCidr"`
	PatContainerIP string              `json:"patContainerIP"`
	PatVMIP        string              `json:"patVmIP"`
	PatPortIP      string              `json:"patPortIP"`
	PatMacAddr     string              `json:"patMacAddr"`
	PatGateway     string              `json:"patGateway"`
	PatPodName     string              `json:"patPodName"`
	TenantCidr     string              `json:"tenantCidr"`
	NatSubnetList  map[string][]string `json:"natSubnetList"`
	IsDeleted      bool                `json:"isDeleted"`
	IsNewCreated   bool                `json:"isNewCreated"`
	SubMetaDigest  string              `json:"subMetaDigest"`
	// support IPV6
	IPV6PatContainerIP string `json:"ipv6PatContainerIP"`
	IPV6PatGateway     string `json:"ipv6PatGateway"`
	IPV6TenantCidr     string `json:"ipv6TenantCidr"`
}

// FunctionMessage define function message
type FunctionMessage struct {
	Function                FunctionSpec           `json:"function"`
	Deployment              FunctionDeploymentSpec `json:"deployment"`
	SnapshotDeployment      SnapshotDeploymentSpec `json:"snapshotDeployment"`
	NATConfigure            []NATConfigure         `json:"natConfigure"`
	EndpointVpcNATConfigure []NATConfigure         `json:"endpointVpcNatConfigure"`
	PublicVpcNATConfigure   []NATConfigure         `json:"publicVpcNatConfigure"`
	SvcCidr                 string                 `json:"svcCidr"` // SvcCidr is cluster service network segment
}

// FunctionSpecRequest define function spec request
type FunctionSpecRequest struct {
	ValidateOnly bool            `json:"validate_only"`
	Message      FunctionMessage `json:"message"`
}

// PoolRequest is used for scale up work form kernelinvoker to kernelmanager
type PoolRequest struct {
	CPU         int64  `json:"cpu" valid:"int,required"`
	Mem         int64  `json:"mem" valid:"int,required"`
	RunTimeName string `json:"runtimename" valid:"required"`
}

// InstanceResource describes the cpu and memory info of an instance
type InstanceResource struct {
	CPU    string
	Memory string
}

// CustomContainerConfig contains the metadata for custom container
type CustomContainerConfig struct {
	ControlPath string   `json:"control_path" valid:",optional"`
	Image       string   `json:"image" valid:",optional"`
	Command     []string `json:"command" valid:",optional"`
	Args        []string `json:"args" valid:",optional"`
	WorkingDir  string   `json:"working_dir" valid:",optional"`
	UID         int      `json:"uid" valid:",optional"`
	GID         int      `json:"gid" valid:",optional"`
	UseUserAuth bool     `json:"use_user_auth"`
	User        string   `json:"user"`
	Pwd         string   `json:"pwd"`
	Port        int      `json:"port" valid:",optional"`
	UsePoolPod  bool     `json:"usePoolPod"`
	IsPublic    bool     `json:"is_public"`
}

// DNSDomainInfo dns domain info
type DNSDomainInfo struct {
	ID         string `json:"id"`
	DomainName string `json:"domain_name"`
	Type       string `json:"type" valid:",optional"`
	ZoneType   string `json:"zone_type" valid:",optional"`
}

// WorkersView proxy workers view
type WorkersView struct {
	// OwnedNum -1 num means scale up 1 worker by force
	// other num means proxy owned worker nums
	OwnedNum int `json:"ownedNum"`
	// CurrentWorkerNum -
	CurrentWorkerNum int `json:"currentWorkerNum"`
	// ScalingWorkersNum -
	ScalingWorkersNum int `json:"scalingWorkersNum"`
}

// Instance define interface using by proxy and worker-manager
type Instance struct {
	IP             string               `json:"ip"`
	Port           string               `json:"port"`
	InstanceID     string               `json:"instanceID"`
	DeployedIP     string               `json:"deployed_ip"`
	DeployedNode   string               `json:"deployed_node"`
	TenantID       string               `json:"tenant_id"`
	Version        string               `json:"version"`
	OwnerIP        string               `json:"owner_ip"`
	IsReserved     bool                 `json:"isReserved"`
	IsDirectFunc   bool                 `json:"isDirectFunc"`
	HasInitializer bool                 `json:"hasInitializer"`
	BusinessType   string               `json:"businessType,omitempty"`
	PodUID         string               `json:"podUID,omitempty"`
	ContainerIDs   vpa.ContainerInfo    `json:"containerIDs,omitempty"`
	Resource       vpa.PodResourceInfo  `json:"resource,omitempty"`
	ViewResource   vpa.ViewResourceInfo `json:"viewResource,omitempty"`
}

// Layer define layer info
type Layer struct {
	AppID          string `json:"appId" valid:"stringlength(1|128),optional"`
	BucketID       string `json:"bucketId" valid:"stringlength(1|255),optional"`
	ObjectID       string `json:"objectId" valid:"stringlength(1|255),optional"`
	BucketURL      string `json:"bucketUrl" valid:"url,optional"`
	ETag           string `json:"etag" valid:"optional"`
	Sha256         string `json:"sha256" valid:"optional"`
	Name           string `json:"name" valid:",optional"`
	Version        string `json:"version" valid:",optional"`
	DependencyType string `json:"dependencyType" valid:",optional"`
}

// MemoryControlConfig Memory use control config
type MemoryControlConfig struct {
	LowerMemoryPercent     float64 `json:"lowerMemoryPercent" valid:",optional"`
	HighMemoryPercent      float64 `json:"highMemoryPercent" valid:",optional"`
	StatefulHighMemPercent float64 `json:"statefulHighMemoryPercent" valid:",optional"`
	BodyThreshold          uint64  `json:"bodyThreshold" valid:",optional"`
	MemDetectIntervalMs    int     `json:"memDetectIntervalMs" valid:",optional"`
}

// RateLimitControlConfig rate limit control config
type RateLimitControlConfig struct {
	TraceIDBucketSize int     `json:"traceIDBucketSize" valid:",optional"`
	TraceIDFrequency  float64 `json:"traceIDFrequency" valid:",optional"`
}

// DomainNameResp domain name response
type DomainNameResp struct {
	Links      Link        `json:"links"`
	Recordsets []Recordset `json:"recordsets"`
	ErrorCode  string      `json:"error_code"`
}

// Link link current resource or another resource
type Link struct {
	Self string `json:"self"`
	Next string `json:"next"`
}

// Recordset record set
// https://support.huaweicloud.com/api-dns/dns_api_64003.html
type Recordset struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Description string   `json:"description"`
	ZoneID      string   `json:"zone_id"`
	ZoneName    string   `json:"zone_name"`
	TTL         int64    `json:"ttl"`
	Records     []string `json:"records"`
	CreateAt    string   `json:"create_at"`
	UpdateAt    string   `json:"update_at"`
	Status      string   `json:"status"`
	Default     bool     `json:"default"`
	ProjectID   string   `json:"project_id"`
	Links       Link     `json:"links"`
}

// FaaSServiceInfo faas scheduler service info
type FaaSServiceInfo struct {
	InstanceID  string `json:"instanceID" valid:"string,required"`
	NodeIP      string `json:"nodeIP" valid:"string,required"`
	IP          string `json:"ip" valid:"string,required"`
	Port        string `json:"port" valid:"string,required"`
	RunningMode string `json:"runningMode"`
}

// ProfileMetaData define profile meta data of functions based on analysis and prediction
type ProfileMetaData struct {
	ResourceModel ResourceModel `json:"resourceModel" valid:",optional"`
	IsInit        bool          `json:"isInit" valid:",optional"`
}

// ResourceModel define resource model of functions
type ResourceModel struct {
	IsCPUBound    bool `json:"isCPUBound" valid:"optional"`
	IsMemOccupied bool `json:"isMemOccupied" valid:"optional"`
	IsIOBound     bool `json:"isIOBound" valid:"optional"`
}

// InstanceRouter router info of instance
type InstanceRouter struct {
	InstanceID              string
	WorkerAddr              string
	NodeIP                  string
	FunctionKeyWithResource string
	IsMinInstance           bool
	IsDirectFunc            bool
	HasInitializer          bool
}

// FailedInfo restore failed information
type FailedInfo struct {
	FailedCount int64 `json:"failedCount"`
	Time        int64 `json:"time"`
}

// PreStop include pre_stop handler and timeout
type PreStop struct {
	Handler string `json:"pre_stop_handler" valid:",optional"`
	Timeout int    `json:"pre_stop_timeout" valid:",optional"`
	Type    string `json:"pre_stop_type" valid:",optional"`
}

// QueryInstance define query instances info to ASBRes
type QueryInstance struct {
	ClusterID  string   `json:"clusterID"`
	PodsName   []string `json:"podsName"`
	ScalingNum int64    `json:"scalingNum"`
	ExpectNum  int      `json:"expected_num"`
	// expected min instances in cluster
	ExpectedMinIns int `json:"expected_min_ins"`
	// expected snapshot in cluster
	ExpectedSnap   int               `json:"expected_snap"`
	BridgePodsInfo map[string]string `json:"bridgePodsInfo,omitempty"` // key is stateID, value is podName
}

// QueryInstanceResponse is response to query instances
type QueryInstanceResponse struct {
	Code      int           `json:"code"`
	Message   string        `json:"message"`
	Instances QueryInstance `json:"instances"`
}

// CustomMetricsReport include customMetricsReport handler、timeout and collectTime
type CustomMetricsReport struct {
	Handler     string `json:"report_handler" valid:",optional"`
	Path        string `json:"report_path" valid:",optional"`
	CollectTime int    `json:"report_collect_time" valid:",optional"`
	Timeout     int    `json:"report_timeout" valid:",optional"`
}

// DNSConfig user defined dns config
type DNSConfig struct {
	DNS        []string `json:"dns,omitempty"`
	DNSOptions []string `json:"dns_options,omitempty"`
	DNSSearch  []string `json:"dns_search,omitempty"`
}

// StateCreateOptions is stateful function create options
type StateCreateOptions struct {
	CPU    int64 `json:"cpu,omitempty"`
	Memory int64 `json:"memory,omitempty"`
}

// CAECustomS3ConfigInfo for cae function upload dump file to target obs bucket
// FilePath file upload path in obs bucket
type CAECustomS3ConfigInfo struct {
	CustomAK   string `json:"custom_ak" valid:",optional"`
	CustomSK   string `json:"custom_sk" valid:",optional"`
	BucketName string `json:"bucket_name" valid:",optional"`
	Endpoint   string `json:"endpoint" valid:",optional"`
	FilePath   string `json:"file_path" valid:",optional"`
}

// CustomUploadConfig -
type CustomUploadConfig struct {
	ObsKey string
	CAECustomS3ConfigInfo
}

// CAEAppProperty cae application properties
type CAEAppProperty struct {
	ApplicationID   string `json:"application_id" valid:",optional"`
	ApplicationName string `json:"application_name" valid:",optional"`
	ComponentID     string `json:"component_id" valid:",optional"`
	ComponentName   string `json:"component_name" valid:",optional"`
	EnvironmentID   string `json:"environment_id" valid:",optional"`
	EnvironmentName string `json:"environment_name" valid:",optional"`
}

// CaePoolInfo cae pool info from worker to runtime-manager
type CaePoolInfo struct {
	RunningMode     string          `json:"runningMode" valid:"optional"`
	CaeLogPath      string          `json:"caeLogPath" valid:"optional"`
	UsePoolPod      bool            `json:"usePoolPod" valid:"optional"`
	CustomJavaArgs  string          `json:"customJavaArgs" valid:"optional"`
	LogPaths        []string        `json:"logPaths" valid:"optional"`
	FileMountConfig FileMountConfig `json:"file_mount_config" valid:",omitempty"`
}

// FileMountConfig  file mount config
type FileMountConfig struct {
	S3Config    *S3Config          `json:"s3Config" valid:",optional"`
	MountFiles  []*MountFile       `json:"mount_files" valid:",optional"`
	SecretFiles []*SecretFileMount `json:"secretFiles" valid:",optional"`
}

// MountFile  mount file obs info
type MountFile struct {
	AppID     string `json:"appId" valid:"stringlength(1|128),optional"`
	BucketID  string `json:"bucketId" valid:"stringlength(1|255),optional"`
	ObjectID  string `json:"objectId" valid:"stringlength(1|255),optional"`
	BucketURL string `json:"bucketUrl" valid:"url,optional"`
	FilePath  string `json:"filePath" valid:",optional"`
}

// SecretFileMount mount file from meta etcd
type SecretFileMount struct {
	EncryptedFileContent string `json:"encryptedFileContent"`
	FileMountPath        string `json:"fileMountPath"`
}

// S3Config contains the parameters to connect obs
type S3Config struct {
	UseSSL    bool   `yaml:"useSSL" valid:",optional"`
	AccessKey string `yaml:"accessKey" valid:"stringlength(1|255)"`
	SecretKey string `yaml:"secretKey" valid:"stringlength(1|255)"`
	Endpoint  string `yaml:"endpoint" valid:"required"`
	CaFile    string `yaml:"caFile" valid:"optional"`
	TrustedCA bool   `yaml:"trustedCA" valid:"optional"`
}

// LayerPreloadResult -
type LayerPreloadResult struct {
	LayerVersionName string `json:"layerVersionName" valid:",optional"`
	Result           string `json:"result" valid:",optional"`
}

// LayerPreloadResultArray -
type LayerPreloadResultArray struct {
	Results []LayerPreloadResult `json:"results" valid:",optional"`
}

// ClusterBridgeInstance { "functionUrn": "gotest", "bridgeInstances": { "cluster1": { "instance1": true } } }
type ClusterBridgeInstance struct {
	FunctionURN     string                     `json:"functionUrn"`
	BridgeInstances map[string]map[string]bool `json:"bridgeInstances"`
}

// BridgeResourceMetrics -
type BridgeResourceMetrics struct {
	MaxActiveNumber     int32 `json:"maxActiveNumber"`
	MinActiveNumber     int32 `json:"minActiveNumber"`
	CurrentActiveNumber int32 `json:"currentActiveNumber"`
	MaxTotal            int32 `json:"maxTotal"`
	// executeTime send to redis unit is microseconds
	MaxExecuteTime     float64 `json:"maxExecuteTime"`
	MinExecuteTime     float64 `json:"minExecuteTime"`
	AverageExecuteTime float64 `json:"averageExecuteTime"`
}

// BridgeReportMetrics -
type BridgeReportMetrics struct {
	NumActive int32 `json:"numActive"`
	MaxTotal  int32 `json:"maxTotal"`
	MaxIdle   int32 `json:"maxIdle"`
	NumIdle   int32 `json:"numIdle"`
	// executeTimeList unit is nanoseconds.
	ExecuteTimeList []int64 `json:"executeTimeList"`
}
