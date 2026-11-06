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

// Package config define config of worker-manager
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asaskevich/govalidator"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/trace"
	"functioncore/pkg/common/types"
)

const (
	upLimitSize = 100
)

var (
	// MonitoringPasswd -
	MonitoringPasswd []byte
	configWatcher    monitor.FileWatcher

	// SSDPoolLabelsMap -
	SSDPoolLabelsMap = sync.Map{}
)

// NodeInfo describe CPU and MEMORY info of node
type NodeInfo struct {
	CPUTotal             int64 `json:"cpu_total,omitempty" valid:"int,optional"`
	CPUUsed              int64 `json:"cpu_used,omitempty" valid:"int,optional"`
	MemoryTotal          int64 `json:"memory_total,omitempty" valid:"int,optional"`
	MemoryUsed           int64 `json:"memory_used,omitempty" valid:"int,optional"`
	UsedPods             int64 `json:"-" valid:"int,optional"`
	RequestNvidiaGPU     int64 `json:"-"`
	RequestGPUPercentage int64 `json:"-"`
	RequestGPUMem128     int64 `json:"-"`
}

// ClusterUsage describe CPU usage of cluster
type ClusterUsage struct {
	ClusterID string
	CPUTotal  int64 `json:"cpu_total,omitempty" valid:"int,optional"`
	CPUUsed   int64 `json:"cpu_used,omitempty" valid:"int,optional"`
}

// RuntimeConfig define runtime config
type RuntimeConfig struct {
	Language    string `json:"language,omitempty" valid:"stringlength(1|63),optional"`
	DeployImage string `json:"deployImage,omitempty" valid:"stringlength(1|255),optional"`
	WorkerImage string `json:"workerImage,omitempty" valid:"stringlength(1|255),optional"`
}

// AutoScalerConfig define autoscaler config
type AutoScalerConfig struct {
	Name             string `json:"name,omitempty" valid:"stringlength(1|255),optional"`
	Type             string `json:"type,omitempty" valid:"stringlength(1|255),optional"`
	IsScaleUp        bool   `json:"isScaleUp,omitempty" valid:",optional"`
	Duration         string `json:"duration,omitempty" valid:"stringlength(1|255),optional"`
	RepeatInterval   string `json:"repeatInterval,omitempty" valid:"stringlength(1|255),optional"`
	Tolerance        int64  `json:"tolerance,omitempty" valid:"int,optional"`
	MinimumScale     int64  `json:"minimumScale,omitempty" valid:"int,optional"`
	MaximumScale     string `json:"maximumScale,omitempty" valid:"stringlength(1|63),optional"`
	MetricsCondition string `json:"metricsCondition,omitempty" valid:"stringlength(1|63),optional"`
	MetricsType      string `json:"metricsType,omitempty" valid:"stringlength(1|63),optional"`
	ProjectID        string `json:"projectId,omitempty" valid:"stringlength(1|32),optional"`
	ScaleStep        int64  `json:"scaleStep,omitempty" valid:"int,optional"`
	WorkloadID       string `json:"workloadId,omitempty" valid:"stringlength(1|63),optional"`
}

// ResourceLockLeaseConfig resource lock lease config
type ResourceLockLeaseConfig struct {
	LeaseDuration int `json:"leaseDuration"       valid:"optional"`
	RenewDeadline int `json:"renewDeadline"       valid:"optional"`
	RetryPeriod   int `json:"retryPeriod"         valid:"optional"`
}

// FaasConfig define faas config
type FaasConfig struct {
	LocalAuth                     *localauth.AuthConfig              `json:"localAuth" valid:"optional"`
	HTTPSConfig                   *tls.InternalHTTPSConfig           `json:"httpsConfig" valid:"optional"`
	AutoScalerConfig              []AutoScalerConfig                 `json:"autoScalerConfig,omitempty" valid:",optional"`
	Etcd                          *etcd3.ClientConfig                `json:"etcd" valid:"required"`
	RouterETCD                    *etcd3.ClientConfig                `json:"routerETCD" valid:"required"`
	CAEMetaEtcd                   *etcd3.ClientConfig                `json:"caeMetaEtcd" valid:"optional"`
	CAERouterEtcd                 *etcd3.ClientConfig                `json:"caeRouterEtcd,omitempty" valid:",optional"`
	MetaEtcdProxy                 *etcd3.ClientConfig                `json:"metaEtcdProxy" valid:"optional"`
	CAEMetaEtcdProxy              *etcd3.ClientConfig                `json:"caeMetaEtcdProxy" valid:"optional"`
	EtcdCluser                    EtcdClusterConfig                  `json:"etcdcluster,omitempty" valid:",optional"`
	PoolMgrConfig                 PoolMgrConfig                      `json:"poolMgrConfig,omitempty" valid:",optional"`
	CaeCustomizePoolSpec          map[string]PoolSpec                `json:"caeCustomizePoolSpec" valid:",optional"`
	CustomJavaArgsWhiteList       []string                           `json:"customJavaArgsWhiteList" valid:",optional"`
	InstanceTimeoutExitWhiteList  []string                           `json:"instanceTimeoutExitWhiteList" valid:",optional"`
	BusProtocolType               string                             `json:"busProtocolType,omitempty"`
	NameSpace                     string                             `json:"namespace" valid:"stringlength(1|63),optional"`
	AuthenticationEnable          bool                               `json:"authenticationEnable" valid:"optional"`
	FunctionNameSeparator         string                             `json:"functionNameSeparator" valid:"optional"`
	KubeCfgTimeout                int64                              `json:"kubeCfgTimeout,omitempty" valid:",optional"`
	PodGeneralizeTimeout          int                                `json:"podGeneralizeTimeout,omitempty" valid:",optional"`
	MonitoringServerConfig        MonitoringRedisServerConfig        `json:"monitoringServerConfig,omitempty" valid:",optional"`
	K8sCidr                       K8sCidr                            `json:"k8sCidr" valid:"required"`
	DeployDir                     string                             `json:"deployDir" valid:"stringlength(1|255)"`
	AlarmServerAddress            string                             `json:"alarmServerAddress" valid:"optional"`
	EnableRemoteWorkerFlag        bool                               `json:"enableRemoteWorkerFlag" valid:"optional"`
	SmartWarmerEnable             bool                               `json:"smartWarmerEnable" valid:"optional"`
	HeterogeneousSchedulingConfig HeterogeneousSchedulingConfig      `json:"heterogeneousSchedulingConfig" valid:"optional"`
	NodeBalance                   NodeBalance                        `json:"nodeBalance" valid:"optional"`
	Resync                        ResyncConfig                       `json:"resync" valid:"optional"`
	PoolMgrProactiveConfig        *PoolManagerProactiveElasticConfig `json:"proactivePoolMgrElasticConfig" valid:"optional"`
	ProactiveEnableList           map[string]bool                    `json:"proactiveEnableList" valid:"optional"`
	ProactiveElasticScalingConfig *ElasticScalingConfig              `json:"proactiveElasticScalingConfig" valid:"optional"`
	ElasticScalingConfig          *ElasticScalingConfig              `json:"elasticScalingConfig" valid:"optional"`
	ResourceLockLeaseConfig       *ResourceLockLeaseConfig           `json:"resourceLockLeaseConfig" valid:"optional"`
	ImageAffinityCacheConfig      *ImageAffinityCacheConfig          `json:"imageAffinityCacheConfig" valid:"optional"`
	BinPackEnable                 bool                               `json:"binPackEnable" valid:"optional"`
	SchedulingEnable              bool                               `json:"schedulingEnable" valid:"optional"`
	WorkerMetricsTick             int64                              `json:"workerMetricsTick" valid:"optional"`
	DisableEtcdRev                uint32                             `json:"disableEtcdRev,omitempty" valid:"optional"`
	DisableWaitBeforeChangeNet    bool                               `json:"disableWaitBeforeChangeNet,omitempty" valid:"optional"`
	TaintsNodeCheckInterval       int64                              `json:"taintsNodeCheckInterval,omitempty" valid:"optional"`
	BodySizeLimitWhitelist        map[string]int64                   `json:"bodySizeLimitWhitelist" valid:"optional"`
	UpdateFunctionCacheEnable     bool                               `json:"updateFunctionCacheEnable" valid:"optional"`
	// enable worker-manager upload used pod records to Redis
	EnableUploadUsedPodRecordToRedis bool              `json:"enableUploadUsedPodRecordToRedis" valid:"optional"`
	ResourceUsageTick                int64             `json:"resourceUsageTick" valid:"optional"`
	CaeMetricsTick                   int64             `json:"caeMetricsTick" valid:"optional"`
	DecoyFileEnabled                 bool              `json:"decoyFileEnabled,omitempty" valid:"optional"`
	ScheduleWithDisk                 bool              `json:"scheduleWithDisk" valid:"optional"`
	RetryTimeWhenNoSpace             int64             `json:"retryTimeWhenNoSpace" valid:"optional"`
	SSDPoolLabels                    map[string]string `json:"ssdPoolLabels" valid:",optional"`
	NpmConfigRegistry                string            `json:"npm_config_registry" valid:"optional"`
	UVIndexURL                       string            `json:"uv_index_url" valid:"optional"`
}

// K8sCidr describe the cluster network features.
type K8sCidr struct {
	SvcCidr  string `json:"svcCidr" valid:"required"`
	PodCidr  string `json:"podCidr" valid:"required"`
	HostCidr string `json:"hostCidr" valid:"required"`
}

// MonitoringRedisServerConfig define monitoring server config
type MonitoringRedisServerConfig struct {
	ServerAddr  string                  `json:"serverAddr,omitempty" valid:",optional"`
	ServerMode  string                  `json:"serverMode,omitempty" valid:",optional"`
	Password    string                  `json:"password,omitempty" valid:",optional"`
	EnableTLS   bool                    `json:"enableTLS,omitempty" valid:",optional"`
	ClusterID   string                  `json:"clusterID,omitempty" valid:",optional"`
	TimeoutConf redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
}

// EtcdClusterConfig define etcd cluster config
type EtcdClusterConfig struct {
	ElectionTTL    int  `json:"electionttl,omitempty" valid:"int,optional"`
	ForwardRequest bool `json:"forwardrequest,omitempty" valid:",optional"`
}

// HeterogeneousSchedulingConfig define heterogeneous scheduling config
type HeterogeneousSchedulingConfig struct {
	Enable   bool   `json:"enable,omitempty" valid:",optional"`
	Strategy string `json:"strategy,omitempty" valid:",optional"`
}

// MultiSnapshotConfig define multi snapshot config
type MultiSnapshotConfig struct {
	Enable            bool                    `json:"enable,omitempty" valid:",optional"`
	SnapshotCheckInfo types.SnapshotCheckInfo `json:"snapshotCheckInfo,omitempty" valid:",optional"`
}

// SnapshotTenantIDsConfig define snapshot tenantID config
type SnapshotTenantIDsConfig struct {
	Enable    bool     `json:"enable,omitempty" valid:",optional"`
	TenantIDs []string `json:"tenantIDs,omitempty" valid:",optional"`
}

// FunctionMetaInfo define function meta info
type FunctionMetaInfo struct {
	FuncMetaData     FuncMetaData                 `json:"funcMetaData" valid:",optional"`
	S3MetaData       S3MetaData                   `json:"s3MetaData" valid:",optional"`
	SnapshotMetaData types.SnapshotDeploymentSpec `json:"snapshotMetaData" valid:",optional"`
	EnvMetaData      EnvMetaData                  `json:"envMetaData" valid:",optional"`
	ResourceMetaData ResourceMetaData             `json:"resourceMetaData" valid:",optional"`
	InstanceMetaData InstanceMetaData             `json:"instanceMetaData" valid:",optional"`
	ExtendedMetaData ExtendedMetaData             `json:"extendedMetaData" valid:",optional"`
	ProfileMetaData  types.ProfileMetaData        `json:"profileMetaData" valid:",optional"`
}

// FuncMetaData define meta data of functions
type FuncMetaData struct {
	Layers                  []*types.Layer        `json:"layers" valid:",optional"`
	Name                    string                `json:"name"`
	FunctionDescription     string                `json:"description" valid:"stringlength(1|1024)"`
	FunctionURN             string                `json:"functionUrn"`
	ReversedConcurrency     int                   `json:"reversedConcurrency" valid:"int"`
	Tags                    map[string]string     `json:"tags" valid:",optional"`
	FunctionUpdateTime      string                `json:"functionUpdateTime" valid:",optional"`
	FunctionVersionURN      string                `json:"functionVersionUrn"`
	CodeSize                int                   `json:"codeSize" valid:"int"`
	CodeSha256              string                `json:"codeSha256" valid:"stringlength(1|64),optional"`
	CodeSha512              string                `json:"codeSha512" valid:"stringlength(1|128),optional"`
	Handler                 string                `json:"handler" valid:"stringlength(1|255)"`
	Runtime                 string                `json:"runtime" valid:"stringlength(1|63)"`
	Timeout                 int64                 `json:"timeout" valid:"required"`
	Version                 string                `json:"version" valid:"stringlength(1|42)"`
	VersionDescription      string                `json:"versionDescription" valid:"stringlength(1|1024)"`
	DeadLetterConfig        string                `json:"deadLetterConfig" valid:"stringlength(1|255)"`
	LatestVersionUpdateTime string                `json:"latestVersionUpdateTime" valid:",optional"`
	PublishTime             string                `json:"publishTime" valid:",optional"`
	BusinessID              string                `json:"businessId"  valid:"stringlength(1|32)"`
	MetaID                  string                `json:"id" valid:",optional"`
	FuncID                  string                `json:"func_id" valid:",optional"`
	FuncName                string                `json:"func_name" valid:",optional"`
	DomainID                string                `json:"domain_id" valid:",optional"`
	ProjectName             string                `json:"project_name" valid:",optional"`
	Service                 string                `json:"service" valid:",optional"`
	Dependencies            string                `json:"dependencies" valid:",optional"`
	RevisionID              string                `json:"revisionId" valid:"stringlength(1|20),optional"`
	CreateTime              string                `json:"created" valid:",optional"`
	VPCTriggerImage         string                `json:"vpcTriggerImage" valid:",optional"`
	EnableAuthInHeader      bool                  `json:"enable_auth_in_header" valid:"optional"`
	IsStatefulFunction      bool                  `json:"isStatefulFunction" valid:"optional"`
	DNSDomainCfg            []types.DNSDomainInfo `json:"dns_domain_cfg" valid:",optional"`
	EndpointDomainCfg       []types.DNSDomainInfo `json:"endpoint_domain_cfg" valid:",optional"`
	IsStreamEnable          bool                  `json:"isStreamEnable" valid:"optional"`
	StreamResponse          bool                  `json:"stream_response" valid:"optional"`
	IsBridgeFunction        bool                  `json:"isBridgeFunction" valid:"optional"`
	DNSConfig               types.DNSConfig       `json:"dns_config"`
	EnableSnapshot          string                `json:"enable_snapshot" valid:"optional"`
	DisableInsecureAkSk     bool                  `json:"disable_insecure_ak_sk" valid:",optional"`
	BusinessType            string                `json:"businessType" valid:"optional"`
	LogPolicy               types.LogPolicy       `json:"logPolicy" valid:"optional"`
	EnableRemoteDebug       bool                  `json:"enableRemoteDebug"`
	RemoteDebugArgs         string                `json:"remoteDebugArgs"`
	LtsCustomTag            map[string]string     `json:"lts_custom_tag" valid:"optional"`
}

// S3MetaData define meta function info for OBS
type S3MetaData struct {
	AppID                   string   `json:"appId" valid:"stringlength(1|128),optional"`
	BucketID                string   `json:"bucketId" valid:"stringlength(1|255),optional"`
	ObjectID                string   `json:"objectId" valid:"stringlength(1|255),optional"`
	BucketURL               string   `json:"bucketUrl" valid:"url,optional"`
	CodeType                string   `json:"code_type" valid:",optional"`
	CodeURL                 string   `json:"code_url" valid:",optional"`
	CodeFileName            string   `json:"code_filename" valid:",optional"`
	FuncCode                FuncCode `json:"func_code" valid:",optional"`
	CodePlaintextKmsCipher  string   `json:"code_plaintext_kms_key" valid:",optional"`
	CodeEncryptKmsKeyID     string   `json:"code_encrypt_kms_key_id" valid:",optional"`
	CodeEncryptKmsKeyCipher string   `json:"code_encrypt_kms_key_cipher" valid:",optional"`
}

// EnvMetaData define env info of functions
type EnvMetaData struct {
	EnvKey                      string `json:"envKey,omitempty" valid:",optional"`
	Environment                 string `json:"environment" valid:",optional"`
	EncryptedUserData           string `json:"encrypted_user_data" valid:",optional"`
	UserDataEncryptKmsKeyID     string `json:"user_data_encrypt_kms_key_id" valid:",optional"`
	UserDataEncryptKmsKeyCipher string `json:"user_data_encrypt_kms_key_cipher" valid:",optional"`
}

// ResourceMetaData include resource data such as cpu and memory
type ResourceMetaData struct {
	CPU                int64  `json:"cpu"`
	Memory             int64  `json:"memory"`
	GpuMemory          int64  `json:"gpu_memory"`
	GpuType            string `json:"gpu_type"`
	EphemeralStorage   int64  `json:"ephemeral_storage"`
	CustomResources    string `json:"customResources" valid:",optional"`
	EnableTmpExpansion bool   `json:"enable_tmp_expansion" valid:",optional"`
}

// InstanceMetaData define instance meta data of functions
type InstanceMetaData struct {
	IdleMode      bool   `json:"idleMode" valid:",optional"`
	MaxInstance   int64  `json:"maxInstance" valid:",optional"`
	MinInstance   int64  `json:"minInstance" valid:",optional"`
	ConcurrentNum int64  `json:"concurrentNum" valid:",optional"`
	InstanceType  string `json:"instanceType" valid:",optional"`
}

// TenantInstanceMetaData define tenant instance quota
type TenantInstanceMetaData struct {
	MaxOnDemandInstance int64 `json:"maxOnDemandInstance" valid:",optional"`
	MaxReversedInstance int64 `json:"maxReversedInstance" valid:",optional"`
}

// TenantMetaInfo define tenant meta info
type TenantMetaInfo struct {
	TenantInstanceMetaData TenantInstanceMetaData `json:"tenantInstanceMetaData" valid:",optional"`
}

// PredictionInfo -
type PredictionInfo struct {
	Num int `json:"num"`
}

// PredictionNotify -
type PredictionNotify struct {
	PredictionTime  int64 `json:"predictionTime" valid:",required"`
	ValidTimeMinute int   `json:"validTimeMinute" valid:",required"`
}

// TenantVpcRouter vpc router config of tenant
type TenantVpcRouter struct {
	TenantVpcCidr string   `json:"tenantVpcCidr"`
	Gateways      []string `json:"gateways"`
}

// ExtendedMetaData define external meta data of functions
type ExtendedMetaData struct {
	ImageName             string                      `json:"image_name" valid:",optional"`
	Role                  types.Role                  `json:"role" valid:",optional"`
	FuncVpc               Vpc                         `json:"func_vpc" valid:",optional"`
	EndpointTenantVpc     Vpc                         `json:"endpoint_tenant_vpc" valid:",optional"`
	FuncMountConfig       types.FuncMountConfig       `json:"mount_config" valid:",optional"`
	StrategyConfig        StrategyConfig              `json:"strategy_config" valid:",optional"`
	ExtendConfig          string                      `json:"extend_config" valid:",optional"`
	Initializer           types.Initializer           `json:"initializer" valid:",optional"`
	EnterpriseProjectID   string                      `json:"enterprise_project_id" valid:",optional"`
	LogTankService        logservice.LogTankService   `json:"log_tank_service" valid:",optional"`
	TraceService          trace.TraceService          `json:"tracing_config" valid:",optional"`
	CustomContainerConfig types.CustomContainerConfig `json:"custom_container_config" valid:",optional"`
	Heartbeat             types.Heartbeat             `json:"heartbeat" valid:",optional"`
	StartupHeartbeat      types.Heartbeat             `json:"startupHeartbeat" valid:",optional"`
	RestoreHook           types.RestoreHook           `json:"restore_hook,omitempty" valid:",optional"`
	NetworkController     types.NetworkController     `json:"network_controller" valid:",optional"`
	EnableClassIsolation  bool                        `json:"enable_class_isolation"`
	EnableGCLog           bool                        `json:"enable_gc_log" valid:",optional"`
	ApigRouteEnable       bool                        `json:"apigRouteEnable" valid:"optional"`
	PreStop               types.PreStop               `json:"pre_stop" valid:",optional"`
	CustomMetricsReport   types.CustomMetricsReport   `json:"customMetricsReport" valid:",optional"`
	EnableCustomRPC       bool                        `json:"enableCustomRpc" valid:",optional"`
	FileMountConfig       FileMountConfig             `json:"file_mount_config" valid:",optional"`
	CustomJavaArgs        string                      `json:"customJavaArgs"  valid:"optional"`
	Properties            map[string]string           `json:"properties"  valid:"optional"`
	FgsTags               map[string]string           `json:"fgs_tags"  valid:"optional"`
	IsMcpFunction         bool                        `json:"is_mcp_function" valid:"optional"`
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

// VpcControllerRequester define request message for vpc_controller
type VpcControllerRequester struct {
	TraceID         string   `json:"trace_id"`
	Delegate        Delegate `json:"delegate"`
	Vpc             Vpc      `json:"vpc"`
	NeedInit        bool     `json:"need_init"`
	NeedNewVersion  bool     `json:"need_new_version"`
	NeedPublicPat   bool     `json:"need_public_pat"`
	PublicBandWidth string   `json:"public_bandwidth"`
}

// Vpc include info of function vpc
type Vpc struct {
	ID             string   `json:"id,omitempty"`
	DomainID       string   `json:"domain_id,omitempty"`
	Namespace      string   `json:"namespace,omitempty"`
	VpcName        string   `json:"vpc_name,omitempty"`
	VpcID          string   `json:"vpc_id,omitempty"`
	SubnetName     string   `json:"subnet_name,omitempty"`
	SubnetID       string   `json:"subnet_id,omitempty"`
	TenantCidr     string   `json:"tenant_cidr,omitempty"`
	HostVMCidr     string   `json:"host_vm_cidr,omitempty"`
	Gateway        string   `json:"gateway,omitempty"`
	Xrole          string   `json:"xrole,omitempty"`
	SecurityGroups []string `json:"security_groups"`
	// support IPV6
	IPV6Enable        bool   `json:"ipv6_enable"`
	IPV6TenantCidr    string `json:"ipv6_tenant_cidr,omitempty"`
	IPV6HostVMCidr    string `json:"ipv6_host_vm_cidr,omitempty"`
	IPV6Gateway       string `json:"ipv6_gateway,omitempty"`
	NeutronSubnetID   string `json:"neutron_subnet_id,omitempty"`
	NeutronSubnetIDV6 string `json:"neutron_subnet_id_v6,omitempty"`
	IsSafety          bool   `json:"is_safety"`
}

// PatPodRequestParam -
type PatPodRequestParam struct {
	FuncSvcID          string
	PodName            string
	TraceID            string
	Timeout            time.Duration
	PatPodInfo         []types.NATConfigure
	EndpointPatPodInfo []types.NATConfigure
	PublicPatPodInfo   []types.NATConfigure
}

// Delegate include Xrole and AppXrole
type Delegate struct {
	Xrole    string `json:"xrole,omitempty"`
	AppXrole string `json:"app_xrole,omitempty"`
}

// FuncCode include function code file and link info
type FuncCode struct {
	File string `json:"file" valid:",optional"`
	Link string `json:"link" valid:",optional"`
}

// StrategyConfig include concurrency of function
type StrategyConfig struct {
	Concurrency int `json:"concurrency" valid:",optional"`
}

// PoolMgrConfig defines pool manager config
type PoolMgrConfig struct {
	EnableLocalCache             bool `json:"enableLocalCache" valid:",optional"`
	EnableMinio                  bool `json:"enableMinio" valid:",optional"`
	EnablePublicNetworkPat       bool `json:"enablePublicNetworkPat" valid:",optional"`
	PoolContainerTags            PoolContainerImageTags
	PoolList                     PoolList                `json:"poolList,omitempty" valid:",optional"`
	GcConfig                     GcConfig                `json:"gcConfig,omitempty" valid:",optional"`
	VolumeConfig                 VolumeConfig            `json:"volumeConfig,omitempty" valid:",optional"`
	CustomCAConfig               CustomCAConfig          `json:"customCAConfig,omitempty" valid:",optional"`
	IsSharedContainer            bool                    `json:"isShareContainer,omitempty" valid:",optional"`
	WorkerCaps                   ContainerCapabilities   `json:"workerCaps" valid:",optional"`
	RuntimeCaps                  ContainerCapabilities   `json:"runtimeCaps" valid:",optional"`
	WorkerAuthEnable             bool                    `json:"workerAuthEnable,omitempty" valid:",optional"`
	PoolRecordingCFG             PoolRecordingConfig     `json:"poolRecordingCFG,omitempty" valid:",optional"`
	CustomContainerConfig        CustomContainerConfig   `json:"customContainerConfig,omitempty" valid:",optional"`
	CheckpointPodConfig          CheckpointPodConfig     `json:"checkpointPodConfig,omitempty" valid:",optional"`
	GPUPodConfig                 GPUPodConfig            `json:"gpuPodConfig,omitempty" valid:",optional"`
	ReservedAntiAffinity         string                  `json:"reservedAntiAffinity,omitempty" valid:",optional"`
	DisablePriorityQueue         uint32                  `json:"disablePriorityQueue,omitempty"`
	DisableCircuitBreaker        uint32                  `json:"disableCircuitBreaker,omitempty"`
	MultiSnapshotConfig          MultiSnapshotConfig     `json:"multiSnapshotConfig,omitempty"`
	SnapshotTenantIDsConfig      SnapshotTenantIDsConfig `json:"snapshotTenantIDsConfig,omitempty"`
	CoreCPUFeatures              string                  `json:"coreCPUFeatures,omitempty" valid:",optional"`
	WorkerContentSignatureEnable bool                    `json:"workerContentSignatureEnable,omitempty" valid:",optional"`
	DisablePoolRollout           uint32                  `json:"disablePoolRollout"`
}

// GPUPodConfig -
type GPUPodConfig struct {
	GPUMemoryCapacity map[string]int64 `json:"gpuMemoryCapacity" valid:",optional"`
	WaitReadyTimeout  int64            `json:"waitReadyTimeout" valid:",optional"`
}

// ImageAffinityCacheConfig -
type ImageAffinityCacheConfig struct {
	DisableImageAffinityCache uint32   `json:"disableImageAffinityCache" valid:"optional"`
	TenantWhiteList           []string `json:"tenantWhiteList" valid:"optional"`
	CacheNodesNumber          int      `json:"cacheNodesNumber" valid:"optional"`
	CacheNodesPercentage      float64  `json:"cacheNodesPercentage" valid:"optional"`
}

// ElasticScalingConfig elastic scaling config of pool pod
type ElasticScalingConfig struct {
	// exceed percentage of all pods will scale up and will change by pressure
	UsedPodNumsExceedPercentage float64 `json:"usedPodNumsExceedPercentage" valid:",optional"`
	// scale up pod nums percentage of all pods and will change by pressure
	ScaleUpPodNumsPercentage float64 `json:"scaleUpPodNumsPercentage" valid:",optional"`
	// extra scale up pod nums percentage of all pods
	ExtraScaleUpPodNumsPercentage float64 `json:"extraScaleUpPodNumsPercentage" valid:",optional"`
	// below percentage of all pods will scale down
	UsedPodNumsBelowPercentage float64 `json:"usedPodNumsBelowPercentage" valid:",optional"`
	// scale down pod nums percentage of all pods
	ScaleDownPodNumsPercentage float64 `json:"scaleDownPodNumsPercentage" valid:",optional"`
	// effective threshold replicas when it begins to scale up and scale down
	EffectiveThresholdReplicas int32 `json:"effectiveThresholdReplicas" valid:",optional"`
	// max scale up pod nums per standard pool
	MaxScaleUpPodNums int32 `json:"maxScaleUpPodNums" valid:",optional"`
	// unit: seconds
	ScaleDownInterval int `json:"scaleDownInterval" valid:",optional"`
	// cluster average resource usage threshold, if usage exceeds the threshold, it will not scale up pool replicas
	ThresholdResourceUsage float64 `json:"thresholdResourceUsage" valid:",optional"`
	// exceed percentage in free condition
	UsedPodNumsExceedFreePercentage float64 `json:"usedPodNumsExceedFreePercentage" valid:",optional"`
	// exceed percentage in heavy condition
	UsedPodNumsExceedHeavyPercentage float64 `json:"usedPodNumsExceedHeavyPercentage" valid:",optional"`
	// scale up pod nums percentage in free condition
	ScaleUpPodNumsFreePercentage float64 `json:"scaleUpPodNumsFreePercentage" valid:",optional"`
	// scale up pod nums percentage of all pods
	ScaleUpPodNumsHeavyPercentage float64 `json:"scaleUpPodNumsHeavyPercentage" valid:",optional"`
	// scale up percentage when pool replica is zero
	MaxTransitionScaleUpPercentage float64 `json:"maxTransitionScaleUpPercentage" valid:",optional"`
}

// PoolRecordingConfig pool recording config
type PoolRecordingConfig struct {
	// record pool info to redis by interval, unit is second, default is 5s
	RecordingInterval int `json:"recordingInterval" valid:",optional"`
	// set record expired time in redis, unit is second, default is 900s
	RecordExpiredTime int `json:"recordExpiredTime" valid:",optional"`
}

// CustomContainerConfig for custom-container. Unit is second.
type CustomContainerConfig struct {
	WaitReadyTimeout     *int64 `json:"waitReadyTimeout" valid:",optional"`
	UnschedulableTimeout *int64 `json:"unschedulableTimeout" valid:",optional"`
	CustomImageClass     string `json:"customImageClass" valid:",optional"`
	DisableSWRAddOn      bool   `json:"disableSWRAddOn" valid:",optional"`
}

// CheckpointPodConfig for checkpoint-pod. Unit is second.
type CheckpointPodConfig struct {
	WaitReadyTimeout        int64  `json:"waitReadyTimeout" valid:",optional"`
	FailureRetryInterval    int    `json:"failureRetryInterval" valid:",optional"`
	FailureRetryTimes       int    `json:"failureRetryTimes" valid:",optional"`
	MaxConcurrency          int    `json:"maxConcurrency" valid:"range(0|1000),optional"`
	CRIUCheckpointOpts      string `json:"criuCheckpointOpts" valid:",optional"`
	RestoreFailureThreshold int64  `json:"restoreFailureThreshold" valid:"range(0|1000),optional"`
	SnapshotStorageType     string `json:"snapshotStorageType" valid:",optional"`
	// TODO: CPUExpansionPercent/MemExpansionPercent/StorageExpansionPercent
}

// NodeBalance config for NodeBalance
type NodeBalance struct {
	Disabled               uint32 `json:"disabled"`
	RefreshInterval        int64  `json:"refreshInterval"`
	NodePressureThreshold  int64  `json:"nodePressureThreshold"`
	CPUPercentThreshold    int64  `json:"cpuPercentThreshold"`
	CPUPercentLowThreshold int64  `json:"cpuPercentLowThreshold"`
	ExpectNodeCPUPercent   int64  `json:"expectNodeCPUPercent"`
}

// ResyncConfig config for resyncer
type ResyncConfig struct {
	Interval int64  `json:"interval"`
	SyncNow  int64  `json:"syncNow"`
	Disabled uint32 `json:"disabled"`
}

// ContainerCapabilities for runtime and worker
type ContainerCapabilities struct {
	Add  []string `json:"add" valid:",optional"`
	Drop []string `json:"drop" valid:",optional"`
}

// PoolContainerImageTags for pool container ImageTag
type PoolContainerImageTags struct {
	WorkerInitImageTag string
	WorkerImageTag     string
	RuntimeImageTag    string
	DebugAgentImageTag string
}

// PoolList defines PoolList struct
type PoolList struct {
	NodeAffinity *NodeAffinity      `json:"nodeAffinity" valid:"optional"`
	PodGcTime    int32              `json:"podgctime" valid:"required"`
	HostAliases  []corev1.HostAlias `json:"hostaliaseshostname" valid:"required"`
	Pool         []PoolConfig       `json:"pool,omitempty" valid:"required"`
}

// NodeAffinity defines node where the pool is placed
type NodeAffinity struct {
	NodeAffinityKey   string   `json:"nodeAffinityKey" valid:"required"`
	NodeAffinityValue []string `json:"nodeAffinityValue" valid:"required"`
}

// PoolConfig defines PoolConfig struct
type PoolConfig struct {
	Name                          string            `json:"name" valid:"required"`
	RunTimeName                   string            `json:"runtimename" valid:"required"`
	LimitCPU                      int64             `json:"limitcpu" valid:"int,required"`
	LimitMemory                   int64             `json:"limitmem" valid:"int,required"`
	RequestCPU                    int64             `json:"requestcpu" valid:"int,required"`
	RequestMemory                 int64             `json:"requestmem" valid:"int,required"`
	ActualCPU                     int64             `json:"actualcpu" valid:"int,optional"`
	EphemeralStorage              int64             `json:"ephemeralStorage" valid:"int,optional"`
	LivenessProbePeriodSeconds    int32             `json:"livenessProbePeriodSeconds" valid:"int,optional"`
	LivenessProbeFailureThreshold int32             `json:"livenessProbeFailureThreshold" valid:"int,optional"`
	PoolSize                      int32             `json:"poolsize" valid:"-"`
	PoolScaleLimit                float64           `json:"poolScaleLimit" valid:"-"`
	CustomResources               map[string]int64  `json:"customResources" valid:"optional"`
	Annotations                   map[string]string `json:"annotations" valid:"required"`
	Label                         map[string]string `json:"label,omitempty" valid:"optional"`
	NonIsolated                   bool              `json:"nonIsolated" valid:"optional"`
}

// GcConfig defines GcConfig struct
type GcConfig struct {
	TerminationGracePeriodSeconds int64 `json:"terminationGracePeriodSeconds,omitempty" valid:"int,optional"`
}

// ClusterConfig define standby cluster election leader
type ClusterConfig struct {
	Port           string `json:"port" valid:"required"`
	IsClusterMode  bool   `json:"clusterMode" valid:"required"`
	ClusterType    string `json:"clusterType" valid:"required"`
	ForwardRequest bool   `json:"forwardRequest" valid:"required"`
}

// VolumeConfig defines volume of worker
type VolumeConfig struct {
	// logMountPath is used by ES to collect logs
	LogMountPath             string                    `json:"logMountPath" valid:"required"`
	UserLogMountPath         string                    `json:"userLogMountPath" valid:"optional"`
	RuntimeLogMountPath      string                    `json:"runtimeLogMountPath" valid:"required"`
	RuntimeMgrLogPath        string                    `json:"runtimeMgrLogPath" valid:"optional"`
	LogHostPath              LogHostPath               `json:"logHostPath" valid:"optional"`
	PackageHostPath          string                    `json:"packageHostPath,omitempty" valid:"optional"`
	CustomResourceMountPaths []CustomResourceMountPath `json:"customResourceMountPaths,omitempty" valid:"optional"`
}

// LogHostPath -
type LogHostPath struct {
	UserLog    string `json:"userLog" valid:",optional"`
	ServiceLog string `json:"serviceLog" valid:",optional"`
}

// CustomResourceMountPath defines custom resource mount path
type CustomResourceMountPath struct {
	Name          string `json:"name" valid:"required"`
	ContainerPath string `json:"containerPath" valid:"required"`
	HostPath      string `json:"hostPath" valid:"required"`
}

// CustomCAConfig defines volume of worker
type CustomCAConfig struct {
	CustomCAEnable bool   `json:"customCAEnable,omitempty" valid:",optional"`
	CaFilePath     string `json:"caFilePath,omitempty" valid:",optional"`
}

// RequestInfo Request info
type RequestInfo struct {
	SnapshotDeployment       *types.SnapshotDeploymentSpec
	PodTemplate              *corev1.Pod
	PatPodInfo               []types.NATConfigure
	EndpointTalentPatPodInfo []types.NATConfigure
	PublicPatPodInfo         []types.NATConfigure
	Namespace                string
	URN                      string
	FunctionName             string
	BusinessID               string `json:"businessId"`
	TenantID                 string `json:"tenantId"`
	FunctionVersion          string `json:"functionVersion"`
	Creator                  string `json:"creator"`
	WorkerID                 string
	CallType                 string
	TraceID                  string
	// for OnDemand instance
	Applier     string `json:"applier"`
	DeployNode  string `json:"deployNode"`
	OwnerIP     string `json:"ownerIp"`
	ForceDeploy string
	NodeLabel   string
	TraceParent string
	// for duration
	StartTime time.Time
	// GetPod/SpecifyPod/DeployFunction/DeployLayers/StartFunction/DescribePod
	Duration        string
	TriggerFlag     string
	OwnedWorkerView types.WorkersView
	CPU             int64  `json:"cpu"`
	Memory          int64  `json:"memory"`
	StateID         string `json:"stateID,omitempty"`
	IsFirstInstance bool
	IsMigrating     bool
	MigratedFrom    string
	LoadErr         *snerror.BadResponse `json:"-"`
	// IsReload
	IsReload      bool
	ExpectedNum   int                       `json:"expected_num,omitempty"`
	CreationTime  int64                     `json:"creationTime,omitempty"`
	CreateOptions *types.StateCreateOptions `json:"createOptions,omitempty"`
}

var (
	configOnce             sync.Once
	configPath             = "/home/sn/config/worker-manager.conf"
	configInstance         *FaasConfig
	configChangedCallbacks = make([]ChangedCallback, 0)
)

// ChangedCallback config changed callback func
type ChangedCallback func()

// InitConfigPath Set Config Path
func InitConfigPath(path string) {
	configPath = path
}

// GetConfig singleton pattern thread safe
func GetConfig() (*FaasConfig, error) {
	var err error
	configOnce.Do(func() {
		configInstance, err = newConfig(configPath)
		if err != nil {
			log.GetLogger().Errorf("config files error: %s", err.Error())
			return
		}
		_, err = govalidator.ValidateStruct(configInstance)
		if err != nil {
			log.GetLogger().Errorf("param check error, err is: %s", err.Error())
			configInstance = nil
			return
		}
	})

	if err != nil {
		return nil, err
	}

	if configInstance == nil {
		return nil, errors.New("empty configInstance")
	}

	return configInstance, nil
}

// The block defines the config which will not be changed
var (
	deployDir            = "/opt/function/code"
	busProtocolType      = "BUS_HTTP"
	namespace            = "default"
	forceStrategy        = "force"
	kubeCfgTimeout       = int64(30)
	podGeneralizeTimeout = 33
)

// PoolSpec pool specification
type PoolSpec struct {
	LimitCPU         int64 `json:"limitCpu" valid:"optional"`
	LimitMEM         int64 `json:"limitMem" valid:"optional"`
	RequestCPU       int64 `json:"requestCpu" valid:"optional"`
	RequestMEM       int64 `json:"requestMem" valid:"optional"`
	EphemeralStorage int64 `json:"ephemeralStorage" valid:"optional"`
}

// HotLoadConfig defines config in hot load conf file
type HotLoadConfig struct {
	Etcd                          *etcd3.ClientConfig           `json:"etcd" valid:"optional"`
	PoolSizeConfig                map[string]int32              `json:"poolSizeConfig" valid:",optional"`
	CaeCustomizePoolSpec          map[string]PoolSpec           `json:"caeCustomizePoolSpec" valid:",optional"`
	CustomJavaArgsWhiteList       []string                      `json:"customJavaArgsWhiteList" valid:",optional"`
	InstanceTimeoutExitWhiteList  []string                      `json:"instanceTimeoutExitWhiteList" valid:",optional"`
	PoolScaleLimit                map[string]float64            `json:"poolScaleLimit" valid:",optional"`
	ReservedAntiAffinity          string                        `json:"reservedAntiAffinity,omitempty" valid:",optional"`
	DisablePriorityQueue          uint32                        `json:"disablePriorityQueue,omitempty"`
	DisableCircuitBreaker         uint32                        `json:"disableCircuitBreaker,omitempty"`
	HeterogeneousSchedulingConfig HeterogeneousSchedulingConfig `json:"heterogeneousSchedulingConfig" valid:"optional"`
	// monitoring server config
	ServerAddr                string                    `json:"monitorServerAddr,omitempty" valid:",optional"`
	MonitoringTimeConf        redisclient.TimeoutConf   `json:"timeoutConf,omitempty" valid:",optional"`
	SmartWarmerEnable         bool                      `json:"smartWarmerEnable,omitempty" valid:",optional"`
	UpdateFunctionCacheEnable bool                      `json:"updateFunctionCacheEnable,omitempty" valid:",optional"`
	MultiSnapshotConfig       MultiSnapshotConfig       `json:"multiSnapshotConfig,omitempty"`
	SnapshotTenantIDsConfig   SnapshotTenantIDsConfig   `json:"snapshotTenantIDsConfig,omitempty"`
	CheckpointPodConfig       CheckpointPodConfig       `json:"checkpointPodConfig,omitempty" valid:",optional"`
	NodeBalance               NodeBalance               `json:"nodeBalance,omitempty" valid:"optional"`
	Resync                    ResyncConfig              `json:"resync,omitempty" valid:"optional"`
	ImageAffinityCacheConfig  *ImageAffinityCacheConfig `json:"imageAffinityCacheConfig" valid:"optional"`
	// pool size prediction config
	PoolMgrProactiveConfig        *PoolManagerProactiveElasticConfig `json:"proactivePoolMgrElasticConfig" valid:"optional"`
	ProactiveEnableList           map[string]bool                    `json:"proactiveEnableList" valid:"optional"`
	ProactiveElasticScalingConfig *ElasticScalingConfig              `json:"proactiveElasticScalingConfig" valid:"optional"`
	ElasticScalingConfig          *ElasticScalingConfig              `json:"elasticScalingConfig,omitempty" valid:"optional"`
	ResourceLockLeaseConfig       *ResourceLockLeaseConfig           `json:"resourceLockLeaseConfig,omitempty" valid:"optional"`
	BinPackEnable                 bool                               `json:"binPackEnable,omitempty" valid:"optional"`
	SchedulingEnable              bool                               `json:"schedulingEnable,omitempty" valid:"optional"`
	WorkerMetricsTick             int64                              `json:"workerMetricsTick,omitempty" valid:"optional"`
	TaintsNodeCheckInterval       int64                              `json:"taintsNodeCheckInterval,omitempty" valid:"optional"`
	DisableEtcdRev                uint32                             `json:"disableEtcdRev,omitempty" valid:"optional"`
	DisableWaitBeforeChangeNet    bool                               `json:"disableWaitBeforeChangeNet,omitempty" valid:"optional"`
	CoreCPUFeatures               string                             `json:"coreCPUFeatures,omitempty" valid:",optional"`
	BodySizeLimitWhitelist        map[string]int64                   `json:"bodySizeLimitWhitelist" valid:"optional"`
	// pool size prediction: if enable worker-manager upload used pod records to Redis
	EnableUploadUsedPodRecordToRedis bool              `json:"enableUploadUsedPodRecordToRedis,omitempty" valid:"optional"`
	ResourceUsageTick                int64             `json:"resourceUsageTick,omitempty" valid:"optional"`
	CaeMetricsTick                   int64             `json:"caeMetricsTick,omitempty" valid:"optional"`
	DecoyFileEnabled                 bool              `json:"decoyFileEnabled,omitempty" valid:"optional"`
	ScheduleWithDisk                 bool              `json:"scheduleWithDisk" valid:"optional"`
	RetryTimeWhenNoSpace             int64             `json:"retryTimeWhenNoSpace" valid:"optional"`
	DisablePoolRollout               uint32            `json:"disablePoolRollout"`
	SSDPoolLabels                    map[string]string `json:"ssdPoolLabels" valid:",optional"`
	NpmConfigRegistry                string            `json:"npm_config_registry" valid:"optional"`
	UVIndexURL                       string            `json:"uv_index_url" valid:"optional"`
	EnablePublicNetworkPat           bool              `json:"enablePublicNetworkPat,omitempty" valid:"optional"`
}

func readHotLoadConfig(p string) (*HotLoadConfig, error) {
	r, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	// close file when fail or succeed
	defer func() {
		err := r.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close file %s %s", p, err.Error())
		}
	}()

	configContent, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var hotLoadConfig HotLoadConfig
	err = json.Unmarshal(configContent, &hotLoadConfig)
	if err != nil {
		return nil, err
	}
	return &hotLoadConfig, nil
}

func newConfig(p string) (*FaasConfig, error) {
	// get base config from env or default value
	config := &FaasConfig{
		LocalAuth:                        getLocalAuthConfig(),
		HTTPSConfig:                      getHTTPSConfig(),
		AutoScalerConfig:                 getAutoScalerConfig(),
		Etcd:                             getEtcdConfig(),
		RouterETCD:                       getRouterEtcdConfig(),
		CAEMetaEtcd:                      getCAEMetaEtcdConfig(),
		CAERouterEtcd:                    getCAERouterEtcdConfig(),
		MetaEtcdProxy:                    getMetaEtcdProxyConfig(),
		CAEMetaEtcdProxy:                 getCAEMetaEtcdProxyConfig(),
		EtcdCluser:                       getEtcdClusterConfig(),
		PoolMgrConfig:                    getPoolMgrConfig(),
		BusProtocolType:                  busProtocolType,
		NameSpace:                        namespace,
		AuthenticationEnable:             getAuthenticationEnable(),
		FunctionNameSeparator:            getFunctionNameSeparator(),
		KubeCfgTimeout:                   kubeCfgTimeout,
		PodGeneralizeTimeout:             podGeneralizeTimeout,
		MonitoringServerConfig:           getMonitoringRedisServerConfig(),
		K8sCidr:                          getK8sCidrConfig(),
		DeployDir:                        deployDir,
		AlarmServerAddress:               getAlarmServerAddress(),
		EnableRemoteWorkerFlag:           getEnableRemoteWorkerFlag(),
		HeterogeneousSchedulingConfig:    getHeterogeneousSchedulingConfig(),
		PoolMgrProactiveConfig:           getPoolManagerProactiveElasticConfig(),
		ProactiveEnableList:              getProactiveModePoolList(),
		ProactiveElasticScalingConfig:    getProactiveElasticScalingConfig(),
		BinPackEnable:                    false,
		SchedulingEnable:                 false,
		ElasticScalingConfig:             getElasticScalingConfig(),
		ResourceLockLeaseConfig:          &ResourceLockLeaseConfig{},
		EnableUploadUsedPodRecordToRedis: getEnableUploadUsedPodRecordToRedis(),
		ImageAffinityCacheConfig:         getImageAffinityCacheConfig(),
	}

	addSSDPoolLabels(config.SSDPoolLabels)
	// get hotload config from file refreshed by configmap
	hotLoadConfig, err := readHotLoadConfig(p)
	if err != nil {
		return nil, err
	}

	// merge hotload config
	setHotLoadConfig(config, hotLoadConfig)
	log.GetLogger().Infof("succeed to merge hot load config")

	for _, pool := range config.PoolMgrConfig.PoolList.Pool {
		if pool.LimitMemory < pool.RequestMemory || pool.LimitCPU < pool.RequestCPU {
			log.GetLogger().Warnf("resource config error: limit resources (cpu %d, memory %d) should "+
				"large than request resources (cpu %d, memory %d)", pool.LimitCPU, pool.LimitMemory,
				pool.RequestCPU, pool.RequestMemory)
			return nil, errors.New("request resources are not valid")
		}
	}

	err = addPoolContainerTags(config)
	if err != nil {
		return nil, err
	}

	return config, nil
}

// HotLoadCallBack callback func for hot-load config
type HotLoadCallBack func(config *FaasConfig, hotLoadConfig *HotLoadConfig)

var hotLoadNodeBalanceCallBack HotLoadCallBack

var hotLoadResyncCallBack HotLoadCallBack

var hotLoadScaleDownTickerCallBack HotLoadCallBack

// RegisterHotLoadNodeBalanceCallBack register hot-load callback for node-balance
func RegisterHotLoadNodeBalanceCallBack(f HotLoadCallBack) {
	hotLoadNodeBalanceCallBack = f
}

// RegisterHotLoadResyncCallBack register hot-load callback for resync
func RegisterHotLoadResyncCallBack(f HotLoadCallBack) {
	hotLoadResyncCallBack = f
}

// RegisterHotLoadScaleDownTickerCallBack register hot-load callback for scale down ticker
func RegisterHotLoadScaleDownTickerCallBack(f HotLoadCallBack) {
	hotLoadScaleDownTickerCallBack = f
}

func setHotLoadConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	hotLoadPoolSize(config, hotLoadConfig)
	hotLoadCustomJavaArgsWhiteList(config, hotLoadConfig)
	hotLoadCaeCustomizePoolSize(config, hotLoadConfig)
	hotLoadReservedAntiAffinity(config, hotLoadConfig)
	hotLoadDisablePriorityQueue(config, hotLoadConfig)
	hotLoadDisableCircuitBreaker(config, hotLoadConfig)
	hotLoadDisablePoolRollout(config, hotLoadConfig)
	hotLoadHeterogeneousSchedulingConfig(config, hotLoadConfig)
	hotLoadServerAddr(config, hotLoadConfig)
	hotLoadSmartWarmerEnable(config, hotLoadConfig)
	hotLoadUpdateFunctionCacheEnable(config, hotLoadConfig)
	hotLoadEnableMultiSnapshot(config, hotLoadConfig)
	hotLoadSnapshotTenanIds(config, hotLoadConfig)
	hotLoadCheckpointPodConfig(config, hotLoadConfig)
	hotLoadDecoyFileEnabled(config, hotLoadConfig)
	hotLoadPoolMgrProactiveConfig(config, hotLoadConfig)
	hotLoadProactiveEnableList(config, hotLoadConfig)
	hotLoadProactiveElasticScalingConfig(config, hotLoadConfig)
	hotLoadBinPackEnable(config, hotLoadConfig)
	hotLoadSchedulingEnable(config, hotLoadConfig)
	hotLoadElasticScalingConfig(config, hotLoadConfig)
	hotLoadImageAffinityCacheConfig(config, hotLoadConfig)
	hotLoadResourceLockLeaseConfig(config, hotLoadConfig)
	hotLoadWorkerMetricsTick(config, hotLoadConfig)
	hotLoadTaintsNodeCheckInterval(config, hotLoadConfig)
	hotLoadDisableEtcdRev(config, hotLoadConfig)
	hotDisableWaitBeforeChangeNet(config, hotLoadConfig)
	hotLoadCoreCPUFeature(config, hotLoadConfig)
	hotLoadEtcdConfig(config, hotLoadConfig)
	hotLoadBodySizeLimitWhitelist(config, hotLoadConfig)
	hotLoadEnableUploadUsedPodRecordToRedisConfig(config, hotLoadConfig)
	hotLoadResourceUsageTick(config, hotLoadConfig)
	hotLoadCaeMetricsTick(config, hotLoadConfig)
	hotLoadScheduleWithDisk(config, hotLoadConfig)
	hotLoadRetryTimeWhenNoSpace(config, hotLoadConfig)
	hotLoadSsdPoolLabels(config, hotLoadConfig)
	hotLoadNpmConfigRegistryAndUVIndexURL(config, hotLoadConfig)
	hotLoadInstanceTimeoutExitWhiteList(config, hotLoadConfig)
	hotLoadEnablePublicNetworkPat(config, hotLoadConfig)

	if hotLoadNodeBalanceCallBack != nil {
		hotLoadNodeBalanceCallBack(config, hotLoadConfig)
	}
	if hotLoadResyncCallBack != nil {
		hotLoadResyncCallBack(config, hotLoadConfig)
	}
}

const (
	defaultTaintsNodeCheckInterval = 900
)

func hotLoadTaintsNodeCheckInterval(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	old := atomic.LoadInt64(&config.TaintsNodeCheckInterval)
	if hotLoadConfig.TaintsNodeCheckInterval == 0 {
		hotLoadConfig.TaintsNodeCheckInterval = defaultTaintsNodeCheckInterval
	}

	if old != hotLoadConfig.TaintsNodeCheckInterval {
		log.GetLogger().Infof("update taintsNodeCheckInterval from %d to %d", old,
			hotLoadConfig.TaintsNodeCheckInterval)
		atomic.StoreInt64(&config.TaintsNodeCheckInterval, hotLoadConfig.TaintsNodeCheckInterval)
	}
}

func hotLoadWorkerMetricsTick(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	old := atomic.LoadInt64(&config.WorkerMetricsTick)
	if old != hotLoadConfig.WorkerMetricsTick {
		log.GetLogger().Infof("update workerMetricsTick from %d to %d", old, hotLoadConfig.WorkerMetricsTick)
		atomic.StoreInt64(&config.WorkerMetricsTick, hotLoadConfig.WorkerMetricsTick)
	}
}

func hotLoadResourceUsageTick(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	old := atomic.LoadInt64(&config.ResourceUsageTick)
	if old != hotLoadConfig.ResourceUsageTick {
		log.GetLogger().Infof("update resourceUsageTick from %d to %d", old, hotLoadConfig.ResourceUsageTick)
		atomic.StoreInt64(&config.ResourceUsageTick, hotLoadConfig.ResourceUsageTick)
	}
}

func hotLoadCaeMetricsTick(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	old := atomic.LoadInt64(&config.CaeMetricsTick)
	if old != hotLoadConfig.CaeMetricsTick {
		log.GetLogger().Infof("update caeMetricsTick from %d to %d", old, hotLoadConfig.CaeMetricsTick)
		atomic.StoreInt64(&config.CaeMetricsTick, hotLoadConfig.CaeMetricsTick)
	}
}

func hotLoadDisableEtcdRev(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	old := atomic.LoadUint32(&config.DisableEtcdRev)
	if old != hotLoadConfig.DisableEtcdRev {
		log.GetLogger().Infof("update disableEtcdRev from %d to %d", old, hotLoadConfig.DisableEtcdRev)
		atomic.StoreUint32(&config.DisableEtcdRev, hotLoadConfig.DisableEtcdRev)
	}
}

func hotDisableWaitBeforeChangeNet(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	log.GetLogger().Infof("disableWaitBeforeChangeNet change from %t to %t",
		config.DisableWaitBeforeChangeNet, hotLoadConfig.DisableWaitBeforeChangeNet)
	config.DisableWaitBeforeChangeNet = hotLoadConfig.DisableWaitBeforeChangeNet
}

func hotLoadEtcdConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldConfig := config.Etcd
	hotLoadEtcdConfig := hotLoadConfig.Etcd
	if hotLoadEtcdConfig != nil && len(hotLoadEtcdConfig.ServerList) > 0 {
		log.GetLogger().Infof("update etcd config from %v to %v", oldConfig.ServerList, hotLoadEtcdConfig.ServerList)
		oldConfig.ServerList = hotLoadEtcdConfig.ServerList
	}
}

func hotLoadResourceLockLeaseConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig.ResourceLockLeaseConfig == nil {
		return
	}
	oldConfig := config.ResourceLockLeaseConfig
	hotLoadScalingConfig := hotLoadConfig.ResourceLockLeaseConfig
	if hotLoadScalingConfig.LeaseDuration > 0 {
		log.GetLogger().Infof("update LeaseDuration new: %d", hotLoadScalingConfig.LeaseDuration)
		oldConfig.LeaseDuration = hotLoadScalingConfig.LeaseDuration
	}
	if hotLoadScalingConfig.RenewDeadline > 0 {
		log.GetLogger().Infof("update RenewDeadline new: %d", hotLoadScalingConfig.RenewDeadline)
		oldConfig.RenewDeadline = hotLoadScalingConfig.RenewDeadline
	}
	if hotLoadScalingConfig.RetryPeriod > 0 {
		log.GetLogger().Infof("update RetryPeriod new: %d", hotLoadScalingConfig.RetryPeriod)
		oldConfig.RetryPeriod = hotLoadScalingConfig.RetryPeriod
	}
}

// hotLoadImageAffinityCacheConfig -
func hotLoadImageAffinityCacheConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig.ImageAffinityCacheConfig == nil {
		return
	}
	oldConfig := config.ImageAffinityCacheConfig
	hotLoadCacheConfig := hotLoadConfig.ImageAffinityCacheConfig

	oldConfig.TenantWhiteList = hotLoadCacheConfig.TenantWhiteList

	oldSwitch := atomic.LoadUint32(&config.ImageAffinityCacheConfig.DisableImageAffinityCache)
	newSwitch := hotLoadCacheConfig.DisableImageAffinityCache
	if oldSwitch != newSwitch {
		atomic.StoreUint32(&config.ImageAffinityCacheConfig.DisableImageAffinityCache, newSwitch)
		log.GetLogger().Infof("image affinity cache config update disable from %d to %d", oldSwitch, newSwitch)
	}
	if hotLoadCacheConfig.CacheNodesPercentage != 0 && oldConfig.CacheNodesPercentage !=
		hotLoadCacheConfig.CacheNodesPercentage {
		log.GetLogger().Infof("image affinity cache config update cache nodes percentage from %d to %d",
			oldConfig.CacheNodesPercentage, hotLoadCacheConfig.CacheNodesPercentage)
		oldConfig.CacheNodesPercentage = hotLoadCacheConfig.CacheNodesPercentage
	}
	if hotLoadCacheConfig.CacheNodesNumber != 0 && oldConfig.CacheNodesNumber != hotLoadCacheConfig.CacheNodesNumber {
		log.GetLogger().Infof("image affinity cache config update cache nodes number from %d to %d",
			oldConfig.CacheNodesNumber, hotLoadCacheConfig.CacheNodesNumber)
		oldConfig.CacheNodesNumber = hotLoadCacheConfig.CacheNodesNumber
	}
}

func hotLoadElasticScalingConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig.ElasticScalingConfig == nil {
		return
	}
	oldConfig := config.ElasticScalingConfig
	hotLoadScalingConfig := hotLoadConfig.ElasticScalingConfig
	if hotLoadScalingConfig.UsedPodNumsExceedPercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsExceedPercentage from old: %f, new: %f",
			oldConfig.UsedPodNumsExceedPercentage, hotLoadScalingConfig.UsedPodNumsExceedPercentage)
		oldConfig.UsedPodNumsExceedPercentage = hotLoadScalingConfig.UsedPodNumsExceedPercentage
	}
	if hotLoadScalingConfig.ScaleUpPodNumsPercentage > 0 {
		log.GetLogger().Infof("update ScaleUpPodNumsPercentage from old: %f, new: %f",
			oldConfig.ScaleUpPodNumsPercentage, hotLoadScalingConfig.ScaleUpPodNumsPercentage)
		oldConfig.ScaleUpPodNumsPercentage = hotLoadScalingConfig.ScaleUpPodNumsPercentage
	}
	if hotLoadScalingConfig.ExtraScaleUpPodNumsPercentage > 0 {
		log.GetLogger().Infof("update ExtraScaleUpPodNumsPercentage from old: %f, new: %f",
			oldConfig.ExtraScaleUpPodNumsPercentage, hotLoadScalingConfig.ExtraScaleUpPodNumsPercentage)
		oldConfig.ExtraScaleUpPodNumsPercentage = hotLoadScalingConfig.ExtraScaleUpPodNumsPercentage
	}
	if hotLoadScalingConfig.UsedPodNumsBelowPercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsBelowPercentage from old: %f, new: %f",
			oldConfig.UsedPodNumsBelowPercentage, hotLoadScalingConfig.UsedPodNumsBelowPercentage)
		oldConfig.UsedPodNumsBelowPercentage = hotLoadScalingConfig.UsedPodNumsBelowPercentage
	}
	if hotLoadScalingConfig.ScaleDownPodNumsPercentage > 0 {
		log.GetLogger().Infof("update ScaleDownPodNumsPercentage from old: %f, new: %f",
			oldConfig.ScaleDownPodNumsPercentage, hotLoadScalingConfig.ScaleDownPodNumsPercentage)
		oldConfig.ScaleDownPodNumsPercentage = hotLoadScalingConfig.ScaleDownPodNumsPercentage
	}
	if hotLoadScalingConfig.EffectiveThresholdReplicas > 0 {
		log.GetLogger().Infof("update EffectiveThresholdReplicas from old: %d, new: %d",
			oldConfig.EffectiveThresholdReplicas, hotLoadScalingConfig.EffectiveThresholdReplicas)
		oldConfig.EffectiveThresholdReplicas = hotLoadScalingConfig.EffectiveThresholdReplicas
	}
	if hotLoadScalingConfig.MaxScaleUpPodNums > 0 {
		log.GetLogger().Infof("update MaxScaleUpPodNums from old: %d, new: %d",
			oldConfig.MaxScaleUpPodNums, hotLoadScalingConfig.MaxScaleUpPodNums)
		oldConfig.MaxScaleUpPodNums = hotLoadScalingConfig.MaxScaleUpPodNums
	}
	if hotLoadScalingConfig.ThresholdResourceUsage > 0 {
		log.GetLogger().Infof("update ThresholdResourceUsage from old: %f, new: %f",
			oldConfig.ThresholdResourceUsage, hotLoadScalingConfig.ThresholdResourceUsage)
		oldConfig.ThresholdResourceUsage = hotLoadScalingConfig.ThresholdResourceUsage
	}
	if hotLoadScalingConfig.UsedPodNumsExceedFreePercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsExceedFreePercentage from old: %f, new: %f",
			oldConfig.UsedPodNumsExceedFreePercentage, hotLoadScalingConfig.UsedPodNumsExceedFreePercentage)
		oldConfig.UsedPodNumsExceedFreePercentage = hotLoadScalingConfig.UsedPodNumsExceedFreePercentage
	}
	if hotLoadScalingConfig.UsedPodNumsExceedHeavyPercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsExceedHeavyPercentage from old: %f, new: %f",
			oldConfig.UsedPodNumsExceedHeavyPercentage, hotLoadScalingConfig.UsedPodNumsExceedHeavyPercentage)
		oldConfig.UsedPodNumsExceedHeavyPercentage = hotLoadScalingConfig.UsedPodNumsExceedHeavyPercentage
	}
	if hotLoadScalingConfig.ScaleUpPodNumsFreePercentage > 0 {
		log.GetLogger().Infof("update ScaleUpPodNumsFreePercentage from old: %f, new: %f",
			oldConfig.ScaleUpPodNumsFreePercentage, hotLoadScalingConfig.ScaleUpPodNumsFreePercentage)
		oldConfig.ScaleUpPodNumsFreePercentage = hotLoadScalingConfig.ScaleUpPodNumsFreePercentage
	}
	if hotLoadScalingConfig.ScaleUpPodNumsHeavyPercentage > 0 {
		log.GetLogger().Infof("update ScaleUpPodNumsHeavyPercentage from old: %f, new: %f",
			oldConfig.ScaleUpPodNumsHeavyPercentage, hotLoadScalingConfig.ScaleUpPodNumsHeavyPercentage)
		oldConfig.ScaleUpPodNumsHeavyPercentage = hotLoadScalingConfig.ScaleUpPodNumsHeavyPercentage
	}
	if hotLoadScalingConfig.MaxTransitionScaleUpPercentage > 0 {
		log.GetLogger().Infof("update MaxTransitionScaleUpPercentage from old: %f, new: %f",
			oldConfig.MaxTransitionScaleUpPercentage, hotLoadScalingConfig.MaxTransitionScaleUpPercentage)
		oldConfig.MaxTransitionScaleUpPercentage = hotLoadScalingConfig.MaxTransitionScaleUpPercentage
	}
	if hotLoadScaleDownTickerCallBack != nil {
		hotLoadScaleDownTickerCallBack(config, hotLoadConfig)
	}
}

// WatchConfig start config watcher and register callback
func WatchConfig(stopCh <-chan struct{}, callback ChangedCallback) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
		return err
	}

	configWatcher = watcher
	configChangedCallbacks = append(configChangedCallbacks, callback)
	configWatcher.RegisterCallback(configPath, hotLoadConfig)
	return nil
}

// hotLoadConfig hot load config and call funcs to process config changes
func hotLoadConfig(filename string, opType monitor.OpType) {
	log.GetLogger().Infof("start to hot load config file: %s", filename)
	newHotLoadConfig, err := readHotLoadConfig(filename)
	if err != nil {
		log.GetLogger().Errorf("failed to get new config file: %s, opType: %d, err: %s", filename, opType, err.Error())
		return
	}
	_, err = govalidator.ValidateStruct(newHotLoadConfig)
	if err != nil {
		log.GetLogger().Errorf("new config file param check error: %s, err is: %s", filename, err.Error())
		return
	}

	// only hot load the params we care about
	setHotLoadConfig(configInstance, newHotLoadConfig)
	log.GetLogger().Infof("succeed to refresh hotload params in configInstance")

	// take turns to invoke callback func to process changed config
	if configChangedCallbacks != nil {
		for _, configChangedCallback := range configChangedCallbacks {
			configChangedCallback()
		}
	}
}

func hotLoadPoolMgrProactiveConfig(faasConfig *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig == nil || hotLoadConfig.PoolMgrProactiveConfig == nil {
		log.GetLogger().Warnf("skip to update pool manager proactive config because of nil of hot load config")
		return
	}
	if faasConfig == nil {
		faasConfig = &FaasConfig{}
	}
	if faasConfig.PoolMgrProactiveConfig == nil {
		faasConfig.PoolMgrProactiveConfig = &PoolManagerProactiveElasticConfig{}
	}
	curConfig := faasConfig.PoolMgrProactiveConfig
	newMgrConfig := hotLoadConfig.PoolMgrProactiveConfig
	log.GetLogger().Infof("hot load PoolMgrProactiveConfig, value before update: %v", curConfig)
	if newMgrConfig.UsedPodRecordResetAndUploadInterval > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.UsedPodRecordResetAndUploadInterval, "+
			"old: %d, new: %d",
			curConfig.UsedPodRecordResetAndUploadInterval, newMgrConfig.UsedPodRecordResetAndUploadInterval)
		curConfig.UsedPodRecordResetAndUploadInterval = newMgrConfig.UsedPodRecordResetAndUploadInterval
	}
	if newMgrConfig.UsedPodRecordExpiration > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.UsedPodRecordExpiration, old: %d, new: %d",
			curConfig.UsedPodRecordExpiration, newMgrConfig.UsedPodRecordExpiration)
		curConfig.UsedPodRecordExpiration = newMgrConfig.UsedPodRecordExpiration
	}
	if newMgrConfig.PoolPredictionResultDownloadInterval > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.PoolPredictionResultDownloadInterval, "+
			"old: %d, new: %d",
			curConfig.PoolPredictionResultDownloadInterval, newMgrConfig.PoolPredictionResultDownloadInterval)
		curConfig.PoolPredictionResultDownloadInterval = newMgrConfig.PoolPredictionResultDownloadInterval
	}
	if newMgrConfig.PoolPredictionResultLength > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.PoolPredictionResultLength, old: %d, new: %d",
			curConfig.PoolPredictionResultLength, newMgrConfig.PoolPredictionResultLength)
		curConfig.PoolPredictionResultLength = newMgrConfig.PoolPredictionResultLength
	}
	if newMgrConfig.ApplyPredictionResultToPoolInterval > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.ApplyPredictionResultToPoolInterval, "+
			"old: %d, new: %d",
			curConfig.ApplyPredictionResultToPoolInterval, newMgrConfig.ApplyPredictionResultToPoolInterval)
		curConfig.ApplyPredictionResultToPoolInterval = newMgrConfig.ApplyPredictionResultToPoolInterval
	}
	if newMgrConfig.ScalingPlanTimeWindow > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.ScalingPlanTimeWindow, old: %d, new: %d",
			curConfig.ScalingPlanTimeWindow, newMgrConfig.ScalingPlanTimeWindow)
		curConfig.ScalingPlanTimeWindow = newMgrConfig.ScalingPlanTimeWindow
	}
	if newMgrConfig.ReactiveScalingUpResetInterval > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.ReactiveScalingUpResetInterval, old: %d, new: %d",
			curConfig.ReactiveScalingUpResetInterval, newMgrConfig.ReactiveScalingUpResetInterval)
		curConfig.ReactiveScalingUpResetInterval = newMgrConfig.ReactiveScalingUpResetInterval
	}
	if newMgrConfig.EnableFusionPolicy != curConfig.EnableFusionPolicy {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.EnableFusionPolicy, old: %d, new: %d",
			curConfig.EnableFusionPolicy, newMgrConfig.EnableFusionPolicy)
		curConfig.EnableFusionPolicy = newMgrConfig.EnableFusionPolicy
	}
	if newMgrConfig.ScalingUpTriggerTimeThreshold > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.ScalingUpTriggerTimeThreshold, old: %d, new: %d",
			curConfig.ScalingUpTriggerTimeThreshold, newMgrConfig.ScalingUpTriggerTimeThreshold)
		curConfig.ScalingUpTriggerTimeThreshold = newMgrConfig.ScalingUpTriggerTimeThreshold
	}
	if newMgrConfig.CooldownTime > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.CooldownTime, old: %d, new: %d",
			curConfig.CooldownTime, newMgrConfig.CooldownTime)
		curConfig.CooldownTime = newMgrConfig.CooldownTime
	}
	if newMgrConfig.UpperBoundFactor >= 1 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.UpperBoundFactor, old: %f, new: %f",
			curConfig.UpperBoundFactor, newMgrConfig.UpperBoundFactor)
		curConfig.UpperBoundFactor = newMgrConfig.UpperBoundFactor
	}
	if newMgrConfig.LowerBoundFactor <= 1 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.LowerBoundFactor, old: %f, new: %f",
			curConfig.LowerBoundFactor, newMgrConfig.LowerBoundFactor)
		curConfig.LowerBoundFactor = newMgrConfig.LowerBoundFactor
	}
	if newMgrConfig.MaxScalingStep > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.MaxScalingStep, old: %d, new: %d",
			curConfig.MaxScalingStep, newMgrConfig.MaxScalingStep)
		curConfig.MaxScalingStep = newMgrConfig.MaxScalingStep
	}
	if newMgrConfig.ScalingPlanFactor > 0 {
		log.GetLogger().Infof("update PoolMgrProactiveConfig.ScalingPlanFactor, old: %f, new: %f",
			curConfig.ScalingPlanFactor, newMgrConfig.ScalingPlanFactor)
		curConfig.ScalingPlanFactor = newMgrConfig.ScalingPlanFactor
	}
	log.GetLogger().Infof("hot load PoolMgrProactiveConfig, value after update: %v", curConfig)
}

func hotLoadProactiveEnableList(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig == nil || config == nil || hotLoadConfig.ProactiveEnableList == nil {
		return
	}
	log.GetLogger().Infof("hot load ProactiveEnableList, value before update: %v", config.ProactiveEnableList)
	newProactiveEnableList := map[string]bool{}
	for k, v := range hotLoadConfig.ProactiveEnableList {
		newProactiveEnableList[k] = v
	}
	config.ProactiveEnableList = newProactiveEnableList
	log.GetLogger().Infof("hot load ProactiveEnableList, value after update: %v", config.ProactiveEnableList)
}

func hotLoadDecoyFileEnabled(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	log.GetLogger().Infof("update DecoyFileEnabled from %t to %t", config.DecoyFileEnabled,
		hotLoadConfig.DecoyFileEnabled)
	config.DecoyFileEnabled = hotLoadConfig.DecoyFileEnabled

}

func hotLoadScheduleWithDisk(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	log.GetLogger().Infof("update ScheduleWithDisk from %t to %t", config.ScheduleWithDisk,
		hotLoadConfig.ScheduleWithDisk)
	config.ScheduleWithDisk = hotLoadConfig.ScheduleWithDisk
}

func hotLoadRetryTimeWhenNoSpace(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	log.GetLogger().Infof("update RetryTimeWhenNoSpace from %d to %d", config.RetryTimeWhenNoSpace,
		hotLoadConfig.RetryTimeWhenNoSpace)
	config.RetryTimeWhenNoSpace = hotLoadConfig.RetryTimeWhenNoSpace
}

func hotLoadNpmConfigRegistryAndUVIndexURL(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldNpmConfigRegistry := config.NpmConfigRegistry
	oldUVIndexURL := config.UVIndexURL

	newNpmConfigRegistry := hotLoadConfig.NpmConfigRegistry
	newUVIndexURL := hotLoadConfig.UVIndexURL
	if oldNpmConfigRegistry != newNpmConfigRegistry || oldUVIndexURL != newUVIndexURL {
		log.GetLogger().Infof("npm config registry or uv index url changed, old npm config registry: %s, "+
			"new npm config registry: %s, old uv index url: %s, new uv index url : %s", oldNpmConfigRegistry,
			newNpmConfigRegistry, oldUVIndexURL, newUVIndexURL)
		config.NpmConfigRegistry = hotLoadConfig.NpmConfigRegistry
		config.UVIndexURL = hotLoadConfig.UVIndexURL
	}
}

func hotLoadInstanceTimeoutExitWhiteList(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldWhiteList := config.InstanceTimeoutExitWhiteList
	newWhiteList := hotLoadConfig.InstanceTimeoutExitWhiteList
	oldWhiteListStr, err := json.Marshal(oldWhiteList)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal oldWhiteList in hotLoadInstanceTimeoutExitWhiteList: %s",
			err.Error())
		return
	}
	newWhiteListStr, err := json.Marshal(newWhiteList)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal newWhiteList in hotLoadInstanceTimeoutExitWhiteList: %s",
			err.Error())
		return
	}
	if string(oldWhiteListStr) == string(newWhiteListStr) {

		log.GetLogger().Warnf("instance timeout exit whiteList not changed, ignore it")
		return
	}
	log.GetLogger().Infof("instance timeout exit whiteList changed, oldWhiteList: %s, newWhiteList: %s",

		oldWhiteListStr, newWhiteListStr)
	config.InstanceTimeoutExitWhiteList = newWhiteList
}

func hotLoadSsdPoolLabels(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldSSDPoolLabels := config.SSDPoolLabels
	newSSDPoolLabels := hotLoadConfig.SSDPoolLabels
	oldSSDPoolLabelsByte, err := json.Marshal(oldSSDPoolLabels)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal oldSSDPoolLabels in ssdPoolLabels: %s", err.Error())
		return
	}
	newSSDPoolLabelsByte, err := json.Marshal(newSSDPoolLabels)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal newSSDPoolLabels in ssdPoolLabels: %s", err.Error())
		return
	}
	if string(oldSSDPoolLabelsByte) == string(newSSDPoolLabelsByte) {
		log.GetLogger().Infof("ssdPoolLabels not changed, ignore it")
		return
	}
	log.GetLogger().Infof("ssdPoolLabels changed, oldSSDPoolLabels: %v, newSSDPoolLabels: %v", oldSSDPoolLabels,
		newSSDPoolLabels)
	config.SSDPoolLabels = newSSDPoolLabels
	updateSSDPoolLabels(oldSSDPoolLabels, newSSDPoolLabels)
}

func hotLoadProactiveElasticScalingConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig == nil || config == nil || hotLoadConfig.ProactiveElasticScalingConfig == nil {
		return
	}
	if config.ProactiveElasticScalingConfig == nil {
		config.ProactiveElasticScalingConfig = &ElasticScalingConfig{}
	}
	hotLoadScalingConfig := hotLoadConfig.ProactiveElasticScalingConfig
	elasticScalingConfig := config.ProactiveElasticScalingConfig
	if hotLoadScalingConfig.UsedPodNumsExceedPercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsExceedPercentage from old: %f, new: %f",
			elasticScalingConfig.UsedPodNumsExceedPercentage, hotLoadScalingConfig.UsedPodNumsExceedPercentage)
		elasticScalingConfig.UsedPodNumsExceedPercentage = hotLoadScalingConfig.UsedPodNumsExceedPercentage
	}
	if hotLoadScalingConfig.ScaleUpPodNumsPercentage > 0 {
		log.GetLogger().Infof("update ScaleUpPodNumsPercentage from old: %f, new: %f",
			elasticScalingConfig.ScaleUpPodNumsPercentage, hotLoadScalingConfig.ScaleUpPodNumsPercentage)
		elasticScalingConfig.ScaleUpPodNumsPercentage = hotLoadScalingConfig.ScaleUpPodNumsPercentage
	}
	if hotLoadScalingConfig.ExtraScaleUpPodNumsPercentage > 0 {
		log.GetLogger().Infof("update ExtraScaleUpPodNumsPercentage from old: %f, new: %f",
			elasticScalingConfig.ExtraScaleUpPodNumsPercentage, hotLoadScalingConfig.ExtraScaleUpPodNumsPercentage)
		elasticScalingConfig.ExtraScaleUpPodNumsPercentage = hotLoadScalingConfig.ExtraScaleUpPodNumsPercentage
	}
	if hotLoadScalingConfig.UsedPodNumsBelowPercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsBelowPercentage from old: %f, new: %f",
			elasticScalingConfig.UsedPodNumsBelowPercentage, hotLoadScalingConfig.UsedPodNumsBelowPercentage)
		elasticScalingConfig.UsedPodNumsBelowPercentage = hotLoadScalingConfig.UsedPodNumsBelowPercentage
	}
	if hotLoadScalingConfig.ScaleDownPodNumsPercentage > 0 {
		log.GetLogger().Infof("update ScaleDownPodNumsPercentage from old: %f, new: %f",
			elasticScalingConfig.ScaleDownPodNumsPercentage, hotLoadScalingConfig.ScaleDownPodNumsPercentage)
		elasticScalingConfig.ScaleDownPodNumsPercentage = hotLoadScalingConfig.ScaleDownPodNumsPercentage
	}
	if hotLoadScalingConfig.EffectiveThresholdReplicas > 0 {
		log.GetLogger().Infof("update EffectiveThresholdReplicas from old: %d, new: %d",
			elasticScalingConfig.EffectiveThresholdReplicas, hotLoadScalingConfig.EffectiveThresholdReplicas)
		elasticScalingConfig.EffectiveThresholdReplicas = hotLoadScalingConfig.EffectiveThresholdReplicas
	}
	if hotLoadScalingConfig.MaxScaleUpPodNums > 0 {
		log.GetLogger().Infof("update MaxScaleUpPodNums from old: %d, new: %d",
			elasticScalingConfig.MaxScaleUpPodNums, hotLoadScalingConfig.MaxScaleUpPodNums)
		elasticScalingConfig.MaxScaleUpPodNums = hotLoadScalingConfig.MaxScaleUpPodNums
	}
	if hotLoadScalingConfig.ScaleDownInterval > 0 {
		log.GetLogger().Infof("update ScaleDownInterval from old: %d, new: %d",
			elasticScalingConfig.ScaleDownInterval, hotLoadScalingConfig.ScaleDownInterval)
		elasticScalingConfig.ScaleDownInterval = hotLoadScalingConfig.ScaleDownInterval
	}
	if hotLoadScalingConfig.ThresholdResourceUsage > 0 {
		log.GetLogger().Infof("update ThresholdResourceUsage from old: %f, new: %f",
			elasticScalingConfig.ThresholdResourceUsage, hotLoadScalingConfig.ThresholdResourceUsage)
		elasticScalingConfig.ThresholdResourceUsage = hotLoadScalingConfig.ThresholdResourceUsage
	}
	if hotLoadScalingConfig.UsedPodNumsExceedFreePercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsExceedFreePercentage from old: %f, new: %f",
			elasticScalingConfig.UsedPodNumsExceedFreePercentage, hotLoadScalingConfig.UsedPodNumsExceedFreePercentage)
		elasticScalingConfig.UsedPodNumsExceedFreePercentage = hotLoadScalingConfig.UsedPodNumsExceedFreePercentage
	}
	if hotLoadScalingConfig.UsedPodNumsExceedHeavyPercentage > 0 {
		log.GetLogger().Infof("update UsedPodNumsExceedHeavyPercentage from old: %f, new: %f",
			elasticScalingConfig.UsedPodNumsExceedHeavyPercentage, hotLoadScalingConfig.UsedPodNumsExceedHeavyPercentage)
		elasticScalingConfig.UsedPodNumsExceedHeavyPercentage = hotLoadScalingConfig.UsedPodNumsExceedHeavyPercentage
	}
	if hotLoadScalingConfig.ScaleUpPodNumsFreePercentage > 0 {
		log.GetLogger().Infof("update ScaleUpPodNumsFreePercentage from old: %f, new: %f",
			elasticScalingConfig.ScaleUpPodNumsFreePercentage, hotLoadScalingConfig.ScaleUpPodNumsFreePercentage)
		elasticScalingConfig.ScaleUpPodNumsFreePercentage = hotLoadScalingConfig.ScaleUpPodNumsFreePercentage
	}
	if hotLoadScalingConfig.ScaleUpPodNumsHeavyPercentage > 0 {
		log.GetLogger().Infof("update ScaleUpPodNumsHeavyPercentage from old: %f, new: %f",
			elasticScalingConfig.ScaleUpPodNumsHeavyPercentage, hotLoadScalingConfig.ScaleUpPodNumsHeavyPercentage)
		elasticScalingConfig.ScaleUpPodNumsHeavyPercentage = hotLoadScalingConfig.ScaleUpPodNumsHeavyPercentage
	}
	if hotLoadScalingConfig.MaxTransitionScaleUpPercentage > 0 {
		log.GetLogger().Infof("update MaxTransitionScaleUpPercentage from old: %f, new: %f",
			elasticScalingConfig.MaxTransitionScaleUpPercentage, hotLoadScalingConfig.MaxTransitionScaleUpPercentage)
		elasticScalingConfig.MaxTransitionScaleUpPercentage = hotLoadScalingConfig.MaxTransitionScaleUpPercentage
	}
}

func hotLoadEnableUploadUsedPodRecordToRedisConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if config.EnableUploadUsedPodRecordToRedis != hotLoadConfig.EnableUploadUsedPodRecordToRedis {
		log.GetLogger().Infof("update EnableUploadUsedPodRecordToRedis config from %t to %t",
			config.EnableUploadUsedPodRecordToRedis, hotLoadConfig.EnableUploadUsedPodRecordToRedis)
		config.EnableUploadUsedPodRecordToRedis = hotLoadConfig.EnableUploadUsedPodRecordToRedis
	}
}

func hotLoadPoolSize(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldPools := config.PoolMgrConfig.PoolList.Pool
	newPoolSizeConfig := hotLoadConfig.PoolSizeConfig
	poolScaleConfig := hotLoadConfig.PoolScaleLimit
	for index, oldPool := range oldPools {
		newPoolSize, ok := newPoolSizeConfig[oldPool.Name]
		if ok && newPoolSize != oldPool.PoolSize {
			log.GetLogger().Infof("pool: %s config changed, old poolsize: %d, new poolsize: %d",
				oldPool.Name, oldPool.PoolSize, newPoolSize)
			config.PoolMgrConfig.PoolList.Pool[index].PoolSize = newPoolSize
		}
		scaleLimit, scaleLimitOK := poolScaleConfig[oldPool.Name]
		if scaleLimitOK && scaleLimit > 0 {
			config.PoolMgrConfig.PoolList.Pool[index].PoolScaleLimit = scaleLimit
		}
	}
}

func hotLoadCaeCustomizePoolSize(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldPoolSpec := config.CaeCustomizePoolSpec
	newPoolSpec := hotLoadConfig.CaeCustomizePoolSpec
	oldPoolSpecStr, err := json.Marshal(oldPoolSpec)
	if err != nil {
		log.GetLogger().Errorf("hotLoadCaeCustomizePoolSize marshal oldPoolSpec failed: %s", err.Error())
		return
	}
	newPoolSpecStr, err := json.Marshal(newPoolSpec)
	if err != nil {
		log.GetLogger().Errorf("hotLoadCaeCustomizePoolSize marshal newPoolSpec failed: %s", err.Error())
		return
	}
	if string(oldPoolSpecStr) == string(newPoolSpecStr) {
		log.GetLogger().Warnf("cae customize pool spec not changed, ignore it")
		return
	}
	log.GetLogger().Infof("cae customize pool spec changed, oldPoolSpec: %s, newPoolSpec: %s",
		oldPoolSpecStr, newPoolSpecStr)
	config.CaeCustomizePoolSpec = newPoolSpec
}

func hotLoadCustomJavaArgsWhiteList(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldWhiteList := config.CustomJavaArgsWhiteList
	newWhiteList := hotLoadConfig.CustomJavaArgsWhiteList
	oldWhiteListStr, err := json.Marshal(oldWhiteList)
	if err != nil {
		log.GetLogger().Errorf("hotLoadCustomJavaArgsWhiteList marshal oldWhiteList failed: %s", err.Error())
		return
	}
	newWhiteListStr, err := json.Marshal(newWhiteList)
	if err != nil {
		log.GetLogger().Errorf("hotLoadCustomJavaArgsWhiteList marshal newWhiteList failed: %s", err.Error())
		return
	}
	if string(oldWhiteListStr) == string(newWhiteListStr) {
		log.GetLogger().Warnf("custom java args whitelist not changed, ignore it")
		return
	}
	log.GetLogger().Infof("custom java args whitelist changed, oldWhiteList: %s, newWhiteList: %s",
		oldWhiteListStr, newWhiteListStr)
	config.CustomJavaArgsWhiteList = newWhiteList
}

func hotLoadBodySizeLimitWhitelist(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig == nil || config == nil {
		return
	}
	log.GetLogger().Infof("hot load BodySizeLimitWhitelist, "+
		"value before update: %v", config.BodySizeLimitWhitelist)
	newWhitelist := map[string]int64{}
	for k, v := range hotLoadConfig.BodySizeLimitWhitelist {
		if v > 0 && v <= upLimitSize {
			newWhitelist[k] = v
		} else {
			log.GetLogger().Infof("skip invalid BodySizeLimit, tenantID: %s, limitSize %d", k, v)
		}
	}
	config.BodySizeLimitWhitelist = newWhitelist
	log.GetLogger().Infof("hot load BodySizeLimitWhitelist, "+
		"value after update: %v", config.BodySizeLimitWhitelist)
}

func hotLoadBinPackEnable(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldBinPackEnable := config.BinPackEnable
	newBinPackEnable := hotLoadConfig.BinPackEnable
	if oldBinPackEnable != newBinPackEnable {
		log.GetLogger().Infof("BinPackEnable changed, old BinPackEnable: %t, new BinPackEnable: %t",
			oldBinPackEnable, newBinPackEnable)
		config.BinPackEnable = newBinPackEnable
	}
}

func hotLoadSchedulingEnable(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldSchedulingEnable := config.SchedulingEnable
	newSchedulingEnable := hotLoadConfig.SchedulingEnable
	if oldSchedulingEnable != newSchedulingEnable {
		log.GetLogger().Infof("SchedulingEnable changed, old SchedulingEnable: %t, new SchedulingEnable: %t",
			oldSchedulingEnable, newSchedulingEnable)
		config.SchedulingEnable = newSchedulingEnable
	}
}

func hotLoadReservedAntiAffinity(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldReservedAnti := config.PoolMgrConfig.ReservedAntiAffinity
	newReservedAnti := hotLoadConfig.ReservedAntiAffinity
	if oldReservedAnti != newReservedAnti {
		config.PoolMgrConfig.ReservedAntiAffinity = newReservedAnti
	}
}

func hotLoadDisablePriorityQueue(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldCfg := atomic.LoadUint32(&config.PoolMgrConfig.DisablePriorityQueue)
	newCfg := hotLoadConfig.DisablePriorityQueue
	if oldCfg != newCfg {
		log.GetLogger().Infof("update disablePriorityQueue from %d to %d", oldCfg, newCfg)
		atomic.StoreUint32(&config.PoolMgrConfig.DisablePriorityQueue, newCfg)
	}
}

func hotLoadDisableCircuitBreaker(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldCfg := atomic.LoadUint32(&config.PoolMgrConfig.DisableCircuitBreaker)
	newCfg := hotLoadConfig.DisableCircuitBreaker
	if oldCfg != newCfg {
		log.GetLogger().Infof("update disableCircuitBreaker from %d to %d", oldCfg, newCfg)
		atomic.StoreUint32(&config.PoolMgrConfig.DisableCircuitBreaker, newCfg)
	}
}

func hotLoadDisablePoolRollout(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldCfg := atomic.LoadUint32(&config.PoolMgrConfig.DisablePoolRollout)
	newCfg := hotLoadConfig.DisablePoolRollout
	if oldCfg != newCfg {
		log.GetLogger().Infof("update disablePoolRollout from %d to %d", oldCfg, newCfg)
		atomic.StoreUint32(&config.PoolMgrConfig.DisablePoolRollout, newCfg)
	}
}

func hotLoadSnapshotTenanIds(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldCfg := config.PoolMgrConfig.SnapshotTenantIDsConfig
	newCfg := hotLoadConfig.SnapshotTenantIDsConfig
	// remove duplicates
	oldCfg.TenantIDs = removeDuplicates(oldCfg.TenantIDs)
	newCfg.TenantIDs = removeDuplicates(newCfg.TenantIDs)

	if newCfg.Enable != oldCfg.Enable || !isStringArrEqual(oldCfg.TenantIDs, newCfg.TenantIDs) {
		log.GetLogger().Infof("update SnapshotTenantIDsConfig from %s to %s", oldCfg, newCfg)
		config.PoolMgrConfig.SnapshotTenantIDsConfig = newCfg
	}
}

func isStringArrEqual(oldArr []string, newArr []string) bool {
	// compare len first
	oldArrLen := len(oldArr)
	newArrLen := len(newArr)
	if oldArrLen != newArrLen {
		return false
	}
	// compare arr string content
	sortedOldArr := make([]string, oldArrLen)
	sortedNewArr := make([]string, newArrLen)
	copy(sortedOldArr, oldArr)
	copy(sortedNewArr, newArr)
	sort.Strings(sortedOldArr)
	sort.Strings(sortedNewArr)
	return reflect.DeepEqual(sortedOldArr, sortedNewArr)
}

func removeDuplicates(arr []string) []string {
	uniqueMap := make(map[string]bool)
	result := []string{}

	for _, str := range arr {
		if !uniqueMap[str] {
			uniqueMap[str] = true
			result = append(result, str)
		}
	}

	return result
}

func hotLoadEnableMultiSnapshot(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldCfg := config.PoolMgrConfig.MultiSnapshotConfig
	newCfg := hotLoadConfig.MultiSnapshotConfig

	if newCfg.SnapshotCheckInfo.JavaRuntimeSize == 0 || newCfg.SnapshotCheckInfo.JavaRuntimeHash == "" ||
		newCfg.SnapshotCheckInfo.ApmJavaAgentSize == 0 || newCfg.SnapshotCheckInfo.RunTimeSize == 0 ||
		newCfg.SnapshotCheckInfo.ImageOS == "" {
		content, err := ioutil.ReadFile(snapshotCheckInfoPath)
		if err != nil {
			log.GetLogger().Errorf("Read snapshot check info from file failed, err is %s", err)
			return
		}
		var snapshotCheckInfo types.SnapshotCheckInfo
		unmarshalErr := json.Unmarshal(content, &snapshotCheckInfo)
		if unmarshalErr != nil {
			log.GetLogger().Errorf("Unmarshal content failed, err is %s", unmarshalErr)
			return
		}
		newCfg.SnapshotCheckInfo.JavaRuntimeSize = snapshotCheckInfo.JavaRuntimeSize
		newCfg.SnapshotCheckInfo.JavaRuntimeHash = snapshotCheckInfo.JavaRuntimeHash
		newCfg.SnapshotCheckInfo.ApmJavaAgentSize = snapshotCheckInfo.ApmJavaAgentSize
		newCfg.SnapshotCheckInfo.RunTimeSize = snapshotCheckInfo.RunTimeSize
		newCfg.SnapshotCheckInfo.ImageOS = snapshotCheckInfo.ImageOS
		log.GetLogger().Infof("snapshotCheckInfo javaRuntime size is %d, ApmJavaAgentSize is %d, "+
			"RunTimeSize is %d, ImageOS is %s, hash is %s", snapshotCheckInfo.JavaRuntimeSize,
			snapshotCheckInfo.ApmJavaAgentSize, snapshotCheckInfo.RunTimeSize, snapshotCheckInfo.ImageOS,
			snapshotCheckInfo.JavaRuntimeHash)
	}

	if oldCfg != newCfg {
		log.GetLogger().Infof("update MultiSnapshotConfig from %s to %s", oldCfg, newCfg)
		config.PoolMgrConfig.MultiSnapshotConfig = newCfg
	}
}

func hotLoadCheckpointPodConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldConfig := &config.PoolMgrConfig.CheckpointPodConfig
	newCfg := &hotLoadConfig.CheckpointPodConfig
	if newCfg == nil || oldConfig == nil {
		return
	}
	if oldConfig != newCfg {
		if newCfg.WaitReadyTimeout > 0 {
			log.GetLogger().Infof("CheckpointPodConfig WaitReadyTimeout update old: %d, new: %d",
				oldConfig.WaitReadyTimeout, newCfg.WaitReadyTimeout)
			oldConfig.WaitReadyTimeout = newCfg.WaitReadyTimeout
		}

		if newCfg.FailureRetryInterval > 0 {
			log.GetLogger().Infof("CheckpointPodConfig FailureRetryInterval update old: %d, new: %d",
				oldConfig.FailureRetryInterval, newCfg.FailureRetryInterval)
			oldConfig.FailureRetryInterval = newCfg.FailureRetryInterval
		}

		if newCfg.FailureRetryTimes > 0 {
			log.GetLogger().Infof("CheckpointPodConfig FailureRetryTimes update old: %d, new: %d",
				oldConfig.FailureRetryTimes, newCfg.FailureRetryTimes)
			oldConfig.FailureRetryTimes = newCfg.FailureRetryTimes
		}

		if newCfg.MaxConcurrency > 0 && oldConfig.MaxConcurrency != newCfg.MaxConcurrency {
			log.GetLogger().Infof("CheckpointPodConfig MaxConcurrency update old: %d, new: %d",
				oldConfig.MaxConcurrency, newCfg.MaxConcurrency)
			oldConfig.MaxConcurrency = newCfg.MaxConcurrency
		}

		if newCfg.CRIUCheckpointOpts != "" {
			log.GetLogger().Infof("CheckpointPodConfig CRIUCheckpointOpts update old: %s, new: %s",
				oldConfig.CRIUCheckpointOpts, newCfg.CRIUCheckpointOpts)
			oldConfig.CRIUCheckpointOpts = newCfg.CRIUCheckpointOpts
		}

		// RestoreFailureThreshold default is 10
		if oldConfig.RestoreFailureThreshold == 0 {
			oldConfig.RestoreFailureThreshold = 10
		}

		if newCfg.RestoreFailureThreshold > 0 {
			log.GetLogger().Infof("CheckpointPodConfig RestoreFailureThreshold update old: %s, new: %s",
				oldConfig.RestoreFailureThreshold, newCfg.RestoreFailureThreshold)
			oldConfig.RestoreFailureThreshold = newCfg.RestoreFailureThreshold
		}

		if newCfg.SnapshotStorageType != "" && oldConfig.SnapshotStorageType != newCfg.SnapshotStorageType {
			log.GetLogger().Infof("CheckpointPodConfig SnapshotStorageType update old: %s, new: %s",
				oldConfig.SnapshotStorageType, newCfg.SnapshotStorageType)
			oldConfig.SnapshotStorageType = newCfg.SnapshotStorageType
		}
	}
}

func hotLoadHeterogeneousSchedulingConfig(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldStrategy := config.HeterogeneousSchedulingConfig.Strategy
	newStrategy := hotLoadConfig.HeterogeneousSchedulingConfig.Strategy
	if oldStrategy != newStrategy {
		log.GetLogger().Infof("update heterogeneousSchedulingConfig from %s to %s", oldStrategy, newStrategy)
		config.HeterogeneousSchedulingConfig.Strategy = newStrategy
	}
}

func hotLoadServerAddr(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldServerAddr := config.MonitoringServerConfig.ServerAddr
	newServerAddr := hotLoadConfig.ServerAddr
	if oldServerAddr != newServerAddr {
		log.GetLogger().Infof("update serverAddr from %s to %s", oldServerAddr, newServerAddr)
		config.MonitoringServerConfig.ServerAddr = newServerAddr
	}
	oldTimeoutConf := config.MonitoringServerConfig.TimeoutConf
	newTimeoutConf := hotLoadConfig.MonitoringTimeConf
	if oldTimeoutConf != newTimeoutConf {
		log.GetLogger().Infof("update timeout from %v to %v", oldServerAddr, newServerAddr)
		config.MonitoringServerConfig.TimeoutConf = newTimeoutConf
	}
}

func hotLoadSmartWarmerEnable(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	log.GetLogger().Infof("update SmartWarmerEnable from %t to %t", config.SmartWarmerEnable,
		hotLoadConfig.SmartWarmerEnable)
	config.SmartWarmerEnable = hotLoadConfig.SmartWarmerEnable
}

func hotLoadUpdateFunctionCacheEnable(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	log.GetLogger().Infof("update UpdateFunctionCacheEnable from %t to %t", config.UpdateFunctionCacheEnable,
		hotLoadConfig.UpdateFunctionCacheEnable)
	config.UpdateFunctionCacheEnable = hotLoadConfig.UpdateFunctionCacheEnable
}

func hotLoadCoreCPUFeature(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	if hotLoadConfig.CoreCPUFeatures != "" {
		log.GetLogger().Infof("update CoreCPUFeature from %s to %s", config.PoolMgrConfig.CoreCPUFeatures,
			hotLoadConfig.CoreCPUFeatures)
		config.PoolMgrConfig.CoreCPUFeatures = hotLoadConfig.CoreCPUFeatures
	}
}

func hotLoadEnablePublicNetworkPat(config *FaasConfig, hotLoadConfig *HotLoadConfig) {
	oldEnablePublicNetworkPat := config.PoolMgrConfig.EnablePublicNetworkPat
	newEnablePublicNetworkPat := hotLoadConfig.EnablePublicNetworkPat
	if oldEnablePublicNetworkPat != newEnablePublicNetworkPat {
		log.GetLogger().Infof("EnablePublicNetworkPat changed, old EnablePublicNetworkPat: "+
			"%t, new EnablePublicNetworkPat: %t", oldEnablePublicNetworkPat, newEnablePublicNetworkPat)
		config.PoolMgrConfig.EnablePublicNetworkPat = newEnablePublicNetworkPat
	}
}

// AddConfigChangedCallback add callback func to process config update
func AddConfigChangedCallback(callback ChangedCallback) {
	configChangedCallbacks = append(configChangedCallbacks, callback)
}

func getEnableRemoteWorkerFlag() bool {
	enableRemoteWorkerFlag := os.Getenv("ENABLE_REMOTE_WORKER")
	if enableRemoteWorkerFlag == "true" {
		log.GetLogger().Infof("remote worker switch is on")
		return true
	}
	return false
}

func getHeterogeneousSchedulingConfig() HeterogeneousSchedulingConfig {
	config := os.Getenv("HETEROGENEOUS_SCHEDULING_CONFIG")
	heterogeneousSchedulingConfig := HeterogeneousSchedulingConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &heterogeneousSchedulingConfig)
		if err != nil {
			log.GetLogger().Error("environment HETEROGENEOUS_SCHEDULING_CONFIG is not json string")
			return HeterogeneousSchedulingConfig{}
		}
		if _, err := govalidator.ValidateStruct(heterogeneousSchedulingConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the heterogeneous scheduling config: %s", err.Error())
			return HeterogeneousSchedulingConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment HETEROGENEOUS_SCHEDULING_CONFIG")
		return HeterogeneousSchedulingConfig{}
	}
	return heterogeneousSchedulingConfig
}

func addPoolContainerTags(config *FaasConfig) error {
	workerInitTag := os.Getenv("WORKER_INIT_TAG")
	if workerInitTag == "" {
		return errors.New("no worker-init image tag")
	}
	config.PoolMgrConfig.PoolContainerTags.WorkerInitImageTag = workerInitTag

	workerTag := os.Getenv("WORKER_TAG")
	if workerTag == "" {
		return errors.New("no worker image tag")
	}
	config.PoolMgrConfig.PoolContainerTags.WorkerImageTag = workerTag

	runtimeManagerTag := os.Getenv("RUNTIME_MANAGER_TAG")
	if runtimeManagerTag == "" {
		return errors.New("no runtime-manager image tag")
	}
	config.PoolMgrConfig.PoolContainerTags.RuntimeImageTag = runtimeManagerTag
	debugAgentTag := os.Getenv("DEBUG_AGENT_TAG")
	if debugAgentTag == "" {
		log.GetLogger().Warnf("no debug-agent image tag")
	}
	config.PoolMgrConfig.PoolContainerTags.DebugAgentImageTag = debugAgentTag

	return nil
}

// String return Marshal Faas Config
func (c *FaasConfig) String() string {
	s, err := json.Marshal(c)
	if err != nil {
		return ""
	}

	r := string(s)
	return r
}

// GetLocalAuth returns local auth
func GetLocalAuth() *localauth.AuthConfig {
	if configInstance != nil {
		return configInstance.LocalAuth
	}
	return nil
}

// GetK8sCidr returns k8s cidr
func GetK8sCidr() *K8sCidr {
	if configInstance != nil {
		return &configInstance.K8sCidr
	}
	return nil
}

// GetEnableRemoteWorkerFlag -
func GetEnableRemoteWorkerFlag() bool {
	if configInstance != nil {
		return configInstance.EnableRemoteWorkerFlag
	}
	return false
}

// IsHeterogeneousScheduling -
func IsHeterogeneousScheduling() bool {
	if configInstance != nil {
		return configInstance.HeterogeneousSchedulingConfig.Enable
	}
	return false
}

// IsForceStrategy -
func IsForceStrategy() bool {
	if configInstance != nil {
		return configInstance.HeterogeneousSchedulingConfig.Strategy == forceStrategy
	}
	return true
}

var (
	// radicalAutoscaler will use radical autoscaler if set true(default);
	// otherwise it will use origin autoscaler
	radicalAutoscaler = parseRadicalAutoscaler()
	// radicalRedirectTimeout when worker scaled down by radical autoscaler, workermanager should
	// return existing worker on other node during this timeout
	radicalRedirectTimeout = parseRadicalRedirectTimeout()
)

const (
	defaultRadicalRedirectTimeout = 100 * time.Millisecond
	snapshotCheckInfoPath         = "/home/sn/bin/snapshotCheckInfo"
)

func parseRadicalAutoscaler() bool {
	// if DISABLE_RADICAL_AUTOSCALER is set true, should disable RadicalAutoscaler and set it to false
	if os.Getenv("DISABLE_RADICAL_AUTOSCALER") == "true" {
		log.GetLogger().Infof("radical autoscaler disabled")
		return false
	}
	return true
}

// EnableRadicalAutoscaler returns whether radicalAutoscaler is enabled
func EnableRadicalAutoscaler() bool {
	return radicalAutoscaler
}

func parseRadicalRedirectTimeout() time.Duration {
	if t := os.Getenv("RADICAL_REDIRECT_TIMEOUT"); t != "" {
		ti, err := strconv.Atoi(t)
		if err != nil || ti <= 0 {
			return defaultRadicalRedirectTimeout
		}
		return time.Duration(ti) * time.Millisecond
	}
	return defaultRadicalRedirectTimeout
}

// GetRadicalRedirectTimeout returns radicalRedirectTimeout
func GetRadicalRedirectTimeout() time.Duration {
	return radicalRedirectTimeout
}

// EnableHTTPS checks if https is enabled and lazily initialize the tls library
func EnableHTTPS() bool {
	cfg, err := GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get configuration, %s", err.Error())
		return false
	}
	if cfg.HTTPSConfig == nil {
		return false // HTTPSConfig is not provided, default to turn off
	}

	enabled := cfg.HTTPSConfig.HTTPSEnable
	if enabled {
		// It is OK to call InitTLSConfig multiple times because it uses sync.Once internally
		err = tls.InitTLSConfig(cfg.HTTPSConfig.TLSProtocol, cfg.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return false
		}
	}
	return enabled
}

// PoolManagerProactiveElasticConfig proactive scaling config of pool pod from env json string
type PoolManagerProactiveElasticConfig struct {
	// set used pod record to redis
	UsedPodRecordResetAndUploadInterval int `json:"usedPodRecordResetAndUploadInterval" valid:",optional"`
	UsedPodRecordExpiration             int `json:"usedPodRecordExpiration" valid:",optional"`
	// get used pod prediction result from redis
	PoolPredictionResultDownloadInterval int `json:"poolPredictionResultDownloadInterval" valid:",optional"`
	PoolPredictionResultLength           int `json:"poolPredictionResultLength" valid:",optional"`
	ApplyPredictionResultToPoolInterval  int `json:"applyPredictionResultToPoolInterval" valid:",optional"`
	ScalingPlanTimeWindow                int `json:"scalingPlanTimeWindow" valid:",optional"`
	ReactiveScalingUpResetInterval       int `json:"reactiveScalingUpResetInterval" valid:",optional"`
	// cooldown time for prediction mode fusion
	EnableFusionPolicy            int32   `json:"enableFusionPolicy" valid:",optional"`
	ScalingUpTriggerTimeThreshold int32   `json:"scalingUpTriggerTimeThreshold" valid:",optional"`
	CooldownTime                  int32   `json:"cooldownTime" valid:",optional"`
	UpperBoundFactor              float64 `json:"upperBoundFactor" valid:",optional"`
	LowerBoundFactor              float64 `json:"lowerBoundFactor" valid:",optional"`
	MaxScalingStep                int32   `json:"maxScalingStep" valid:",optional"`
	ScalingPlanFactor             float64 `json:"scalingPlanFactor" valid:",optional"`
}

var (
	defaultElasticScalingConfig = &ElasticScalingConfig{
		UsedPodNumsExceedPercentage:      0.5,
		UsedPodNumsExceedFreePercentage:  0.75,
		UsedPodNumsExceedHeavyPercentage: 0.5,
		ScaleUpPodNumsPercentage:         0.25,
		ScaleUpPodNumsFreePercentage:     0.25,
		ScaleUpPodNumsHeavyPercentage:    0.5,
		ExtraScaleUpPodNumsPercentage:    0.1,
		UsedPodNumsBelowPercentage:       0.1,
		ScaleDownPodNumsPercentage:       0.125,
		EffectiveThresholdReplicas:       0,
		MaxScaleUpPodNums:                600,
		ScaleDownInterval:                60,
		ThresholdResourceUsage:           0.75,
	}
	defaultProactiveElasticScalingConfig = &ElasticScalingConfig{
		UsedPodNumsExceedPercentage:      0.7,
		UsedPodNumsExceedFreePercentage:  0.85,
		UsedPodNumsExceedHeavyPercentage: 0.7,
		ScaleUpPodNumsPercentage:         0.2,
		ScaleUpPodNumsFreePercentage:     0.2,
		ScaleUpPodNumsHeavyPercentage:    0.3,
		// same as reactive scaling config
		ExtraScaleUpPodNumsPercentage: 0.1,
		EffectiveThresholdReplicas:    0,
		ThresholdResourceUsage:        0.75,
		MaxScaleUpPodNums:             600,
		// turn off scaling down in proactive mode
		UsedPodNumsBelowPercentage: 0.1,
		ScaleDownPodNumsPercentage: 0.125,
		ScaleDownInterval:          60,
	}
	defaultProactiveModePoolList = map[string]bool{
		"pool22-300-128-fusion": false,
		"pool23-400-256-fusion": false,
		"pool24-600-512-fusion": false,
	}
	defaultPoolManagerProactiveElasticConfig = &PoolManagerProactiveElasticConfig{
		// set used pod record to redis
		UsedPodRecordResetAndUploadInterval: 1,
		UsedPodRecordExpiration:             8 * 24,
		// get used pod prediction result from redis
		PoolPredictionResultDownloadInterval: 10,
		PoolPredictionResultLength:           30,
		ApplyPredictionResultToPoolInterval:  1,
		ScalingPlanTimeWindow:                5,
		ReactiveScalingUpResetInterval:       15,
		ScalingUpTriggerTimeThreshold:        3,
		CooldownTime:                         15,
		EnableFusionPolicy:                   1,
		UpperBoundFactor:                     1.2,
		LowerBoundFactor:                     0.8,
		MaxScalingStep:                       30,
		ScalingPlanFactor:                    1.35,
	}
)

// GetPoolManagerProactiveElasticConfig get pool manager proactive config for pool manager
func getPoolManagerProactiveElasticConfig() *PoolManagerProactiveElasticConfig {
	configStr := os.Getenv("POOL_MANAGER_PROACTIVE_ELASTIC_CONFIG")
	if configStr == "" {
		log.GetLogger().Warnf("POOL_MANAGER_PROACTIVE_ELASTIC_CONFIG env is empty and use default config")
		return defaultPoolManagerProactiveElasticConfig
	}
	var config *PoolManagerProactiveElasticConfig
	if err := json.Unmarshal([]byte(configStr), &config); err != nil {
		log.GetLogger().Errorf("json unmarshal error: %s with proactive scaling config: %s",
			err.Error(), configStr)
		return defaultPoolManagerProactiveElasticConfig
	}
	return config
}

// GetProactiveModePoolList return pool list which can be predicted
func getProactiveModePoolList() map[string]bool {
	configStr := os.Getenv("PROACTIVE_SCALING_POOL_LIST")
	if configStr == "" {
		log.GetLogger().Warnf("PROACTIVE_SCALING_POOL_LIST env is empty and use default pool list")
		return defaultProactiveModePoolList
	}
	var proactiveModePoolList map[string]bool
	if err := json.Unmarshal([]byte(configStr), &proactiveModePoolList); err != nil {
		log.GetLogger().Errorf("json unmarshal error: %s with proactive pool list: %s",
			err.Error(), configStr)
		return defaultProactiveModePoolList
	}
	return proactiveModePoolList
}

// GetProactiveElasticScalingConfig get proactive elastic scaling config for resource pool prediction
func getProactiveElasticScalingConfig() *ElasticScalingConfig {
	configStr := os.Getenv("PROACTIVE_SCALING_CONFIG")
	if configStr == "" {
		log.GetLogger().Warnf("env PROACTIVE_SCALING_CONFIG is empty and use default value")
		return defaultProactiveElasticScalingConfig
	}
	proactiveElasticScalingConfig := &ElasticScalingConfig{}
	if err := json.Unmarshal([]byte(configStr), &proactiveElasticScalingConfig); err != nil {
		log.GetLogger().Errorf("json unmarshal error: %s with proactive scaling config: %s",
			err.Error(), configStr)
		return defaultProactiveElasticScalingConfig
	}
	return proactiveElasticScalingConfig
}

// GetElasticScalingConfig get elastic scaling config
func GetElasticScalingConfig() *ElasticScalingConfig {
	if configInstance != nil {
		return configInstance.ElasticScalingConfig
	}
	return getElasticScalingConfig()
}

// getEnableUploadUsedPodRecordToRedis get env variable for EnableUploadUsedPodRecordToRedis
func getEnableUploadUsedPodRecordToRedis() bool {
	configStr := os.Getenv("ENABLE_UPLOAD_USED_POD_RECORD_TO_REDIS")
	if configStr == "" {
		log.GetLogger().Warnf("ENABLE_UPLOAD_USED_POD_RECORD_TO_REDIS env is empty and use default value: false")
		return false
	}
	return strings.EqualFold(strings.TrimSpace(configStr), "true")
}

// getElasticScalingConfig get elastic scaling config from env: ELASTIC_SCALING_CONFIG
func getElasticScalingConfig() *ElasticScalingConfig {
	configStr := os.Getenv("ELASTIC_SCALING_CONFIG")
	if configStr == "" {
		log.GetLogger().Warnf("ELASTIC_SCALING_CONFIG env is empty and use default elastic scaling config")
		return defaultElasticScalingConfig
	}
	elasticScalingCfg := &ElasticScalingConfig{}
	if err := json.Unmarshal([]byte(configStr), elasticScalingCfg); err != nil {
		log.GetLogger().Errorf("json unmarshal error: %s with elastic scaling config: %s", err.Error(), configStr)
		return defaultElasticScalingConfig
	}
	return elasticScalingCfg
}

// getImageAffinityCacheConfig get image affinity cache config from env: IMAGE_AFFINITY_CACHE_CONFIG
func getImageAffinityCacheConfig() *ImageAffinityCacheConfig {
	configStr := os.Getenv("IMAGE_AFFINITY_CACHE_CONFIG")
	if configStr == "" {
		log.GetLogger().Warnf("IMAGE_AFFINITY_CACHE_CONFIG env is empty and disable cache")
		return &ImageAffinityCacheConfig{
			DisableImageAffinityCache: 1,
			TenantWhiteList:           nil,
			CacheNodesPercentage:      1,
			CacheNodesNumber:          0,
		}
	}
	imageAffinityCacheCfg := &ImageAffinityCacheConfig{}
	if err := json.Unmarshal([]byte(configStr), imageAffinityCacheCfg); err != nil {
		log.GetLogger().Errorf("json unmarshal error: %s with image affinity cache config: %s",
			err.Error(), configStr)
		return &ImageAffinityCacheConfig{
			DisableImageAffinityCache: 1,
			TenantWhiteList:           nil,
			CacheNodesPercentage:      1,
			CacheNodesNumber:          0,
		}
	}
	log.GetLogger().Infof("success get image affinity cache config, disable: %d",
		imageAffinityCacheCfg.DisableImageAffinityCache)
	return imageAffinityCacheCfg
}

// GetHotloadMonitorConf get real-time config info from env
func GetHotloadMonitorConf() (string, redisclient.TimeoutConf, error) {
	if configInstance != nil {
		log.GetLogger().Infof("get serverAddr: %s, timeoutConf: %v",
			configInstance.MonitoringServerConfig.ServerAddr, configInstance.MonitoringServerConfig.TimeoutConf)
		return configInstance.MonitoringServerConfig.ServerAddr, configInstance.MonitoringServerConfig.TimeoutConf, nil
	}
	return "", redisclient.GetDefaultTimeoutConf(),
		fmt.Errorf("configInstance is nil when get RealTimeMonitorServerAddr")
}

func getAutoScalerConfig() []AutoScalerConfig {
	config := os.Getenv("AUTO_SCALER_CONFIG")
	autoScalerConfig := make([]AutoScalerConfig, 0)
	if config != "" {
		err := json.Unmarshal([]byte(config), &autoScalerConfig)
		if err != nil {
			log.GetLogger().Error("environment AUTO_SCALER_CONFIG is not json string")
			return make([]AutoScalerConfig, 0)
		}
	} else {
		log.GetLogger().Error("failed to get environment AUTO_SCALER_CONFIG")
		return make([]AutoScalerConfig, 0)
	}
	return autoScalerConfig
}

func getEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv("ETCD_CONFIG")
	etcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), etcdConfig)
		if err != nil {
			log.GetLogger().Error("environment ETCD_CONFIG is not json string")
			return &etcd3.ClientConfig{}
		}
		if _, err := govalidator.ValidateStruct(etcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the etcd config: %s", err.Error())
			return &etcd3.ClientConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment ETCD_CONFIG")
		return &etcd3.ClientConfig{}
	}
	return etcdConfig
}

func getRouterEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv("ROUTER_ETCD_CONFIG")
	routerEtcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), routerEtcdConfig)
		if err != nil {
			log.GetLogger().Error("environment ROUTER_ETCD_CONFIG is not json string")
			return &etcd3.ClientConfig{}
		}
		if _, err := govalidator.ValidateStruct(routerEtcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the router etcd config: %s", err.Error())
			return &etcd3.ClientConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment ROUTER_ETCD_CONFIG")
		return &etcd3.ClientConfig{}
	}
	return routerEtcdConfig
}

func getCAEMetaEtcdConfig() *etcd3.ClientConfig {
	config := &etcd3.ClientConfig{}
	if err := json.Unmarshal([]byte(os.Getenv("CAE_META_ETCD_CONFIG")), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal cae meta etcd config %s", err.Error())
		return nil
	}
	if len(config.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae meta etcd server list, skip connect")
		return nil
	}
	return config
}

func getCAERouterEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv("CAE_ROUTER_ETCD_CONFIG")
	caeRouterEtcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), caeRouterEtcdConfig)
		if err != nil {
			log.GetLogger().Error("environment CAE_ROUTER_ETCD_CONFIG is not json string")
			return nil
		}
		if _, err := govalidator.ValidateStruct(caeRouterEtcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the global router etcd config: %s", err.Error())
			return nil
		}
	} else {
		log.GetLogger().Error("failed to get environment CAE_ROUTER_ETCD_CONFIG")
		return nil
	}
	if len(caeRouterEtcdConfig.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae router etcd server list, skip connect")
		return nil
	}
	return caeRouterEtcdConfig
}

func getMetaEtcdProxyConfig() *etcd3.ClientConfig {
	config := os.Getenv("META_ETCD_PROXY_CONFIG")
	metaEtcdProxyConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), metaEtcdProxyConfig)
		if err != nil {
			log.GetLogger().Errorf("environment META_ETCD_PROXY_CONFIG is not json string: %s", err.Error())
			return nil
		}
		if _, err := govalidator.ValidateStruct(metaEtcdProxyConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the etcd proxy config: %s", err.Error())
			return nil
		}
	} else {
		log.GetLogger().Warnf("failed to get environment META_ETCD_PROXY_CONFIG")
		return nil
	}
	if len(metaEtcdProxyConfig.ServerList) == 0 {
		log.GetLogger().Warnf("empty meta etcd proxy server list, skip connect")
		return nil
	}
	return metaEtcdProxyConfig
}

func getCAEMetaEtcdProxyConfig() *etcd3.ClientConfig {
	config := os.Getenv("CAE_META_ETCD_PROXY_CONFIG")
	caeMetaEtcdProxy := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), caeMetaEtcdProxy)
		if err != nil {
			log.GetLogger().Errorf("environment CAE_ETCD_PROXY_CONFIG is not json string: %s", err.Error())
			return nil
		}
		if _, err := govalidator.ValidateStruct(caeMetaEtcdProxy); err != nil {
			log.GetLogger().Errorf("failed to validate the cae etcd proxy config: %s", err.Error())
			return nil
		}
	} else {
		log.GetLogger().Warnf("failed to get environment CAE_ETCD_PROXY_CONFIG")
		return nil
	}
	if len(caeMetaEtcdProxy.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae meta etcd proxy server list, skip connect")
		return nil
	}
	return caeMetaEtcdProxy
}

func getEtcdClusterConfig() EtcdClusterConfig {
	config := os.Getenv("ETCD_CLUSTER")
	etcdClusterConfig := EtcdClusterConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &etcdClusterConfig)
		if err != nil {
			log.GetLogger().Error("environment ETCD_CLUSTER is not json string")
			return EtcdClusterConfig{}
		}
		if _, err := govalidator.ValidateStruct(etcdClusterConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the etcd cluster config: %s", err.Error())
			return EtcdClusterConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment ETCD_CLUSTER")
		return EtcdClusterConfig{}
	}
	return etcdClusterConfig
}

func getK8sCidrConfig() K8sCidr {
	config := os.Getenv("K8S_CIDR")
	k8sCidrConfig := K8sCidr{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &k8sCidrConfig)
		if err != nil {
			log.GetLogger().Error("environment K8S_CIDR is not json string")
			return K8sCidr{}
		}
		if _, err := govalidator.ValidateStruct(k8sCidrConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the k8s cidr config: %s", err.Error())
			return K8sCidr{}
		}
	} else {
		log.GetLogger().Error("failed to get environment K8S_CIDR")
		return K8sCidr{}
	}
	return k8sCidrConfig
}

func getMonitoringRedisServerConfig() MonitoringRedisServerConfig {
	config := os.Getenv("MONITORING_SERVER_CONFIG")
	monitoringServerConfig := MonitoringRedisServerConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &monitoringServerConfig)
		if err != nil {
			log.GetLogger().Error("environment MONITORING_SERVER_CONFIG is not json string")
			return MonitoringRedisServerConfig{}
		}
		if _, err := govalidator.ValidateStruct(monitoringServerConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the monitoring redis server config: %s", err.Error())
			return MonitoringRedisServerConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment MONITORING_SERVER_CONFIG")
		return MonitoringRedisServerConfig{}
	}
	return monitoringServerConfig
}

func getAuthenticationEnable() bool {
	config := os.Getenv("AUTHENTICATION_ENABLE")
	return strings.EqualFold(strings.TrimSpace(config), "true")
}

func getHTTPSConfig() *tls.InternalHTTPSConfig {
	config := os.Getenv("HTTPS_CONFIG")
	httpsConfig := &tls.InternalHTTPSConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &httpsConfig)
		if err != nil {
			log.GetLogger().Error("environment HTTPS_CONFIG is not json string")
			return &tls.InternalHTTPSConfig{}
		}
		if _, err := govalidator.ValidateStruct(httpsConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the https config: %s", err.Error())
			return &tls.InternalHTTPSConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment HTTPS_CONFIG")
		return &tls.InternalHTTPSConfig{}
	}
	return httpsConfig
}

func getAlarmServerAddress() string {
	config := os.Getenv("ALARM_SERVER_ADDRESS")
	return config
}

func getLocalAuthConfig() *localauth.AuthConfig {
	config := os.Getenv("LOCAL_AUTH")
	localAuthConfig := &localauth.AuthConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &localAuthConfig)
		if err != nil {
			log.GetLogger().Error("environment LOCAL_AUTH is not json string")
			return &localauth.AuthConfig{}
		}
		if _, err := govalidator.ValidateStruct(localAuthConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the local auth config: %s", err.Error())
			return &localauth.AuthConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment LOCAL_AUTH")
		return &localauth.AuthConfig{}
	}
	return localAuthConfig
}

func getFunctionNameSeparator() string {
	config := os.Getenv("FUNCTION_NAME_SEPARATOR")
	return config
}

func getPoolMgrConfig() PoolMgrConfig {
	config := os.Getenv("POOLMGR_CONFIG")
	poolMgrConfigConfig := PoolMgrConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &poolMgrConfigConfig)
		if err != nil {
			log.GetLogger().Errorf("environment POOLMGR_CONFIG is not json string, %s", err.Error())
			return PoolMgrConfig{}
		}
		if _, err := govalidator.ValidateStruct(poolMgrConfigConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the poolmgr config: %s", err.Error())
			return PoolMgrConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment POOLMGR_CONFIG")
		return PoolMgrConfig{}
	}
	return poolMgrConfigConfig
}

// GetSchedulerWithDisk -
func GetSchedulerWithDisk() bool {
	config, err := GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config when calculateNotSchedulerList")
		return false
	}
	if config == nil {
		log.GetLogger().Errorf("config is nil when calculateNotSchedulerList")
		return false
	}

	return config.ScheduleWithDisk
}

func addSSDPoolLabels(ssdPoolLabels map[string]string) {
	for key := range ssdPoolLabels {
		val := ssdPoolLabels[key]
		if val == "" {
			log.GetLogger().Errorf("empty value of key %s", key)
			continue
		}
		SSDPoolLabelsMap.Store(key, val)
	}
}

func updateSSDPoolLabels(oldSSDPoolLabels, newSSDPoolLabels map[string]string) {
	for key := range newSSDPoolLabels {
		val := newSSDPoolLabels[key]
		if val == "" {
			log.GetLogger().Errorf("empty value of key %s", key)
			continue
		}
		SSDPoolLabelsMap.Store(key, val)
	}
	for key := range oldSSDPoolLabels {
		_, ok := newSSDPoolLabels[key]
		if !ok {
			SSDPoolLabelsMap.Delete(key)
			log.GetLogger().Infof("delete already moved key %s", key)
		}
	}
}
