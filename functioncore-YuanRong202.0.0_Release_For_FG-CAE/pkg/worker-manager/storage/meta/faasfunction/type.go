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

// Package faasfunction faas cloud function
package faasfunction

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/trace"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/config"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DeadLetterConfigSpec defines dead letter config spec
// +k8s:openapi-gen=true
type DeadLetterConfigSpec struct {
	URL string `json:"url"`
}

// NspLocationSpec define nsp location spec
type NspLocationSpec struct {
	AppID                string `json:"appId"`
	BucketID             string `json:"bucketId"`
	ObjectID             string `json:"objectId"`
	BucketURL            string `json:"bucketUrl"`
	CodeCipherKeyID      string `json:"codeCipherKeyID" valid:",optional"`
	CodeEncryptKmsCipher string `json:"codeEncryptKmsCipher" valid:",optional"`
}

// LocationSpec define location spec
type LocationSpec struct {
	Nsp NspLocationSpec `json:"nsp"`
}

// CodeSpec define code spec
type CodeSpec struct {
	Location   LocationSpec `json:"location"`
	CodeSha256 string       `json:"codeSha256" valid:",optional"`
	CodeSha512 string       `json:"codeSha512" valid:",optional"`
	CodeSize   int64        `json:"codeSize,omitempty"`
}

// ConcurrencySpec define concurrency spec
type ConcurrencySpec struct {
	ReservedValue int64 `json:"reservedValue,omitempty"`
}

// HandlerSpec define handler spec
type HandlerSpec struct {
	Module string `json:"module"`
	Entry  string `json:"entry"`
}

// LibSpec define lib spec
type LibSpec struct {
	LibVersionID string `json:"libVersionId"`
	Size         int64  `json:"size"`
}

// ResourceSpec define resource spec
type ResourceSpec struct {
	CPU                int64  `json:"cpu"`
	Mem                int64  `json:"mem"`
	GpuMemory          int64  `json:"gpu_memory"`
	GpuType            string `json:"gpu_type"`
	EphemeralStorage   int64  `json:"ephemeral_storage"`
	CustomResources    string `json:"customResources"`
	EnableTmpExpansion bool   `json:"enable_tmp_expansion" valid:"optional"`
}

// VersionSpec define version spec
type VersionSpec struct {
	Number string `json:"number"`
	Name   string `json:"name"`
}

// RuntimeSpec define runtime spec
type RuntimeSpec struct {
	Language  string `json:"language"`
	WorkImage string `json:"workimage"`
	Initimage string `json:"initimage"`
}

// AutoScalerConfig define autoscaler config
type AutoScalerConfig struct {
	Name             string `json:"name,omitempty"`
	Type             string `json:"type,omitempty"`
	IsScaleUp        bool   `json:"isScaleUp,omitempty"`
	Duration         string `json:"duration,omitempty"`
	RepeatInterval   string `json:"repeatInterval,omitempty"`
	Tolerance        int64  `json:"tolerance,omitempty"`
	IdleMode         bool   `json:"idleMode,omitempty"`
	MinimumScale     int64  `json:"minimumScale,omitempty"`
	MaximumScale     int64  `json:"maximumScale,omitempty"`
	InstanceType     string `json:"instanceType,omitempty"`
	MetricsCondition string `json:"metricsCondition,omitempty"`
	MetricsType      string `json:"metricsType,omitempty"`
	ProjectID        string `json:"projectId,omitempty"`
	ScaleStep        int64  `json:"scaleStep,omitempty"`
	WorkloadID       string `json:"workloadId,omitempty"`
}

// Initializer include initializer handler and timeout
type Initializer struct {
	Handler string `json:"initializer_handler" valid:",optional"`
	Timeout int64  `json:"initializer_timeout" valid:",optional"`
}

// Role include x_role and app_x_role
type Role struct {
	XRole    string `json:"xrole" valid:",optional"`
	AppXRole string `json:"app_xrole" valid:",optional"`
}

// ConfigurationSpec define configuration spec
type ConfigurationSpec struct {
	Layers                []*types.Layer              `json:"layers"`
	FunctionName          string                      `json:"functionName,omitempty"`
	TenantID              string                      `json:"tenantId,omitempty"`
	FunctionVersion       string                      `json:"functionVersion,omitempty"`
	FunctionURN           string                      `json:"functionUrn"`
	Description           string                      `json:"description,omitempty"`
	DeadLetterConfig      DeadLetterConfigSpec        `json:"deadletterConfig,omitempty"`
	EnvCipherKeyID        string                      `json:"envCipherKeyID,omitempty"`
	Env                   []corev1.EnvVar             `json:"env,omitempty"`
	FunctionVersionURN    string                      `json:"functionVersionUrn,omitempty"`
	Handler               HandlerSpec                 `json:"handler"`
	Libs                  LibSpec                     `json:"libs,omitempty"`
	Resource              ResourceSpec                `json:"resource,omitempty"`
	Runtime               RuntimeSpec                 `json:"runtime"`
	AutoScalerConfig      AutoScalerConfig            `json:"autoScalerConfig,omitempty"`
	Version               VersionSpec                 `json:"version,omitempty"`
	Created               string                      `json:"created,omitempty"`
	LastModified          string                      `json:"lastModified,omitempty"`
	Timeout               int64                       `json:"timeout,omitempty"`
	ReversionID           string                      `json:"reversionId,omitempty"`
	BusinessID            string                      `json:"businessId,omitempty"`
	Initializer           Initializer                 `json:"initializer" valid:",optional"`
	Role                  Role                        `json:"role" valid:",optional"`
	FuncVpc               Vpc                         `json:"funcVpc,omitempty"`
	EndpointTenantVpc     Vpc                         `json:"endpointTenantVpc,omitempty"`
	VPCTriggerImage       string                      `json:"vpcTriggerImage,omitempty"`
	DomainID              string                      `json:"domain_id" valid:",optional"`
	LogTankService        logservice.LogTankService   `json:"logTankService" valid:",optional"`
	TraceService          trace.TraceService          `json:"traceService" valid:",optional"`
	FuncMountConfig       FuncMountConfig             `json:"mount_config" valid:",optional"`
	ProjectName           string                      `json:"project_name" valid:",optional"`
	CustomContainerConfig types.CustomContainerConfig `json:"custom_container_config" valid:",optional"`
	EnableAuthInHeader    bool                        `json:"enable_auth_in_header" valid:"optional"`
	IsStatefulFunction    bool                        `json:"isStatefulFunction" valid:"optional"`
	Heartbeat             types.Heartbeat             `json:"heartbeat" valid:",optional"`
	StartupHeartbeat      types.Heartbeat             `json:"startupHeartbeat" valid:",optional"`
	DNSDomainCfg          []types.DNSDomainInfo       `json:"dns_domain_cfg" valid:",optional"`
	DNSConfig             types.DNSConfig             `json:"dns_config"`
	EndpointDomainCfg     []types.DNSDomainInfo       `json:"endpoint_domain_cfg" valid:",optional"`
	IsStreamEnable        bool                        `json:"isStreamEnable" valid:"optional"`
	StreamResponse        bool                        `json:"stream_response" valid:"optional"`
	IsBridgeFunction      bool                        `json:"isBridgeFunction" valid:"optional"`
	RestoreHook           types.RestoreHook           `json:"restore_hook,omitempty" valid:",optional"`
	NetworkController     types.NetworkController     `json:"network_controller" valid:",optional"`
	EnableClassIsolation  bool                        `json:"enable_class_isolation" valid:"optional"`
	EnableGCLog           bool                        `json:"enable_gc_log" valid:"optional"`
	EnableSnapshot        string                      `json:"enable_snapshot" valid:"optional"`
	ApigRouteEnable       bool                        `json:"apigRouteEnable" valid:"optional"`
	PreStop               types.PreStop               `json:"pre_stop" valid:",optional"`
	CustomMetricsReport   types.CustomMetricsReport   `json:"customMetricsReport" valid:",optional"`
	DisableInsecureAkSk   bool                        `json:"disable_insecure_ak_sk" valid:",optional"`
	BusinessType          string                      `json:"businessType" valid:"optional"`
	FileMountConfig       config.FileMountConfig      `json:"file_mount_config" valid:",omitempty"`
	LogPolicy             types.LogPolicy             `json:"logPolicy" valid:"optional"`
	EnableRemoteDebug     bool                        `json:"enableRemoteDebug"`
	RemoteDebugArgs       string                      `json:"remoteDebugArgs"`
	CustomJavaArgs        string                      `json:"customJavaArgs"  valid:"optional"`
	Properties            map[string]string           `json:"properties"  valid:"optional"`
	FgsTags               map[string]string           `json:"fgs_tags"  valid:"optional"`
	LtsCustomTag          map[string]string           `json:"lts_custom_tag" valid:"optional"`
	IsMcpFunction         bool                        `json:"is_mcp_function" valid:"optional"`
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

// FunctionServiceSpec defines the desired state of FunctionService
type FunctionServiceSpec struct {
	// INSERT ADDITIONAL SPEC FIELDS - desired state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book.kubebuilder.io/beyond_basics/generating_crd.html
	Replicas           *int32                       `json:"replicas"`
	DeadLetterConfig   DeadLetterConfigSpec         `json:"deadLetterConfig,omitempty"`
	Code               CodeSpec                     `json:"code,omitempty"`
	SnapshotDeployment types.SnapshotDeploymentSpec `json:"snapshotDeployment,omitempty"`
	Configuration      ConfigurationSpec            `json:"configuration"`
	Concurrency        ConcurrencySpec              `json:"concurrency,omitempty"`
	Tags               map[string]string            `json:"tags,omitempty"`
}

// WorkerInstance define worker instance
type WorkerInstance struct {
	InstanceID     string `json:"instanceId,omitempty"`
	PodName        string `json:"podName,omitempty"`
	IP             string `json:"ip,omitempty"`
	Port           string `json:"port,omitempty"`
	InstanceStatus string `json:"instanceStatus,omitempty"`
	P2pPort        string `json:"p2pPort,omitempty"`
	NodeIP         string `json:"nodeIP,omitempty"`
	NodePort       string `json:"nodePort,omitempty"`
	NodeName       string `json:"nodeName,omitempty"`
	Applier        string `json:"applier,omitempty"`
	OwnerIP        string `json:"ownerIP,omitempty"`
	CPU            int    `json:"cpu,omitempty"`
	Memory         int    `json:"memory,omitempty"`
}

// FunctionServiceStatus defines the observed state of FunctionService
// +k8s:openapi-gen=true
type FunctionServiceStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "operator-sdk generate k8s" to regenerate code after modifying this file
	// Add custom validation using kubebuilder tags: https://book.kubebuilder.io/beyond_basics/generating_crd.html
	State     string           `json:"state,omitempty"`
	Instances []WorkerInstance `json:"instances,omitempty"`

	FunctionServiceName    string `json:"functionServiceName,omitempty"`
	FunctionDeploymentName string `json:"functionDeploymentName,omitempty"`

	TemporaryPods []string `json:"temporaryPods,omitempty"`
}

// ObjectMeta define object meta
type ObjectMeta struct {
	ID              string            `json:"id,omitempty"`
	Namespace       string            `json:"namespace,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
}

// FunctionService is the Schema for the functionservices API
// +k8s:openapi-gen=true
type FunctionService struct {
	Version    string `json:"version,omitempty"`
	ObjectMeta `json:"metadata,omitempty"`

	// Spec holds the desired state of the FunctionServiceSpec
	Spec   FunctionServiceSpec   `json:"spec,omitempty"`
	Status FunctionServiceStatus `json:"status,omitempty"`
	// IsUpgrading indicates the cluster is in upgrding phase
	IsUpgrading string `json:"isUpgrading,omitempty"`
}

// FunctionServiceList contains a list of FunctionService
type FunctionServiceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []FunctionService `json:"items"`
}

// Type return Function Service Type
func (f *FunctionService) Type() (string, error) {
	fType := Type()
	return fType, nil
}

// Data return Function Service Marshal To String
func (f *FunctionService) Data() (string, error) {
	data, err := json.Marshal(f)
	return string(data), err
}

// Type return string function service
func Type() string {
	return "functionservice"
}

// AppendEnvVariables Append Env Variables
func (f *FunctionService) AppendEnvVariables(key string, value string) {
	f.Spec.Configuration.Env = append(f.Spec.Configuration.Env, corev1.EnvVar{
		Name:  key,
		Value: value,
	})
}

// UpdateBucketID Update Bucket ID
func (f *FunctionService) UpdateBucketID(bucketID string) {
	f.Spec.Code.Location.Nsp.BucketID = bucketID
}

// UpdateObjectID Update Object ID
func (f *FunctionService) UpdateObjectID(objectID string) {
	f.Spec.Code.Location.Nsp.ObjectID = objectID
}

// UpdateAppID Update App ID
func (f *FunctionService) UpdateAppID(appID string) {
	f.Spec.Code.Location.Nsp.AppID = appID
}

// UpdateBucketURL Update Bucket Url
func (f *FunctionService) UpdateBucketURL(url string) {
	f.Spec.Code.Location.Nsp.BucketURL = url
}

// UpdateEncryptKmsCipher Append Kms encrypt Cipher
func (f *FunctionService) UpdateEncryptKmsCipher(cipher string) {
	f.Spec.Code.Location.Nsp.CodeEncryptKmsCipher = cipher
}

// UpdateKmsKeyID Append Kms key ID
func (f *FunctionService) UpdateKmsKeyID(KeyID string) {
	f.Spec.Code.Location.Nsp.CodeCipherKeyID = KeyID
}

// AppendLayer Append Layer
func (f *FunctionService) AppendLayer(layer *types.Layer) {
	f.Spec.Configuration.Layers = append(f.Spec.Configuration.Layers, layer)
}
