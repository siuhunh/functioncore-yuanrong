/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package config define config of bridgeManager
package config

import (
	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/trace"
	"functioncore/pkg/common/types"
)

// FunctionMetaInfo define function meta info
type FunctionMetaInfo struct {
	FuncMetaData     FuncMetaData     `json:"funcMetaData" valid:",optional"`
	S3MetaData       S3MetaData       `json:"s3MetaData" valid:",optional"`
	EnvMetaData      EnvMetaData      `json:"envMetaData" valid:",optional"`
	ResourceMetaData ResourceMetaData `json:"resourceMetaData" valid:",optional"`
	InstanceMetaData InstanceMetaData `json:"instanceMetaData" valid:",optional"`
	ExtendedMetaData ExtendedMetaData `json:"extendedMetaData" valid:",optional"`
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
	IsBridgeFunction        bool                  `json:"isBridgeFunction" valid:"optional"`
	DNSDomainCfg            []types.DNSDomainInfo `json:"dns_domain_cfg" valid:",optional"`
	IsStreamEnable          bool                  `json:"isStreamEnable" valid:"optional"`
}

// S3MetaData define meta function info for OBS
type S3MetaData struct {
	AppID        string   `json:"appId" valid:"stringlength(1|128),optional"`
	BucketID     string   `json:"bucketId" valid:"stringlength(1|255),optional"`
	ObjectID     string   `json:"objectId" valid:"stringlength(1|255),optional"`
	BucketURL    string   `json:"bucketUrl" valid:"url,optional"`
	CodeType     string   `json:"code_type" valid:",optional"`
	CodeURL      string   `json:"code_url" valid:",optional"`
	CodeFileName string   `json:"code_filename" valid:",optional"`
	FuncCode     FuncCode `json:"func_code" valid:",optional"`
}

// EnvMetaData define env info of functions
type EnvMetaData struct {
	EnvKey            string `json:"envKey,omitempty" valid:",optional"`
	Environment       string `json:"environment" valid:",optional"`
	EncryptedUserData string `json:"encrypted_user_data" valid:",optional"`
}

// ResourceMetaData include resource data such as cpu and memory
type ResourceMetaData struct {
	CPU             int64  `json:"cpu"`
	Memory          int64  `json:"memory"`
	CustomResources string `json:"customResources" valid:",optional"`
}

// InstanceMetaData define instance meta data of functions
type InstanceMetaData struct {
	IdleMode      bool   `json:"idleMode" valid:",optional"`
	MaxInstance   int64  `json:"maxInstance" valid:",optional"`
	MinInstance   int64  `json:"minInstance" valid:",optional"`
	ConcurrentNum int64  `json:"concurrentNum" valid:",optional"`
	InstanceType  string `json:"instanceType" valid:",optional"`
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
	CustomMetricsReport   types.CustomMetricsReport   `json:"customMetricsReport" valid:",optional"`
	EnterpriseProjectID   string                      `json:"enterprise_project_id" valid:",optional"`
	LogTankService        logservice.LogTankService   `json:"log_tank_service" valid:",optional"`
	TraceService          trace.TraceService          `json:"tracing_config" valid:",optional"`
	CustomContainerConfig types.CustomContainerConfig `json:"custom_container_config" valid:",optional"`
	Heartbeat             types.Heartbeat             `json:"heartbeat" valid:",optional"`
}

// Vpc include info of function vpc
type Vpc struct {
	ID         string `json:"id,omitempty"`
	DomainID   string `json:"domain_id,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	VpcName    string `json:"vpc_name,omitempty"`
	VpcID      string `json:"vpc_id,omitempty"`
	SubnetName string `json:"subnet_name,omitempty"`
	SubnetID   string `json:"subnet_id,omitempty"`
	TenantCidr string `json:"tenant_cidr,omitempty"`
	HostVMCidr string `json:"host_vm_cidr,omitempty"`
	Gateway    string `json:"gateway,omitempty"`
	Xrole      string `json:"xrole,omitempty"`
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

// WorkerInstance define worker instance
type WorkerInstance struct {
	InstanceID string `json:"instanceID,omitempty"`
	PodName    string `json:"podName,omitempty"`
	IP         string `json:"ip,omitempty"`
	Port       string `json:"port,omitempty"`
	Status     string `json:"status,omitempty"`
	P2pPort    string `json:"p2pPort,omitempty"`
	NodeIP     string `json:"nodeIP,omitempty"`
	NodePort   string `json:"nodePort,omitempty"`
	NodeName   string `json:"nodeName,omitempty"`
	StateID    string `json:"stateID,omitempty"`
	Applier    string `json:"applier,omitempty"`
	BusinessID string `json:"businessID,omitempty"`
	TenantID   string `json:"tenantID,omitempty"`
	FuncName   string `json:"funcName,omitempty"`
	Version    string `json:"version,omitempty"`
	OwnerIP    string `json:"ownerIP,omitempty"`
	CPU        int    `json:"cpu,omitempty"`
	Memory     int    `json:"memory,omitempty"`
	Signature  string `json:"signature"`
}
