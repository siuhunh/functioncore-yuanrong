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
package config

import (
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
)

// CacheManagerConfig :
type CacheManagerConfig struct {
	HTTPSConfig         *tls.InternalHTTPSConfig `json:"httpsConfig" valid:"optional"`
	LocalAuth           *localauth.AuthConfig    `json:"localAuth" valid:"optional"`
	EtcdConfig          *etcd3.ClientConfig      `json:"etcdConfig,omitempty"`
	MetaEtcdProxy       *etcd3.ClientConfig      `json:"metaEtcdProxy,omitempty"`
	S3Config            S3Config                 `json:"s3Config"`
	Persistence         PersistenceConfig        `json:"persistence" valid:"optional"`
	LimitCodeSize       int                      `json:"limitCodeSize" valid:"optional"`
	SmallCodePoolSize   int
	LargeCodePoolSize   int
	AlarmServerAddress  string                     `json:"alarmServerAddress" valid:"optional"`
	MemoryControlConfig *types.MemoryControlConfig `json:"memoryControlConfig" valid:"optional"`
}

// HotLoadConfig defines config in hot load conf file
type HotLoadConfig struct {
	EtcdConfig          *etcd3.ClientConfig        `json:"etcdConfig" valid:",optional"`
	MemoryControlConfig *types.MemoryControlConfig `json:"memoryControlConfig" valid:"optional"`
}

// S3Config contains the parameters to connect obs
type S3Config struct {
	UseSSL    bool   `yaml:"useSSL" valid:"optional"`
	AccessKey string `yaml:"accessKey" valid:"stringlength(1|255)"`
	SecretKey string `yaml:"secretKey" valid:"stringlength(1|255)"`
	Endpoint  string `yaml:"endpoint" valid:"required"`
	CaFile    string `yaml:"caFile" valid:"optional"`
	TrustedCA bool   `yaml:"trustedCA" valid:"optional"`
}

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
	Layers                  []*types.Layer    `json:"layers" valid:",optional"`
	Name                    string            `json:"name"`
	FunctionDescription     string            `json:"description" valid:"stringlength(1|1024)"`
	FunctionURN             string            `json:"functionUrn"`
	ReversedConcurrency     int               `json:"reversedConcurrency" valid:"int"`
	Tags                    map[string]string `json:"tags" valid:",optional"`
	FunctionUpdateTime      string            `json:"functionUpdateTime" valid:",optional"`
	FunctionVersionURN      string            `json:"functionVersionUrn"`
	CodeSize                int               `json:"codeSize" valid:"int"`
	CodeSha256              string            `json:"codeSha256" valid:"stringlength(1|64),optional"`
	CodeSha512              string            `json:"codeSha512" valid:"stringlength(1|128),optional"`
	Handler                 string            `json:"handler" valid:"stringlength(1|255)"`
	Runtime                 string            `json:"runtime" valid:"stringlength(1|63)"`
	Timeout                 int64             `json:"timeout" valid:"required"`
	Version                 string            `json:"version" valid:"stringlength(1|42)"`
	VersionDescription      string            `json:"versionDescription" valid:"stringlength(1|1024)"`
	DeadLetterConfig        string            `json:"deadLetterConfig" valid:"stringlength(1|255)"`
	LatestVersionUpdateTime string            `json:"latestVersionUpdateTime" valid:",optional"`
	PublishTime             string            `json:"publishTime" valid:",optional"`
	BusinessID              string            `json:"businessId"  valid:"stringlength(1|32)"`
	MetaID                  string            `json:"id" valid:",optional"`
	FuncID                  string            `json:"func_id" valid:",optional"`
	FuncName                string            `json:"func_name" valid:",optional"`
	DomainID                string            `json:"domain_id" valid:",optional"`
	ProjectName             string            `json:"project_name" valid:",optional"`
	Service                 string            `json:"service" valid:",optional"`
	Dependencies            string            `json:"dependencies" valid:",optional"`
	RevisionID              string            `json:"revisionId" valid:"stringlength(1|20),optional"`
	CreateTime              string            `json:"created" valid:",optional"`
	VPCTriggerImage         string            `json:"vpcTriggerImage" valid:",optional"`
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
	MaxInstance   int64 `json:"maxInstance" valid:",optional"`
	MinInstance   int64 `json:"minInstance" valid:",optional"`
	ConcurrentNum int64 `json:"concurrentNum" valid:",optional"`
}

// ExtendedMetaData define external meta data of functions
type ExtendedMetaData struct {
	ImageName             string                      `json:"image_name" valid:",optional"`
	Role                  types.Role                  `json:"role" valid:",optional"`
	FuncVpc               FuncVpc                     `json:"func_vpc" valid:",optional"`
	FuncMountConfig       types.FuncMountConfig       `json:"mount_config" valid:",optional"`
	StrategyConfig        StrategyConfig              `json:"strategy_config" valid:",optional"`
	ExtendConfig          string                      `json:"extend_config" valid:",optional"`
	Initializer           types.Initializer           `json:"initializer" valid:",optional"`
	EnterpriseProjectID   string                      `json:"enterprise_project_id" valid:",optional"`
	CustomContainerConfig types.CustomContainerConfig `json:"custom_container_config" valid:",optional"`
	LogTankService        logservice.LogTankService   `json:"log_tank_service" valid:",optional"`
}

// FuncCode include function code file and link info
type FuncCode struct {
	File string `json:"file" valid:",optional"`
	Link string `json:"link" valid:",optional"`
}

// FuncVpc include info of function vpc
type FuncVpc struct {
	ID         string `json:"id,omitempty"`
	DomainID   string `json:"domain_id,omitempty"`
	Namespace  string `json:"namespace,omitempty"`
	VpcName    string `json:"vpc_name,omitempty"`
	VpcID      string `json:"vpc_id,omitempty"`
	SubnetName string `json:"subnet_name,omitempty"`
	SubnetID   string `json:"subnet_id,omitempty"`
	TenantCidr string `json:"tenant_cidr,omitempty"`
	HostVmCidr string `json:"host_vm_cidr,omitempty"`
	Gateway    string `json:"gateway,omitempty"`
	IsSafety   bool   `json:"is_safety,omitempty"`
}

// StrategyConfig include concurrency of function
type StrategyConfig struct {
	Concurrency int `json:"concurrency" valid:",optional"`
}

// PersistenceConfig include enable and path
type PersistenceConfig struct {
	Enable          bool   `json:"enable" valid:"optional"`
	PersistencePath string `json:"persistencePath" valid:"optional"`
}

// CacheCode include buffer checksum and timestamp
type CacheCode struct {
	ObjectID  string
	Buffer    []byte
	Checksum  string
	TimeStamp time.Time
}
