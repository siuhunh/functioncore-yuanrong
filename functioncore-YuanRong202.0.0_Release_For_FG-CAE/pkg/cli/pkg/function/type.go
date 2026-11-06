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

// Package function is function api
package function

import (
	"time"

	"functioncore/pkg/common/types"
)

// EtcdData function etcd data
type EtcdData struct {
	MetaData         `json:"funcMetaData"`
	S3MetaData       `json:"s3MetaData"`
	EnvMetaData      `json:"envMetaData"`
	ResourceMetaData `json:"resourceMetaData"`
	InstanceMetaData `json:"instanceMetaData"`
	ExtendedMetaData `json:"extendedMetaData"`
}

// MetaData -
type MetaData struct {
	Layers                  []Layer       `json:"layers"`
	Name                    string        `json:"name"`
	Description             string        `json:"description"`
	FunctionUrn             string        `json:"functionUrn"`
	TenantID                string        `json:"tenantId"`
	ReversedConcurrency     int           `json:"reversedConcurrency"`
	Tags                    interface{}   `json:"tags"`
	Created                 string        `json:"created"`
	FunctionUpdateTime      time.Time     `json:"functionUpdateTime"`
	FunctionVersionUrn      string        `json:"functionVersionUrn"`
	RevisionID              string        `json:"revisionId"`
	CodeSize                int           `json:"codeSize"`
	CodeSha512              string        `json:"codeSha512"`
	Handler                 string        `json:"handler"`
	Runtime                 string        `json:"runtime"`
	Timeout                 int           `json:"timeout"`
	Version                 string        `json:"version"`
	VersionDescription      string        `json:"versionDescription"`
	DeadLetterConfig        string        `json:"deadLetterConfig"`
	LatestVersionUpdateTime time.Time     `json:"latestVersionUpdateTime"`
	PublishTime             string        `json:"publishTime"`
	BusinessID              string        `json:"businessId"`
	FunctionType            string        `json:"functionType"`
	ID                      string        `json:"id"`
	FuncID                  string        `json:"func_id"`
	FuncName                string        `json:"func_name"`
	DomainID                string        `json:"domain_id"`
	ProjectName             string        `json:"project_name"`
	Service                 string        `json:"service"`
	Dependencies            string        `json:"dependencies"`
	IsStatefulFunction      bool          `json:"isStatefulFunction"`
	Type                    string        `json:"type"`
	EnableAuthInHeader      bool          `json:"enable_auth_in_header"`
	DNSDomainCfg            []interface{} `json:"dns_domain_cfg"`
	DisableInsecureAkSk     bool          `json:"disable_insecure_ak_sk"`
}

// Layer -
type Layer struct {
	BucketURL      string `json:"bucketUrl"`
	ObjectID       string `json:"objectId"`
	BucketID       string `json:"bucketId"`
	AppID          string `json:"appId"`
	Etag           string `json:"etag"`
	Link           string `json:"link"`
	Name           string `json:"name"`
	Sha256         string `json:"sha256"`
	DependencyType string `json:"dependencyType"`
}

// S3MetaData -
type S3MetaData struct {
	AppID        string `json:"appId"`
	BucketID     string `json:"bucketId"`
	ObjectID     string `json:"objectId"`
	BucketURL    string `json:"bucketUrl"`
	CodeType     string `json:"code_type"`
	CodeURL      string `json:"code_url"`
	CodeFilename string `json:"code_filename"`
	FuncCode     struct {
		File string `json:"file"`
		Link string `json:"link"`
	} `json:"func_code"`
}

// EnvMetaData -
type EnvMetaData struct {
	Environment       string `json:"environment"`
	EncryptedUserData string `json:"encrypted_user_data"`
}

// ResourceMetaData -
type ResourceMetaData struct {
	CPU                 int    `json:"cpu"`
	Memory              int    `json:"memory"`
	EnableDynamicMemory bool   `json:"enable_dynamic_memory"`
	CustomResources     string `json:"customResources"`
}

// InstanceData -
type InstanceData struct {
	InstanceMetaData `json:"instanceMetaData"`
}

// InstanceMetaData -
type InstanceMetaData struct {
	MaxInstance   int    `json:"maxInstance"`
	MinInstance   int    `json:"minInstance"`
	ConcurrentNum int    `json:"concurrentNum"`
	InstanceType  string `json:"instanceType"`
	IdleMode      bool   `json:"idleMode"`
}

// Role -
type Role struct {
	Xrole    string `json:"xrole"`
	AppXrole string `json:"app_xrole"`
}

// ExtendedMetaData -
type ExtendedMetaData struct {
	ImageName           string      `json:"image_name"`
	Role                Role        `json:"role"`
	FuncVpc             interface{} `json:"func_vpc"`
	EndpointTenantVpc   interface{} `json:"endpoint_tenant_vpc"`
	MountConfig         interface{} `json:"mount_config"`
	StrategyConfig      `json:"strategy_config"`
	ExtendConfig        string `json:"extend_config"`
	Initializer         `json:"initializer"`
	types.PreStop       `json:"pre_stop"`
	Heartbeat           interface{} `json:"heartbeat"`
	EnterpriseProjectID string      `json:"enterprise_project_id"`
	LogTankService      struct {
		LogGroupID  string `json:"logGroupId"`
		LogStreamID string `json:"logStreamId"`
	} `json:"log_tank_service"`
	TracingConfig struct {
		Enabled   bool   `json:"enabled"`
		TracingAk string `json:"tracing_ak"`
		TracingSk string `json:"tracing_sk"`
	} `json:"tracing_config"`
	CustomContainerConfig `json:"custom_container_config"`
	AsyncConfigLoaded     bool `json:"async_config_loaded"`
}

// CustomContainerConfig -
type CustomContainerConfig struct {
	ControlPath string      `json:"control_path"`
	Image       string      `json:"image"`
	Command     interface{} `json:"command"`
	Args        interface{} `json:"args"`
	WorkingDir  string      `json:"working_dir"`
	UID         int         `json:"uid"`
	Gid         int         `json:"gid"`
}

// StrategyConfig -
type StrategyConfig struct {
	Concurrency   int `json:"concurrency"`
	ConcurrentNum int `json:"concurrentNum"`
}

// Initializer -
type Initializer struct {
	InitializerHandler string `json:"initializer_handler"`
	InitializerTimeout int    `json:"initializer_timeout"`
}

// Function -
type Function struct {
	Name            string            `json:"name"`
	Description     string            `json:"description"`
	ID              string            `json:"id,omitempty"`
	Environment     map[string]string `json:"environment,omitempty"`
	Created         string            `json:"Created,omitempty"`
	LastModified    string            `json:"LastModified,omitempty"`
	Memory          int               `json:"memory,omitempty"`
	Runtime         string            `json:"runtime,omitempty"`
	VersionDesc     string            `json:"versionDesc,omitempty"`
	VersionNumber   string            `json:"versionNumber,omitempty"`
	Timeout         int               `json:"timeout,omitempty"`
	FunctionURN     string            `json:"functionUrn,omitempty"`
	Layers          []string          `json:"layers,omitempty"`
	Handler         string            `json:"handler,omitempty"`
	CodeSha256      string            `json:"codeSha256,omitempty"`
	CodeSize        int32             `json:"codeSize,omitempty"`
	RevisionID      string            `json:"revisionId,omitempty"`
	MinInstance     int               `json:"minInstance,omitempty"`
	MaxInstance     int               `json:"maxInstance,omitempty"`
	ConcurrentNum   int               `json:"concurrentNum,omitempty"`
	DownloadURL     string            `json:"downloadUrl,omitempty"`
	Status          string            `json:"status,omitempty"`
	CustomResources map[string]int32  `json:"customResources,omitempty"`
}

// Agency -
type Agency struct {
	AccessKey     string `json:"accessKey"`
	SecretKey     string `json:"secretKey"`
	Token         string `json:"token"`
	SecurityAk    string `json:"securityAk"`
	SecuritySk    string `json:"securitySk"`
	SecurityToken string `json:"securityToken"`
}
