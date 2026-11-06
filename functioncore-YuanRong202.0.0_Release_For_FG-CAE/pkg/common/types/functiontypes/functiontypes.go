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

// Package functiontypes contains function info stored on etcd
package functiontypes

import "encoding/json"

const (
	// KernelTenantID kernel function type
	KernelTenantID = ""

	// KernelBusinessID kernel function business id
	KernelBusinessID = ""
)

// FunctionEtcdInfo define the data of function stored in etcd
type FunctionEtcdInfo struct {
	FuncMetaData     FuncMetaData     `json:"funcMetaData" valid:",optional"`
	S3MetaData       S3MetaData       `json:"s3MetaData" valid:",optional"`
	EnvMetaData      EnvMetaData      `json:"envMetaData" valid:",optional"`
	ResourceMetaData ResourceMetaData `json:"resourceMetaData" valid:",optional"`
	InstanceMetaData InstanceMetaData `json:"instanceMetaData" valid:",optional"`
	ExtendedMetaData ExtendedMetaData `json:"extendedMetaData" valid:"optional"`
}

// S3MetaData define meta function info for OBS
type S3MetaData struct {
	AppID     string `json:"appId"`
	BucketURL string `json:"bucketUrl"`
	BucketID  string `json:"bucketId"`
	ObjectID  string `json:"objectId"`
}

// FuncMetaData define meta data of functions
type FuncMetaData struct {
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	FunctionURN         string            `json:"functionUrn"`
	TenantID            string            `json:"tenantId"`
	BusinessID          string            `json:"businessId"`
	ReversedConcurrency int               `json:"reversedConcurrency"`
	Tags                map[string]string `json:"tags"`
	Handler             string            `json:"handler"`
	Runtime             string            `json:"runtime"`
	Timeout             int64             `json:"timeout"`
	Version             string            `json:"version"`
	VersionURN          string            `json:"functionVersionUrn"`
	VersionDescription  string            `json:"versionDescription"`
	CodeSize            int64             `json:"codeSize"`
	CodeSha256          string            `json:"codeSha256"`
	StatefulFlag        bool              `json:"statefulFlag"`
	DeadLetterConfig    string            `json:"deadLetterConfig"`
	LayerBucket         []LayerBucket     `json:"layers"`
	Created             string            `json:"created"`
	RevisionID          string            `json:"revisionId"`
	BusinessType        string            `json:"businessType"`
	EnableRemoteDebug   bool              `json:"enableRemoteDebug"`
	RemoteDebugArgs     string            `json:"remoteDebugArgs"`
}

// EnvMetaData define env info of functions
type EnvMetaData struct {
	Environment string `json:"environment"`
	EnvKey      string `json:"envKey"`
}

// ResourceMetaData include resource data such as cpu and memory
type ResourceMetaData struct {
	CPU             int    `json:"cpu"`
	Memory          int    `json:"memory"`
	CustomResources string `json:"customResources"`
}

// InstanceMetaData define instance meta data of functions
type InstanceMetaData struct {
	IdleMode      bool   `json:"idleMode" valid:",optional"`
	MaxInstance   int64  `json:"maxInstance" valid:",optional"`
	MinInstance   int64  `json:"minInstance" valid:",optional"`
	ConcurrentNum int    `json:"concurrentNum"`
	InstanceType  string `json:"instanceType" valid:",optional"`
}

// LayerBucket -
type LayerBucket struct {
	BucketURL string `json:"bucketUrl"`
	ObjectID  string `json:"objectId"`
	BucketID  string `json:"bucketId"`
	AppID     string `json:"appId"`
	Sha256    string `json:"sha256"`
}

// AsbResConfig -
type AsbResConfig struct {
	// TargetCPUUtilPercentage -
	TargetCPUUtilPercentage int64 `json:"targetCpuUtilPercentage"`
	// TargetMemUtilPercentage -
	TargetMemUtilPercentage   int64 `json:"targetMemUtilPercentage"`
	ScaleStrategy             []string
	FuncLoadCalculationPeriod int `json:"funcLoadCalculationPeriod"`
}

// ExtendedMetaData -
type ExtendedMetaData struct {
	Initializer       Initializer  `json:"initializer"`
	AsbResConfig      AsbResConfig `json:"asbres_config"`
	FuncVpc           Vpc          `json:"func_vpc" valid:",optional"`
	EndpointTenantVpc Vpc          `json:"endpoint_tenant_vpc" valid:",optional"`
	IsMcpFunction     bool         `json:"is_mcp_function" valid:"optional"`
}

// Vpc include info of function vpc
type Vpc struct {
	VpcID string `json:"vpc_id,omitempty"`
}

// FuncMetaDataParts defines some parts of metadata of function
type FuncMetaDataParts struct {
	Timeout           int64  `json:"timeout"`
	IsCustomContainer bool   `json:"-"`
	BusinessType      string `json:"businessType"`
	EnableRemoteDebug bool   `json:"enableRemoteDebug"`
}

// UnmarshalJSON -
func (f *FuncMetaDataParts) UnmarshalJSON(data []byte) error {
	type Alias FuncMetaDataParts
	alias := &struct {
		Runtime string `json:"runtime"`
		*Alias
	}{
		Alias: (*Alias)(f),
	}
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	f.IsCustomContainer = alias.Runtime == "custom image"
	return nil
}

// Initializer defines initializer metadata of function
type Initializer struct {
	InitializerTimeout int64 `json:"initializer_timeout"`
}
