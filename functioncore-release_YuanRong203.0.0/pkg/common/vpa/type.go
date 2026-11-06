/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package vpa type
package vpa

import (
	"fmt"
	"os"
)

// SLO agent and worker manager request url info
const (
	SLOAgentPort                  = "8889"
	SLOAgentProtocol              = "http"
	sloAgentInstanceInfoURLSuffix = "/slo-agent/v1/instanceview/instanceinfo"
	sloAgentResourceInfoURLSuffix = "/slo-agent/v1/resourceview"
	// WorkerManagerResourceInfoURLSuffix sync resource view to worker manager
	WorkerManagerResourceInfoURLSuffix = "/worker-manager/v1/instanceview/resourceview"
)

var (
	// SLOAgentIP functiontask pod IP
	SLOAgentIP = os.Getenv("POD_IP")
	// SLOAgentInstanceInfoURL register or unregister function instance monitor in SLO agent
	SLOAgentInstanceInfoURL = fmt.Sprintf("%s://%s:%s%s",
		SLOAgentProtocol, SLOAgentIP, SLOAgentPort, sloAgentInstanceInfoURLSuffix)
	// SLOAgentResourceInfoURL retrieve resource view from SLO agent
	SLOAgentResourceInfoURL = fmt.Sprintf("%s://%s:%s%s",
		SLOAgentProtocol, SLOAgentIP, SLOAgentPort, sloAgentResourceInfoURLSuffix)
)

// operations name for function instance
const (
	FuncInsCreate = "create"
	FuncInsReady  = "ready"
	FuncInsDelete = "delete"
)

// DeleteFuncInsHandler delete local worker call back to delete cache in SLO agent
type DeleteFuncInsHandler func(payload []byte, workerID string)

// FuncInstanceInfoWrapper used to sync resource view to worker manager
type FuncInstanceInfoWrapper struct {
	FuncInstanceInfo []*FuncInstanceInfo `json:"instanceList" valid:",optional"`
}

// FuncInstanceInfo used to register or unregister function instance to SLO agent
type FuncInstanceInfo struct {
	Action              string          `json:"action" valid:",optional"`
	BusinessType        string          `json:"businessType" valid:",optional"`
	FuncName            string          `json:"funcName" valid:",optional"`
	PodName             string          `json:"podName" valid:",optional"`
	PodUID              string          `json:"podUID" valid:",optional"`
	ContainerIDs        *ContainerInfo  `json:"containerIDs" valid:",optional"`
	Resource            *ResourceConfig `json:"resources" valid:",optional"`
	RuntimeInitResource *ResourceConfig `json:"runtimeInitResources,omitempty" valid:",optional"`
	WorkerInitResource  *ResourceConfig `json:"workerInitResources,omitempty" valid:",optional"`
	RuntimePoolResource *ResourceConfig `json:"runtimePoolResources,omitempty" valid:",optional"`
	WorkerPoolResource  *ResourceConfig `json:"workerPoolResources,omitempty" valid:",optional"`
	HostIP              string          `json:"-"`
}

// ResourceConfig sub-struct of FuncInstanceInfo
type ResourceConfig struct {
	CPULimit      int64 `json:"cpuLimit" valid:",optional"` // unit: milli-cores(m)
	CPURequest    int64 `json:"cpuRequest" valid:",optional"`
	MemoryLimit   int64 `json:"memoryLimit" valid:",optional"` // unit: byte
	MemoryRequest int64 `json:"memoryRequest" valid:",optional"`
}

// ContainerInfo sub-struct of FuncInstanceInfo
type ContainerInfo struct {
	Worker  string `json:"worker" valid:",optional"`
	Runtime string `json:"runtime" valid:",optional"`
}

// FuncInsVpaView defines func instance vpa list by SLO agent response
type FuncInsVpaView struct {
	VpaInsView []FuncInstanceVpaResp `json:"vpaInsView,omitempty"`
}

// FuncInstanceVpaResp is the response by bus to worker-manager
type FuncInstanceVpaResp struct {
	FuncInstanceInfo
	VpaResource FuncInsVpaResource `json:"vpaResources,omitempty" valid:"optional"`
}

// CPU defines vpa pod cpu resource, fields of the int64 type in the protobuf are converted to the string type
type CPU struct {
	Quota int64 `json:"quota,string"`
	Share int64 `json:"share,string"`
}

// Memory defines vpa pod memory, fields of the int64 type in the protobuf are converted to the string type
type Memory struct {
	Limit int64 `json:"limit,string"`
}

// FuncInsVpaResource defines vpa pod resource
type FuncInsVpaResource struct {
	CPU    CPU    `json:"cpu"`
	Memory Memory `json:"memory"`
}

// PodResourceInfo describe actual resource info of pod
type PodResourceInfo struct {
	Worker  ResourceConfig `json:"worker,omitempty"`
	Runtime ResourceConfig `json:"runtime,omitempty"`
}

// ViewResourceInfo describe the showed resource info of pod
type ViewResourceInfo struct {
	Worker  ResourceConfig `json:"worker,omitempty"`
	Runtime ResourceConfig `json:"runtime,omitempty"`
}
