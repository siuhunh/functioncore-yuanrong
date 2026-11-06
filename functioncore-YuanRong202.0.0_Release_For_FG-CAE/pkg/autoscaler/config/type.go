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

package config

import (
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types"
)

// Metric define resource metric
type Metric struct {
	FunctionMetrics []FunctionMetric `json:"functionMetrics"`
	UpdateTime      string           `json:"updateTime" valid:",optional"`
}

// ClusterInsCache define ClusterInsCache
type ClusterInsCache struct {
	ClusterID string `json:"clusterID"`
	Count     int    `json:"count"`
}

// EtcdClients -
type EtcdClients struct {
	MetadataEtcdClient *etcd3.EtcdClient
	RouterETCDClient   *etcd3.EtcdClient
}

// APIClients -
type APIClients struct {
	EtcdClients *EtcdClients
	KubeClient  kubernetes.Interface
}

// InstanceCache define InstanceCache
type InstanceCache struct {
	TotalCurrentCount int64             `json:"currentCount"`
	ClusterInsCache   []ClusterInsCache `json:"cluster"`
}

// ClusterUsage describe CPU usage of cluster
type ClusterUsage struct {
	ClusterID string
	CPUTotal  int64 `json:"cpu_total,omitempty" valid:"int,optional"`
	CPUUsed   int64 `json:"cpu_used,omitempty" valid:"int,optional"`
}

// GetWorkerSuccessResponse define
type GetWorkerSuccessResponse struct {
	Worker   *Worker         `json:"worker"`
	Instance *types.Instance `json:"instance"`
	Code     int             `json:"code"`
	Message  string          `json:"message"`
}

// Worker define a worker
type Worker struct {
	Instances       []*types.Instance `json:"instances"`
	FunctionName    string            `json:"functionname"`
	FunctionVersion string            `json:"functionversion"`
	Tenant          string            `json:"tenant"`
	Business        string            `json:"business"`
}

// FunctionMetric define resource struct for function
type FunctionMetric struct {
	// key：podName
	FunctionResourceMetric map[string]*Resource `json:"podResourceMetric"`
}

// Resource define resource struct
type Resource struct {
	CPUUsed  float64 `json:"cpuUsed"`
	CPULimit int     `json:"cpuLimit"`
	MemUsed  float64 `json:"memUsed"`
	MemLimit int     `json:"memLimit"`
	Time     string  `json:"time" valid:",optional"`
}

// WorkerInstance define worker instance
type WorkerInstance struct {
	InstanceID   string `json:"-"`
	PodName      string `json:"-"`
	IP           string `json:"ip,omitempty"`
	Port         string `json:"port,omitempty"`
	Status       string `json:"status,omitempty"`
	P2pPort      string `json:"p2pPort,omitempty"`
	NodeIP       string `json:"nodeIP,omitempty"`
	NodePort     string `json:"nodePort,omitempty"`
	NodeName     string `json:"-"`
	StateID      string `json:"stateID,omitempty"`
	Applier      string `json:"applier,omitempty"`
	BusinessID   string `json:"-"`
	TenantID     string `json:"-"`
	FuncName     string `json:"-"`
	Version      string `json:"-"`
	OwnerIP      string `json:"ownerIP,omitempty"`
	CPU          int    `json:"cpu,omitempty"`
	Memory       int    `json:"memory,omitempty"`
	WillDelete   bool   `json:"willDelete,omitempty"`
	Signature    string `json:"-"`
	IsMigrating  bool   `json:"-"`
	MigratedFrom string `json:"-"`
	BusinessType string `json:"businessType,omitempty"`
}
