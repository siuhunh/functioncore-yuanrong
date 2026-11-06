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

// Package utils defines utils used for autoscaler
package utils

import (
	"errors"
	"fmt"
	"time"
)

const (
	// BusinessTypeCAE cae business type
	BusinessTypeCAE = "CAE"
	// KeepConnAliveTTL -
	KeepConnAliveTTL = 3 * time.Second
	// InternalScaleBusinessID is business ID used to scale up/down worker
	InternalScaleBusinessID = "yrk"
	// DefaultMaxInstanceNum define the default value for maximum instance
	DefaultMaxInstanceNum = 1000
	// DefaultMinInstanceNum define the default value for minimum instance
	DefaultMinInstanceNum = 0
	// FunctionRepoEventKind define a function repo etcd event kind name
	FunctionRepoEventKind = "FunctionRepo"
	// FunctionMetadataPrefix define the key prefix of etcd for func metadata
	FunctionMetadataPrefix = "/sn/functions/business"
	// InstanceMetadataEventKind define a etcd event kind name about Instance metadata
	InstanceMetadataEventKind = "InstanceMetadata"
	// WorkManagerEventKind define a pod event kind name
	WorkManagerEventKind = "WorkerManagerInstance"
	// InstanceMetadataPrefix define the key prefix of etcd for Instance metadata
	InstanceMetadataPrefix = "/instances/business/yrk/cluster"
	// CaeAvailableClusterEventKind define event kind name for cae available cluster info
	CaeAvailableClusterEventKind = "CaeAvailableCluster"
	// CaeAvailableClusterPrefix define the key prefix of etcd for cae available cluster
	CaeAvailableClusterPrefix = "/cae/avaiablecluster/business"
	// MaxEtcdRetryAttempts define the max etcd operation times
	MaxEtcdRetryAttempts = 3
	// RetryInterval refer to 3 * 8ms: time required each time
	RetryInterval = 24 * time.Millisecond
	// WorkerInstancePrefix  define the key prefix of etcd for client instance metadata
	WorkerInstancePrefix = "/sn/workers/business"
	// WorkerManagerInstancePrefix -
	WorkerManagerInstancePrefix = "/sn/worker-manager/instances"
	// MetaCache -
	MetaCache = "metaCache"
	// FuncInsCache -
	FuncInsCache = "funcInsCache"
	// Healthy -
	Healthy = "healthy"
	// UnHealthy -
	UnHealthy = "unHealthy"
	// SubHealthy -
	SubHealthy = "subHealthy"
	// WorkerManagerPort -
	WorkerManagerPort = "30007"
	// LoadBalanceHighest -
	LoadBalanceHighest = "highest"
	// LoadBalanceLowest -
	LoadBalanceLowest = "lowest"
	// FlowIn -
	FlowIn = "FLOW_IN"
	// FlowOut -
	FlowOut = "FLOW_OUT"
)

const (
	// UnAuthorized -
	UnAuthorized = 220401
	// InternalError -
	InternalError = 220500
)

var (
	// ErrRouterETCDOperation RouterETCD operation error
	ErrRouterETCDOperation = errors.New("failed to operate RouterETCD")
	// ErrETCDNotFound no data is found from etcd
	ErrETCDNotFound = fmt.Errorf("the value got from etcd is empty")
)
