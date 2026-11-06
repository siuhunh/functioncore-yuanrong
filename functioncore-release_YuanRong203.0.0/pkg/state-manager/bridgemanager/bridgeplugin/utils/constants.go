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

// Package utils gets function services from URNs
package utils

import "os"

const (
	// ProxyFunction -
	ProxyFunction = "function-task"

	// RefCountAddOne define refCount plus one
	RefCountAddOne = 1

	// RefCountMinusOne define refCount minus one
	RefCountMinusOne = -1

	// FunctionRepoEventKind define a function repo etcd event kind name
	FunctionRepoEventKind = "FunctionRepo"

	// ClientInstanceEventKind define a client instance etcd event kind name
	ClientInstanceEventKind = "ClientInstance"

	// BridgeInstanceEventKind define a bridge instance etcd event kind name
	BridgeInstanceEventKind = "BridgeInstance"

	// StateInfoEventKind define a stateID information etcd event kind name
	StateInfoEventKind = "StateInfo"

	// ClientInfoEventKind define a client information etcd event kind name
	ClientInfoEventKind = "ClientInfo"

	// StateWorkersEventKind define a state workers etcd event kind name
	StateWorkersEventKind = "StateWorkers"

	// WorkManagerEventKind define a worker-manager instance etcd event kind name
	WorkManagerEventKind = "WorkerManagerInstance"

	// FunctionMetadataPrefix define the key prefix of etcd for func metadata
	FunctionMetadataPrefix = "/sn/functions/business"

	// WorkerInstancePrefix  define the key prefix of etcd for client instance metadata
	WorkerInstancePrefix = "/sn/workers/business"

	// BridgeCachePrefix  define the key prefix of etcd for BridgeManager persistent data
	BridgeCachePrefix = "/sn/bridgemanager/business"

	// ClientCachePrefix  define the key prefix of etcd for BridgeClient persistent data
	ClientCachePrefix = "/sn/bridgeclient/business"

	// StateWorkersPrefix  define the key prefix of etcd for state workers data
	StateWorkersPrefix = "/sn/stateworkers/business"

	// WorkerManagerInstancePrefix define the key prefix of etcd for worker-manager instance
	WorkerManagerInstancePrefix = "/sn/worker-manager/instances"

	// BridgeCacheStoreKey define the key of etcd for storing BridgeManager persistent data
	// /sn/bridgemanager/business/yrk/tenant/${tenantID}/function/${complexFuncName}/version/latest/defaultaz/${stateID}
	BridgeCacheStoreKey = "/sn/bridgemanager/business/yrk/tenant/%s/function/%s/version/latest/defaultaz/%s"

	// ClientCacheStoreKey define the key of etcd for storing BridgeClient persistent data
	// /sn/bridgeclient/business/yrk/tenant/${tenantID}/function/${complexFuncName}/version/latest/defaultaz/defaultaz-#-${podName}
	ClientCacheStoreKey = "/sn/bridgeclient/business/yrk/tenant/%s/function/%s/version/latest/defaultaz/%s"

	// InitMetaETCDError define error of init meta ETCD client
	InitMetaETCDError = "failed to init meta ETCD client, wait for restart"
	// InitStateETCDError define error of init state ETCD client
	InitStateETCDError = "failed to init state ETCD client, wait for restart"

	// BridgeVersionLatest bridge version is latest
	BridgeVersionLatest = "latest"
)

var (
	// ClusterID define the cluster ID info of current region
	ClusterID = os.Getenv("CLUSTER_ID")
)
