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

// Package cluster const
package cluster

const (
	// ClusterRoleLeader define the cluster role of leader
	ClusterRoleLeader = "leader"

	// ClusterRoleNode define the cluster role of node
	ClusterRoleNode = "node"

	// EndpointsResourceLock indicate use endpoint resource lock for leader elect
	EndpointsResourceLock = "endpoints"

	// ConfigMapsResourceLock indicate use configmap resource lock for leader elect
	ConfigMapsResourceLock = "configmaps"

	// LeasesResourceLock indicate use lease resource lock for leader elect
	LeasesResourceLock = "leases"
)
