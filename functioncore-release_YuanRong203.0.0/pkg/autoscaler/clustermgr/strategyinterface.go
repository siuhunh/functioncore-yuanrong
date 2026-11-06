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

// Package clustermgr manages worker-manager from all clusters
package clustermgr

import (
	"sync"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
)

const (
	strategyDefault = iota
)

var (
	defaultStrategy Strategy
	strategyOnce    sync.Once
	strategyList    = []int{strategyDefault}
)

// Strategy -
type Strategy interface {
	// InitPool -
	InitPool()
	// GetClusterForScaleUp -
	GetClusterForScaleUp(string, map[string]config.ClusterUsage) Cluster
	// GetClusterForScaleDown -
	GetClusterForScaleDown(string, map[string]int64, map[string]config.ClusterUsage) Cluster
	// RemoveCluster -
	RemoveCluster(clusterID string)
	// ClusterOffline -
	ClusterOffline(cluster *utils.WorkerMgrInstance)
	// AddCluster -
	AddCluster(clusterID string, clusterInfo Cluster)
	// ClusterFlowOut -
	ClusterFlowOut(clusterID string)
	// ClusterFlowIn -
	ClusterFlowIn(clusterID string)
	// GetClusterInfo -
	GetClusterInfo(clusterID string) Cluster
	// GetClusterPool -
	GetClusterPool() map[string]Cluster
	// GetAvailablePool -
	GetAvailablePool() map[string]Cluster
	// GetFuncAvailablePool -
	GetFuncAvailablePool(string) map[string]Cluster
	// UpdateClusterPool -
	UpdateClusterPool(clusterPool map[string]Cluster)
	// UpdateClusterItem -
	UpdateClusterItem(cluster Cluster)
}

func init() {
	initStrategy()
}

func initStrategy() {
	strategyOnce.Do(func() {
		defaultStrategy = NewLBStrategy()
	})
}

// GetStrategy -
func GetStrategy(strategyType int) Strategy {
	if strategyType == strategyDefault {
		return defaultStrategy
	}
	return defaultStrategy
}

// CreateStrategy -
func CreateStrategy(strategyType int) Strategy {
	return GetStrategy(strategyType)
}
