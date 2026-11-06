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
	"sort"
	"sync"
	"time"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/logger/log"
)

const (
	// MaxInstanceSize is the max instance size be stored in hash ring
	MaxInstanceSize = 100
)

type uint32Slice []uint32

// LoadBalanceStrategy -
type LoadBalanceStrategy struct {
	clusterPool            map[string]Cluster `json:"clusters"`
	clusterSwitchFlowState map[string]bool
	poolLock               sync.RWMutex
}

// Cluster -
type Cluster struct {
	ClusterID   string     `json:"clusterId"`
	ActiveNodes NodeStatus `json:"-"`
	State       string     `json:"-"`
	offlineTime time.Time  `json:"-"`
}

// NodeStatus -
type NodeStatus struct {
	IP              string
	LastSuccessTime time.Time
	LastError       string
}

// NewLBStrategy -
func NewLBStrategy() Strategy {
	return &LoadBalanceStrategy{
		clusterPool:            make(map[string]Cluster),
		clusterSwitchFlowState: make(map[string]bool),
		poolLock:               sync.RWMutex{},
	}
}

// InitPool -
func (g *LoadBalanceStrategy) InitPool() {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	g.clusterPool = make(map[string]Cluster)
	g.clusterSwitchFlowState = make(map[string]bool)
}

// RemoveCluster -
func (g *LoadBalanceStrategy) RemoveCluster(clusterID string) {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	delete(g.clusterPool, clusterID)
}

// ClusterOffline -
func (g *LoadBalanceStrategy) ClusterOffline(clusterID string) {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	clusterInfo, exists := g.clusterPool[clusterID]
	if !exists {
		return
	}
	clusterInfo.State = utils.UnHealthy
	if clusterInfo.offlineTime.IsZero() {
		clusterInfo.offlineTime = time.Now()
	}
	g.clusterPool[clusterID] = clusterInfo
}

// AddCluster -
func (g *LoadBalanceStrategy) AddCluster(clusterID string, clusterInfo Cluster) {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	if g.clusterSwitchFlowState[clusterID] {
		log.GetLogger().Infof("cluster %s flow out, set state unhealthy", clusterID)
		clusterInfo.State = utils.UnHealthy
		clusterInfo.offlineTime = time.Now()
	}
	g.clusterPool[clusterID] = clusterInfo
}

// ClusterFlowOut -
func (g *LoadBalanceStrategy) ClusterFlowOut(clusterID string) {
	g.poolLock.Lock()
	g.clusterSwitchFlowState[clusterID] = true
	g.poolLock.Unlock()
	log.GetLogger().Infof("cluster %s flow out, set cluster offline", clusterID)
	g.ClusterOffline(clusterID)
}

// ClusterFlowIn -
func (g *LoadBalanceStrategy) ClusterFlowIn(clusterID string) {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	delete(g.clusterSwitchFlowState, clusterID)
	clusterInfo, exists := g.clusterPool[clusterID]
	if !exists {
		return
	}
	clusterInfo.State = utils.Healthy
	var flowInTime time.Time
	clusterInfo.offlineTime = flowInTime
	log.GetLogger().Infof("cluster %s flow in, recover cluster state", clusterID)
	g.clusterPool[clusterID] = clusterInfo
}

// GetClusterInfo -
func (g *LoadBalanceStrategy) GetClusterInfo(clusterID string) Cluster {
	g.poolLock.RLock()
	defer g.poolLock.RUnlock()
	clusterInfo, exists := g.clusterPool[clusterID]
	if !exists {
		return Cluster{}
	}
	return clusterInfo
}

// GetClusterForScaleUp -
func (g *LoadBalanceStrategy) GetClusterForScaleUp(clusterUsage map[string]config.ClusterUsage) Cluster {
	availablePool := g.GetAvailablePool()
	// choose the lowest CPU used cluster to scale up
	return getLoadBalanceByType(clusterUsage, availablePool, utils.LoadBalanceLowest, make(map[string]int64))
}

// GetClusterForScaleDown -
func (g *LoadBalanceStrategy) GetClusterForScaleDown(funcInstanceClusterCount map[string]int64,
	clusterUsage map[string]config.ClusterUsage) Cluster {
	availablePool := g.GetAvailablePool()
	// choose the highest CPU used cluster to scale down
	return getLoadBalanceByType(clusterUsage, availablePool, utils.LoadBalanceHighest, funcInstanceClusterCount)
}

// GetAvailablePool -
func (g *LoadBalanceStrategy) GetAvailablePool() map[string]Cluster {
	availablePool := make(map[string]Cluster, 0)
	g.poolLock.RLock()
	defer g.poolLock.RUnlock()
	for _, cluster := range g.clusterPool {
		if cluster.State == utils.Healthy {
			availablePool[cluster.ClusterID] = cluster
		}
	}
	return availablePool
}

// GetClusterPool -
func (g *LoadBalanceStrategy) GetClusterPool() map[string]Cluster {
	g.poolLock.RLock()
	defer g.poolLock.RUnlock()

	clusterPool := make(map[string]Cluster, len(g.clusterPool))
	for clusterID, cluster := range g.clusterPool {
		clusterPool[clusterID] = cluster
	}
	return clusterPool
}

// UpdateClusterPool -
func (g *LoadBalanceStrategy) UpdateClusterPool(clusterPool map[string]Cluster) {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	g.clusterPool = clusterPool
	return
}

// UpdateClusterItem -
func (g *LoadBalanceStrategy) UpdateClusterItem(cluster Cluster) {
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	if _, exist := g.clusterPool[cluster.ClusterID]; exist {
		g.clusterPool[cluster.ClusterID] = cluster
	}
	return
}

func getLoadBalanceByType(clusterMetricCache map[string]config.ClusterUsage,
	availableClusters map[string]Cluster, compare string, funcInstanceClusterCount map[string]int64) Cluster {
	clusterUsages := make([]config.ClusterUsage, 0, len(availableClusters))
	for clusterID, clusterUsage := range clusterMetricCache {
		if _, ok := availableClusters[clusterID]; ok {
			clusterUsage.ClusterID = clusterID
			clusterUsages = append(clusterUsages, clusterUsage)
		}
	}
	sort.Slice(clusterUsages, func(i, j int) bool {
		usageI := float64(clusterUsages[i].CPUUsed) / float64(clusterUsages[i].CPUTotal)
		usageJ := float64(clusterUsages[j].CPUUsed) / float64(clusterUsages[j].CPUTotal)
		return usageI < usageJ
	})

	var cluster Cluster
	if len(clusterUsages) == 0 {
		log.GetLogger().Errorf("failed to get availableClusters!")
		return cluster
	}
	switch compare {
	case utils.LoadBalanceLowest:
		lowestUsageCluster := clusterUsages[0]
		cluster = availableClusters[lowestUsageCluster.ClusterID]
	case utils.LoadBalanceHighest:
		for i := len(clusterUsages) - 1; i >= 0; i-- {
			// means not enough pod in this cluster
			if funcInstanceClusterCount[clusterUsages[i].ClusterID] > 0 {
				cluster = availableClusters[clusterUsages[i].ClusterID]
				break
			}
		}
	default:
		log.GetLogger().Warnf("unknown type")
	}
	return cluster
}
