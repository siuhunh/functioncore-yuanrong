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
	"math/rand"
	"sort"
	"sync"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/strutil"
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

func init() {
	rand.Seed(time.Now().UnixNano())
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
func (g *LoadBalanceStrategy) ClusterOffline(cluster *utils.WorkerMgrInstance) {
	if cluster == nil {
		log.GetLogger().Errorf("failed to make cluster offline, because cluster is nil")
		return
	}
	g.poolLock.Lock()
	defer g.poolLock.Unlock()
	clusterInfo, exists := g.clusterPool[cluster.ClusterID]
	if !exists {
		return
	}

	// if cluster.NodeIp == "", we think it's not triggered by etcd event, like flow out or something else
	if cluster.NodeIP != "" && clusterInfo.ActiveNodes.IP != cluster.NodeIP {
		log.GetLogger().Warnf("cluster offline find the ip is not right, so ignore! the cluster ip now is: %s,"+
			" want handler ip is %s", clusterInfo.ActiveNodes.IP, cluster.NodeIP)
		return
	}

	clusterInfo.State = utils.UnHealthy
	if clusterInfo.offlineTime.IsZero() {
		clusterInfo.offlineTime = time.Now()
	}
	g.clusterPool[cluster.ClusterID] = clusterInfo
	log.GetLogger().Infof("cluster offline happened, and after cluster data is %+v", g.clusterPool)
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
	log.GetLogger().Infof("cluster add, and after add cluster data is %+v", g.clusterPool)
}

// ClusterFlowOut -
func (g *LoadBalanceStrategy) ClusterFlowOut(clusterID string) {
	g.poolLock.Lock()
	g.clusterSwitchFlowState[clusterID] = true
	g.poolLock.Unlock()
	log.GetLogger().Infof("cluster %s flow out, set cluster offline", clusterID)
	g.ClusterOffline(&utils.WorkerMgrInstance{
		ClusterID: clusterID,
	})
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
func (g *LoadBalanceStrategy) GetClusterForScaleUp(functionSvcID string,
	clusterUsage map[string]config.ClusterUsage) Cluster {
	availablePool := g.GetFuncAvailablePool(functionSvcID)
	// choose the lowest CPU used cluster to scale up
	return getLoadBalanceByType(clusterUsage, availablePool, utils.LoadBalanceLowest, make(map[string]int64))
}

// GetClusterForScaleDown -
func (g *LoadBalanceStrategy) GetClusterForScaleDown(functionSvcID string, funcInstanceClusterCount map[string]int64,
	clusterUsage map[string]config.ClusterUsage) Cluster {
	availablePool := g.GetFuncAvailablePool(functionSvcID)
	return getRandomClusterForScaleDown(availablePool, funcInstanceClusterCount)
}

// GetFuncAvailablePool - Get available clusters of one func
// cluster returned should be vpc non-conflicting and worker-manager healthy
func (g *LoadBalanceStrategy) GetFuncAvailablePool(functionSvcID string) map[string]Cluster {
	availablePool := make(map[string]Cluster, 0)
	clusterCache, ok := cache.GetInstance().GetCaeAvailableCluster(functionSvcID)
	if !ok {
		log.GetLogger().Errorf("find cae available clusters err, funcSvcID: %s.", functionSvcID)
		return availablePool
	}
	g.poolLock.RLock()
	defer g.poolLock.RUnlock()
	for _, cluster := range g.clusterPool {
		if cluster.State == utils.Healthy && strutil.IsContain(clusterCache, cluster.ClusterID) {
			availablePool[cluster.ClusterID] = cluster
		}
	}
	return availablePool
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
	clusterInClusterPool, exist := g.clusterPool[cluster.ClusterID]
	if exist {
		clusterInClusterPool.State = cluster.State
		clusterInClusterPool.ActiveNodes.LastError = cluster.ActiveNodes.LastError
		clusterInClusterPool.ActiveNodes.LastSuccessTime = cluster.ActiveNodes.LastSuccessTime
		g.clusterPool[cluster.ClusterID] = clusterInClusterPool
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

func getRandomClusterForScaleDown(availableClusters map[string]Cluster,
	funcInstanceClusterCount map[string]int64) Cluster {
	for k := range availableClusters {
		if funcInstanceClusterCount[k] <= 0 {
			delete(availableClusters, k)
		}
	}
	return GetRandomCluster(availableClusters)
}

// GetRandomCluster -
func GetRandomCluster(allAvailableCluster map[string]Cluster) Cluster {
	if len(allAvailableCluster) == 0 {
		return Cluster{}
	}
	clusters := make([]string, 0, len(allAvailableCluster))
	for k := range allAvailableCluster {
		clusters = append(clusters, k)
	}
	clusterID := clusters[rand.Intn(len(clusters))]
	return allAvailableCluster[clusterID]
}
