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
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/workermanagerutils"
)

var (
	clusterMgr                    *ClusterManager
	retryCountLock                = sync.Mutex{}
	removeOfflineClusterPeriod    = removeOfflineClusterAfterOneDay
	historyAlarmUnHealthyClusters = make(map[string]Cluster)
)

// ClusterManager -
type ClusterManager struct {
	strategy           Strategy
	stopCh             <-chan struct{}
	clusterMetricCache map[string]config.ClusterUsage
	lock               sync.RWMutex
}

// HeartbeatConfig heartbeat config
type HeartbeatConfig struct {
	HeartbeatTimeout          int `json:"heartbeatTimeout"  valid:",optional"`
	HeartbeatInterval         int `json:"heartbeatInterval" valid:"optional"`
	HeartbeatTimeoutThreshold int `json:"heartbeatTimeoutThreshold" valid:"optional"`
}

const (
	healthCheckInterval             = 10 * time.Second
	heartbeatTimeout                = 15 * time.Second
	heartbeatTimeoutThreshold       = 3
	heartbeatRetryInterval          = 3 * time.Second
	removeOfflineClusterAfterOneDay = 1440 * time.Minute
)

// CreateClusterMgr -
func CreateClusterMgr(stopCh <-chan struct{}, conf *config.Config) {
	if conf.RemoveOfflineClusterPeriod > 0 {
		removeOfflineClusterPeriod = time.Duration(conf.RemoveOfflineClusterPeriod) * time.Minute
	}
	clusterMgr = &ClusterManager{
		strategy:           CreateStrategy(strategyDefault),
		stopCh:             stopCh,
		clusterMetricCache: make(map[string]config.ClusterUsage),
		lock:               sync.RWMutex{},
	}
	go clusterMgr.checkClustersStatus()
}

// GetClusterMgr -
func GetClusterMgr() *ClusterManager {
	return clusterMgr
}

// RefreshClusterMetricCache -
func (c *ClusterManager) RefreshClusterMetricCache(clusterMetricCache map[string]config.ClusterUsage) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.clusterMetricCache = clusterMetricCache
	log.GetLogger().Debugf("refresh cluster metric cache: %+v", clusterMetricCache)
}

// GetAvailableClusterForScaleUp return available worker-manager node ip for scale up
func (c *ClusterManager) GetAvailableClusterForScaleUp(functionSvcID string) Cluster {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return Cluster{}
	}
	return c.strategy.GetClusterForScaleUp(functionSvcID, c.clusterMetricCache)
}

// GetAvailableClusterForScaleDown return available worker-manager node ip for scale down
func (c *ClusterManager) GetAvailableClusterForScaleDown(functionSvcID string,
	funcInstanceClusterCount map[string]int64) Cluster {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return Cluster{}
	}
	if len(funcInstanceClusterCount) == 0 {
		log.GetLogger().Warnf("funcInstanceClusterCount is empty")
		return Cluster{}
	}
	return c.strategy.GetClusterForScaleDown(functionSvcID, funcInstanceClusterCount, c.clusterMetricCache)
}

// GetAllAvailableCluster -
func (c *ClusterManager) GetAllAvailableCluster() map[string]Cluster {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return map[string]Cluster{}
	}
	return c.strategy.GetAvailablePool()
}

// GetFuncAllAvailableCluster -
func (c *ClusterManager) GetFuncAllAvailableCluster(funcSvcID string) map[string]Cluster {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return map[string]Cluster{}
	}
	return c.strategy.GetFuncAvailablePool(funcSvcID)
}

// AddCluster -
func (c *ClusterManager) AddCluster(clusterID string, clusterInfo Cluster) {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return
	}
	c.strategy.AddCluster(clusterID, clusterInfo)
}

// RemoveCluster -
func (c *ClusterManager) RemoveCluster(cluster *utils.WorkerMgrInstance) {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return
	}
	c.strategy.ClusterOffline(cluster)
}

// ClusterFlowOut -
func (c *ClusterManager) ClusterFlowOut(clusterID string) {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return
	}
	c.strategy.ClusterFlowOut(clusterID)
}

// ClusterFlowIn -
func (c *ClusterManager) ClusterFlowIn(clusterID string) {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return
	}
	c.strategy.ClusterFlowIn(clusterID)
}

// GetClusterInfo -
func (c *ClusterManager) GetClusterInfo(clusterID string) Cluster {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return Cluster{}
	}
	return c.strategy.GetClusterInfo(clusterID)
}

// checkClustersStatus -
func (c *ClusterManager) checkClustersStatus() {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			log.GetLogger().Infof("worker-manager healthCheck exit")
			return
		case <-ticker.C:
			c.healthCheck()
		}
	}
}

func (c *ClusterManager) healthCheck() {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return
	}
	clusterPool := c.strategy.GetClusterPool()
	currentUnHealthyClusters := make(map[string]Cluster)
	// when alarm info change this time, need to update alarm
	needUpdateAlarmFlag := false
	for clusterID, cluster := range clusterPool {
		if !cluster.offlineTime.IsZero() {
			subOfflineTime := time.Now().Sub(cluster.offlineTime)
			if subOfflineTime > removeOfflineClusterPeriod {
				c.strategy.RemoveCluster(clusterID)
			}
			continue
		}
		// worker-manager health check scheme only support http now
		scheme := tls.GetURLScheme(config.EnableHTTPS())
		url := fmt.Sprintf("%s://%s:%s/componenthealth", scheme, cluster.ActiveNodes.IP, utils.WorkerManagerPort)
		state, err := checkWorkerManagerHealth(url)
		if err != nil {
			cluster.ActiveNodes.LastError = err.Error()
		} else {
			cluster.ActiveNodes.LastSuccessTime = time.Now()
		}

		if updateAlarmFlag(clusterID, state, currentUnHealthyClusters, cluster) {
			needUpdateAlarmFlag = true
		}

		cluster.State = state
		c.strategy.UpdateClusterItem(cluster)
	}

	updateAlarmInfo(needUpdateAlarmFlag, currentUnHealthyClusters)
}

func updateAlarmFlag(clusterID string, state string, currentUnHealthyClusters map[string]Cluster,
	cluster Cluster) bool {
	_, existInHistoryUnHealthyCluster := historyAlarmUnHealthyClusters[clusterID]

	if currentUnHealthyClusters == nil {
		log.GetLogger().Errorf("unhealthy work manager clusters is nil, please check")
		return false
	}

	if state != utils.Healthy {
		currentUnHealthyClusters[clusterID] = cluster
		// The previous alarm does not contain unhealthy information about the cluster. need to report the alarm again.
		if !existInHistoryUnHealthyCluster {
			return true
		}
	}

	// The cluster becomes a healthy cluster and the alarm information needs to be updated.
	if state == utils.Healthy && existInHistoryUnHealthyCluster {
		return true
	}
	return false
}

func updateAlarmInfo(needUpdateAlarmInfo bool, currentUnHealthyClusters map[string]Cluster) {
	if !needUpdateAlarmInfo {
		return
	}

	// All nodes in the cluster are healthy. The alarm information is updated as a clear alarm.
	if len(currentUnHealthyClusters) == 0 {
		log.GetLogger().Infof("clear alarm alarm.AutoScalerCheckWorkManagerUnHealthy")
		alarm.GetClient().ClearAlarm(alarm.AutoScalerCheckWorkManagerUnHealthy)
	} else {
		// there is still an unhealthy cluster and the cluster status is different from that in the previous alarm,
		// therefore, alarm is reported again.
		clusterIDs := make([]string, 0, len(currentUnHealthyClusters))
		for clusterID := range currentUnHealthyClusters {
			clusterIDs = append(clusterIDs, clusterID)
		}
		clusterIDStrings := strings.Join(clusterIDs, ",")

		log.GetLogger().Errorf("unhealthy work manager clusters include=[%s]", clusterIDStrings)
		alarm.GetClient().ClearAlarm(alarm.AutoScalerCheckWorkManagerUnHealthy)
		alarm.GetClient().ReportAlarm(alarm.AutoScalerCheckWorkManagerUnHealthy,
			fmt.Sprintf("unhealthy work manager clusters include=[%s]", clusterIDStrings))
	}
	historyAlarmUnHealthyClusters = currentUnHealthyClusters
}

// GetAllCluster -
func (c *ClusterManager) GetAllCluster() map[string]Cluster {
	if c.strategy == nil {
		log.GetLogger().Warnf("failed to get strategy for clustermgr")
		return map[string]Cluster{}
	}
	return c.strategy.GetClusterPool()
}

func checkWorkerManagerHealth(url string) (string, error) {
	ctx, cancel := context.WithTimeout(utils.GetContext(), heartbeatTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to new request to worker-manager: %s", err.Error())
		return utils.UnHealthy, err
	}
	var state string
	for i := 0; i < heartbeatTimeoutThreshold; i++ {
		state, err = doWorkerManagerRequest(req)
		if err == nil {
			log.GetLogger().Debugf("succeed to doWorkerManagerRequest")
			break
		}
		time.Sleep(heartbeatRetryInterval)
	}

	return state, err
}

func doWorkerManagerRequest(req *http.Request) (string, error) {
	workermanagerutils.FillInWorkerManagerRequestHeaders(req, config.GetLocalAuth())
	resp, err := utils.GetWorkerManagerClient().Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to do request to worker-manager: %s", err.Error())
		return utils.UnHealthy, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusInternalServerError {
		log.GetLogger().Errorf("failed to request worker-manager, err %s ", resp.Body)
		if string(resp.Header.Get(constants.HeaderRouterEtcdState)) == "false" {
			return utils.SubHealthy, errors.New("failed to health check worker-manager due to router etcd issues")
		}
		return utils.UnHealthy, errors.New("failed to request worker-manager")
	}
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("worker-manager health check status ok not ok: %d", resp.StatusCode)
		return utils.UnHealthy, err
	}

	return utils.Healthy, nil
}
