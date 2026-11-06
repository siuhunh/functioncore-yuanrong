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

// Package resourceusage deal the resource usage
package resourceusage

import (
	"context"
	"encoding/json"
	"time"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/urnutils"
)

const (
	resourceUsageKeyPrefix     = "fss-resource-metrics-"
	clusterStateKeyPrefix      = "ClusterState_Metric_"
	defaultDeleteFromRedisTime = 10 * 60
	defaultRetryTimes          = 3
	defaultRetryInterval       = time.Duration(100) * time.Millisecond
	defaultTickerTime          = 20
)

// NodeInfo describe CPU and MEMORY info of node
type NodeInfo struct {
	CPUTotal    int64 `json:"cpu_total,omitempty" valid:"int,optional"`
	CPUUsed     int64 `json:"cpu_used,omitempty" valid:"int,optional"`
	MemoryTotal int64 `json:"memory_total,omitempty" valid:"int,optional"`
	MemoryUsed  int64 `json:"memory_used,omitempty" valid:"int,optional"`
}

// Handler resource usage handler
type Handler struct {
	tickerTime       int64
	stopCh           <-chan struct{}
	monitoringClient *redisclient.Client
	clusters         []string
	refreshCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
				FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
	refreshClusterFunc     func(clusterMetricCache map[string]config.ClusterUsage)
	checkFunctionMetaExist func(funcSvcID string) bool
}

// UploadResourceData upload resource usage data
type UploadResourceData struct {
	Resource   map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList `json:"resource"`
	UploadTime int64                                                                          `json:"uploadTime"`
}

// GetHandler -
func GetHandler() *Handler {
	return &Handler{
		tickerTime: defaultTickerTime,
	}
}

// WithTickerTime -
func (h *Handler) WithTickerTime(tickerTime int64) *Handler {
	if tickerTime > 0 {
		h.tickerTime = tickerTime
	}
	return h
}

// WithStopCh -
func (h *Handler) WithStopCh(stopCh <-chan struct{}) *Handler {
	if stopCh != nil {
		h.stopCh = stopCh
	}
	return h
}

// WithMonitoringClient -
func (h *Handler) WithMonitoringClient(client *redisclient.Client) *Handler {
	if client != nil {
		h.monitoringClient = client
	}
	return h
}

// WithClusters -
func (h *Handler) WithClusters(clusters []string) *Handler {
	if len(clusters) > 0 {
		h.clusters = clusters
	}
	return h
}

// RegisterRefreshCacheFunc -
func (h *Handler) RegisterRefreshCacheFunc(refreshCacheFunc func(resourceMetricCache map[resourceusage.
	ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)) *Handler {
	if refreshCacheFunc != nil {
		h.refreshCacheFunc = refreshCacheFunc
	}
	return h
}

// RegisterRefreshClusterFunc -
func (h *Handler) RegisterRefreshClusterFunc(refreshClusterFunc func(map[string]config.ClusterUsage)) *Handler {
	if refreshClusterFunc != nil {
		h.refreshClusterFunc = refreshClusterFunc
	}
	return h
}

// RegisterCheckFunctionMetaExist -
func (h *Handler) RegisterCheckFunctionMetaExist(checkFunctionMetaExist func(funcSvcID string) bool) *Handler {
	if checkFunctionMetaExist != nil {
		h.checkFunctionMetaExist = checkFunctionMetaExist
	}
	return h
}

// HandlerResourceUsage -
func (h *Handler) HandlerResourceUsage() {
	go h.handlerResourceUsage()
}

func (h *Handler) handlerResourceUsage() {
	if h.tickerTime <= 0 {
		log.GetLogger().Warnf("handler report resource usage disabled, tickerTime: %d", h.tickerTime)
		return
	}

	if h.stopCh == nil {
		log.GetLogger().Errorf("handler report resource usage not start because stopCh is nil")
		return
	}

	log.GetLogger().Infof("start to handler report resource with tickerTime %d", h.tickerTime)
	ticker := time.NewTicker(time.Duration(h.tickerTime) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			resourceMetricCache := h.getAndDealResourceUsage()
			h.refreshCacheFunc(resourceMetricCache)
			clusterMetricCache := h.getClusterResourceUsage()
			h.refreshClusterFunc(clusterMetricCache)
		case <-h.stopCh:
			log.GetLogger().Infof("store handler resource usage stopped")
			return
		}
	}
}

func (h *Handler) getClusterResourceUsage() map[string]config.ClusterUsage {
	clusterStateMap := make(map[string]config.ClusterUsage)
	availableCluster := clustermgr.GetClusterMgr().GetAllAvailableCluster()
	for clusterID := range availableCluster {
		clusterInfo := h.getClusterResourceUsageFromRedis(clusterID)
		clusterStateMap[clusterID] = clusterInfo
	}
	return clusterStateMap
}

func (h *Handler) getAndDealResourceUsage() map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
	PodName]resourceusage.ResourceList {

	resourceMetricCache := make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
		PodName]resourceusage.ResourceList)

	availableCluster := clustermgr.GetClusterMgr().GetAllAvailableCluster()
	for clusterID := range availableCluster {
		uploadResourceData := h.getResourceUsageFromRedis(clusterID)
		resourceMetricCache[resourceusage.ClusterID(clusterID)] = h.changeUploadResourceData(clusterID, uploadResourceData)
	}

	return resourceMetricCache
}

// AllResources -
type AllResources map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList

func initAllResource() AllResources {
	return AllResources{}
}

func (ar *AllResources) pushData(funcSvcID string, podName resourceusage.PodName,
	podResource resourceusage.ResourceList) {
	if _, ok := map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.
		ResourceList(*ar)[resourceusage.FuncSvcID(funcSvcID)]; !ok {
		map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.
			ResourceList(*ar)[resourceusage.FuncSvcID(funcSvcID)] = make(map[resourceusage.
			PodName]resourceusage.ResourceList)
	}
	map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.
		ResourceList(*ar)[resourceusage.FuncSvcID(funcSvcID)][podName] = podResource
}

func (h *Handler) changeUploadResourceData(clusterID string, uploadResourceDataList map[resourceusage.
	NodeName]UploadResourceData) map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList {
	now := time.Now().Unix()

	allResources := initAllResource()
	for nodeName, uploadResourceData := range uploadResourceDataList {
		log.GetLogger().Debugf("get resource usage redis, now:%d, uploadResourceTime: %d, nodeName: %s", now,
			uploadResourceData.UploadTime, nodeName)
		if now-uploadResourceData.UploadTime > defaultDeleteFromRedisTime {
			log.GetLogger().Debugf("need delete data of resource usage redis, clusterID: %s, nodeName: %s",
				clusterID, nodeName)
			h.deleteFromExpireRedis(clusterID, string(nodeName))
			continue
		}

		for funcKey, item := range uploadResourceData.Resource {
			log.GetLogger().Debugf("upload Resource data: %+v", uploadResourceData.Resource)
			funcSvcID := urnutils.CrName(urnutils.ParseFuncKey(string(funcKey)))
			// if this func not in etcd, ignore it
			if !h.checkFunctionMetaExist(funcSvcID) {
				continue
			}
			for podName, podResource := range item {
				allResources.pushData(funcSvcID, podName, podResource)
			}
		}
	}

	return allResources
}

func (h *Handler) getClusterResourceUsageFromRedis(clusterID string) config.ClusterUsage {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to get resource usage from redis because monitoringClient is nil")
		return config.ClusterUsage{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
	defer cancel()
	key := clusterStateKeyPrefix + clusterID
	log.GetLogger().Debugf("get cluster resource usage, key: %s,timeout: %v", key,
		redisclient.DefaultRedisContextTimeout)
	clusterUsageStr, err := h.monitoringClient.Get(ctx, key).Result()
	clusterUsage := config.ClusterUsage{}
	if err != nil {
		log.GetLogger().Warnf("get cluster resource usage exec error: %s", err.Error())
		return clusterUsage
	}
	err = json.Unmarshal([]byte(clusterUsageStr), &clusterUsage)
	if err != nil {
		log.GetLogger().Warnf("get cluster usage err when unmarshal, str is %s", clusterUsageStr)
		return clusterUsage
	}
	log.GetLogger().Debugf("get cluster resource usage clusterUsage: %s,clusterUsage: %+v", key, clusterUsage)
	return clusterUsage
}

func (h *Handler) getResourceUsageFromRedis(clusterID string) map[resourceusage.NodeName]UploadResourceData {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to get resource usage from redis because monitoringClient is nil")
		return map[resourceusage.NodeName]UploadResourceData{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
	defer cancel()
	log.GetLogger().Debugf("get resource usage, key: %s,timeout: %v", resourceUsageKeyPrefix+clusterID,
		redisclient.DefaultRedisContextTimeout)
	resourceUsageMap, err := h.monitoringClient.HGetAll(ctx, resourceUsageKeyPrefix+clusterID).Result()
	if err != nil {
		log.GetLogger().Warnf("get resource usage exec error: %s", err.Error())
		return map[resourceusage.NodeName]UploadResourceData{}
	}

	var resourceUsage = map[resourceusage.NodeName]UploadResourceData{}

	for nodeName, resourceUsageStr := range resourceUsageMap {
		uploadResourceData := UploadResourceData{}
		err = json.Unmarshal([]byte(resourceUsageStr), &uploadResourceData)
		if err != nil {
			log.GetLogger().Warnf("get resource usage err when unmarshal, str is %s", resourceUsageStr)
			continue
		}
		resourceUsage[resourceusage.NodeName(nodeName)] = uploadResourceData
	}

	return resourceUsage
}

func (h *Handler) deleteFromExpireRedis(clusterID, nodeName string) {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to delete expire resource usage from redis because monitoringClient is nil")
		return
	}

	for i := 0; i < defaultRetryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
		err := h.monitoringClient.HDel(ctx, resourceUsageKeyPrefix+clusterID, nodeName).Err()
		cancel()
		if err == nil {
			log.GetLogger().Infof("Success to delete resource usage key: %s, hash key: %s",
				resourceUsageKeyPrefix+clusterID, nodeName)
			break
		}
		log.GetLogger().Warnf("failed to delete resource usage in redis, error: %s", err.Error())
		time.Sleep(defaultRetryInterval)
	}
}
