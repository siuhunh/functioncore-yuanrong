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
	"fmt"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/urnutils"
)

const (
	resourceUsageKeyPrefix = "fss-resource-metrics-"
	caeMetricsKeyPrefix    = "cae-resource-metrics-"
	clusterStateKeyPrefix  = "ClusterState_Metric_"
	// get data from redis
	predictResourceUsageKey = "fss-predict-resource-metrics-output"
	// put data into redis
	historyResourceUsageKey    = "fss-predict-resource-metrics-input"
	defaultDeleteFromRedisTime = 10 * 60
	defaultRetryTimes          = 3
	defaultRetryInterval       = time.Duration(100) * time.Millisecond
	defaultTickerTime          = 20
	defaultPredictDownloadTime = 30 * 60
	defaultPredictUploadTime   = 10 * 60
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
	tickerTime            int64
	predictDownloadTicker int64
	predictUploadTicker   int64
	predictEnable         bool
	stopCh                <-chan struct{}
	monitoringClient      *redisclient.Client
	clusters              []string
	refreshCacheFunc      func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
				FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
	refreshCaeMetricsCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]caemetrics.List)
	refreshClusterFunc         func(clusterMetricCache map[string]config.ClusterUsage)
	refreshPredictResourceFunc func(map[resourceusage.FuncSvcID]resourceusage.PredictResourceList)
	deletePredictInputCache    func(id resourceusage.FuncSvcID)
	checkFunctionMetaExist     func(funcSvcID string) bool
}

// UploadResourceData upload resource usage data
type UploadResourceData struct {
	Resource   map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList `json:"resource"`
	UploadTime int64                                                                          `json:"uploadTime"`
}

// UploadCaeMetricsData upload cae metrics data
type UploadCaeMetricsData struct {
	Metrics    map[resourceusage.FuncKey]map[resourceusage.PodName]caemetrics.List `json:"metrics"`
	UploadTime int64                                                               `json:"uploadTime"`
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

// WithPredictDownloadTickerTime - fetch data from redis ticker
func (h *Handler) WithPredictDownloadTickerTime(tickerTime int64) *Handler {
	if tickerTime > 0 {
		h.predictDownloadTicker = tickerTime
	} else {
		h.predictDownloadTicker = defaultPredictDownloadTime
	}
	return h
}

// WithPredictUploadTicker - push data into redis ticker
func (h *Handler) WithPredictUploadTicker(tickerTime int64) *Handler {
	if tickerTime > 0 {
		h.predictUploadTicker = tickerTime
	} else {
		h.predictUploadTicker = defaultPredictUploadTime
	}
	return h
}

// WithPredictEnable -
func (h *Handler) WithPredictEnable(enable bool) *Handler {
	h.predictEnable = enable
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

// RegisterRefreshCaeMetricsCacheFunc -
func (h *Handler) RegisterRefreshCaeMetricsCacheFunc(
	refreshCaeMetricsCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
		FuncSvcID]map[resourceusage.PodName]caemetrics.List)) *Handler {

	if refreshCaeMetricsCacheFunc != nil {
		h.refreshCaeMetricsCacheFunc = refreshCaeMetricsCacheFunc
	}
	return h
}

// RegisterPredictResourceFunc -
func (h *Handler) RegisterPredictResourceFunc(predictResourceFunc func(map[resourceusage.
	FuncSvcID]resourceusage.PredictResourceList)) *Handler {
	if predictResourceFunc != nil {
		h.refreshPredictResourceFunc = predictResourceFunc
	}
	return h
}

// RegisterDeletePredictInputCache -
func (h *Handler) RegisterDeletePredictInputCache(deleteFunc func(id resourceusage.FuncSvcID)) *Handler {
	if deleteFunc != nil {
		h.deletePredictInputCache = deleteFunc
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
	predictDownloadTicker := time.NewTicker(time.Duration(h.predictDownloadTicker) * time.Second)
	predictUploadTicker := time.NewTicker(time.Duration(h.predictUploadTicker) * time.Second)
	defer ticker.Stop()
	defer predictDownloadTicker.Stop()
	defer predictUploadTicker.Stop()

	for {
		select {
		case <-ticker.C:
			resourceMetricCache := h.getAndDealResourceUsage()
			h.refreshCacheFunc(resourceMetricCache)
			// for cae metrics
			caeMetricsCache := h.getAndDealCaeMetrics()
			h.refreshCaeMetricsCacheFunc(caeMetricsCache)

			clusterMetricCache := h.getClusterResourceUsage()
			h.refreshClusterFunc(clusterMetricCache)
		case <-predictDownloadTicker.C:
			if h.predictEnable {
				predictResourceUsage := h.getAndDealPredictResourceUsage()
				h.refreshPredictResourceFunc(predictResourceUsage)
			}
		case <-predictUploadTicker.C:
			if h.predictEnable {
				h.pushResourceUsage2Redis()
			}
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

func (h *Handler) getAndDealCaeMetrics() map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
	PodName]caemetrics.List {
	resourceMetricCache := make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
		PodName]caemetrics.List)

	availableCluster := clustermgr.GetClusterMgr().GetAllAvailableCluster()
	for clusterID := range availableCluster {
		uploadResourceData := h.getCaeMetricsFromRedis(clusterID)
		resourceMetricCache[resourceusage.ClusterID(clusterID)] = h.changeUploadCaeMetricsData(uploadResourceData)
	}
	return resourceMetricCache
}

func (h *Handler) getAndDealPredictResourceUsage() map[resourceusage.FuncSvcID]resourceusage.PredictResourceList {

	predictionResourceData := h.getPredictUsageFromRedis()

	return h.changeUploadPredictResourceData(predictionResourceData)
}

// AllResources -
type AllResources map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList

func initAllResource() AllResources {
	return AllResources{}
}

// AllPredictResources -
type AllPredictResources map[resourceusage.FuncSvcID]resourceusage.PredictResourceList

func initAllPredictResource() AllPredictResources {
	return AllPredictResources{}
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

// AllCaeMetricsResources -
type AllCaeMetricsResources map[resourceusage.FuncSvcID]map[resourceusage.PodName]caemetrics.List

func initAllCaeMetricsResources() AllCaeMetricsResources {
	return AllCaeMetricsResources{}
}

func (ar *AllCaeMetricsResources) pushData(funcSvcID string, podName resourceusage.PodName,
	podResource caemetrics.List) {
	if _, ok := map[resourceusage.FuncSvcID]map[resourceusage.PodName]caemetrics.
		List(*ar)[resourceusage.FuncSvcID(funcSvcID)]; !ok {
		map[resourceusage.FuncSvcID]map[resourceusage.PodName]caemetrics.
			List(*ar)[resourceusage.FuncSvcID(funcSvcID)] = make(map[resourceusage.
			PodName]caemetrics.List)
	}
	map[resourceusage.FuncSvcID]map[resourceusage.PodName]caemetrics.
		List(*ar)[resourceusage.FuncSvcID(funcSvcID)][podName] = podResource
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
			h.deleteFromExpireRedis(resourceUsageKeyPrefix+clusterID, string(nodeName))
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

func (h *Handler) changeUploadCaeMetricsData(uploadCaeMetricsResourceDataList map[resourceusage.
	NodeName]UploadCaeMetricsData) map[resourceusage.FuncSvcID]map[resourceusage.PodName]caemetrics.List {
	now := time.Now().Unix()

	allResources := initAllCaeMetricsResources()
	for nodeName, uploadCaeMetricsData := range uploadCaeMetricsResourceDataList {
		log.GetLogger().Debugf("get cae metrics redis, now:%d, uploadResourceTime: %d, nodeName: %s", now,
			uploadCaeMetricsData.UploadTime, nodeName)
		// ignore too old data
		if now-uploadCaeMetricsData.UploadTime > defaultDeleteFromRedisTime {
			continue
		}

		for funcKey, item := range uploadCaeMetricsData.Metrics {
			log.GetLogger().Debugf("upload Resource data: %+v", uploadCaeMetricsData.Metrics)
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

// filter data we need
func (h *Handler) changeUploadPredictResourceData(
	uploadResourceDataList map[resourceusage.FuncSvcID]resourceusage.PredictResourceList) map[resourceusage.
	FuncSvcID]resourceusage.PredictResourceList {
	cfg, errCfg := config.GetConfig()
	if errCfg != nil {
		log.GetLogger().Errorf("get config err when get predict from redis, err: %s", errCfg.Error())
		return map[resourceusage.FuncSvcID]resourceusage.PredictResourceList{}
	}
	allResources := initAllPredictResource()
	for funcSvcID, resourceList := range uploadResourceDataList {
		log.GetLogger().Infof("get predict resource usage redis, funcSvcID: %s", funcSvcID)
		// filter function not in predict white list
		if !cache.CheckPredictWhiteList(cfg.PredictConfig.PredictFuncWhiteList, string(funcSvcID)) {
			h.deleteFromExpireRedis(predictResourceUsageKey, string(funcSvcID))
			h.deleteFromExpireRedis(historyResourceUsageKey, string(funcSvcID))
			if h.deletePredictInputCache != nil {
				h.deletePredictInputCache(funcSvcID)
			}
			continue
		}
		allResources[funcSvcID] = resourceList
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

func (h *Handler) getCaeMetricsFromRedis(clusterID string) map[resourceusage.NodeName]UploadCaeMetricsData {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to get cae metrics from redis because monitoringClient is nil")
		return map[resourceusage.NodeName]UploadCaeMetricsData{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
	defer cancel()
	log.GetLogger().Debugf("get cae metrics, key: %s,timeout: %v", caeMetricsKeyPrefix+clusterID,
		redisclient.DefaultRedisContextTimeout)
	caeMetricsMap, err := h.monitoringClient.HGetAll(ctx, caeMetricsKeyPrefix+clusterID).Result()
	if err != nil {
		log.GetLogger().Warnf("get cae metrics exec error: %s", err.Error())
		return map[resourceusage.NodeName]UploadCaeMetricsData{}
	}

	var caeMetrics = map[resourceusage.NodeName]UploadCaeMetricsData{}

	for nodeName, caeMetricsStr := range caeMetricsMap {
		uploadCaeMetricsData := UploadCaeMetricsData{}
		err = json.Unmarshal([]byte(caeMetricsStr), &uploadCaeMetricsData)
		if err != nil {
			log.GetLogger().Warnf("get cae metrics err when unmarshal, str is %s", caeMetricsStr)
			continue
		}
		caeMetrics[resourceusage.NodeName(nodeName)] = uploadCaeMetricsData
	}

	return caeMetrics
}

func (h *Handler) getPredictUsageFromRedis() map[resourceusage.FuncSvcID]resourceusage.PredictResourceList {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to get resource usage from redis because monitoringClient is nil")
		return map[resourceusage.FuncSvcID]resourceusage.PredictResourceList{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultGetPredictUsageFromRedisTimeout)
	defer cancel()
	log.GetLogger().Infof("get predict resource usage from redis, key: %s", predictResourceUsageKey)
	resourceUsageMap, err := h.monitoringClient.HGetAll(ctx, predictResourceUsageKey).Result()
	if err != nil {
		log.GetLogger().Warnf("get resource usage exec error: %s", err.Error())
		return map[resourceusage.FuncSvcID]resourceusage.PredictResourceList{}
	}

	predictResourceUsage := make(map[resourceusage.FuncSvcID]resourceusage.PredictResourceList, len(resourceUsageMap))
	for funcSvcID, predictUsageStr := range resourceUsageMap {
		predictResourceList := resourceusage.PredictResourceList{}
		err = json.Unmarshal([]byte(predictUsageStr), &predictResourceList)
		if err != nil {
			log.GetLogger().Warnf("get predict resource usage err when unmarshal, err: %s", err.Error())
			continue
		}
		predictResourceUsage[resourceusage.FuncSvcID(funcSvcID)] = predictResourceList
	}

	return predictResourceUsage
}

func (h *Handler) uploadPredictUsage2Redis(resourceListMap resourceusage.PredictResourceListMap) error {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to get resource usage from redis because monitoringClient is nil")
		return fmt.Errorf("failed to get resource usage from redis because monitoringClient is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultUploadPredictUsage2RedisTimeout)
	defer cancel()
	list := resourceListMap.Convert2List4Redis()
	now := time.Now()
	if _, err := h.monitoringClient.HMSet(ctx, historyResourceUsageKey, list...).Result(); err != nil {
		log.GetLogger().Errorf("failed to put %s into redis, err:%s", historyResourceUsageKey, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to upload predict resource usage to redis, key: %s,spent time: %dms",
		historyResourceUsageKey,
		time.Since(now).Milliseconds())
	return nil
}

func (h *Handler) pushResourceUsage2Redis() {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config err when push predict into redis, err: %s", err.Error())
		return
	}
	inputPredictResource := make(resourceusage.PredictResourceListMap)
	for funcSvcID, metricsCache := range cache.GetInstance().PredictMetricsInputCache {
		if !cache.CheckPredictWhiteList(cfg.PredictConfig.PredictFuncWhiteList, string(funcSvcID)) ||
			len(metricsCache) == 0 {
			go h.deleteFromExpireRedis(historyResourceUsageKey, string(funcSvcID))
			continue
		}
		inputPredictResource[funcSvcID] = metricsCache
	}

	if len(inputPredictResource) == 0 {
		log.GetLogger().Debugf("inputPredictResource is empty")
		return
	}
	go func() {
		if err := h.uploadPredictUsage2Redis(inputPredictResource); err != nil {
			log.GetLogger().Errorf("failed to upload original predict resource to redis, err:%s", err.Error())
		}
	}()
}

func (h *Handler) deleteFromExpireRedis(key, filed string) {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to delete expire resource usage from redis because monitoringClient is nil")
		return
	}

	for i := 0; i < defaultRetryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
		err := h.monitoringClient.HDel(ctx, key, filed).Err()
		cancel()
		if err == nil {
			log.GetLogger().Infof("Success to delete resource usage key: %s, hash key: %s",
				key, filed)
			break
		}
		log.GetLogger().Warnf("failed to delete resource usage in redis, error: %s", err.Error())
		time.Sleep(defaultRetryInterval)
	}
}
