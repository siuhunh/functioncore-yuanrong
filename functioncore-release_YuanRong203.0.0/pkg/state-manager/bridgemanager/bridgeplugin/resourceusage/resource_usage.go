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

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/urnutils"
)

const (
	resourceUsageKeyPrefix     = "bridge-resource-metrics-"
	defaultDeleteFromRedisTime = 10 * 60
	defaultRetryTimes          = 3
	defaultRetryInterval       = time.Duration(100) * time.Millisecond
	defaultTickerTime          = 15
)

// Handler resource usage handler
type Handler struct {
	tickerTime       int64
	stopCh           <-chan struct{}
	monitoringClient *redisclient.Client
	clusterID        resourceusage.ClusterID
	refreshCacheFunc func(resourceMetricCache map[resourceusage.
				FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList)
}

// UploadResourceData upload resource usage data
type UploadResourceData struct {
	Resource   map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.BridgeResourceList `json:"metrics"`
	UploadTime int64                                                                                `json:"uploadTime"`
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

// WithClusterID -
func (h *Handler) WithClusterID(clusterID string) *Handler {
	if clusterID != "" {
		h.clusterID = resourceusage.ClusterID(clusterID)
	}
	return h
}

// RegisterRefreshCacheFunc -
func (h *Handler) RegisterRefreshCacheFunc(refreshCacheFunc func(resourceMetricCache map[resourceusage.
	FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList)) *Handler {
	if refreshCacheFunc != nil {
		h.refreshCacheFunc = refreshCacheFunc
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
			h.refreshCacheFunc(h.changeUploadResourceData(h.clusterID, h.getResourceUsageFromRedis(h.clusterID)))
		case <-h.stopCh:
			log.GetLogger().Infof("store handler resource usage stopped")
			return
		}
	}
}

// AllResources -
type AllResources map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList

func initAllResource() AllResources {
	return AllResources{}
}

// AllPredictResources -
type AllPredictResources map[resourceusage.FuncSvcID]resourceusage.PredictResourceList

func (h *Handler) changeUploadResourceData(clusterID resourceusage.ClusterID, uploadResourceDataList map[resourceusage.
	NodeName]UploadResourceData) map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList {
	now := time.Now().Unix()

	allResources := initAllResource()
	for nodeName, uploadResourceData := range uploadResourceDataList {
		log.GetLogger().Debugf("get resource usage redis, now: %d, uploadResourceTime: %d, nodeName: %s", now,
			uploadResourceData.UploadTime, nodeName)
		// if ten minutes without new data, this key in hash data will be deleted
		if now-uploadResourceData.UploadTime > defaultDeleteFromRedisTime {
			log.GetLogger().Debugf("need delete data of resource usage redis, clusterID: %s, nodeName: %s",
				clusterID, nodeName)
			h.deleteFromExpireRedis(resourceUsageKeyPrefix+string(clusterID), string(nodeName))
			continue
		}

		for funcKey, item := range uploadResourceData.Resource {
			log.GetLogger().Debugf("upload Resource data: %+v", uploadResourceData.Resource)
			funcSvcID := urnutils.CrName(urnutils.ParseFuncKey(string(funcKey)))
			for podName, podResource := range item {
				allResources.pushData(funcSvcID, podName, podResource)
			}
		}
	}

	return allResources
}

func (ar *AllResources) pushData(funcSvcID string, podName resourceusage.PodName,
	podResource resourceusage.BridgeResourceList) {
	if _, ok := map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.
		BridgeResourceList(*ar)[resourceusage.FuncSvcID(funcSvcID)]; !ok {
		map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.
			BridgeResourceList(*ar)[resourceusage.FuncSvcID(funcSvcID)] = make(map[resourceusage.
			PodName]resourceusage.BridgeResourceList)
	}
	map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.
		BridgeResourceList(*ar)[resourceusage.FuncSvcID(funcSvcID)][podName] = podResource
}

func (h *Handler) getResourceUsageFromRedis(clusterID resourceusage.
	ClusterID) map[resourceusage.NodeName]UploadResourceData {
	if h.monitoringClient == nil {
		log.GetLogger().Errorf("failed to get resource usage from redis because monitoringClient is nil")
		return map[resourceusage.NodeName]UploadResourceData{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
	defer cancel()
	log.GetLogger().Debugf("get resource usage, key: %s,timeout: %v", resourceUsageKeyPrefix+clusterID,
		redisclient.DefaultRedisContextTimeout)
	resourceUsageMap, err := h.monitoringClient.HGetAll(ctx, resourceUsageKeyPrefix+string(clusterID)).Result()
	if err != nil {
		log.GetLogger().Warnf("get resource usage exec error: %s", err.Error())
		return map[resourceusage.NodeName]UploadResourceData{}
	}
	var resourceUsage = map[resourceusage.NodeName]UploadResourceData{}

	log.GetLogger().Debugf("get resource usage from redis, key is %s, resourceUsageMap is %+v",
		resourceUsageKeyPrefix+string(clusterID), resourceUsageMap)

	for nodeName, resourceUsageStr := range resourceUsageMap {
		uploadResourceData := UploadResourceData{}
		err = json.Unmarshal([]byte(resourceUsageStr), &uploadResourceData)
		if err != nil {
			log.GetLogger().Warnf("get resource usage err when unmarshal, str is %s", resourceUsageStr)
			continue
		}
		resourceUsage[resourceusage.NodeName(nodeName)] = uploadResourceData
	}

	log.GetLogger().Debugf("get data from redis, resouceUsage is %+v", resourceUsage)
	return resourceUsage
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
