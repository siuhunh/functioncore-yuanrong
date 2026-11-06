/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package bridgemetrics -
package bridgemetrics

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
)

const (
	defaultTick    = 1
	defaultDataLen = 3
)

var bridgeMetricsHandler *Handler
var bridgeMetricsHandlerOnce sync.Once

type funcKey string
type podName string

// List bridge metrics
type List []common.BridgeFuncMetrics

// Handler handle bridge metrics
type Handler struct {
	metrics        map[funcKey]map[podName]List
	resourceMutex  sync.RWMutex
	tickerTime     int64
	dataLen        int64
	stopCh         <-chan struct{}
	metricsClient  *redisclient.Client
	uploadCallBack func(c *redisclient.Client, bridgeMetricsBytes []byte)
}

// UploadBridgeMetricsData upload bridge metrics data
type UploadBridgeMetricsData struct {
	Metrics    map[funcKey]map[podName]List `json:"metrics"`
	UploadTime int64                        `json:"uploadTime"`
}

// GetBridgeMetricsHandler get bridge metrics handler instance
func GetBridgeMetricsHandler() *Handler {
	bridgeMetricsHandlerOnce.Do(func() {
		bridgeMetricsHandler = &Handler{
			metrics:       make(map[funcKey]map[podName]List),
			resourceMutex: sync.RWMutex{},
			tickerTime:    defaultTick,
			dataLen:       defaultDataLen,
			metricsClient: nil,
			uploadCallBack: func(c *redisclient.Client, bridgeMetricsBytes []byte) {
				log.GetLogger().Warnf("not register upload call back in bridge metrics handler")
			},
		}
	})
	return bridgeMetricsHandler
}

func (l *List) storeDataWithDataLen(dataLen int64, metrics common.BridgeFuncMetrics) {
	if dataLen <= 0 {
		log.GetLogger().Errorf("store data with data len err because dataLen err: %d", dataLen)
		return
	}

	*l = append(*l, metrics)

	if len(*l) > int(dataLen) {
		*l = []common.BridgeFuncMetrics(*l)[len(*l)-int(dataLen):]
	}
}

func (l *List) getLatestDataTime() int64 {
	if len(*l) == 0 {
		return 0
	}
	last := []common.BridgeFuncMetrics(*l)[len(*l)-1]
	return last.TimeStamp
}

// Store store resource usage data
func (h *Handler) Store(bridgeMetrics common.BridgeFuncMetrics) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()

	instanceID := utils.GenerateInstanceID(bridgeMetrics.PodName)
	if !internal.IsOwnedInstancesExist(instanceID) {
		log.GetLogger().Warnf("store bridge metrics ignore because not exist owned instance: "+
			"podName: %s, instanceID: %s", bridgeMetrics.PodName, instanceID)
		return
	}

	// RemoveByInstance funcKey: tenantID/0@default@funcName/version
	serviceFuncName := fmt.Sprintf("%s@%s@%s", "0", bridgeMetrics.Package, bridgeMetrics.FuncName)
	functionKey := urnutils.GetFuncKey(bridgeMetrics.TenantID, serviceFuncName, bridgeMetrics.Version)

	podMap, resourceOk := h.metrics[funcKey(functionKey)]
	// if this function metrics data is empty in this node, we need add a new pod map
	if !resourceOk {
		log.GetLogger().Debugf("store bridge metrics is empty, funcKey: %s", functionKey)
		newPodMap := make(map[podName]List)
		bridgeMetricsList := List{}
		bridgeMetricsList.storeDataWithDataLen(h.dataLen, bridgeMetrics)
		newPodMap[podName(bridgeMetrics.PodName)] = bridgeMetricsList
		h.metrics[funcKey(functionKey)] = newPodMap
		return
	}

	oldBridgeMetrics, oldBridgeMetricsOK := podMap[podName(bridgeMetrics.PodName)]

	// if bridge metrics is existed before, we need add this data
	if oldBridgeMetricsOK {
		log.GetLogger().Debugf("store bridge metrics pod not empty, funcKey:%s, podName: %s, resource %+v",
			functionKey, bridgeMetrics.PodName, oldBridgeMetrics)

		lastReportTime := oldBridgeMetrics.getLatestDataTime()
		if lastReportTime > bridgeMetrics.TimeStamp {
			log.GetLogger().Warnf("Ignore outdated data of bridge metrics:"+
				"lastReportTime: %d, this report time: %d", lastReportTime, bridgeMetrics.TimeStamp)
			return
		}
		oldBridgeMetrics.storeDataWithDataLen(h.dataLen, bridgeMetrics)
		podMap[podName(bridgeMetrics.PodName)] = oldBridgeMetrics
		h.metrics[funcKey(functionKey)] = podMap
		return
	}

	log.GetLogger().Debugf("store bridge metrics pod not existed before: funcKey:%s, podName: %s",
		functionKey, bridgeMetrics.PodName)
	// if not exist before, we need add a new resourceList
	resourceList := List{}
	resourceList.storeDataWithDataLen(h.dataLen, bridgeMetrics)
	podMap[podName(bridgeMetrics.PodName)] = resourceList
}

// RemoveByInstance store resource usage data
func (h *Handler) RemoveByInstance(functionKey, instanceID string) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()

	log.GetLogger().Debugf("remove bridge metrics by instance, functionKey: %s, instanceID: %s",
		functionKey, instanceID)

	podNameFromInstanceID := utils.GetPodNameByInstanceID(instanceID)
	if podNameFromInstanceID == "" {
		log.GetLogger().Warnf("remove by instance failed because get podName failed, funcKey: %s, instanceID: %s",
			functionKey, instanceID)
		return
	}

	_, resourceOk := h.metrics[funcKey(functionKey)]
	// if this metrics is empty in this node
	if !resourceOk {
		log.GetLogger().Warnf("try to remove bridge resource usage failed because not exist, funcKey: %s", functionKey)
		return
	}

	delete(h.metrics[funcKey(functionKey)], podName(podNameFromInstanceID))
	if len(h.metrics[funcKey(functionKey)]) == 0 {
		delete(h.metrics, funcKey(functionKey))
	}
	log.GetLogger().Debugf("delete bridge metrics finished, functionKey : %s, instanceID: %s, len map is: %d, "+
		"func pod list len is %d", functionKey, instanceID, len(h.metrics), len(h.metrics[funcKey(functionKey)]))
}

// RemoveByFuncKey store resource usage data
func (h *Handler) RemoveByFuncKey(functionKey string) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()

	log.GetLogger().Debugf("remove bridge metrics by function key, functionKey: %s", functionKey)

	_, resourceOk := h.metrics[funcKey(functionKey)]
	// if this bridge metrics is empty in this node
	if !resourceOk {
		log.GetLogger().Warnf("remove by function key failed because funcKey not exist: %s", functionKey)
		return
	}

	delete(h.metrics, funcKey(functionKey))
	log.GetLogger().Debugf("delete bridge metrics by func key finished, functionKey: %s, len map is: %d, "+
		"func pod list len is %d", functionKey, len(h.metrics), len(h.metrics[funcKey(functionKey)]))
}

// WithTickerTime -
func (h *Handler) WithTickerTime(tickerTime int64) *Handler {
	if tickerTime > 0 {
		h.tickerTime = tickerTime
	}
	return h
}

// WithDataLen -
func (h *Handler) WithDataLen(dataLen int64) *Handler {
	if dataLen > 0 {
		h.dataLen = dataLen
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

// WithMetricsClient -
func (h *Handler) WithMetricsClient(client *redisclient.Client) *Handler {
	if client != nil {
		h.metricsClient = client
	}
	return h
}

// RegisterReportCallBack -
func (h *Handler) RegisterReportCallBack(callback func(c *redisclient.Client, bridgeMetricsBytes []byte)) *Handler {
	if callback != nil {
		h.uploadCallBack = callback
	}
	return h
}

// StartReportBridgeMetricsToRedis start ticker of report resource usage
func (h *Handler) StartReportBridgeMetricsToRedis() {
	go h.storeBridgeMetricsToRedis()
}

func (h *Handler) getMetrics() map[funcKey]map[podName]List {
	return h.metrics
}

func (h *Handler) storeBridgeMetricsToRedis() {
	if h.tickerTime <= 0 {
		log.GetLogger().Warnf("store report bridge metrics disabled, tickerTime: %d", h.tickerTime)
		return
	}

	if h.stopCh == nil {
		log.GetLogger().Errorf("store report bridge metrics not start because stopCh is nil")
		return
	}
	log.GetLogger().Infof("start to store bridge metrics with tickerTime %d", h.tickerTime)
	ticker := time.NewTicker(time.Duration(h.tickerTime) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.resourceMutex.RLock()

			if len(h.metrics) == 0 {
				log.GetLogger().Debugf("skip this store report bridge metrics because no metrics")
				h.resourceMutex.RUnlock()
				continue
			}

			uploadBridgeMetricsData := UploadBridgeMetricsData{
				Metrics:    h.getMetrics(),
				UploadTime: time.Now().Unix(),
			}
			bridgeMetricsBytes, err := json.Marshal(uploadBridgeMetricsData)
			if err != nil {
				log.GetLogger().Warnf("json marshal bridge metrics error when save to redis:%s", err.Error())
				h.resourceMutex.RUnlock()
				continue
			}
			h.resourceMutex.RUnlock()
			h.uploadCallBack(h.metricsClient, bridgeMetricsBytes)
		case <-h.stopCh:
			log.GetLogger().Infof("store report bridge metrics stopped")
			return
		}
	}
}
