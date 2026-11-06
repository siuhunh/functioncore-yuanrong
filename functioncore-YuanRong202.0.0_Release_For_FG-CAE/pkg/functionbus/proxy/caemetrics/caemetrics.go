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

// Package caemetrics deal the cae metrics
package caemetrics

import (
	"fmt"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/queue"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/tcpbustool"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
)

const (
	defaultTick    = 15
	defaultDataLen = 4
)

var caeMetricsHandler *Handler
var caeMetricsHandlerOnce sync.Once

// FuncKey -
type funcKey string

// PodName -
type podName string

// List cae metrics
type List []Metrics

// Handler handle cae metrics
type Handler struct {
	resources       map[funcKey]map[podName]List
	resourceMutex   sync.RWMutex
	tickerTime      int64
	dataLen         int64
	stopCh          <-chan struct{}
	metricsClient   *redisclient.Client
	uploadCallBack  func(c *redisclient.Client, uploadCaeMetricsData UploadCaeMetricsData)
	podErrInfoQueue queue.Queue
	queueMutex      sync.RWMutex
}

// Metrics the metric for cae
type Metrics struct {
	CPUUsageRate              float64                        `json:"cpuUsageRate"`
	MemoryUsageRate           float64                        `json:"memoryUsageRate"`
	ReceiveBytesPerSecond     float64                        `json:"receiveBytesPerSecond"`
	ReceivePackagesPerSecond  float64                        `json:"receivePackagesPerSecond"`
	TransmitBytesPerSecond    float64                        `json:"transmitBytesPerSecond"`
	TransmitPackagesPerSecond float64                        `json:"transmitPackagesPerSecond"`
	FileSystemReadRate        float64                        `json:"fileSystemReadRate"`
	FileSystemWriteRate       float64                        `json:"fileSystemWriteRate"`
	CustomReportMetrics       tcpbustool.CustomReportMetrics `json:"customReportMetrics"`
	ReportTime                int64                          `json:"reportTime"`
	Properties                map[string]string              `json:"properties"`
	FileSystemUsageRate       float64                        `json:"fileSystemUsageRate"`
	FileSystemEmptySpace      float64                        `json:"fileSystemEmptySpace"`
	FileSystemCapacity        float64                        `json:"fileSystemCapacity"`
	MemoryUsage               float64                        `json:"memoryUsage"`
	MemoryCapacity            float64                        `json:"memoryCapacity"`
	CPUUsage                  float64                        `json:"cpuUsage"`
	CPUCapacity               float64                        `json:"cpuCapacity"`
	Status                    int64                          `json:"status"`
}

// UploadCaeMetricsData upload cae metrics data
type UploadCaeMetricsData struct {
	Resource   map[funcKey]map[podName]List `json:"metrics"`
	UploadTime int64                        `json:"uploadTime"`
}

type podErrInfoPair struct {
	funcKey string
	podName string
}

// GetCaeMetricsHandler get cae metrics handler instance
func GetCaeMetricsHandler() *Handler {
	caeMetricsHandlerOnce.Do(func() {
		caeMetricsHandler = &Handler{
			resources:     make(map[funcKey]map[podName]List),
			resourceMutex: sync.RWMutex{},
			tickerTime:    defaultTick,
			dataLen:       defaultDataLen,
			metricsClient: nil,
			uploadCallBack: func(c *redisclient.Client, uploadCaeMetricsData UploadCaeMetricsData) {
				log.GetLogger().Warnf("not register upload call back in cae metrics handler")
			},
			podErrInfoQueue: queue.NewFifoQueue(),
			queueMutex:      sync.RWMutex{},
		}
	})
	return caeMetricsHandler
}

func (l *List) storeDataWithDataLen(dataLen int64, metrics Metrics) {
	if dataLen <= 0 {
		log.GetLogger().Errorf("store data with data len err because dataLen err: %d", dataLen)
		return
	}

	*l = append(*l, metrics)

	if len(*l) > int(dataLen) {
		*l = []Metrics(*l)[len(*l)-int(dataLen):]
	}

	return
}

func (l *List) getLatestDataTime() int64 {
	if len(*l) == 0 {
		return 0
	}
	last := []Metrics(*l)[len(*l)-1]
	return last.ReportTime
}

// PushPodErrMetrics push podErrorMetrics to queue, queue will pop all metrics to resources to submit
func (h *Handler) PushPodErrMetrics(funcUrn string, podErrInfo poderrormessage.PodErrorInfo) {
	functionKey := transformToFunctionKey(funcUrn)
	if functionKey == "" {
		log.GetLogger().Warnf("transfrom funcUrn: %s to functionKey is empty, skip push", funcUrn)
		return
	}
	properties := make(map[string]string)
	properties[constants.CAEPropertiesEnvName] = podErrInfo.EnvName
	properties[constants.CAEPropertiesComponentName] = podErrInfo.ComponentName
	properties[constants.CAEPropertiesAppName] = podErrInfo.ApplicationName
	properties[constants.CAEPropertiesEnvID] = podErrInfo.EnvID
	properties[constants.CAEPropertiesComponentID] = podErrInfo.ComponentID
	properties[constants.CAEPropertiesAppID] = podErrInfo.ApplicationID
	h.queueMutex.Lock()
	defer h.queueMutex.Unlock()
	podErrMetrics := common.InitCaeMetrics().WithFunctionKey(functionKey).
		WithPodName(podErrInfo.PodName).WithProperties(properties).WithStatus(1)
	// only push to queue, no submit
	log.GetLogger().Debugf("push error metrics to queue, functionKey: %s, podName: %s", functionKey, podErrInfo.PodName)
	err := h.podErrInfoQueue.Push(podErrMetrics)
	if err != nil {
		log.GetLogger().Errorf("push %s %s error metrics failed", functionKey, podErrInfo.PodName)
		return
	}
}

func (h *Handler) storeErrPodMetrics() []podErrInfoPair {
	h.queueMutex.Lock()
	defer h.queueMutex.Unlock()
	pairList := make([]podErrInfoPair, 0, h.podErrInfoQueue.Len())
	for h.podErrInfoQueue.Len() > 0 {
		if obj, err := h.podErrInfoQueue.Pop(); err == nil {
			metrics, ok := obj.(*common.CaeMetrics)
			if !ok {
				log.GetLogger().Errorf("failed to convert obj to CaeMetrics")
				continue
			}
			// replace to current time
			metrics.ReportTime = time.Now().Unix()
			h.Store(*metrics)
			pairList = append(pairList, podErrInfoPair{
				funcKey: metrics.FunctionKey,
				podName: metrics.PodName,
			})
		} else {
			log.GetLogger().Errorf("pop podErrInfoQueue failed, err: %s", err.Error())
			break
		}
	}
	return pairList
}

// Store store resource usage data
func (h *Handler) Store(caeMetrics common.CaeMetrics) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()

	if !internal.IsOwnedInstancesExist(utils.GenerateInstanceID(caeMetrics.PodName)) &&
		caeMetrics.Status != int64(1) {
		log.GetLogger().Warnf("store cae metrics ignore because not exist owned instance: "+
			"podName: %s, instanceID: %s", caeMetrics.PodName, utils.GenerateInstanceID(caeMetrics.PodName))
		return
	}

	resource := constructMetrics(caeMetrics)

	podMap, resourceOk := h.resources[funcKey(caeMetrics.FunctionKey)]
	// if this function cae metrics data is empty in this node
	if !resourceOk {
		log.GetLogger().Debugf("store cae metrics is empty: funcKey:", caeMetrics.FunctionKey)
		newPodMap := make(map[podName]List)
		caeMetricsList := List{}
		caeMetricsList.storeDataWithDataLen(h.dataLen, resource)
		newPodMap[podName(caeMetrics.PodName)] = caeMetricsList
		h.resources[funcKey(caeMetrics.FunctionKey)] = newPodMap
		return
	}

	oldResource, oldResourceOk := podMap[podName(caeMetrics.PodName)]

	// if cae metrics is existed before, we need add this data
	if oldResourceOk {
		log.GetLogger().Debugf("store cae metrics pod is empty: funcKey:%s, podName: %s",
			caeMetrics.FunctionKey, caeMetrics.PodName)

		lastReportTime := oldResource.getLatestDataTime()
		if lastReportTime > resource.ReportTime {
			log.GetLogger().Warnf("store cae metrics get not recent data so ignore: "+
				"lastReportTime: %d, this report time: %d", lastReportTime, resource.ReportTime)
			return
		}
		oldResource.storeDataWithDataLen(h.dataLen, resource)
		podMap[podName(caeMetrics.PodName)] = oldResource
		h.resources[funcKey(caeMetrics.FunctionKey)] = podMap
		return
	}

	log.GetLogger().Debugf("store cae metrics pod not existed before: funcKey:%s, podName: %s",
		caeMetrics.FunctionKey, caeMetrics.PodName)
	// if not exist before, we need add a new resourceList
	resourceList := List{}
	resourceList.storeDataWithDataLen(h.dataLen, resource)
	podMap[podName(caeMetrics.PodName)] = resourceList
	return
}

func constructMetrics(caeMetrics common.CaeMetrics) Metrics {
	return Metrics{
		CPUUsageRate:              caeMetrics.CPUUsageRate,
		MemoryUsageRate:           caeMetrics.MemoryUsageRate,
		ReceiveBytesPerSecond:     caeMetrics.ReceiveBytesPerSecond,
		ReceivePackagesPerSecond:  caeMetrics.ReceivePackagePerSecond,
		TransmitBytesPerSecond:    caeMetrics.TransmitBytesPerSecond,
		TransmitPackagesPerSecond: caeMetrics.TransmitPackagePerSecond,
		FileSystemReadRate:        caeMetrics.FileSystemReadBytesPerSecond,
		FileSystemWriteRate:       caeMetrics.FileSystemWriteBytesPerSecond,
		CustomReportMetrics:       caeMetrics.CustomReportMetrics,
		ReportTime:                caeMetrics.ReportTime,
		Properties:                caeMetrics.Properties,
		FileSystemUsageRate:       caeMetrics.FileSystemUsageRate,
		FileSystemEmptySpace:      caeMetrics.FileSystemEmptySpace,
		FileSystemCapacity:        caeMetrics.FileSystemCapacity,
		MemoryUsage:               caeMetrics.MemoryUsage,
		MemoryCapacity:            caeMetrics.MemoryCapacity,
		CPUUsage:                  caeMetrics.CPUUsage,
		CPUCapacity:               caeMetrics.CPUCapacity,
		Status:                    caeMetrics.Status,
	}
}

// RemoveByInstance store resource usage data
func (h *Handler) RemoveByInstance(functionKey, instanceID string) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()

	log.GetLogger().Debugf("remove cae metrics by instance, functionKey: %s, instanceID: %s",
		functionKey, instanceID)

	podNameFromInstanceID := utils.GetPodNameByInstanceID(instanceID)
	if podNameFromInstanceID == "" {
		log.GetLogger().Warnf("remove by instance failed because get podName failed, instanceID: %s", instanceID)
		return
	}

	_, resourceOk := h.resources[funcKey(functionKey)]
	// if this cae metrics is empty in this node
	if !resourceOk {
		log.GetLogger().Warnf("try to remove resource usage failed because not exist,funcKey: %s", functionKey)
		return
	}

	delete(h.resources[funcKey(functionKey)], podName(podNameFromInstanceID))
	if len(h.resources[funcKey(functionKey)]) == 0 {
		delete(h.resources, funcKey(functionKey))
	}
	log.GetLogger().Debugf("delete cae metrics finished, functionKey : %s, instanceID: %s, len map is: %d, "+
		"func pod list len is %d", functionKey, instanceID, len(h.resources), len(h.resources[funcKey(functionKey)]))
}

// RemoveByFuncKey store resource usage data
func (h *Handler) RemoveByFuncKey(functionKey string) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()

	log.GetLogger().Debugf("remove cae metrics by function key, functionKey: %s", functionKey)

	_, resourceOk := h.resources[funcKey(functionKey)]
	// if this cae metrics is empty in this node
	if !resourceOk {
		log.GetLogger().Warnf("remove by function key failed because funcKey not exist: %s", functionKey)
		return
	}

	delete(h.resources, funcKey(functionKey))
	log.GetLogger().Debugf("delete cae metrics by func key finished, functionKey: %s, len map is: %d, "+
		"func pod list len is %d", len(h.resources), len(h.resources[funcKey(functionKey)]))
}

func (h *Handler) removeByPodErrInfoList(pairList []podErrInfoPair) {
	h.resourceMutex.Lock()
	defer h.resourceMutex.Unlock()
	for _, pair := range pairList {
		_, resourceOk := h.resources[funcKey(pair.funcKey)]
		// if this cae metrics is empty in this node
		if !resourceOk {
			continue
		}
		delete(h.resources[funcKey(pair.funcKey)], podName(pair.podName))
		if len(h.resources[funcKey(pair.funcKey)]) == 0 {
			delete(h.resources, funcKey(pair.funcKey))
		}
	}
	log.GetLogger().Debugf("delete cae metrics by podErrInfoPairList finished, len map is: %d",
		len(pairList))
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
func (h *Handler) RegisterReportCallBack(callback func(c *redisclient.Client,
	uploadCaeMetricsData UploadCaeMetricsData)) *Handler {
	if callback != nil {
		h.uploadCallBack = callback
	}
	return h
}

// StartReportCaeMetricsToRedis start ticker of report resource usage
func (h *Handler) StartReportCaeMetricsToRedis() {
	go h.storeCaeMetricsToRedis()
}

func (h *Handler) getResource() map[funcKey]map[podName]List {
	return h.resources
}

func (h *Handler) storeCaeMetricsToRedis() {
	if h.tickerTime <= 0 {
		log.GetLogger().Warnf("store report cae metrics disabled, tickerTime: %d", h.tickerTime)
		return
	}

	if h.stopCh == nil {
		log.GetLogger().Errorf("store report cae metrics not start because stopCh is nil")
		return
	}
	log.GetLogger().Infof("start to store cae metrics with tickerTime %s", h.tickerTime)
	ticker := time.NewTicker(time.Duration(h.tickerTime) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pairList := h.storeErrPodMetrics()
			h.resourceMutex.RLock()

			if len(h.resources) == 0 {
				log.GetLogger().Debugf("skip this store report cae metrics because no resources")
				h.resourceMutex.RUnlock()
				continue
			}

			uploadCaeMetricsData := UploadCaeMetricsData{
				Resource:   h.getResource(),
				UploadTime: time.Now().Unix(),
			}

			h.uploadCallBack(h.metricsClient, uploadCaeMetricsData)

			h.resourceMutex.RUnlock()
			h.removeByPodErrInfoList(pairList)
		case <-h.stopCh:
			log.GetLogger().Infof("store report resource usage stopped")
			return
		}
	}
}

// transformToFunctionKey transfer funcUrnWithVersion to funcKey
func transformToFunctionKey(funcUrnWithVersion string) string {
	funcUrn := urnutils.BaseURN{}
	err := funcUrn.ParseFrom(funcUrnWithVersion)
	if err != nil {
		log.GetLogger().Warnf("failed to parse urn %s err %s", funcUrnWithVersion, err.Error())
		return ""
	}
	return fmt.Sprintf("%s/%s/%s", funcUrn.TenantID, funcUrn.Name, funcUrn.Version)
}
