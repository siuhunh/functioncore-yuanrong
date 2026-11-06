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
	"encoding/json"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
)

const (
	defaultTick    = 1
	defaultDataLen = 3
)

var nodeResourceUsage *NodeResourceUsage
var nodeResourceOnce sync.Once

type funcKey string
type podName string

// NodeResourceUsage node resource usage
type NodeResourceUsage struct {
	resources      map[funcKey]map[podName]ResourceList
	resourceMutex  sync.RWMutex
	tickerTime     int64
	dataLen        int64
	stopCh         <-chan struct{}
	metricsClient  *redisclient.Client
	uploadCallBack func(c *redisclient.Client, resourceBytes []byte)
}

// ResourceList -
type ResourceList []resourceusage.Resource

// UploadResourceData upload resource usage data
type UploadResourceData struct {
	Resource   map[funcKey]map[podName]ResourceList `json:"resource"`
	UploadTime int64                                `json:"uploadTime"`
}

// GetNodeResourceUsageInstance get resource usage instance
func GetNodeResourceUsageInstance() *NodeResourceUsage {
	nodeResourceOnce.Do(func() {
		nodeResourceUsage = &NodeResourceUsage{
			resources:      make(map[funcKey]map[podName]ResourceList),
			resourceMutex:  sync.RWMutex{},
			tickerTime:     defaultTick,
			dataLen:        defaultDataLen,
			metricsClient:  nil,
			uploadCallBack: func(c *redisclient.Client, resourceBytes []byte) {},
		}
	})
	return nodeResourceUsage
}

func (rl *ResourceList) storeDataWithDataLen(dataLen int64, resource resourceusage.Resource) {
	if dataLen <= 0 {
		log.GetLogger().Errorf("store data with data len err because dataLen err: %d", dataLen)
		return
	}

	*rl = append(*rl, resource)

	if len(*rl) > int(dataLen) {
		*rl = []resourceusage.Resource(*rl)[len(*rl)-int(dataLen):]
	}

	return
}

func (rl *ResourceList) getLatestDataTime() int64 {
	if len(*rl) == 0 {
		return 0
	}
	last := []resourceusage.Resource(*rl)[len(*rl)-1]
	return last.ReportTime
}

// Store store resource usage data
func (r *NodeResourceUsage) Store(resourceUsage common.ResourceUsage) {
	r.resourceMutex.Lock()
	defer r.resourceMutex.Unlock()
	log.GetLogger().Debugf("store resource usage, %+v", resourceUsage)

	instanceID := utils.GenerateInstanceID(resourceUsage.PodName)
	if !internal.IsOwnedInstancesExist(instanceID) {
		log.GetLogger().Warnf("not exist owned instance, so ignore %s for %s", resourceUsage.PodName, instanceID)
		return
	}

	resource := resourceusage.Resource{
		CPUUsage: resourceUsage.CPUUsage, MemoryUsage: resourceUsage.MemoryUsage, CPULimit: resourceUsage.CPULimit,
		MemoryLimit: resourceUsage.MemoryLimit, ReportTime: resourceUsage.ReportTime,
		ReceiveBytesPerSecond:         resourceUsage.ReceiveBytesPerSecond,
		ReceivePackagePerSecond:       resourceUsage.ReceivePackagePerSecond,
		TransmitBytesPerSecond:        resourceUsage.TransmitBytesPerSecond,
		TransmitPackagePerSecond:      resourceUsage.TransmitPackagePerSecond,
		FileSystemReadBytesPerSecond:  resourceUsage.FileSystemReadBytesPerSecond,
		FileSystemWriteBytesPerSecond: resourceUsage.FileSystemWriteBytesPerSecond,
		CustomReportMetrics:           resourceUsage.CustomReportMetrics.ConvertToCommonCustomMetric(),
	}

	podMap, resourceOk := r.resources[funcKey(resourceUsage.FunctionKey)]
	// if this function usage data is empty in this node
	if !resourceOk {
		log.GetLogger().Debugf("store resource usage function is empty: funcKey:", resourceUsage.FunctionKey)
		newPodMap := make(map[podName]ResourceList)
		resourceList := ResourceList{}
		resourceList.storeDataWithDataLen(r.dataLen, resource)
		newPodMap[podName(resourceUsage.PodName)] = resourceList
		r.resources[funcKey(resourceUsage.FunctionKey)] = newPodMap
		return
	}

	oldResource, oldResourceOk := podMap[podName(resourceUsage.PodName)]

	// if resource usage is existed before, we need add this data
	if oldResourceOk {
		log.GetLogger().Debugf("store is empty %s for %s", resourceUsage.FunctionKey, resourceUsage.PodName)

		lastReportTime := oldResource.getLatestDataTime()
		if lastReportTime > resource.ReportTime {
			log.GetLogger().Warnf("store resource usage data get not recent data so ignore: "+
				"lastReportTime: %d, this report time: %d", lastReportTime, resource.ReportTime)
			return
		}
		oldResource.storeDataWithDataLen(r.dataLen, resource)
		podMap[podName(resourceUsage.PodName)] = oldResource
		r.resources[funcKey(resourceUsage.FunctionKey)] = podMap
		return
	}

	log.GetLogger().Debugf("store resource usage pod is existed before: funcKey:%s, podName: %s",
		resourceUsage.FunctionKey, resourceUsage.PodName)
	// if not exist before, we need add a new resourceList
	resourceList := ResourceList{}
	resourceList.storeDataWithDataLen(r.dataLen, resource)
	podMap[podName(resourceUsage.PodName)] = resourceList
	return
}

// RemoveByInstance store resource usage data
func (r *NodeResourceUsage) RemoveByInstance(functionKey, instanceID string) {
	r.resourceMutex.Lock()
	defer r.resourceMutex.Unlock()

	log.GetLogger().Debugf("remove resource usage by instance, functionKey: %s, instanceID: %s",
		functionKey, instanceID)

	podNameFromInstanceID := utils.GetPodNameByInstanceID(instanceID)
	if podNameFromInstanceID == "" {
		log.GetLogger().Warnf("remove by instance failed because get podName failed, instanceID: %s", instanceID)
		return
	}

	_, resourceOk := r.resources[funcKey(functionKey)]
	// if this function usage data is empty in this node
	if !resourceOk {
		log.GetLogger().Warnf("try to remove resource usage failed because not exist,funcKey: %s", functionKey)
		return
	}

	delete(r.resources[funcKey(functionKey)], podName(podNameFromInstanceID))
	if len(r.resources[funcKey(functionKey)]) == 0 {
		delete(r.resources, funcKey(functionKey))
	}
	log.GetLogger().Debugf("delete resource usage finished, functionKey : %s, instanceID: %s, len map is: %d, "+
		"func pod list len is %d", functionKey, instanceID, len(r.resources), len(r.resources[funcKey(functionKey)]))
}

// RemoveByFuncKey store resource usage data
func (r *NodeResourceUsage) RemoveByFuncKey(functionKey string) {
	r.resourceMutex.Lock()
	defer r.resourceMutex.Unlock()

	log.GetLogger().Debugf("remove resource usage by function key, functionKey: %s", functionKey)

	_, resourceOk := r.resources[funcKey(functionKey)]
	// if this function usage data is empty in this node
	if !resourceOk {
		log.GetLogger().Warnf("remove by function key failed because funcKey not exist: %s", functionKey)
		return
	}

	delete(r.resources, funcKey(functionKey))
	log.GetLogger().Debugf("delete resource usage by func key finished, functionKey: %s, len map is: %d, "+
		"func pod list len is %d", len(r.resources), len(r.resources[funcKey(functionKey)]))
}

// WithTickerTime -
func (r *NodeResourceUsage) WithTickerTime(tickerTime int64) *NodeResourceUsage {
	if tickerTime > 0 {
		r.tickerTime = tickerTime
	}
	return r
}

// WithDataLen -
func (r *NodeResourceUsage) WithDataLen(dataLen int64) *NodeResourceUsage {
	if dataLen > 0 {
		r.dataLen = dataLen
	}
	return r
}

// WithStopCh -
func (r *NodeResourceUsage) WithStopCh(stopCh <-chan struct{}) *NodeResourceUsage {
	if stopCh != nil {
		r.stopCh = stopCh
	}
	return r
}

// WithMetricsClient -
func (r *NodeResourceUsage) WithMetricsClient(client *redisclient.Client) *NodeResourceUsage {
	if client != nil {
		r.metricsClient = client
	}
	return r
}

// RegisterReportCallBack -
func (r *NodeResourceUsage) RegisterReportCallBack(callback func(c *redisclient.Client,
	resourceBytes []byte)) *NodeResourceUsage {
	if callback != nil {
		r.uploadCallBack = callback
	}
	return r
}

// StartReportResourceUsageToRedis start ticker of report resource usage
func (r *NodeResourceUsage) StartReportResourceUsageToRedis() {
	go r.storeResourceUsageToRedis()
}

func (r *NodeResourceUsage) getResource() map[funcKey]map[podName]ResourceList {
	return r.resources
}

func (r *NodeResourceUsage) storeResourceUsageToRedis() {
	if r.tickerTime <= 0 {
		log.GetLogger().Warnf("store report resource usage disabled, tickerTime: %d", r.tickerTime)
		return
	}

	if r.stopCh == nil {
		log.GetLogger().Errorf("store report resource usage not start because stopCh is nil")
		return
	}
	log.GetLogger().Infof("start to store report resource with tickerTime %s", r.tickerTime)
	ticker := time.NewTicker(time.Duration(r.tickerTime) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.resourceMutex.RLock()
			if len(r.resources) == 0 {
				log.GetLogger().Debugf("skip this store report resource usage because no resources")
				r.resourceMutex.RUnlock()
				continue
			}

			uploadResourceData := UploadResourceData{
				Resource:   r.getResource(),
				UploadTime: time.Now().Unix(),
			}
			resourceBytes, err := json.Marshal(uploadResourceData)
			if err != nil {
				log.GetLogger().Warnf("json marshal upload resource error when save to redis:%s", err.Error())
			}
			r.resourceMutex.RUnlock()
			r.uploadCallBack(r.metricsClient, resourceBytes)
		case <-r.stopCh:
			log.GetLogger().Infof("store report resource usage stopped")
			return
		}
	}
}
