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

// Package resourceusage deal the resource usage in common
package resourceusage

import (
	"encoding/json"
	"time"

	"functioncore/pkg/common/logger/log"
)

// FuncKey -
type FuncKey string

// PodName -
type PodName string

// NodeName -
type NodeName string

// FuncSvcID -
type FuncSvcID string

// ClusterID -
type ClusterID string

// Resource resource
type Resource struct {
	CPUUsage                      float64             `json:"cpuUsage"`
	MemoryUsage                   float64             `json:"memoryUsage"`
	CPULimit                      int64               `json:"cpuLimit"`
	MemoryLimit                   int64               `json:"memoryLimit"`
	ReportTime                    int64               `json:"time"`
	ReceiveBytesPerSecond         float64             `json:"receiveBPS"`
	ReceivePackagePerSecond       float64             `json:"receivePPS"`
	TransmitBytesPerSecond        float64             `json:"transmitBPS"`
	TransmitPackagePerSecond      float64             `json:"transmitPPS"`
	FileSystemReadBytesPerSecond  float64             `json:"fileSystemReadBPS"`
	FileSystemWriteBytesPerSecond float64             `json:"fileSystemWriteBPS"`
	CustomReportMetrics           CustomReportMetrics `json:"customReportMetrics"`
}

// ResourceList -
type ResourceList []Resource

// Convert2PredictResourceList -
func (rl *ResourceList) Convert2PredictResourceList() PredictResourceList {
	predictResourceList := make(PredictResourceList, len(*rl))

	for index, resource := range *rl {
		inputPredictResource := PredictResource{
			CPUUtil:    resource.CPUUsage,
			MemoryUtil: resource.MemoryUsage,
			Timestamp:  resource.ReportTime,
		}
		predictResourceList[index] = inputPredictResource
	}
	return predictResourceList
}

// GetLatestDataTime -
func (rl *ResourceList) GetLatestDataTime() int64 {
	if len(*rl) == 0 {
		return 0
	}
	last := []Resource(*rl)[len(*rl)-1]
	return last.ReportTime
}

// GetLatestData -
func (rl *ResourceList) GetLatestData() *Resource {
	if len(*rl) == 0 {
		return nil
	}
	last := []Resource(*rl)[len(*rl)-1]
	return &last
}

// GetAvgDataInTime -
func (rl *ResourceList) GetAvgDataInTime(now, noUseTime int64) *Resource {
	if len(*rl) == 0 {
		return nil
	}

	cpuUsageTotal, cpuLimit, memoryUsageTotal, memoryLimit, reportTime, dataNum := float64(0),
		int64(0), float64(0), int64(0), int64(0), float64(0)

	for _, resourceItem := range *rl {
		if now-resourceItem.ReportTime <= noUseTime {
			dataNum++
			cpuUsageTotal += resourceItem.CPUUsage
			cpuLimit = resourceItem.CPULimit
			memoryUsageTotal += resourceItem.MemoryUsage
			memoryLimit = resourceItem.MemoryLimit
			// use last one report time
			reportTime = resourceItem.ReportTime
		}
	}

	if dataNum <= 0 {
		return nil
	}

	resource := &Resource{
		CPUUsage:    cpuUsageTotal / dataNum,
		CPULimit:    cpuLimit,
		MemoryUsage: memoryUsageTotal / dataNum,
		MemoryLimit: memoryLimit,
		ReportTime:  reportTime,
	}

	log.GetLogger().Debugf("GetAvgDataInTime: rl is %+v, avg resource is %+v", rl, resource)
	if resource.CPUUsage == 0 || resource.CPULimit == 0 || resource.MemoryUsage == 0 || resource.MemoryLimit == 0 {
		return nil
	}

	return resource
}

// GetAvgExtendSystemDataInTime get avg extend system data in time
func (rl *ResourceList) GetAvgExtendSystemDataInTime(now, noUseTime int64) *Resource {
	if len(*rl) == 0 {
		return nil
	}

	receiveBytesPerSecond, transmitBytesPerSecond, fileSystemReadBytesPerSecond, fileSystemWriteBytesPerSecond,
		reportTime, dataNum := float64(0), float64(0), float64(0), float64(0), int64(0), float64(0)

	for _, resourceItem := range *rl {
		if now-resourceItem.ReportTime <= noUseTime {
			dataNum++

			receiveBytesPerSecond += resourceItem.ReceiveBytesPerSecond
			transmitBytesPerSecond += resourceItem.TransmitBytesPerSecond
			fileSystemReadBytesPerSecond += resourceItem.FileSystemReadBytesPerSecond
			fileSystemWriteBytesPerSecond += resourceItem.FileSystemWriteBytesPerSecond

			reportTime = resourceItem.ReportTime
		}
	}

	if dataNum <= 0 {
		return nil
	}

	resource := &Resource{
		ReceiveBytesPerSecond:         receiveBytesPerSecond / dataNum,
		TransmitBytesPerSecond:        transmitBytesPerSecond / dataNum,
		FileSystemWriteBytesPerSecond: fileSystemWriteBytesPerSecond / dataNum,
		FileSystemReadBytesPerSecond:  fileSystemReadBytesPerSecond / dataNum,
		ReportTime:                    reportTime,
	}

	return resource
}

// CaeMetricResource the metric resource for cae
type CaeMetricResource struct {
	CPUUsageRate              float64 `json:"cpuUsageRate"`
	MemoryUsageRate           float64 `json:"memoryUsageRate"`
	ReceiveBytesPerSecond     uint64  `json:"receiveBytesPerSecond"`
	TransmitBytesPerSecond    uint64  `json:"transmitBytesPerSecond"`
	ReceivePackagesPerSecond  uint64  `json:"receivePackagesPerSecond"`
	TransmitPackagesPerSecond uint64  `json:"transmitPackagesPerSecond"`
	FileSystemReadRate        uint64  `json:"file_system_read_rate "`
	FileSystemWriteRate       uint64  `json:"file_system_write_rate"`
	ReportTime                uint64  `json:"time"`
}

// PredictResource -
type PredictResource struct {
	CPUUtil    float64 `json:"CpuUtil"`
	MemoryUtil float64 `json:"MemoryUtil"`
	Timestamp  int64   `json:"timestamp"`
}

// PredictResourceList -
type PredictResourceList []PredictResource

// AggregateSumPredictResource -
func (l *PredictResourceList) AggregateSumPredictResource() PredictResource {
	predictResource := PredictResource{}
	for _, resource := range *l {
		predictResource.CPUUtil += resource.CPUUtil
		predictResource.MemoryUtil += resource.MemoryUtil
		predictResource.Timestamp = resource.Timestamp
	}
	return predictResource
}

// GetAvgData -
func (l *PredictResourceList) GetAvgData() PredictResource {
	cpuUsageTotal, memoryUsageTotal, timeStamp, dataNum := float64(0), float64(0), int64(0), float64(0)

	for _, resource := range *l {
		dataNum++
		cpuUsageTotal += resource.CPUUtil
		memoryUsageTotal += resource.MemoryUtil
		timeStamp = resource.Timestamp
	}
	if dataNum == 0 {
		return PredictResource{}
	}
	return PredictResource{
		CPUUtil:    cpuUsageTotal / dataNum,
		MemoryUtil: memoryUsageTotal / dataNum,
		Timestamp:  time.Unix(timeStamp, 0).Truncate(time.Minute).Unix(),
	}
}

// AggregateMinutePredictResource -
func (l *PredictResourceList) AggregateMinutePredictResource() PredictResourceList {
	aggregated := make(map[string]PredictResourceList)
	newPredictResourceList := PredictResourceList{}
	for _, resource := range *l {
		t := time.Unix(resource.Timestamp, 0)
		minute := t.Format("2006-01-02 15:04:00")
		aggregated[minute] = append(aggregated[minute], resource)
	}

	for _, resources := range aggregated {
		predictResource := resources.GetAvgData()
		newPredictResourceList = append(newPredictResourceList, predictResource)
	}
	return newPredictResourceList
}

// PredictResourceListMap -
type PredictResourceListMap map[FuncSvcID]PredictResourceList

// Convert2List4Redis convert to list []interface{}   ["filed1", "value1", "filed2", "value2"] every minute
func (lm *PredictResourceListMap) Convert2List4Redis() []interface{} {
	list := make([]interface{}, 0, len(*lm)*2)
	for funcSvcID, resourceList := range *lm {
		marshal, err := json.Marshal(resourceList.AggregateMinutePredictResource())
		if err != nil {
			log.GetLogger().Errorf("failed to marshal resourceList, err:%s", err.Error())
			continue
		}
		list = append(list, string(funcSvcID), string(marshal))
	}
	return list
}

// BridgeFuncMetrics -
type BridgeFuncMetrics struct {
	TimeStamp           int64
	PodName             string
	MemoryUsage         float64
	MemoryCapacity      float64
	CPUUsage            float64
	CPUCapacity         float64
	TotalReqCount       int64
	TotalFailedCount    int64
	AverageTimeCost     float64
	TenantID            string
	FuncName            string
	Package             string
	Version             string
	ClusterID           string
	BridgeType          string
	CustomReportMetrics CustomReportMetrics
	ForBridgeManager    bool
}

// BridgeResourceList -
type BridgeResourceList []BridgeFuncMetrics

// CustomReportMetric -
type CustomReportMetric struct {
	Val              string
	CustomReportTime int64
}

// CustomReportMetrics -
type CustomReportMetrics []CustomReportMetric
