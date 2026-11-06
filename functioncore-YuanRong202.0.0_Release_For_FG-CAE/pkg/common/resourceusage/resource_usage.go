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
	CPUUsage    float64 `json:"cpuUsage"`
	MemoryUsage float64 `json:"memoryUsage"`
	CPULimit    int64   `json:"cpuLimit"`
	MemoryLimit int64   `json:"memoryLimit"`
	ReportTime  int64   `json:"time"`
}

// ResourceList -
type ResourceList []Resource

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
