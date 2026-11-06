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

// Package caemetrics deal the cae metrics in common
package caemetrics

// List cae metrics
type List []Metrics

// Metrics the metric for cae
type Metrics struct {
	CPUUsageRate              float64           `json:"cpuUsageRate"`
	MemoryUsageRate           float64           `json:"memoryUsageRate"`
	ReceiveBytesPerSecond     float64           `json:"receiveBytesPerSecond"`
	ReceivePackagesPerSecond  float64           `json:"receivePackagesPerSecond"`
	TransmitBytesPerSecond    float64           `json:"transmitBytesPerSecond"`
	TransmitPackagesPerSecond float64           `json:"transmitPackagesPerSecond"`
	FileSystemReadRate        float64           `json:"fileSystemReadRate"`
	FileSystemWriteRate       float64           `json:"fileSystemWriteRate"`
	ReportTime                int64             `json:"reportTime"`
	Properties                map[string]string `json:"properties"`
	FileSystemUsageRate       float64           `json:"fileSystemUsageRate"`
	FileSystemEmptySpace      float64           `json:"fileSystemEmptySpace"`
	FileSystemCapacity        float64           `json:"fileSystemCapacity"`
	MemoryUsage               float64           `json:"memoryUsage"`
	MemoryCapacity            float64           `json:"memoryCapacity"`
	CPUUsage                  float64           `json:"cpuUsage"`
	CPUCapacity               float64           `json:"cpuCapacity"`
	Status                    int64             `json:"status"`
}

// GetLatestData -
func (rl *List) GetLatestData() *Metrics {
	if len(*rl) == 0 {
		return nil
	}
	last := []Metrics(*rl)[len(*rl)-1]
	return &last
}
