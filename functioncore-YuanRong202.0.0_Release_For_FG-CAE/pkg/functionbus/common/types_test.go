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

// Package common
package common

import (
	"gotest.tools/assert"
	"testing"

	"functioncore/pkg/functionbus/bus/tcpbustool"
)

func TestInitCaeMetrics(t *testing.T) {
	functionKey := "test"
	podName := "podA"
	customMetrics := tcpbustool.CustomReportMetrics{}
	avgCaeMetrics := AvgCaeMetrics{
		ReceiveBytesPerSecond:         100,
		ReceivePackagePerSecond:       200,
		TransmitBytesPerSecond:        300,
		TransmitPackagePerSecond:      400,
		FileSystemReadBytesPerSecond:  500,
		FileSystemWriteBytesPerSecond: 600,
	}

	properties := map[string]string{
		"a": "a",
		"b": "b",
	}

	cpuUsed := 0.02
	usedLimitRate := float64(1000)
	cpuLimit := 300
	memoryUsed := float64(100)
	memoryLimit := 128

	storageUsed := float64(100)
	fileSystemCapacity := float64(512)

	caeMetrics := InitCaeMetrics().WithFunctionKey(functionKey).WithPodName(podName).WithCustomMetrics(customMetrics).
		WithAvgCaeMetrics(avgCaeMetrics).WithProperties(properties).WithCPUMetrics(cpuUsed*usedLimitRate, float64(cpuLimit)).
		WithMemoryMetrics(memoryUsed, float64(memoryLimit)).WithFileSystemData(storageUsed, fileSystemCapacity)

	caeMetricsWant := &CaeMetrics{
		FunctionKey:                   functionKey,
		PodName:                       podName,
		CPUUsageRate:                  (cpuUsed * usedLimitRate * percent) / float64(cpuLimit),
		MemoryUsageRate:               (memoryUsed * percent) / float64(memoryLimit),
		ReceiveBytesPerSecond:         avgCaeMetrics.ReceiveBytesPerSecond,
		ReceivePackagePerSecond:       avgCaeMetrics.ReceivePackagePerSecond,
		TransmitBytesPerSecond:        avgCaeMetrics.TransmitBytesPerSecond,
		TransmitPackagePerSecond:      avgCaeMetrics.TransmitPackagePerSecond,
		FileSystemReadBytesPerSecond:  avgCaeMetrics.FileSystemReadBytesPerSecond,
		FileSystemWriteBytesPerSecond: avgCaeMetrics.FileSystemWriteBytesPerSecond,
		CustomReportMetrics:           customMetrics,
		ReportTime:                    caeMetrics.ReportTime,
		Properties:                    properties,
		FileSystemUsageRate:           (storageUsed * percent) / fileSystemCapacity,
		FileSystemEmptySpace:          fileSystemCapacity - storageUsed,
		FileSystemCapacity:            fileSystemCapacity,
		MemoryUsage:                   memoryUsed,
		MemoryCapacity:                float64(memoryLimit),
		CPUUsage:                      cpuUsed * usedLimitRate,
		CPUCapacity:                   float64(cpuLimit),
	}

	assert.DeepEqual(t, caeMetrics, caeMetricsWant)
}
