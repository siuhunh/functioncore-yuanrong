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

package scaler

import (
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/functiontypes"
)

const (
	// CPU resource name for cpu
	CPU = "cpu"
	// MEM resource name for mem
	MEM = "mem"
	// FileSystemReadRate file system read rate
	FileSystemReadRate = "fileSystemReadRate"
	// FileSystemWriteRate file system write rate
	FileSystemWriteRate = "fileSystemWriteRate"
	// ReceiveBytesPerSecond net i
	ReceiveBytesPerSecond = "receiveBytesPerSecond"
	// TransmitBytesPerSecond net o
	TransmitBytesPerSecond = "transmitBytesPerSecond"

	// MetricTypeSystemString system
	MetricTypeSystemString = "system"
	// MetricTypeCustomString custom
	MetricTypeCustomString = "custom"
)

const (
	// MetricTypeSystem -
	MetricTypeSystem = iota
	// MetricTypeCustom -
	MetricTypeCustom
)

// ResourceConfig Type definition for resources which are used for scaling.
type ResourceConfig struct {
	resourceName                 string
	targetResourceUtilPercentage int64
	scaleUpTolerance             float64
	scaleDownTolerance           float64
	metricType                   int
}

func (resCfg *ResourceConfig) getResourceName() string {
	return resCfg.resourceName
}

func (resCfg *ResourceConfig) getMetricType() string {
	if resCfg.metricType == MetricTypeSystem {
		return MetricTypeSystemString
	}
	return MetricTypeCustomString
}

func (resCfg *ResourceConfig) isSystemMetric() bool {
	return resCfg.metricType == MetricTypeSystem
}

func (resCfg *ResourceConfig) isCPUOrMem() bool {
	return resCfg.metricType == MetricTypeSystem && (resCfg.resourceName == CPU || resCfg.resourceName == MEM)
}

func (resCfg *ResourceConfig) isFileSystemMetric() bool {
	return resCfg.metricType == MetricTypeSystem &&
		(resCfg.resourceName == FileSystemReadRate || resCfg.resourceName == FileSystemWriteRate)
}

func (resCfg *ResourceConfig) isNetMetric() bool {
	return resCfg.metricType == MetricTypeSystem &&
		(resCfg.resourceName == ReceiveBytesPerSecond || resCfg.resourceName == TransmitBytesPerSecond)
}

func (resCfg *ResourceConfig) getTargetResourceUtilPercentage() int64 {
	return resCfg.targetResourceUtilPercentage
}

func (resCfg *ResourceConfig) getScaleUpTolerance() float64 {
	return resCfg.scaleUpTolerance
}

func (resCfg *ResourceConfig) getScaleDownTolerance() float64 {
	return resCfg.scaleDownTolerance
}

func (resCfg *ResourceConfig) getResourceStatus(res *resourceusage.Resource) (float64, int64) {
	switch resCfg.resourceName {
	case CPU:
		return res.CPUUsage, res.CPULimit
	case MEM:
		return res.MemoryUsage, res.MemoryLimit
	default:
		//default branch
		return 0, 0
	}
}

func (resCfg *ResourceConfig) getExtendSystemMetrics(res *resourceusage.Resource) float64 {
	if resCfg.metricType != MetricTypeSystem {
		return 0
	}
	switch resCfg.resourceName {
	case FileSystemReadRate:
		return res.FileSystemReadBytesPerSecond
	case FileSystemWriteRate:
		return res.FileSystemWriteBytesPerSecond
	case ReceiveBytesPerSecond:
		return res.ReceiveBytesPerSecond
	case TransmitBytesPerSecond:
		return res.TransmitBytesPerSecond
	default:
		//default branch
		return 0
	}

	return 0
}

func (resCfg *ResourceConfig) getExtendSystemCaeMetrics(res *caemetrics.Metrics) float64 {
	if resCfg.metricType != MetricTypeSystem {
		return 0
	}
	switch resCfg.resourceName {
	case FileSystemReadRate:
		return res.FileSystemReadRate
	case FileSystemWriteRate:
		return res.FileSystemWriteRate
	case TransmitBytesPerSecond:
		return res.TransmitBytesPerSecond
	case ReceiveBytesPerSecond:
		return res.ReceiveBytesPerSecond
	default:
		//default branch
		return 0
	}

	return 0
}

func (resCfg *ResourceConfig) getUsedLimitRate() float64 {
	switch resCfg.resourceName {
	case CPU:
		return usedLimitRate
	case MEM:
		return 1
	default:
		return 1
	}

}

func createMEMResourceConfig(config config.MetricConfig) ResourceConfig {
	return ResourceConfig{
		resourceName:                 MEM,
		targetResourceUtilPercentage: config.TargetMemUtilPercentage,
		scaleUpTolerance:             config.ScaleUpTolerance,
		scaleDownTolerance:           config.ScaleDownTolerance,
		metricType:                   MetricTypeSystem,
	}
}

func createCPUResourceConfig(config config.MetricConfig) ResourceConfig {
	return ResourceConfig{
		resourceName:                 CPU,
		targetResourceUtilPercentage: config.TargetCPUUtilPercentage,
		scaleUpTolerance:             config.ScaleUpTolerance,
		scaleDownTolerance:           config.ScaleDownTolerance,
		metricType:                   MetricTypeSystem,
	}
}

func createExtendStrategiesResourceConfig(extendScaleStrategies []functiontypes.ExtendScaleStrategy,
	config config.MetricConfig) []ResourceConfig {
	resourceConfig := make([]ResourceConfig, 0)

	for _, item := range extendScaleStrategies {

		metricType := MetricTypeCustom
		if item.MetricType == MetricTypeSystemString {
			metricType = MetricTypeSystem
		}

		if needIgnoreMetric(item.MetricName, metricType) {
			continue
		}

		resourceConfig = append(resourceConfig, ResourceConfig{
			resourceName:                 item.MetricName,
			targetResourceUtilPercentage: item.TargetVal,
			scaleUpTolerance:             config.ScaleUpTolerance,
			scaleDownTolerance:           config.ScaleDownTolerance,
			metricType:                   metricType,
		})
	}
	return resourceConfig
}

func needIgnoreMetric(metricName string, metricType int) bool {
	// all user config not need ignore
	if metricType == MetricTypeCustom {
		// TODO: need changed to false when support custom
		return true
	}

	// if not exist in extendSystemMetrics, ignore it
	extendSystemMetrics := []string{FileSystemReadRate, FileSystemWriteRate, ReceiveBytesPerSecond, TransmitBytesPerSecond}

	for _, extendSystemMetric := range extendSystemMetrics {
		if extendSystemMetric == metricName {
			return false
		}
	}

	return true
}
