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

package scaler

import (
	"testing"

	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/resourceusage"
)

func TestResourceConfig_getMetricType(t *testing.T) {
	resCfg := ResourceConfig{
		metricType: MetricTypeSystem,
	}
	metricType := resCfg.getMetricType()
	if metricType != MetricTypeSystemString {
		t.Errorf("failed to test getMetricType")
	}

	resCfg.metricType = MetricTypeCustom
	metricType = resCfg.getMetricType()
	if metricType != MetricTypeCustomString {
		t.Errorf("failed to test getMetricType")
	}

	if isSystem := resCfg.isSystemMetric(); isSystem != false {
		t.Errorf("failed to test isSystemMetric")
	}

	resCfg.metricType = MetricTypeSystem
	resCfg.resourceName = FileSystemReadRate
	metrics := resCfg.getExtendSystemMetrics(&resourceusage.Resource{FileSystemReadBytesPerSecond: 100})
	if metrics != 100 {
		t.Errorf("failed to test getExtendSystemMetrics")
	}

	resCfg.resourceName = FileSystemWriteRate
	metrics = resCfg.getExtendSystemMetrics(&resourceusage.Resource{FileSystemWriteBytesPerSecond: 100})
	if metrics != 100 {
		t.Errorf("failed to test getExtendSystemMetrics")
	}

	resCfg.resourceName = ReceiveBytesPerSecond
	metrics = resCfg.getExtendSystemMetrics(&resourceusage.Resource{ReceiveBytesPerSecond: 100})
	if metrics != 100 {
		t.Errorf("failed to test getExtendSystemMetrics")
	}

	resCfg.resourceName = TransmitBytesPerSecond
	metrics = resCfg.getExtendSystemMetrics(&resourceusage.Resource{TransmitBytesPerSecond: 100})
	if metrics != 100 {
		t.Errorf("failed to test getExtendSystemMetrics")
	}

	resCfg.resourceName = "aaaa"
	metrics = resCfg.getExtendSystemMetrics(&resourceusage.Resource{})
	if metrics != 0 {
		t.Errorf("failed to test getExtendSystemMetrics")
	}

	if needIgnore := needIgnoreMetric("metricName", MetricTypeSystem); needIgnore == false {
		t.Errorf("failed to test needIgnoreMetric")
	}

	if needIgnore := needIgnoreMetric(FileSystemReadRate, MetricTypeSystem); needIgnore == true {
		t.Errorf("failed to test needIgnoreMetric")
	}

}

func TestResourceConfig_isFileSystemMetric(t *testing.T) {
	resCfg := ResourceConfig{
		metricType:   MetricTypeSystem,
		resourceName: FileSystemReadRate,
	}
	isFileSystemMetric := resCfg.isFileSystemMetric()
	if !isFileSystemMetric {
		t.Errorf("failed to check isFileSystemMetric")
	}

	metric := resCfg.getExtendSystemCaeMetrics(&caemetrics.Metrics{
		FileSystemReadRate: 100,
	})

	if metric == 0 {
		t.Errorf("failed to test getExtendSystemCaeMetrics")
	}

	resCfg = ResourceConfig{
		metricType:   MetricTypeSystem,
		resourceName: TransmitBytesPerSecond,
	}
	isNetMetric := resCfg.isNetMetric()
	if !isNetMetric {
		t.Errorf("failed to check isFileSystemMetric")
	}

	metric = resCfg.getExtendSystemCaeMetrics(&caemetrics.Metrics{
		TransmitBytesPerSecond: 100,
	})

	if metric == 0 {
		t.Errorf("failed to test getExtendSystemCaeMetrics")
	}

}
