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

// Package options define tcpbustool
package tcpbustool

import (
	"functioncore/pkg/functionbus/bus/pb"
	"testing"
	"time"
)

func TestToPb(t *testing.T) {
	customReportMetrics := make(CustomReportMetrics, 1)
	metrics := CustomReportMetric{
		Val:              "test",
		CustomReportTime: time.Now().Unix(),
	}
	customReportMetrics = append(customReportMetrics, metrics)
	customReportMetrics.ToPb()
	if customReportMetrics.ToPb() == nil {
		t.Error("TestToPb fail")
	}
}

func TestParseFromPb(t *testing.T) {
	customReportMetrics := make(CustomReportMetrics, 1)
	metrics := CustomReportMetric{
		Val:              "test",
		CustomReportTime: time.Now().Unix(),
	}
	customReportMetrics = append(customReportMetrics, metrics)
	customReportMetricsPb := make([]*pb.CustomReportMetric, 0)
	for _, customReportMetric := range customReportMetrics {
		customReportMetricsPb = append(customReportMetricsPb, &pb.CustomReportMetric{
			Val:              customReportMetric.Val,
			CustomReportTime: customReportMetric.CustomReportTime,
		})
	}
	customReportMetrics.ParseFromPb(customReportMetricsPb)
	if customReportMetrics == nil {
		t.Error("TestParseFromPb fail")
	}
}
