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

// Package tcpbustool define bus parse tool
package tcpbustool

import (
	"functioncore/pkg/functionbus/bus/pb"
)

// CustomReportMetric -
type CustomReportMetric struct {
	Val              string
	CustomReportTime int64
}

// CustomReportMetrics -
type CustomReportMetrics []CustomReportMetric

// ToPb -
func (c *CustomReportMetrics) ToPb() []*pb.CustomReportMetric {
	customReportMetricsPb := make([]*pb.CustomReportMetric, 0, len(*c))
	for _, customReportMetric := range *c {
		customReportMetricsPb = append(customReportMetricsPb, &pb.CustomReportMetric{
			Val:              customReportMetric.Val,
			CustomReportTime: customReportMetric.CustomReportTime,
		})
	}

	return customReportMetricsPb
}

// ParseFromPb parse data from pb
func (c *CustomReportMetrics) ParseFromPb(pbCustomReportMetrics []*pb.CustomReportMetric) {
	for _, pbCustomReportMetric := range pbCustomReportMetrics {
		*c = append(*c, CustomReportMetric{
			Val:              pbCustomReportMetric.Val,
			CustomReportTime: pbCustomReportMetric.CustomReportTime,
		})
	}
}
