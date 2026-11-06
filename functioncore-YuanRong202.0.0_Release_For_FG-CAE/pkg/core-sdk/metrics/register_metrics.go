/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

// Package metrics connect to prometheus client
package metrics

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

// MetricServer push client object define
type MetricServer struct {
	gaugeVec *prometheus.GaugeVec
}

// RegisterServer register metrics server
func RegisterServer() {
	gaugeVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "state_load_value",
			Help: "The number of state calls of the current node",
		},
		[]string{"node_name", "data_kind"},
	)
	prometheus.MustRegister(gaugeVec)
}

// SetLoad set current load of node
func (c *MetricServer) SetLoad(source string, value int32) {
	c.gaugeVec.WithLabelValues(os.Getenv("NODE_ID"), source).Set(float64(value))
}
