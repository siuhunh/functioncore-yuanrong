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

// Package clustermgr -
package clustermgr

import (
	"testing"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
)

func Test_getLoadBalanceByType(t *testing.T) {
	type args struct {
		clusterMetricCache       map[string]config.ClusterUsage
		availableClusters        map[string]Cluster
		compare                  string
		funcInstanceClusterCount map[string]int64
	}
	testClusterMetricCache := map[string]config.ClusterUsage{
		"cluster001": {
			CPUTotal: 100,
			CPUUsed:  50,
		},
		"cluster002": {
			CPUTotal: 100,
			CPUUsed:  60,
		},
		"cluster003": {
			CPUTotal: 100,
			CPUUsed:  20,
		},
		"cluster004": {
			CPUTotal: 100,
			CPUUsed:  80,
		},
		"cluster005": {
			CPUTotal: 0,
			CPUUsed:  0,
		},
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"get load balance cluster for scale up",
			args{
				clusterMetricCache: testClusterMetricCache,
				availableClusters: map[string]Cluster{
					"cluster001": {
						ActiveNodes: NodeStatus{
							IP: "10.2.1.1",
						},
						State: utils.Healthy,
					},
					"cluster002": {
						ActiveNodes: NodeStatus{
							IP: "10.2.1.2",
						},
						State: utils.Healthy,
					},
				},
				compare:                  utils.LoadBalanceLowest,
				funcInstanceClusterCount: map[string]int64{"cluster001": 1, "cluster002": 2},
			},
			"10.2.1.1",
		},
		{
			"get load balance cluster for scale down",
			args{
				clusterMetricCache: testClusterMetricCache,
				availableClusters: map[string]Cluster{
					"cluster001": {
						ActiveNodes: NodeStatus{
							IP: "10.2.1.1",
						},
						State: utils.Healthy,
					},
					"cluster002": {
						ActiveNodes: NodeStatus{
							IP: "10.2.1.2",
						},
						State: utils.Healthy,
					},
				},
				compare:                  utils.LoadBalanceHighest,
				funcInstanceClusterCount: map[string]int64{"cluster001": 1, "cluster002": 2},
			},
			"10.2.1.2",
		},
		{
			"get load balance cluster for invalid data",
			args{
				clusterMetricCache: testClusterMetricCache,
				availableClusters: map[string]Cluster{
					"cluster001": {
						ActiveNodes: NodeStatus{
							IP: "10.2.1.1",
						},
						State: utils.Healthy,
					},
					"cluster002": {
						ActiveNodes: NodeStatus{
							IP: "10.2.1.2",
						},
						State: utils.Healthy,
					},
				},
				compare:                  utils.LoadBalanceHighest,
				funcInstanceClusterCount: map[string]int64{"cluster001": 1, "cluster002": 2},
			},
			"10.2.1.2",
		},
		{
			"get load balance cluster for no availableClusters",
			args{
				clusterMetricCache:       testClusterMetricCache,
				availableClusters:        map[string]Cluster{},
				compare:                  utils.LoadBalanceHighest,
				funcInstanceClusterCount: map[string]int64{"cluster001": 1, "cluster002": 2},
			},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getLoadBalanceByType(tt.args.clusterMetricCache, tt.args.availableClusters, tt.args.compare,
				tt.args.funcInstanceClusterCount); got.ActiveNodes.IP != tt.want {
				t.Errorf("getLoadBalanceByType() = %v, want %v", got, tt.want)
			}
		})
	}
}
