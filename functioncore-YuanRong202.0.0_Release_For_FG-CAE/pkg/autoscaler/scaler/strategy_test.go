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

// Package scaler -
package scaler

import (
	"reflect"
	"testing"

	"functioncore/pkg/autoscaler/config"
)

func Test_createStrategy(t *testing.T) {
	type args struct {
		strategy      []string
		functionSvcID string
		config        config.MetricConfig
	}
	tests := []struct {
		name string
		args args
		want Strategy
	}{
		{
			"case test create cpu strategy",
			args{
				[]string{"cpu"},
				"testFunction",
				config.MetricConfig{},
			},
			&ResourceAvgStrategy{
				resources: []ResourceConfig{
					createCPUResourceConfig(config.MetricConfig{}),
				},
				functionSvcID: "testFunction",
			},
		},
		{
			"case test create mem strategy",
			args{
				[]string{"mem"},
				"testFunction",
				config.MetricConfig{},
			},
			&ResourceAvgStrategy{
				resources: []ResourceConfig{
					createMEMResourceConfig(config.MetricConfig{}),
				},
				functionSvcID: "testFunction",
			},
		},
		{
			"case test create mix strategy",
			args{
				[]string{"mem", "cpu"},
				"testFunction",
				config.MetricConfig{},
			},
			&ResourceAvgStrategy{
				resources: []ResourceConfig{
					createCPUResourceConfig(config.MetricConfig{}),
					createMEMResourceConfig(config.MetricConfig{}),
				},
				functionSvcID: "testFunction",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createStrategy(tt.args.strategy, tt.args.functionSvcID, tt.args.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createStrategy() = %v, want %v", got, tt.want)
			}
		})
	}
}
