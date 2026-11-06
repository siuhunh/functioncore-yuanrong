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

func TestFunctionScaler_generateActions(t *testing.T) {
	metricConfig := config.MetricConfig{
		TargetCPUUtilPercentage: 30,
		SyncPeriod:              40,
	}
	strategy := []string{"cpu"}
	testFunctionSvcID := "test"
	totalExpected := 8
	curInsCount := 5
	actions := make(map[string]Action)
	wantActions := map[string]Action{
		testFunctionSvcID: {
			Type:          ActionPull,
			NeedPull:      int64(totalExpected - curInsCount),
			TotalExpected: int64(totalExpected),
		},
	}
	reduceActions := map[string]Action{
		testFunctionSvcID: {
			Type:          ActionReduce,
			NeedReduce:    5,
			TotalExpected: int64(totalExpected),
		},
	}
	type fields struct {
		functionSvcID      string
		metricConfig       config.MetricConfig
		resiliencyStrategy Strategy
	}
	type args struct {
		totalExpected int64
		curInsCount   int64
		actions       map[string]Action
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]Action
	}{
		{
			"case test generate pull actions",
			fields{
				functionSvcID:      testFunctionSvcID,
				metricConfig:       metricConfig,
				resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
			},
			args{
				8,
				5,
				actions,
			},
			wantActions,
		},
		{
			"case test generate reduce actions",
			fields{
				functionSvcID:      testFunctionSvcID,
				metricConfig:       metricConfig,
				resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
			},
			args{
				8,
				13,
				actions,
			},
			reduceActions,
		},
		{
			"case test generate empty actions",
			fields{
				functionSvcID:      testFunctionSvcID,
				metricConfig:       metricConfig,
				resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
			},
			args{
				8,
				13,
				nil,
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &FunctionScaler{
				functionSvcID:      tt.fields.functionSvcID,
				metricConfig:       tt.fields.metricConfig,
				resiliencyStrategy: tt.fields.resiliencyStrategy,
			}
			s.generateActions(tt.args.totalExpected, tt.args.curInsCount, tt.args.actions)
			if got := tt.args.actions; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateActions() = %v, want %v", got, tt.want)
			}
		})
	}
}
