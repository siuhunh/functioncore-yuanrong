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

// Package scalehandler -
package scalehandler

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/config"
	mockutils "functioncore/test/utils"
)

func TestCacheStrategy_calResourceExpectedScaleNums(t *testing.T) {
	type fields struct {
		funcSvcID          string
		curInsCount        int
		scaleRulesItems    map[string]int64
		scaleUpTolerance   float64
		scaleDownTolerance float64
		dataNum            int
		cpuUsageTotal      float64
		cpuLimit           float64
		memoryUsageTotal   float64
		memoryLimit        float64
		connectionTotal    int64
		connectionLimit    int64
	}
	type args struct {
		scaleRuleType string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
		wantNum int64
	}{
		{
			"test dataNum is zero",
			fields{
				dataNum:     0,
				curInsCount: 10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{},
			true,
			0,
		},
		{
			"test is cpu but limit zero",
			fields{
				dataNum:     10,
				curInsCount: 10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{
				scaleRuleType: ScaleRuleKeyOfCPU,
			},
			true,
			0,
		},
		{
			"test is cpu but limit not zero",
			fields{
				dataNum:       10,
				cpuLimit:      100,
				cpuUsageTotal: 1500,
				curInsCount:   10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{
				scaleRuleType: ScaleRuleKeyOfCPU,
			},
			false,
			24,
		},
		{
			"test is memory but limit zero",
			fields{
				dataNum:     10,
				curInsCount: 10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{
				scaleRuleType: ScaleRuleKeyOfMemory,
			},
			true,
			0,
		},
		{
			"test is memory but limit not zero",
			fields{
				dataNum:          10,
				memoryLimit:      100,
				memoryUsageTotal: 1500,
				curInsCount:      10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{
				scaleRuleType: ScaleRuleKeyOfMemory,
			},
			false,
			24,
		},
		{
			"test is connections but limit zero",
			fields{
				dataNum:     10,
				curInsCount: 10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{
				scaleRuleType: ScaleRuleKeyOfConnections,
			},
			true,
			0,
		},
		{
			"test is connections but limit not zero",
			fields{
				dataNum:         10,
				connectionLimit: 100,
				connectionTotal: 1500,
				curInsCount:     10,
				scaleRulesItems: map[string]int64{
					ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
					ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
					ScaleRuleKeyOfConnections: defaultMetricsWantValue,
				},
			},
			args{
				scaleRuleType: ScaleRuleKeyOfConnections,
			},
			false,
			231,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cacheStrategy := &CacheStrategy{
				funcSvcID:          tt.fields.funcSvcID,
				curInsCount:        tt.fields.curInsCount,
				scaleRulesItems:    tt.fields.scaleRulesItems,
				scaleUpTolerance:   tt.fields.scaleUpTolerance,
				scaleDownTolerance: tt.fields.scaleDownTolerance,
				dataNum:            tt.fields.dataNum,
				cpuUsageTotal:      tt.fields.cpuUsageTotal,
				cpuLimit:           tt.fields.cpuLimit,
				memoryUsageTotal:   tt.fields.memoryUsageTotal,
				memoryLimit:        tt.fields.memoryLimit,
				connectionTotal:    tt.fields.connectionTotal,
				connectionLimit:    tt.fields.connectionLimit,
			}
			expectNum, err := cacheStrategy.calResourceExpectedScaleNums(tt.args.scaleRuleType)
			if (err != nil) != tt.wantErr {
				t.Errorf("calResourceExpectedScaleNums() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if expectNum != tt.wantNum {
				t.Errorf("failed to test calResourceExpectedScaleNums, wantNum: %d, expectNum: %d", tt.wantNum, expectNum)
			}
		})
	}
}

func TestCacheStrategy_getExpectedInsCount(t *testing.T) {
	type fields struct {
		funcSvcID          string
		curInsCount        int
		scaleRulesItems    map[string]int64
		scaleUpTolerance   float64
		scaleDownTolerance float64
		dataNum            int
		cpuUsageTotal      float64
		cpuLimit           float64
		memoryUsageTotal   float64
		memoryLimit        float64
		connectionTotal    int64
		connectionLimit    int64
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			"test no scaleRulesItems",
			fields{},
			false,
		},
		{
			"test with connections",
			fields{
				scaleRulesItems: map[string]int64{"cpu": 65, "memory": 65, "connections": 65},
			},
			false,
		},
		{
			"test without connections",
			fields{
				scaleRulesItems: map[string]int64{"cpu": 65, "memory": 65, "connections": 65},
			},
			false,
		},
		{
			"test connections limit not zero",
			fields{
				scaleRulesItems: map[string]int64{"cpu": 65, "memory": 65, "connections": 65},
				connectionLimit: 100,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cacheStrategy := &CacheStrategy{
				funcSvcID:          tt.fields.funcSvcID,
				curInsCount:        tt.fields.curInsCount,
				scaleRulesItems:    tt.fields.scaleRulesItems,
				scaleUpTolerance:   tt.fields.scaleUpTolerance,
				scaleDownTolerance: tt.fields.scaleDownTolerance,
				dataNum:            tt.fields.dataNum,
				cpuUsageTotal:      tt.fields.cpuUsageTotal,
				cpuLimit:           tt.fields.cpuLimit,
				memoryUsageTotal:   tt.fields.memoryUsageTotal,
				memoryLimit:        tt.fields.memoryLimit,
				connectionTotal:    tt.fields.connectionTotal,
				connectionLimit:    tt.fields.connectionLimit,
			}
			_, _, err := cacheStrategy.getExpectedInsCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("getExpectedInsCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

		})
	}
}

func TestCacheStrategy_pushData(t *testing.T) {
	type fields struct {
		funcSvcID          string
		curInsCount        int
		scaleRulesItems    map[string]int64
		scaleUpTolerance   float64
		scaleDownTolerance float64
		dataNum            int
		cpuUsageTotal      float64
		cpuLimit           float64
		memoryUsageTotal   float64
		memoryLimit        float64
		connectionTotal    int64
		connectionLimit    int64
	}
	type args struct {
		metrics resourceusage.BridgeFuncMetrics
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"push data",
			fields{
				cpuLimit:      100,
				cpuUsageTotal: 100,
			},
			args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cacheStrategy := &CacheStrategy{
				funcSvcID:          tt.fields.funcSvcID,
				curInsCount:        tt.fields.curInsCount,
				scaleRulesItems:    tt.fields.scaleRulesItems,
				scaleUpTolerance:   tt.fields.scaleUpTolerance,
				scaleDownTolerance: tt.fields.scaleDownTolerance,
				dataNum:            tt.fields.dataNum,
				cpuUsageTotal:      tt.fields.cpuUsageTotal,
				cpuLimit:           tt.fields.cpuLimit,
				memoryUsageTotal:   tt.fields.memoryUsageTotal,
				memoryLimit:        tt.fields.memoryLimit,
				connectionTotal:    tt.fields.connectionTotal,
				connectionLimit:    tt.fields.connectionLimit,
			}
			cacheStrategy.pushData(tt.args.metrics)
		})
	}
}

func TestResiliencyStrategyFactory_CreateResiliencyStrategy(t *testing.T) {
	type args struct {
		funcSvcID   string
		curInsCount int
		bridgeType  string
	}
	tests := []struct {
		name                 string
		args                 args
		wantscaleUpTolerance float64
		patchesFunc          mockutils.PatchesFunc
	}{
		{
			"test default",
			args{},
			0.1,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			"test use config",
			args{
				bridgeType: CacheBridgeType,
			},
			0.3,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
						return &config.FConfig{
							ScaleData: config.ScaleData{
								ScaleUpTolerance: 0.3,
							},
						}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		factory := ResiliencyStrategyFactory{}
		got := factory.CreateResiliencyStrategy(tt.args.funcSvcID, tt.args.curInsCount, tt.args.bridgeType)
		goStrategy, ok := got.(*CacheStrategy)
		if !ok || goStrategy.scaleUpTolerance != tt.wantscaleUpTolerance {
			t.Errorf("CreateResiliencyStrategy() = %v, want %v", got, tt.wantscaleUpTolerance)
		}
		patches.ResetAll()
	}
}

func Test_createAndMakeResiliencyStrategy(t *testing.T) {
	type args struct {
		funcSvcID         string
		curInsCount       int
		resourceDataByPod map[resourceusage.PodName]resourceusage.BridgeResourceList
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"test create and make ResiliencyStrategy",
			args{
				"funcSvcA",
				10,
				map[resourceusage.PodName]resourceusage.BridgeResourceList{
					"podA": []resourceusage.BridgeFuncMetrics{
						{
							TimeStamp: time.Now().Unix(),
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createAndMakeResiliencyStrategy(tt.args.funcSvcID, tt.args.curInsCount, tt.args.resourceDataByPod); got == nil {
				t.Errorf("createAndMakeResiliencyStrategy() = %v", got)
			}
		})
	}
}

func Test_getSpecificCacheStrategy(t *testing.T) {
	convey.Convey("Test getSpecificCacheStrategy", t, func() {
		cfg := &config.FConfig{
			ScaleData: config.ScaleData{
				ScaleUpTolerance:   0.3,
				ScaleDownTolerance: 0.1,
				ScaleRules: map[string]map[string]int64{
					"cache": {
						"cpu":         30,
						"memory":      40,
						"connections": 40,
					},
				},
			},
			BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
				BridgeFuncWhiteList: map[string]bridgetypes.SpecificBridgeScaleData{
					"funcSvc_all": {
						ScaleUpTolerance:   0.2,
						ScaleDownTolerance: 0.2,
						ScaleRules: map[string]map[string]int64{
							"cache": {
								"cpu":         33,
								"memory":      44,
								"connections": 55}},
					},
					"funcSvc_noRules": {
						ScaleUpTolerance:   0.22,
						ScaleDownTolerance: 0.23,
					},
					"funcSvc_onlyRules": {
						ScaleRules: map[string]map[string]int64{
							"cache": {
								"cpu":         23,
								"memory":      24,
								"connections": 25}},
					},
				},
			},
		}
		factory := ResiliencyStrategyFactory{}
		strategyAllGot := factory.getSpecificCacheStrategy(100, "funcSvc_all", cfg)
		strategyAll := strategyAllGot.(*CacheStrategy)
		convey.So(strategyAll.scaleUpTolerance, convey.ShouldEqual, 0.2)
		convey.So(strategyAll.scaleDownTolerance, convey.ShouldEqual, 0.2)
		convey.So(strategyAll.scaleRulesItems[ScaleRuleKeyOfCPU], convey.ShouldEqual, 33)
		convey.So(strategyAll.scaleRulesItems[ScaleRuleKeyOfMemory], convey.ShouldEqual, 44)
		convey.So(strategyAll.scaleRulesItems[ScaleRuleKeyOfConnections], convey.ShouldEqual, 55)

		strategyNoRulesGot := factory.getSpecificCacheStrategy(100, "funcSvc_noRules", cfg)
		strategyNoRules := strategyNoRulesGot.(*CacheStrategy)
		convey.So(strategyNoRules.scaleUpTolerance, convey.ShouldEqual, 0.22)
		convey.So(strategyNoRules.scaleDownTolerance, convey.ShouldEqual, 0.23)
		convey.So(strategyNoRules.scaleRulesItems[ScaleRuleKeyOfCPU], convey.ShouldEqual, 30)
		convey.So(strategyNoRules.scaleRulesItems[ScaleRuleKeyOfMemory], convey.ShouldEqual, 40)
		convey.So(strategyNoRules.scaleRulesItems[ScaleRuleKeyOfConnections], convey.ShouldEqual, 40)

		strategyOnlyRulesGot := factory.getSpecificCacheStrategy(100, "funcSvc_onlyRules", cfg)
		strategyOnlyRules := strategyOnlyRulesGot.(*CacheStrategy)
		convey.So(strategyOnlyRules.scaleUpTolerance, convey.ShouldEqual, 0.3)
		convey.So(strategyOnlyRules.scaleDownTolerance, convey.ShouldEqual, 0.1)
		convey.So(strategyOnlyRules.scaleRulesItems[ScaleRuleKeyOfCPU], convey.ShouldEqual, 23)
		convey.So(strategyOnlyRules.scaleRulesItems[ScaleRuleKeyOfMemory], convey.ShouldEqual, 24)
		convey.So(strategyOnlyRules.scaleRulesItems[ScaleRuleKeyOfConnections], convey.ShouldEqual, 25)
	})
}
