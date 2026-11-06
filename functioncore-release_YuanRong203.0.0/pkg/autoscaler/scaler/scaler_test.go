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
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/types/functiontypes"
)

func TestFunctionScaler_generateActions(t *testing.T) {
	metricConfig := config.MetricConfig{
		TargetCPUUtilPercentage: 30,
		SyncPeriod:              40,
	}
	strategy := functiontypes.AsbResConfig{
		ScaleStrategy: []string{"cpu"},
	}
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
		totalExpected      int64
		expectedForPredict int64
		curInsCount        int64
		actions            map[string]Action
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
			s.generateActions(tt.args.totalExpected, tt.args.expectedForPredict, tt.args.curInsCount, 0, tt.args.actions)
			if got := tt.args.actions; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("generateActions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFunctionScaler_generateActionsForPredict(t *testing.T) {
	type fields struct {
		functionSvcID      string
		metricConfig       config.MetricConfig
		resiliencyStrategy Strategy
	}
	type args struct {
		totalExpected      int64
		expectedForPredict int64
		curInsCount        int64
		actions            map[string]Action
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]Action
	}{
		{
			"curIns in 0",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected:      10,
				expectedForPredict: 20,
				curInsCount:        0,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionPull,
					NeedPull:      20,
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 20,
				},
			},
		},
		{
			"totalExpected lower than curIns",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected:      5,
				expectedForPredict: 20,
				curInsCount:        15,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionReduce,
					NeedPull:      0,
					NeedReduce:    10,
					IdleMode:      false,
					TotalExpected: 5,
				},
			},
		},
		{
			"totalExpected over than curIns, and totalExpected over than predict",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected:      20,
				expectedForPredict: 17,
				curInsCount:        15,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionPull,
					NeedPull:      5,
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 20,
				},
			},
		},
		{
			"totalExpected over than curIns, and totalExpected lower than predict",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected:      17,
				expectedForPredict: 20,
				curInsCount:        15,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionPull,
					NeedPull:      5,
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 20,
				},
			},
		},
		{
			"totalExpect equals curIns and expectedForPredict over than curIns ",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected:      10,
				expectedForPredict: 20,
				curInsCount:        10,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionPull,
					NeedPull:      10,
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 20,
				},
			},
		},
		{
			"totalExpect equals curIns and expectedForPredict lower than curIns ",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected:      10,
				expectedForPredict: 5,
				curInsCount:        10,
				actions:            make(map[string]Action),
			},
			map[string]Action{},
		},
		{
			"curIns 0 with step",
			fields{
				functionSvcID: "test_step",
			},
			args{
				totalExpected:      10,
				expectedForPredict: 20,
				curInsCount:        0,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test_step": {
					Type:          ActionPull,
					NeedPull:      10, // 20 * 50 % = 10
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 20,
				},
			},
		},
		{
			"totalExpected lower than curIns with step",
			fields{
				functionSvcID: "test_step",
			},
			args{
				totalExpected:      5,
				expectedForPredict: 20,
				curInsCount:        15,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test_step": {
					Type:          ActionReduce,
					NeedPull:      0,
					NeedReduce:    8, // 15 * 50 % = 7.5 -> 8
					IdleMode:      false,
					TotalExpected: 5,
				},
			},
		},
		{
			"totalExpected over than curIns, and totalExpected over than predict with step",
			fields{
				functionSvcID: "test_step",
			},
			args{
				totalExpected:      14,
				expectedForPredict: 12,
				curInsCount:        10,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test_step": {
					Type:          ActionPull,
					NeedPull:      4, // 10 * 50 % = 5 > (14-10), use totalExpected
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 14,
				},
			},
		},
		{
			"totalExpected over than curIns, and predict over than totalExpected with step",
			fields{
				functionSvcID: "test_step",
			},
			args{
				totalExpected:      12,
				expectedForPredict: 14,
				curInsCount:        10,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test_step": {
					Type:          ActionPull,
					NeedPull:      4, // 10 * 50 % = 5 > (14-10), use totalExpected
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 14,
				},
			},
		},
		{
			"totalExpect equals curIns and expectedForPredict over than curIns with step",
			fields{
				functionSvcID: "test_step",
			},
			args{
				totalExpected:      10,
				expectedForPredict: 20,
				curInsCount:        10,
				actions:            make(map[string]Action),
			},
			map[string]Action{
				"test_step": {
					Type:          ActionPull,
					NeedPull:      5, // 10 * 50 % = 5
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 20,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &FunctionScaler{
				functionSvcID:      tt.fields.functionSvcID,
				metricConfig:       tt.fields.metricConfig,
				resiliencyStrategy: tt.fields.resiliencyStrategy,
			}
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					FuncScalePolicyWhiteList: map[string]config.SpecificFuncScalePolicy{
						"test_step": {
							ScaleUpStepPercent:   50,
							ScaleDownStepPercent: 50,
						},
					},
				}, nil
			})
			defer patch.Reset()
			s.generateActionsForPredict(tt.args.totalExpected, tt.args.expectedForPredict, tt.args.curInsCount, 0, tt.args.actions)
			if !reflect.DeepEqual(tt.want, tt.args.actions) {
				t.Errorf("failed to test generateActionsForPredict, want %+v, result: %+v", tt.want, tt.args.actions)
			}
		})
	}
}

func TestFunctionScaler_generateActionsForNormal(t *testing.T) {
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
			"scale up (adjustedScaleNum < scaleNum)",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected: 30, // 30 - 10 = 20
				curInsCount:   10, // 10 * 100 % = 10 < 20
				actions:       make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionPull,
					NeedPull:      10, // use step
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 30, // totalExpected not change
				},
			},
		},
		{
			"scale up (adjustedScaleNum > scaleNum)",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected: 15, // 15 - 10 = 5
				curInsCount:   10, // 10 * 100 % = 10 > 5
				actions:       make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionPull,
					NeedPull:      5, // use scale num
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 15,
				},
			},
		},
		{
			"scale down (adjustedScaleNum < scaleNum)",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected: 1,  // 10 - 1 = 9
				curInsCount:   10, // 10 * 50% = 5 < 9
				actions:       make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionReduce,
					NeedPull:      0,
					NeedReduce:    5,
					IdleMode:      false,
					TotalExpected: 1,
				},
			},
		},
		{
			"scale down (adjustedScaleNum > scaleNum)",
			fields{
				functionSvcID: "test",
			},
			args{
				totalExpected: 8,  // 10 - 8 = 2
				curInsCount:   10, // 10 * 50% = 5 > 2
				actions:       make(map[string]Action),
			},
			map[string]Action{
				"test": {
					Type:          ActionReduce,
					NeedPull:      0,
					NeedReduce:    2,
					IdleMode:      false,
					TotalExpected: 8,
				},
			},
		},
		{
			"scale up (scaleUp policy not config)",
			fields{
				functionSvcID: "test_not_exist",
			},
			args{
				totalExpected: 100,
				curInsCount:   10,
				actions:       make(map[string]Action),
			},
			map[string]Action{
				"test_not_exist": {
					Type:          ActionPull,
					NeedPull:      90,
					NeedReduce:    0,
					IdleMode:      false,
					TotalExpected: 100,
				},
			},
		},
		{
			"scale down (scaleUp policy not config)",
			fields{
				functionSvcID: "test_not_exist",
			},
			args{
				totalExpected: 0,
				curInsCount:   10,
				actions:       make(map[string]Action),
			},
			map[string]Action{
				"test_not_exist": {
					Type:          ActionReduce,
					NeedPull:      0,
					NeedReduce:    10,
					IdleMode:      false,
					TotalExpected: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &FunctionScaler{
				functionSvcID:      tt.fields.functionSvcID,
				metricConfig:       tt.fields.metricConfig,
				resiliencyStrategy: tt.fields.resiliencyStrategy,
			}
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					FuncScalePolicyWhiteList: map[string]config.SpecificFuncScalePolicy{
						"test": {
							ScaleUpStepPercent:   100,
							ScaleDownStepPercent: 50,
						},
					},
				}, nil
			})
			defer patch.Reset()
			s.generateActionsForNormal(tt.args.totalExpected, tt.args.curInsCount, 0, tt.args.actions)
			if !reflect.DeepEqual(tt.want, tt.args.actions) {
				t.Errorf("failed to test generateActionsForNormal, want %+v, result: %+v", tt.want, tt.args.actions)
			}
		})
	}
}

func Test_calculateAdjustedScale(t *testing.T) {
	convey.Convey("test scalePercent invalid", t, func() {
		scaleNum := int64(10)
		if scaleNumActual := calculateAdjustedScale(scaleNum, 10, 0); scaleNumActual != scaleNum {
			t.Errorf("test when scalePercent is 0 error")
		}
		if scaleNumActual := calculateAdjustedScale(scaleNum, 10, -20); scaleNumActual != scaleNum {
			t.Errorf("test when scalePercent is negative error")
		}
	})
	convey.Convey("test saferPercentCalc error", t, func() {
		scaleNum := int64(10)
		patches := gomonkey.ApplyFunc(saferPercentCalc, func(value int64, percent int64) (int64, error) {
			return 0, errors.New("test error")
		})
		defer patches.Reset()
		if scaleNumActual := calculateAdjustedScale(scaleNum, 10, 100); scaleNumActual != scaleNum {
			t.Errorf("test mock saferPercentCalc error")
		}
	})
	convey.Convey("test adjustedScaleNum < scaleNum", t, func() {
		scaleNum := int64(20)
		if scaleNumActual := calculateAdjustedScale(scaleNum, 10, 50); scaleNumActual != 10*50/100 {
			t.Errorf("test when adjustedScaleNum < scaleNum")
		}
	})
	convey.Convey("test adjustedScaleNum > scaleNum", t, func() {
		scaleNum := int64(20)
		if scaleNumActual := calculateAdjustedScale(scaleNum, 30, 100); scaleNumActual != scaleNum {
			t.Errorf("test when adjustedScaleNum > scaleNum")
		}
	})
}
