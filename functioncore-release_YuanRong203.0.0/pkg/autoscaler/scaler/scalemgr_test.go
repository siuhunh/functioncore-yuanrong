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

// scaler -
package scaler

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/instance"
	"functioncore/pkg/common/types/functiontypes"
	mockutils "functioncore/test/utils"
)

func TestScaleMgr_needScaleAction(t *testing.T) {
	metricConfig := config.MetricConfig{
		TargetCPUUtilPercentage: 30,
		SyncPeriod:              40,
	}
	strategy := functiontypes.AsbResConfig{
		ScaleStrategy: []string{"cpu"},
	}
	testFunctionSvcID := "test"
	type fields struct {
		config *config.Config
		stopCh <-chan struct{}
	}
	type args struct {
		minInsCount int64
		maxInsCount int64
		curInsCount int64
		scaler      *FunctionScaler
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		expectNum   int64
		needScale   bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test needScaleAction with curInsCount=maxInsCount",
			fields: fields{
				stopCh: make(chan struct{}),
			},
			args: args{
				minInsCount: 1,
				maxInsCount: 11,
				curInsCount: 11,
				scaler: &FunctionScaler{
					functionSvcID:      testFunctionSvcID,
					resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
				},
			},
			expectNum: 1,
			needScale: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*ResourceAvgStrategy).calExpectedScaleNums, func(avgStrategy *ResourceAvgStrategy) (int64, int64, error) {
						return 1, 1, nil
					}),
				})
				return patches
			},
		},
		{
			name: "test needScaleAction with curInsCount>maxInsCount",
			fields: fields{
				stopCh: make(chan struct{}),
			},
			args: args{
				minInsCount: 1,
				maxInsCount: 11,
				curInsCount: 12,
				scaler: &FunctionScaler{
					functionSvcID:      testFunctionSvcID,
					resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
				},
			},
			expectNum: 2,
			needScale: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*ResourceAvgStrategy).calExpectedScaleNums, func(*ResourceAvgStrategy) (int64, int64, error) {
						return 2, 2, nil
					}),
				})
				return patches
			},
		},
		{
			name: "test needScaleAction with curInsCount<minInsCount",
			fields: fields{
				stopCh: make(chan struct{}),
			},
			args: args{
				minInsCount: 5,
				maxInsCount: 11,
				curInsCount: 4,
				scaler: &FunctionScaler{
					functionSvcID:      testFunctionSvcID,
					resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
				},
			},
			expectNum: 0,
			needScale: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			name: "test needScaleAction with minInsCount=1,maxInsCount=1",
			fields: fields{
				stopCh: make(chan struct{}),
			},
			args: args{
				minInsCount: 1,
				maxInsCount: 1,
				curInsCount: 1,
				scaler: &FunctionScaler{
					functionSvcID:      testFunctionSvcID,
					resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
				},
			},
			expectNum: 2,
			needScale: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*ResourceAvgStrategy).calExpectedScaleNums, func(*ResourceAvgStrategy) (int64, int64, error) {
						return 2, 2, nil
					}),
				})
				return patches
			},
		},
		{
			name: "test needScaleAction with calExpectedScaleNums error",
			fields: fields{
				stopCh: make(chan struct{}),
			},
			args: args{
				minInsCount: 1,
				maxInsCount: 11,
				curInsCount: 11,
				scaler: &FunctionScaler{
					functionSvcID:      testFunctionSvcID,
					resiliencyStrategy: createStrategy(strategy, testFunctionSvcID, metricConfig),
				},
			},
			expectNum: 0,
			needScale: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*ResourceAvgStrategy).calExpectedScaleNums, func(strategy *ResourceAvgStrategy) (int64, int64, error) {
						return 0, 0, errors.New("error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ScaleMgr{
				config:      tt.fields.config,
				stopCh:      tt.fields.stopCh,
				scalerGroup: sync.Map{},
			}
			patches := tt.patchesFunc()
			expectNum, _, needScale := s.needScaleAction(tt.args.minInsCount, 0, tt.args.curInsCount, tt.args.scaler)
			t.Logf("needScaleAction() expectNum = %v, want %v", expectNum, tt.expectNum)
			if expectNum != tt.expectNum {
				t.Errorf("needScaleAction() expectNum = %v, want %v", expectNum, tt.expectNum)
			}
			t.Logf("needScaleAction() needScale = %v, want %v", needScale, tt.needScale)
			if needScale != tt.needScale {
				t.Errorf("needScaleAction() needScale = %v, want %v", needScale, tt.needScale)
			}
			patches.ResetAll()
		})
	}
}

func TestScaleMgr_processScalerGroup(t *testing.T) {
	metricConfig := config.MetricConfig{
		TargetCPUUtilPercentage: 30,
		SyncPeriod:              40,
	}
	strategy := functiontypes.AsbResConfig{
		ScaleStrategy: []string{"cpu"},
	}
	testFunctionSvcID := "test"

	wantActions := map[string]Action{
		testFunctionSvcID: {
			Type:          ActionPull,
			NeedPull:      1,
			TotalExpected: 8,
		},
	}
	type fields struct {
		config *config.Config
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		want        map[string]Action
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case test process scaler group",
			fields{},
			wantActions,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetInstanceMetaCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) *functiontypes.InstanceMetaData {
							return &functiontypes.InstanceMetaData{
								MinInstance: 2,
								MaxInstance: 10,
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 7, 0
						}),
					gomonkey.ApplyFunc((*ScaleMgr).needScaleAction, func(_ *ScaleMgr,
						minInsCount int64, maxInsCount int64, curInsCount int64, scaler *FunctionScaler) (int64, int64, bool) {
						return 8, 8, true
					}),
					gomonkey.ApplyFunc(instance.GetFuncAvailableClusters, func(_ *functiontypes.FunctionEtcdInfo) ([]string, bool) {
						return []string{"cluster"}, true
					}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							Clusters: []string{"cluster"},
						}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			s := &ScaleMgr{
				config:      tt.fields.config,
				stopCh:      tt.fields.stopCh,
				scalerGroup: sync.Map{},
			}
			s.scalerGroup.Store(testFunctionSvcID, &FunctionScaler{
				testFunctionSvcID,
				metricConfig,
				createStrategy(strategy, testFunctionSvcID, metricConfig),
			})
			if got := s.processScalerGroup(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("processScalerGroup() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestCreateScaleMgr(t *testing.T) {
	type args struct {
		stopCh <-chan struct{}
	}
	stopCh := make(<-chan struct{})
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case1 test to create scaler manager",
			args{
				stopCh,
			},
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							DownscaleStabilization: 0,
						}, nil
					}),
					gomonkey.ApplyFunc((*ScaleMgr).Start, func(_ *ScaleMgr) error {
						return nil
					}),
					gomonkey.ApplyFunc((*ScaleMgr).Run, func(_ *ScaleMgr) {
						return
					}),
				})
				return patches
			},
		},
		{
			"case2 test to create scaler manager",
			args{
				stopCh,
			},
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							DownscaleStabilization: 0,
						}, nil
					}),
					gomonkey.ApplyFunc((*ScaleMgr).Start, func(_ *ScaleMgr) error {
						return errors.New("error")
					}),
					gomonkey.ApplyFunc((*ScaleMgr).Run, func(_ *ScaleMgr) {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := CreateScaleMgr(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("CreateScaleMgr() error = %v, wantErr %v", err, tt.wantErr)
			}
			GetScaleManager()
			patches.ResetAll()
		})
	}
}

func TestScaleMgr_Run(t *testing.T) {
	type fields struct {
		config *config.Config
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"test scaler manager run",
			fields{
				config: &config.Config{
					SyncPeriod: 3,
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*ScaleMgr).processScalerGroup, func(_ *ScaleMgr) map[string]Action {
						return map[string]Action{
							"test": {
								Type:          ActionPull,
								NeedPull:      1,
								TotalExpected: 8,
							},
						}
					}),
					gomonkey.ApplyFunc(DoActions, func(actions map[string]Action) {
						return
					}),
					gomonkey.ApplyFunc(instance.UpdateFuncInsCache, func() {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			stopCh := make(chan struct{})
			s := &ScaleMgr{
				config:      tt.fields.config,
				stopCh:      stopCh,
				scalerGroup: sync.Map{},
			}
			go s.Run()
			time.Sleep(6 * time.Second)
			close(stopCh)
			patches.ResetAll()
		})
	}
}

func TestScaleMgr_UpdateScalerGroup(t *testing.T) {
	type fields struct {
		config *config.Config
		stopCh <-chan struct{}
	}
	type args struct {
		functionSvcID string
		asbresConfig  functiontypes.AsbResConfig
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"case test update scaler group",
			fields{
				config: &config.Config{
					SyncPeriod:             10,
					DownscaleStabilization: 10,
				},
			},
			args{
				functionSvcID: "testFunctionID",
				asbresConfig: functiontypes.AsbResConfig{
					TargetCPUUtilPercentage: 10,
					TargetMemUtilPercentage: 10,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ScaleMgr{
				config: tt.fields.config,
				stopCh: tt.fields.stopCh,
			}
			s.UpdateScalerGroup(tt.args.functionSvcID, tt.args.asbresConfig)
			s.RemoveScaler(tt.args.functionSvcID)
		})
	}
}

func TestScaleMgr_Start(t *testing.T) {
	testCache := &cache.AutoScalerCache{
		FunctionMetaCache: sync.Map{},
	}
	testFunctionMetaEtcd := &functiontypes.FunctionEtcdInfo{
		ExtendedMetaData: functiontypes.ExtendedMetaData{
			AsbResConfig: functiontypes.AsbResConfig{
				TargetMemUtilPercentage: 10,
				TargetCPUUtilPercentage: 10,
			},
		},
	}
	testCache.FunctionMetaCache.Store("test", testFunctionMetaEtcd)
	type fields struct {
		config *config.Config
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case test scaler manager start",
			fields{
				config: &config.Config{
					SyncPeriod:             10,
					DownscaleStabilization: 10,
				},
			},
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetInstance, func() *cache.AutoScalerCache {
						return testCache
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pacthes := tt.patchesFunc()
			s := &ScaleMgr{
				config:      tt.fields.config,
				stopCh:      tt.fields.stopCh,
				scalerGroup: sync.Map{},
			}
			if err := s.Start(); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
			pacthes.ResetAll()
		})
	}
}
