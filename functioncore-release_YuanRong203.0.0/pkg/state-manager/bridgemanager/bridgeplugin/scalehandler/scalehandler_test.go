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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	mockutils "functioncore/test/utils"
)

func TestGetHandler(t *testing.T) {
	tests := []struct {
		name string
		want *Handler
	}{
		{
			"getHandlera",
			&Handler{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetHandler(); got == nil {
				t.Errorf("GetHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandler_ClearAndUpdateFuncInsCache(t *testing.T) {
	funcInfoCacheA := sync.Map{}
	funcInfoCacheA.Store(111, struct{}{})

	funcInfoCacheB := sync.Map{}
	funcInfoCacheB.Store("funcSvc1", struct{}{})

	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	tests := []struct {
		name         string
		fields       fields
		funInfoCache sync.Map
		patchesFunc  mockutils.PatchesFunc
	}{
		{
			"test clear and update func ins cache with error funcInfo",
			fields{},
			funcInfoCacheA,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			"test clear and update func ins cache with error funcInfo",
			fields{},
			funcInfoCacheB,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			name:         "test clear and update func is nil",
			funInfoCache: funcInfoCacheB,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&cache.BridgeCache{}), "GetRestrictByURN",
						func(_ *cache.BridgeCache, functionURN string) (cache.BridgeConfigResp, *config.FunctionMetaInfo, error) {
							return cache.BridgeConfigResp{MaxActiveQua: 10, MaxStateKeyQua: 10}, nil, nil
						}),
				})
				return patches
			},
		},
		{
			name:         "test clear and update func ins cache with GetRestrictByURN success",
			funInfoCache: funcInfoCacheB,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&cache.BridgeCache{}), "GetRestrictByURN",
						func(_ *cache.BridgeCache, functionURN string) (cache.BridgeConfigResp, *config.FunctionMetaInfo, error) {
							return cache.BridgeConfigResp{MaxActiveQua: 10, MaxStateKeyQua: 10}, &config.FunctionMetaInfo{
								FuncMetaData: config.FuncMetaData{
									IsStatefulFunction: true,
								},
							}, nil
						}),
				})
				return patches
			},
		},
		{
			name:         "test clear and update func ins cache with GetRestrictByURN success",
			funInfoCache: funcInfoCacheB,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&cache.BridgeCache{}), "GetRestrictByURN",
						func(_ *cache.BridgeCache, functionURN string) (cache.BridgeConfigResp, *config.FunctionMetaInfo, error) {
							return cache.BridgeConfigResp{MaxActiveQua: 10, MaxStateKeyQua: 10}, &config.FunctionMetaInfo{
								FuncMetaData: config.FuncMetaData{
									IsStatefulFunction: true,
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
		cache.Instance().FuncNameInfoCache = tt.funInfoCache
		handle := &Handler{
			syncPeriod:          tt.fields.syncPeriod,
			stopCh:              tt.fields.stopCh,
			functionInstance:    tt.fields.functionInstance,
			resourceMetricCache: tt.fields.resourceMetricCache,
		}
		handle.ClearAndUpdateFuncInsCache()
		patches.ResetAll()
	}
}

func TestHandler_RefreshResourceMetricCache(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		resourceMetricCache map[resourceusage.
			FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test refresh metric cache",
			fields{
				resourceMetricCache: initResourceMetricCache(),
			},
			args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			handle.RefreshResourceMetricCache(tt.args.resourceMetricCache)
		})
	}
}

func TestHandler_Run(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			"test run",
			fields{
				syncPeriod: defaultSyncPeriod,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			if got := handle.Run(); got == nil {
				t.Errorf("Run() = %v", got)
			}
		})
	}
}

func TestHandler_WithStopCh(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantNil bool
	}{
		{
			"test with sync period",
			fields{
				stopCh: nil,
			},
			args{
				stopCh: nil,
			},
			true,
		},
		{
			"test with sync period",
			fields{
				stopCh: nil,
			},
			args{
				stopCh: make(<-chan struct{}),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			handle.WithStopCh(tt.args.stopCh)

			if (handle.stopCh == nil) != tt.wantNil {
				t.Errorf("WithStopCh(), want %v", tt.wantNil)
			}
		})
	}
}

func TestHandler_WithSyncPeriod(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		syncPeriod int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
	}{
		{
			"test with sync period",
			fields{
				syncPeriod: defaultSyncPeriod,
			},
			args{
				syncPeriod: 10,
			},
			10,
		},
		{
			"test with sync period",
			fields{
				syncPeriod: defaultSyncPeriod,
			},
			args{
				syncPeriod: -1,
			},
			defaultSyncPeriod,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			handle.WithSyncPeriod(tt.args.syncPeriod)
			if handle.syncPeriod != tt.want {
				t.Errorf("WithSyncPeriod failed, want %v", tt.want)
			}
		})
	}
}

func TestHandler_calExpectedScaleNums(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		curInsCount int
		funcSvc     string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test curInsCount is zero",
			fields{},
			args{
				curInsCount: 0,
			},
			true,
		},
		{
			"test resiliencyStrategy is nil",
			fields{},
			args{
				curInsCount: 10,
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			_, err := handle.calExpectedScaleNums(tt.args.curInsCount, tt.args.funcSvc)
			if (err != nil) != tt.wantErr {
				t.Errorf("failed to test calExpectedScaleNums, wantErr: %t", tt.wantErr)
			}
		})
	}
}

func TestHandler_generateActions(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		funcSvc          string
		totalExpected    int64
		curInsCount      int64
		functionMetaInfo *config.FunctionMetaInfo
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantPullType ActionType
		wantNum      int64
	}{
		{
			"test pull",
			fields{},
			args{
				funcSvc:          "funcSvc1",
				totalExpected:    10,
				curInsCount:      5,
				functionMetaInfo: nil,
			},
			ActionPull,
			5,
		},
		{
			"test reduce",
			fields{},
			args{
				funcSvc:          "funcSvc1",
				totalExpected:    5,
				curInsCount:      10,
				functionMetaInfo: nil,
			},
			ActionReduce,
			5,
		},
		{
			"test reduce",
			fields{},
			args{
				funcSvc:          "funcSvc1",
				totalExpected:    5,
				curInsCount:      5,
				functionMetaInfo: nil,
			},
			-1,
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			action := handle.generateActions(tt.args.funcSvc, tt.args.totalExpected, tt.args.curInsCount, tt.args.functionMetaInfo)
			if action == nil {
				if tt.wantNum != 0 || tt.wantPullType != -1 {
					t.Errorf("failed to test get nil action")
				}
			} else {
				if tt.wantPullType != action.Type {
					t.Errorf("generateActions() = %v, want %v", action.Type, tt.wantPullType)
				} else {
					if action.Type == ActionPull && tt.wantNum != action.NeedPull {
						t.Errorf("failed to generateActions because pull number not right, got %d, want %d", action.NeedPull, tt.wantNum)
					}
					if action.Type == ActionReduce && tt.wantNum != action.NeedReduce {
						t.Errorf("failed to generateActions because reduce number not right, got %d, want %d", action.NeedReduce, tt.wantNum)
					}
				}
			}
		})
	}
}

func TestHandler_needScaleAction(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		maxInsCount int
		curInsCount int
		funcSvc     string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantScale bool
	}{
		{
			"test curInsCount > maxInsCount",
			fields{},
			args{
				maxInsCount: 20,
				curInsCount: 10,
			},
			false,
		},
		{
			"test curInsCount is zero",
			fields{},
			args{
				maxInsCount: 0,
				curInsCount: 0,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			_, needScale := handle.needScaleAction(tt.args.maxInsCount, tt.args.curInsCount, tt.args.funcSvc)
			if needScale != tt.wantScale {
				t.Errorf("failed to test needScale, want: %t", tt.wantScale)
			}
		})
	}
}

func TestHandler_processScaleGroup(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	tests := []struct {
		name    string
		fields  fields
		wantLen int
	}{
		{
			"test processScaleGroup",
			fields{
				functionInstance: map[string]*functionInstanceData{
					"funSvc1": {},
				},
			},
			0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handle := &Handler{
				syncPeriod:          tt.fields.syncPeriod,
				stopCh:              tt.fields.stopCh,
				functionInstance:    tt.fields.functionInstance,
				resourceMetricCache: tt.fields.resourceMetricCache,
			}
			if got := handle.processScaleGroup(); len(got) != tt.wantLen {
				t.Errorf("processScaleGroup() = %v, want %v", got, tt.wantLen)
			}
		})
	}
}

func TestHandler_run(t *testing.T) {
	stopCh := make(chan struct{})
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			"test closed",
			fields{
				stopCh:     stopCh,
				syncPeriod: 1,
			},
		},
	}
	for _, tt := range tests {

		handle := &Handler{
			syncPeriod:          tt.fields.syncPeriod,
			stopCh:              tt.fields.stopCh,
			functionInstance:    tt.fields.functionInstance,
			resourceMetricCache: tt.fields.resourceMetricCache,
		}
		go handle.run()
		time.Sleep(2 * time.Second)
		stopCh <- struct{}{}
	}
}

func TestHandler_updateFuncInsCacheFromWM(t *testing.T) {
	type fields struct {
		syncPeriod          int64
		stopCh              <-chan struct{}
		functionInstance    map[string]*functionInstanceData
		resourceMetricCache ResourceMetric
	}
	type args struct {
		functionURN      string
		functionMetaInfo *config.FunctionMetaInfo
		maxStateKeyQua   int
	}
	tests := []struct {
		name                      string
		fields                    fields
		args                      args
		wantLenOfFunctionInstance int
		patchesFunc               mockutils.PatchesFunc
	}{
		{
			"test failed to query funcInsCache",
			fields{
				functionInstance: make(map[string]*functionInstanceData),
			},
			args{},
			0,
			func() mockutils.PatchSlice {
				return mockutils.InitPatchSlice()
			},
		},
		{
			"test succeed to query funcInsCache but no pods",
			fields{
				functionInstance: make(map[string]*functionInstanceData),
			},
			args{},
			0,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
						func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
							return &types.QueryInstance{
								PodsName: []string{},
							}, nil
						}),
				})
				return patches
			},
		},
		{
			"test succeed to query funcInsCache with pods",
			fields{
				functionInstance: make(map[string]*functionInstanceData),
			},
			args{},
			1,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
						func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
							return &types.QueryInstance{
								PodsName: []string{"podA", "podB"},
							}, nil
						}),
				})
				return patches
			},
		},
	}

	for _, tt := range tests {
		patches := tt.patchesFunc()
		handle := &Handler{
			syncPeriod:          tt.fields.syncPeriod,
			stopCh:              tt.fields.stopCh,
			functionInstance:    tt.fields.functionInstance,
			resourceMetricCache: tt.fields.resourceMetricCache,
		}
		handle.updateFuncInsCacheFromWM(tt.args.functionURN, tt.args.functionMetaInfo, tt.args.maxStateKeyQua)
		if len(handle.functionInstance) != tt.wantLenOfFunctionInstance {
			t.Errorf("failed to test updateFuncInsCacheFromWM want %d", tt.wantLenOfFunctionInstance)
		}
		patches.ResetAll()
	}
}
