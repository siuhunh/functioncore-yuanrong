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

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func Test_tenantCache_CheckReachedMaxInstance(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		functionService *faasfunction.FunctionService
		requestInfo     *config.RequestInfo
	}
	mockFuncService := &faasfunction.FunctionService{}
	mockFuncService.Spec.Configuration.TenantID = "mock-tenantID"
	mockRequestInfo := &config.RequestInfo{
		TenantID: "mock-tenantID",
		TraceID:  "mock-traceID",
	}
	mockTenantCache := &tenantCache{}
	mockTenantCache.tenantInstanceList.Store("mock-tenantID", &tenantInstance{
		maxOnDemandInstance:   int64(-1),
		maxReversedInstance:   int64(-1),
		onDemandInsNum:        0,
		scalingOnDemandInsNum: 0,
		reversedInsNum:        0,
		scalingReversedInsNum: 0,
		mux:                   sync.RWMutex{},
	})
	tests := []struct {
		name   string
		tc     *tenantCache
		fields fields
		args   args
		want   bool
		want1  bool
		want2  bool
	}{
		{
			name: "test normal",
			tc:   &tenantCache{},
			args: args{
				functionService: mockFuncService,
				requestInfo:     mockRequestInfo,
			},
			want:  false,
			want1: false,
			want2: false,
		},
		{
			name: "test max instance is DefaultUnlimitedInstanceNumPerTenant -1",
			tc:   mockTenantCache,
			args: args{
				functionService: mockFuncService,
				requestInfo:     mockRequestInfo,
			},
			want:  false,
			want1: false,
			want2: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := tt.tc
			got, got1, got2 := tc.CheckReachedMaxInstance(tt.args.functionService, tt.args.requestInfo)
			assert.Equalf(t, tt.want, got, "CheckReachedMaxInstance(%v, %v)", tt.args.functionService, tt.args.requestInfo)
			assert.Equalf(t, tt.want1, got1, "CheckReachedMaxInstance(%v, %v)", tt.args.functionService, tt.args.requestInfo)
			assert.Equalf(t, tt.want2, got2, "CheckReachedMaxInstance(%v, %v)", tt.args.functionService, tt.args.requestInfo)
		})
	}
}

func Test_tenantCache_addInstanceCallback(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID   string
		isReversed bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 add on-demand instance",
			fields: fields{},
			args: args{
				tenantID:   "test",
				isReversed: false,
			},
		}, {
			name:   "case_02 add reversed instance",
			fields: fields{},
			args: args{
				tenantID:   "test",
				isReversed: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.addInstanceCallback(tt.args.tenantID, tt.args.isReversed)
		})
	}
}

func Test_tenantCache_deleteInstanceCallback(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID   string
		isReversed bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 delete on-demand instance",
			fields: fields{},
			args: args{
				tenantID:   "test",
				isReversed: false,
			},
		}, {
			name:   "case_02 delete reversed instance",
			fields: fields{},
			args: args{
				tenantID:   "test",
				isReversed: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.deleteInstanceCallback(tt.args.tenantID, tt.args.isReversed)
		})
	}
}

func Test_tenantCache_resetInstanceCallback(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID            string
		onDemandInstanceNum int64
		reversedInstanceNum int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 reset instance",
			fields: fields{},
			args: args{
				tenantID:            "test",
				onDemandInstanceNum: 1,
				reversedInstanceNum: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.resetInstanceCallback(tt.args.tenantID, tt.args.onDemandInstanceNum, tt.args.reversedInstanceNum)
		})
	}
}

func Test_tenantCache_decreaseScalingNumCallback(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID   string
		isReversed bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 decrease scaling on-demand instance",
			fields: fields{},
			args: args{
				tenantID:   "test",
				isReversed: false,
			},
		}, {
			name:   "case_02 decrease scaling reversed instance",
			fields: fields{},
			args: args{
				tenantID:   "test",
				isReversed: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.decreaseScalingNumCallback(tt.args.tenantID, tt.args.isReversed)
		})
	}
}

func Test_tenantCache_resetScalingNumCallback(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID                   string
		scalingOnDemandInstanceNum int64
		scalingReversedInstanceNum int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 reset scaling instance",
			fields: fields{},
			args: args{
				tenantID:                   "test",
				scalingOnDemandInstanceNum: 1,
				scalingReversedInstanceNum: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.resetScalingNumCallback(tt.args.tenantID, tt.args.scalingOnDemandInstanceNum, tt.args.scalingReversedInstanceNum)
		})
	}
}

func Test_tenantCache_UpdateOrAddTenantCache(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenant Tenant
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{},
			args: args{
				tenant: Tenant{
					TenantID:       "test",
					TenantMetaInfo: config.TenantMetaInfo{TenantInstanceMetaData: config.TenantInstanceMetaData{100, 100}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.UpdateOrAddTenantCache(tt.args.tenant)
		})
	}
}

func Test_tenantCache_DeleteTenantCache(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{},
			args: args{
				tenantID: "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.DeleteTenantCache(tt.args.tenantID)
		})
	}
}

func Test_tenantCache_UpdateDefaultQuota(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		defaultTenant Tenant
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{},
			args: args{
				defaultTenant: Tenant{
					TenantID:       "test",
					TenantMetaInfo: config.TenantMetaInfo{TenantInstanceMetaData: config.TenantInstanceMetaData{1000, 1000}},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			tc.getOrCreateCachePerTenant("test")
			tc.UpdateDefaultQuota(tt.args.defaultTenant)
		})
	}
}

func Test_tenantCache_GetMaxInsNum(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	type args struct {
		tenantID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int64
		want1  int64
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{},
			args:   args{tenantID: "test"},
			want:   1000,
			want1:  1000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			got, got1 := tc.GetMaxInsNum(tt.args.tenantID)
			assert.Equalf(t, tt.want, got, "GetMaxInsNum(%v)", tt.args.tenantID)
			assert.Equalf(t, tt.want1, got1, "GetMaxInsNum(%v)", tt.args.tenantID)
		})
	}
}

func TestUpdateAllTenantCache(t *testing.T) {
	type args struct {
		stopCh   chan struct{}
		interval time.Duration
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01 stop chan is nil",
			args: args{
				stopCh:   nil,
				interval: 10 * time.Second,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case_02 stop chan is not nil, get etcd error",
			args: args{
				stopCh:   make(chan struct{}, 1),
				interval: 1 * time.Second,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetDefaultTenantMetaInfoFromEtcd, func(defaultTenantMetaKey string) (*config.TenantMetaInfo, error) {
						return nil, fmt.Errorf("get etcd error")
					}),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
				})
				return patches
			},
		}, {
			name: "case_03 stop chan is not nil, get etcd success",
			args: args{
				stopCh:   make(chan struct{}, 1),
				interval: 1 * time.Second,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetDefaultTenantMetaInfoFromEtcd, func(defaultTenantMetaKey string) (*config.TenantMetaInfo, error) {
						return &config.TenantMetaInfo{
							TenantInstanceMetaData: config.TenantInstanceMetaData{MaxOnDemandInstance: 100, MaxReversedInstance: 100},
						}, nil
					}),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if tt.args.stopCh != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
			}
			UpdateAllTenantCache(tt.args.stopCh, tt.args.interval)
			patches.ResetAll()
		})
	}
}

func Test_tenantCache_syncTenantCacheByFunc(t *testing.T) {
	type fields struct {
		tenantInstanceList sync.Map
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := &tenantCache{
				tenantInstanceList: tt.fields.tenantInstanceList,
			}
			fc := WorkerCache.createSimpleFunctionCache("test_func", config.InstanceMetaData{
				MinInstance: 100, MaxInstance: 100, IdleMode: false,
			}, types.ProfileMetaData{}, utils.FunctionInfo{})
			WorkerCache.functions.Store("tenant_123", fc)
			tc.syncTenantCacheByFunc()
		})
	}
}

func Test_tenantCache_UpdateDefaultQuota2(t *testing.T) {
	var tenantInstanceList sync.Map
	tc := &tenantCache{
		tenantInstanceList: tenantInstanceList,
	}
	defaultTenant := Tenant{
		TenantID:       "test",
		TenantMetaInfo: config.TenantMetaInfo{TenantInstanceMetaData: config.TenantInstanceMetaData{1000, 1000}},
	}

	tc.getOrCreateCachePerTenant("test")
	tc.UpdateDefaultQuota(defaultTenant)
	maxOnDemandInstance, maxReversedInstance := tc.GetMaxInsNum("test")
	assert.Equal(t, maxOnDemandInstance, int64(1000))
	assert.Equal(t, maxReversedInstance, int64(1000))

	tenant := Tenant{
		TenantID:       "test",
		TenantMetaInfo: config.TenantMetaInfo{TenantInstanceMetaData: config.TenantInstanceMetaData{100, 100}},
	}
	tc.UpdateOrAddTenantCache(tenant)
	maxOnDemandInstance, maxReversedInstance = tc.GetMaxInsNum("test")
	assert.Equal(t, maxOnDemandInstance, int64(100))
	assert.Equal(t, maxReversedInstance, int64(100))
}
