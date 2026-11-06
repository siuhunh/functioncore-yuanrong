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

package podfetcher

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	mockUtils "functioncore/test/utils"
)

// test for get funcSvcName
func Test_autoScalerAction_FuncSvcName(t *testing.T) {
	type fields struct {
		funcSvcName string
		functionKey string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "case_01",
			fields: fields{
				funcSvcName: "func_001",
				functionKey: "id_001",
			},
			want: "func_001",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &autoScalerAction{
				funcSvcName: tt.fields.funcSvcName,
				functionKey: tt.fields.functionKey,
			}
			if got := a.FuncSvcName(); got != tt.want {
				t.Errorf("FuncSvcName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAddAutoScalerMetrics(t *testing.T) {
	type args struct {
		ctx  context.Context
		view workermanager.WorkerView
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 get function cache nil", args{
			ctx: nil,
			view: workermanager.WorkerView{
				InstanceID:  "test",
				FuncName:    "test",
				FuncVersion: "latest",
				TenantID:    "abc",
				BusinessID:  "bcd",
				Metrics:     nil,
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			AddAutoScalerMetrics(tt.args.ctx, tt.args.view)
			patches.ResetAll()
		})
	}
}

func Test_autoScalerAction_DeleteInstances(t *testing.T) {
	type fields struct {
		funcSvcName string
		functionKey string
	}
	type args struct {
		workers []v1alpha1.WorkerInstance
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 workers length is 0, normal delete", fields{
			funcSvcName: "test",
			functionKey: "abc",
		}, args{[]v1alpha1.WorkerInstance{}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableRadicalAutoscaler,
						func() bool {
							return false
						}),
				})
				return patches
			}},
		{"case2 workers length is 0, radicalDelete", fields{
			funcSvcName: "test",
			functionKey: "abc",
		}, args{[]v1alpha1.WorkerInstance{}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableRadicalAutoscaler,
						func() bool {
							return true
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &autoScalerAction{
				funcSvcName: tt.fields.funcSvcName,
				functionKey: tt.fields.functionKey,
			}
			a.DeleteInstances(tt.args.workers)
			patches.ResetAll()
		})
	}
}

func TestAddPendingRequestMetrics(t *testing.T) {
	currentTimeStamp := time.Now().UTC().Unix()
	type args struct {
		view workermanager.PendingRequestView
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1: No data in PendingRequestCache", args{
			view: workermanager.PendingRequestView{
				FuncName:        "test",
				FuncVersion:     "latest",
				TenantID:        "abc",
				BusinessID:      "bcd",
				PendingRequests: 10,
				ReportTimestamp: currentTimeStamp,
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
		{"case2: data already in PendingRequestCache", args{
			view: workermanager.PendingRequestView{
				FuncName:        "test",
				FuncVersion:     "latest",
				TenantID:        "abc",
				BusinessID:      "bcd",
				PendingRequests: 20,
				ReportTimestamp: currentTimeStamp,
			},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			AddPendingRequestMetrics(tt.args.view)
			patches.ResetAll()
		})
	}
}

func Test_autoScalerAction_WillDeleteInstances(t *testing.T) {
	type fields struct {
		funcSvcName string
		functionKey string
	}
	type args struct {
		workers []v1alpha1.WorkerInstance
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1: will delete two worker", fields{
			funcSvcName: "test",
			functionKey: "abc",
		}, args{[]v1alpha1.WorkerInstance{
			{
				InstanceID: "111111111",
				Applier:    "pod-1",
			},
			{
				InstanceID: "222222222",
				Applier:    "pod-2",
			},
		}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcdhandler.PutInstanceToETCD,
						func(ctx context.Context, instance v1alpha1.WorkerInstance) {
							return
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &autoScalerAction{
				funcSvcName: tt.fields.funcSvcName,
				functionKey: tt.fields.functionKey,
			}
			a.WillDeleteInstances(tt.args.workers)
			patches.ResetAll()
		})
	}
}
