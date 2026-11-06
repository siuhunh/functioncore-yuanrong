/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func Test_workerCache_checkMinScale(t *testing.T) {
	type fields struct {
		kubeClient kubernetes.Interface
		functions  sync.Map
	}
	var fm1, fm2, fm3, fm4, fm5, fm6, fm7, fm8, fmWithCAE sync.Map
	fm1.Store("aaa", &functionCache{
		minScale:            1,
		maxScale:            2,
		minScaleUpdatedTime: time.Now(),
	})
	fm2.Store("aaa", &functionCache{
		funcMeta:            &faasfunction.FunctionService{},
		minScale:            1,
		maxScale:            2,
		minScaleUpdatedTime: time.Now(),
		minScaleAlarmSign:   true,
		instances: map[string]v1alpha1.WorkerInstance{
			"aaaa": {Applier: constants.WorkerManagerApplier},
		},
	})
	fm3.Store("aaa", &functionCache{
		funcMeta:            &faasfunction.FunctionService{},
		minScale:            0,
		maxScale:            2,
		minScaleUpdatedTime: time.Now(),
	})
	fm4.Store("aaa", &workerCache{})
	fm5.Store("aaa", &functionCache{
		funcMeta:            &faasfunction.FunctionService{},
		minScale:            1,
		maxScale:            2,
		minScaleUpdatedTime: time.Unix(1500000000, 0),
	})
	fm6.Store("aaa", &functionCache{
		funcMeta: &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					VPCTriggerImage: "ZZZ",
				},
			},
		},
		minScale:            1,
		maxScale:            2,
		minScaleUpdatedTime: time.Unix(1500000000, 0),
	})
	fm7.Store("aaa", &functionCache{
		funcMeta:             &faasfunction.FunctionService{},
		minScale:             1,
		maxScale:             2,
		isUserErrorAlarmSign: true,
		minScaleUpdatedTime:  time.Unix(1500000000, 0),
	})
	fm8.Store("aaa1", &functionCache{
		funcMeta:             &faasfunction.FunctionService{},
		minScale:             1,
		maxScale:             2,
		isUserErrorAlarmSign: true,
		minScaleUpdatedTime:  time.Unix(1500000000, 0),
	})
	fm8.Store("aaa2", &functionCache{
		funcMeta:            &faasfunction.FunctionService{},
		minScale:            1,
		maxScale:            2,
		minScaleUpdatedTime: time.Unix(1500000000, 0),
	})
	fmWithCAE.Store("not cae", &functionCache{
		funcMeta:             &faasfunction.FunctionService{},
		minScale:             1,
		maxScale:             2,
		minScaleUpdatedTime:  time.Unix(1500000000, 0),
	})
	fmWithCAE.Store("cae", &functionCache{
		funcMeta:            &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					BusinessType: constants.BusinessTypeCAE,
				},
			},
		},
		minScale:            1,
		maxScale:            2,
		minScaleUpdatedTime: time.Unix(1500000000, 0),
	})

	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		want1       int64
		want2       int64
	}{
		{
			name: "case1",
			fields: fields{
				functions: fm1,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 0,
			want2: 0,
		},
		{
			name: "case2",
			fields: fields{
				functions: fm2,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 0,
			want2: 0,
		},
		{
			name: "case3",
			fields: fields{
				functions: fm3,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 0,
			want2: 0,
		},
		{
			name: "case4",
			fields: fields{
				functions: fm4,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 0,
			want2: 0,
		},
		{
			name: "case5",
			fields: fields{
				functions: fm5,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 1,
			want2: 0,
		},
		{
			name: "case6",
			fields: fields{
				functions: fm6,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 0,
			want2: 0,
		},
		{
			name: "case7",
			fields: fields{
				functions: fm7,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 0,
			want2: 1,
		},
		{
			name: "case8",
			fields: fields{
				functions: fm8,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 1,
			want2: 1,
		},
		{
			name: "cae function no need checkMinScale",
			fields: fields{
				functions: fmWithCAE,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
			want1: 1,
			want2: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			w := &workerCache{
				functions: tt.fields.functions,
			}
			if got1, got2 := w.checkMinScale("az1-01"); got1 != tt.want1 || got2 != tt.want2 {
				t.Errorf("checkMinScale() = %v, %v, want %v, %v", got1, got2, tt.want1, tt.want2)
			}
			patches.ResetAll()
		})
	}
}

func TestCheckMinInsAndReport(t *testing.T) {
	type args struct {
		stopCh chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(checkMinInsRegularly, func(stopCh <-chan struct{}) {
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
			CheckMinInsAndReport(tt.args.stopCh)
			close(tt.args.stopCh)
			patches.ResetAll()
		})
	}
}

func Test_judgeAndReport(t *testing.T) {
	type args struct {
		clusterID string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1",
			args: args{
				clusterID: "az1-01",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).checkMinScale,
						func(_ *workerCache, _ string) (int64, int64) {
							return int64(0), int64(0)
						}),
					gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
						func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
				})
				return patches
			},
		},
		{
			name: "case2",
			args: args{
				clusterID: "az1-01",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).checkMinScale,
						func(_ *workerCache, _ string) (int64, int64) {
							return int64(1), int64(0)
						}),
					gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
						func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
				})
				return patches
			},
		},
		{
			name: "case3",
			args: args{
				clusterID: "az1-01",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).checkMinScale,
						func(_ *workerCache, _ string) (int64, int64) {
							return int64(0), int64(1)
						}),
					gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
						func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			judgeAndReport(tt.args.clusterID)
			patches.ResetAll()
		})
	}
}

func Test_checkMinInsRegularly(t *testing.T) {
	type args struct {
		stopCh chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
		{
			name: "case2",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "1"
					}),
				})
				return patches
			},
		},
		{
			name: "case3",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			checkMinInsRegularly(tt.args.stopCh)
			close(tt.args.stopCh)
			patches.ResetAll()
		})
	}
}
