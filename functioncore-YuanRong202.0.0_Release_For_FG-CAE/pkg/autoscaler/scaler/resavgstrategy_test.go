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

package scaler

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/resourceusage"
	mockutils "functioncore/test/utils"
)

func TestResAvgStrategy_calExpectedScaleNums(t *testing.T) {
	type fields struct {
		functionSvcID string
		resources     []ResourceConfig
	}
	tests := []struct {
		name        string
		fields      fields
		want        int64
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			// curInsCount 2, TargetCPUUtilPercentage 30, expectedCount 6
			name: "test calExpectedScaleNums with 80% CPUUtilPercentage",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createCPUResourceConfig(
						config.MetricConfig{
							TargetCPUUtilPercentage: 30,
							SyncPeriod:              40,
						}),
				},
			},
			want:    6,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											CPUUsage:    0.48,
											CPULimit:    600,
											MemoryUsage: 100,
											MemoryLimit: 500,
											ReportTime:  time.Now().Unix(),
										}, {
											CPUUsage:    0.48,
											CPULimit:    600,
											MemoryUsage: 100,
											MemoryLimit: 500,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 2
						}),
				})
				return patches
			},
		},
		{
			// curInsCount 2, redis data expired
			name: "test calExpectedScaleNums expired redis data",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createMEMResourceConfig(
						config.MetricConfig{
							TargetCPUUtilPercentage: 30,
							SyncPeriod:              40,
						}),
				},
			},
			want:    0,
			wantErr: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											CPUUsage:    0.48,
											CPULimit:    600,
											MemoryUsage: 100,
											MemoryLimit: 500,
											ReportTime:  time.Now().Unix() - 40,
										}, {
											CPUUsage:    0.48,
											CPULimit:    600,
											MemoryUsage: 100,
											MemoryLimit: 500,
											ReportTime:  time.Now().Unix() - 40,
										},
									},
								},
							}
						}),
				})
				return patches
			},
		},
		{
			// curInsCount 2, TargetCPUUtilPercentage 50, actual 40, scaleDown tolerance 0.2, expectedCount 2
			name: "test calExpectedScaleNums in scale down tolerance, not scale down",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createCPUResourceConfig(
						config.MetricConfig{
							TargetCPUUtilPercentage: 50,
							SyncPeriod:              40,
							ScaleDownTolerance:      0.2,
						}),
				},
			},
			want:    2,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											CPUUsage:    0.4,
											CPULimit:    1000,
											MemoryUsage: 100,
											MemoryLimit: 500,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 2
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ResourceAvgStrategy{
				functionSvcID: tt.fields.functionSvcID,
				resources:     tt.fields.resources,
			}

			patches := tt.patchesFunc()
			got, err := s.calExpectedScaleNums()
			if (err != nil) != tt.wantErr {
				t.Errorf("calExpectedScaleNums() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("calExpectedScaleNums() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestResAvgStrategy_ScaleByMEM_calExpectedScaleNums(t *testing.T) {
	type fields struct {
		functionSvcID string
		resources     []ResourceConfig
	}
	tests := []struct {
		name        string
		fields      fields
		want        int64
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			// curInsCount 2, TargetMEMUtilPercentage 30, expectedCount 6
			name: "Sale up: test calExpectedScaleNums with 80% MEMUtilPercentage",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createMEMResourceConfig(
						config.MetricConfig{
							TargetMemUtilPercentage: 30,
							SyncPeriod:              40,
						}),
				},
			},
			want:    6,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											CPUUsage:    100,
											CPULimit:    1000,
											MemoryUsage: 480,
											MemoryLimit: 600,
											ReportTime:  time.Now().Unix(),
										}, {
											CPUUsage:    100,
											CPULimit:    1000,
											MemoryUsage: 480,
											MemoryLimit: 600,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 2
						}),
				})
				return patches
			},
		},
		{
			// curInsCount 2, TargetCPUUtilPercentage 80, expectedCount 1
			name: "Scale down: test calExpectedScaleNums with 80% MEMUtilPercentage",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createMEMResourceConfig(
						config.MetricConfig{
							TargetMemUtilPercentage: 80,
							SyncPeriod:              40,
						}),
				},
			},
			want:    1,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											CPUUsage:    100,
											CPULimit:    1000,
											MemoryUsage: 160,
											MemoryLimit: 600,
											ReportTime:  time.Now().Unix(),
										}, {
											CPUUsage:    100,
											CPULimit:    1000,
											MemoryUsage: 200,
											MemoryLimit: 600,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 2
						}),
				})
				return patches
			},
		},
		{
			name: "TargetUtilPercent is 0 return 0",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createMEMResourceConfig(
						config.MetricConfig{
							TargetMemUtilPercentage: 0,
							SyncPeriod:              40,
						}),
				},
			},
			want:    0,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											CPUUsage:    100,
											CPULimit:    1000,
											MemoryUsage: 160,
											MemoryLimit: 600,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 1
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ResourceAvgStrategy{
				functionSvcID: tt.fields.functionSvcID,
				resources:     tt.fields.resources,
			}

			patches := tt.patchesFunc()
			got, err := s.calExpectedScaleNums()
			if (err != nil) != tt.wantErr {
				t.Errorf("calExpectedScaleNums() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("calExpectedScaleNums() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestResAvgStrategy_ScaleByMEMAndCPU_calExpectedScaleNums(t *testing.T) {
	type fields struct {
		functionSvcID string
		resources     []ResourceConfig
	}
	tests := []struct {
		name        string
		fields      fields
		want        int64
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			// Scale up: curInsCount 2, TargetCPUUtilPercentage 32, expectedCount 5, TargetCPUUtilPercentage 80, expectedCount 4, final count 5
			name: "Scale up: test calExpectedScaleNums with CPU expect 5 and mem expect 4 actual 5",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createCPUResourceConfig(
						config.MetricConfig{
							TargetCPUUtilPercentage: 32,
							SyncPeriod:              40,
						}),
					createMEMResourceConfig(
						config.MetricConfig{
							TargetMemUtilPercentage: 40,
							SyncPeriod:              40,
						}),
				},
			},
			want:    5,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											MemoryUsage: 480,
											MemoryLimit: 600,
											CPUUsage:    0.48,
											CPULimit:    600,
											ReportTime:  time.Now().Unix(),
										}, {
											MemoryUsage: 480,
											MemoryLimit: 600,
											CPUUsage:    0.48,
											CPULimit:    600,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 2
						}),
				})
				return patches
			},
		},
		{
			// Scale down: curInsCount 3, TargetCPUUtilPercentage 30, expectedCount 2, TargetMEMUtilPercentage 60, expectedCount 1, final count 2
			name: "Scale down: cpu expect 1 and mem expect 2 actual 2",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createCPUResourceConfig(
						config.MetricConfig{
							TargetCPUUtilPercentage: 30,
							SyncPeriod:              40,
						}),
					createMEMResourceConfig(
						config.MetricConfig{
							TargetMemUtilPercentage: 60,
							SyncPeriod:              40,
						}),
				},
			},
			want:    2,
			wantErr: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetResourceMetricCache,
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{
								"cluster001": {
									"podA": resourceusage.ResourceList{
										{
											MemoryUsage: 108,
											MemoryLimit: 600,
											CPUUsage:    0.108,
											CPULimit:    600,
											ReportTime:  time.Now().Unix(),
										}, {
											MemoryUsage: 108,
											MemoryLimit: 600,
											CPUUsage:    0.108,
											CPULimit:    600,
											ReportTime:  time.Now().Unix(),
										},
										{
											MemoryUsage: 108,
											MemoryLimit: 600,
											CPUUsage:    0.108,
											CPULimit:    600,
											ReportTime:  time.Now().Unix(),
										},
									},
								},
							}
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFuncInstanceCurrentCount,
						func(_ *cache.AutoScalerCache, funcSvcID string) int64 {
							return 3
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ResourceAvgStrategy{
				functionSvcID: tt.fields.functionSvcID,
				resources:     tt.fields.resources,
			}

			patches := tt.patchesFunc()
			got, err := s.calExpectedScaleNums()
			if (err != nil) != tt.wantErr {
				t.Errorf("calExpectedScaleNums() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("calExpectedScaleNums() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}
