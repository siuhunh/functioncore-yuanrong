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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/functiontypes"
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 2, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictDownloadTicker: 10000,
							PredictUploadTicker:   10000,
							PredictFuncWhiteList:  []string{},
							PredictEnable:         true,
							PredictUseResult:      true,
						}}, nil
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
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{PredictEnable: false},
							}, nil
						}),
				})
				return patches
			},
		},
		{
			// curInsCount 10, TargetCPUUtilPercentage 50, actual 40, scaleDown tolerance 0.2, expectedCount 10
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
			want:    10,
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 10, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictDownloadTicker: 10000,
							PredictUploadTicker:   10000,
							PredictFuncWhiteList:  []string{},
							PredictEnable:         true,
							PredictUseResult:      true,
						}}, nil
					}),
				})
				return patches
			},
		},
		{
			// curInsCount 10, TargetCPUUtilPercentage 50, actual 55, scaleUp tolerance 0.1, expectedCount 10
			name: "test calExpectedScaleNums in scale up tolerance, not scale up",
			fields: fields{
				functionSvcID: "test123",
				resources: []ResourceConfig{
					createCPUResourceConfig(
						config.MetricConfig{
							TargetCPUUtilPercentage: 50,
							SyncPeriod:              40,
							ScaleUpTolerance:        0.1,
						}),
				},
			},
			want:    10,
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
											CPUUsage:    0.55,
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 10, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictDownloadTicker: 10000,
							PredictUploadTicker:   10000,
							PredictFuncWhiteList:  []string{},
							PredictEnable:         true,
							PredictUseResult:      true,
						}}, nil
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
			got, _, err := s.calExpectedScaleNums()
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 2, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictFuncWhiteList: []string{},
							PredictEnable:        false,
						}}, nil
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 2, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictFuncWhiteList: []string{},
							PredictEnable:        false,
						}}, nil
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 1, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictFuncWhiteList: []string{},
							PredictEnable:        false,
						}}, nil
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
			got, _, err := s.calExpectedScaleNums()
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 2, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictFuncWhiteList: []string{},
							PredictEnable:        false,
						}}, nil
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
						func(_ *cache.AutoScalerCache, funcSvcID string) (int64, int64) {
							return 3, 0
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{PredictConfig: config.PredictionConfig{
							PredictFuncWhiteList: []string{},
							PredictEnable:        false,
						}}, nil
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
			got, _, err := s.calExpectedScaleNums()
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

func TestResourceAvgStrategy_needUsePredictResult(t *testing.T) {
	type fields struct {
		resources     []ResourceConfig
		functionSvcID string
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
		want        bool
	}{
		{
			"test need predict result success",
			fields{
				resources:     nil,
				functionSvcID: "",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{
									PredictUseResult: true,
								},
							}, nil
						}),
				})
				return patches
			},
			true,
		},
		{
			"test need predict result failed",
			fields{
				resources:     nil,
				functionSvcID: "",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{
									PredictUseResult: true,
								},
							}, errors.New("failed to get config")
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {

		s := &ResourceAvgStrategy{
			resources:     tt.fields.resources,
			functionSvcID: tt.fields.functionSvcID,
		}
		patches := tt.patchesFunc()
		if got := s.needUsePredictResult(); got != tt.want {
			t.Errorf("needUsePredictResult() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()

	}
}

func TestResourceAvgStrategy_calPredictionScaleNums(t *testing.T) {
	type fields struct {
		resources     []ResourceConfig
		functionSvcID string
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockutils.PatchesFunc
		want        int64
	}{
		{
			"",
			fields{
				resources: []ResourceConfig{
					{
						resourceName: CPU, targetResourceUtilPercentage: 70,
					},
					{
						resourceName: MEM, targetResourceUtilPercentage: 30,
					},
				},
				functionSvcID: "",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyMethod(reflect.TypeOf(&cache.AutoScalerCache{}), "GetPredictMetricsOutputCache",
						func(_ *cache.AutoScalerCache, funcSvcID string) resourceusage.PredictResourceList {
							return resourceusage.PredictResourceList{
								{
									CPUUtil:    7,
									MemoryUtil: 700,
									Timestamp:  time.Now().Unix() + 5,
								},
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&cache.AutoScalerCache{}), "GetResourceMetricCache",
						func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.
							PodName]resourceusage.ResourceList {
							return map[resourceusage.ClusterID]map[resourceusage.
								PodName]resourceusage.ResourceList{"az101": {
								"podA": []resourceusage.Resource{{
									CPULimit:    1000,
									ReportTime:  time.Now().Unix() - 10,
									CPUUsage:    0.1,
									MemoryUsage: 300,
									MemoryLimit: 1000,
								}}},
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&cache.AutoScalerCache{}), "GetFunctionMeta",
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{
								ResourceMetaData: functiontypes.ResourceMetaData{
									CPU:    1000,
									Memory: 1000,
								},
							}, true
						}),
				})
				return patches
			},
			10,
		},
	}
	for _, tt := range tests {
		s := &ResourceAvgStrategy{
			resources:     tt.fields.resources,
			functionSvcID: tt.fields.functionSvcID,
		}

		patches := tt.patchesFunc()
		if got := s.calPredictionScaleNums(); got != tt.want {
			t.Errorf("calPredictionScaleNums() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()
	}
}

func TestResourceAvgStrategy_needPredict(t *testing.T) {
	type fields struct {
		resources     []ResourceConfig
		functionSvcID string
	}
	tests := []struct {
		name      string
		fields    fields
		patchFunc mockutils.PatchesFunc
		want      bool
	}{
		{
			"test need predict enable true white list empty",
			fields{
				resources:     nil,
				functionSvcID: "",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{
									PredictEnable:        true,
									PredictFuncWhiteList: []string{},
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
				})
				return patches
			},
			false,
		},
		{
			"test need predict enable true white list not empty",
			fields{
				resources:     nil,
				functionSvcID: "test",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{
									PredictEnable:        true,
									PredictFuncWhiteList: []string{"test"},
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
				})
				return patches
			},
			true,
		},
		{
			"test need predict enable false, white list empty",
			fields{
				resources:     nil,
				functionSvcID: "",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{
									PredictEnable:        false,
									PredictFuncWhiteList: []string{},
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
				})
				return patches
			},
			false,
		},
		{
			"test need predict enable false, white list not empty",
			fields{
				resources:     nil,
				functionSvcID: "",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append([]*gomonkey.Patches{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Config, error) {
							return &config.Config{
								PredictConfig: config.PredictionConfig{
									PredictEnable:        false,
									PredictFuncWhiteList: []string{"test"},
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
						func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
							return &functiontypes.FunctionEtcdInfo{}, true
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {

		s := &ResourceAvgStrategy{
			resources:     tt.fields.resources,
			functionSvcID: tt.fields.functionSvcID,
		}

		patches := tt.patchFunc()
		if got := s.needPredict(); got != tt.want {
			t.Errorf("needPredict() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()

	}
}

func Test_createResourceAvgStrategy(t *testing.T) {
	strategy := createResourceAvgStrategy("funcSvcID1", config.MetricConfig{})
	if len(strategy.resources) != 0 {
		t.Errorf("failed to test create strateResourceAvgStrategy")
	}

	strategy.withCPUResource()
	if len(strategy.resources) != 1 {
		t.Errorf("failed to test create strateResourceAvgStrategy")
	}

	strategy.withMemoryResource()
	if len(strategy.resources) != 2 {
		t.Errorf("failed to test create strateResourceAvgStrategy")
	}

	strategy.withExtendStrategies([]functiontypes.ExtendScaleStrategy{})
	if len(strategy.resources) != 2 {
		t.Errorf("failed to test create strateResourceAvgStrategy")
	}

	strategy.withExtendStrategies([]functiontypes.ExtendScaleStrategy{{ValType: "value", MetricType: "system"}})
	if len(strategy.resources) != 2 {
		t.Errorf("failed to test create strateResourceAvgStrategy")
	}

}

func TestResourceAvgStrategy_calResourceUsedWithOutLimit(t *testing.T) {
	s := &ResourceAvgStrategy{
		resources:     []ResourceConfig{},
		functionSvcID: "funcSvcID1",
		config:        config.MetricConfig{},
	}
	averageResUsedPercentage, err := s.calResourceUsedWithoutLimit(ResourceConfig{})
	if averageResUsedPercentage != 0 || err == nil {
		t.Errorf("failed to test calResourceUsedWithOutLimit")
	}

	s = &ResourceAvgStrategy{
		resources: []ResourceConfig{
			{
				resourceName:                 MEM,
				targetResourceUtilPercentage: 80,
				scaleUpTolerance:             0.1,
				scaleDownTolerance:           0.1,
				metricType:                   0,
			},
		},
		functionSvcID: "funcSvcID1",
		config:        config.MetricConfig{},
	}
	averageResUsedPercentage, err = s.calResourceUsedWithoutLimit(ResourceConfig{
		targetResourceUtilPercentage: 100,
	})

	if averageResUsedPercentage != 0 || err != nil {
		t.Errorf("failed to test calResourceUsedWithOutLimit")
	}

	patches := mockutils.InitPatchSlice()
	patches.Append(mockutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&cache.AutoScalerCache{}), "GetResourceMetricCache",
			func(_ *cache.AutoScalerCache, funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.
				PodName]resourceusage.ResourceList {
				return map[resourceusage.ClusterID]map[resourceusage.
					PodName]resourceusage.ResourceList{"az101": {
					"podA": []resourceusage.Resource{{
						CPULimit:                      1000,
						ReportTime:                    time.Now().Unix() - 10,
						CPUUsage:                      0.1,
						MemoryUsage:                   300,
						MemoryLimit:                   1000,
						ReceiveBytesPerSecond:         100,
						ReceivePackagePerSecond:       100,
						TransmitBytesPerSecond:        100,
						TransmitPackagePerSecond:      100,
						FileSystemReadBytesPerSecond:  100,
						FileSystemWriteBytesPerSecond: 100,
					}}},
				}
			}),
	})
	s = &ResourceAvgStrategy{
		resources: []ResourceConfig{
			{
				resourceName:                 FileSystemReadRate,
				targetResourceUtilPercentage: 80,
				scaleUpTolerance:             0.1,
				scaleDownTolerance:           0.1,
				metricType:                   0,
			},
		},
		functionSvcID: "funcSvcID1",
		config:        config.MetricConfig{},
	}
	averageResUsedPercentage, err = s.calResourceUsedWithoutLimit(ResourceConfig{
		targetResourceUtilPercentage: 100,
		metricType:                   MetricTypeSystem,
		resourceName:                 FileSystemReadRate,
	})
	if averageResUsedPercentage != 100 || err != nil {
		t.Errorf("failed to test calResourceUsedWithOutLimit")
	}

	patches.ResetAll()
}

func TestResourceAvgStrategy_calResourceUsedFromCaeMetrics(t *testing.T) {
	s := ResourceAvgStrategy{
		resources: []ResourceConfig{
			{
				targetResourceUtilPercentage: 100,
			},
		},
	}

	cache.GetInstance().RefreshCaeMetricCache(map[resourceusage.ClusterID]map[resourceusage.
		FuncSvcID]map[resourceusage.PodName]caemetrics.List{
		"cluster001": {
			"funcSvc1": {
				"podA": {
					{
						FileSystemWriteRate: 100,
						ReportTime:          time.Now().Unix(),
					},
				},
			},
		},
	})
	_, err := s.calResourceUsedFromCaeMetrics(ResourceConfig{
		targetResourceUtilPercentage: 100,
		metricType:                   MetricTypeSystem,
		resourceName:                 FileSystemWriteRate,
	})

	if err != nil {
		t.Errorf("failed to test calResourceUsedFromCaeMetrics")
	}
}
