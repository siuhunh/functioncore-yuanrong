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

package poolmanager

import (
	"fmt"
	"math"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	mockUtils "functioncore/test/utils"
)

func Test_newScalingConfig(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				ScalingUpTriggerTimeThreshold: 5,
				EnableFusionPolicy:            0,
			},
		}, nil
	}
	type args struct {
		poolSize int32
		standard standard
	}
	mockPoolSize := int32(1)
	mockStandard := standard("pool22")
	tests := []struct {
		name        string
		args        args
		want        *ScalingConfig
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test001",
			args: args{poolSize: mockPoolSize, standard: mockStandard},
			want: &ScalingConfig{
				elasticScalingCfg: &config.ElasticScalingConfig{
					UsedPodNumsExceedPercentage:      0.5,
					UsedPodNumsExceedFreePercentage:  0.75,
					UsedPodNumsExceedHeavyPercentage: 0.5,
					ScaleUpPodNumsPercentage:         0.25,
					ScaleUpPodNumsFreePercentage:     0.25,
					ScaleUpPodNumsHeavyPercentage:    0.5,
					ExtraScaleUpPodNumsPercentage:    0.1,
					UsedPodNumsBelowPercentage:       0.1,
					ScaleDownPodNumsPercentage:       0.125,
					EffectiveThresholdReplicas:       0,
					MaxScaleUpPodNums:                600,
					ScaleDownInterval:                60,
					ThresholdResourceUsage:           0.75,
				},
				replicas:                  mockPoolSize,
				basePoolSize:              mockPoolSize,
				standard:                  mockStandard,
				poolSizeLimit:             math.MaxInt32,
				scalingUpTriggerThreshold: 5,
				enableFusionPolicy:        0,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		},
	}
	os.Setenv("ELASTIC_SCALING_CONFIG", `{
		"usedPodNumsExceedPercentage": 0.5,
		"usedPodNumsExceedFreePercentage": 0.75,
		"usedPodNumsExceedHeavyPercentage": 0.5,
		"scaleUpPodNumsPercentage": 0.25,
		"scaleUpPodNumsFreePercentage": 0.25,
		"scaleUpPodNumsHeavyPercentage": 0.5,
		"extraScaleUpPodNumsPercentage": 0.1,
		"usedPodNumsBelowPercentage": 0.1,
		"scaleDownPodNumsPercentage": 0.125,
		"effectiveThresholdReplicas": 0,
		"maxScaleUpPodNums": 600,
		"scaleDownInterval": 60,
		"thresholdResourceUsage": 0.75
	}`)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := newScalingConfig(tt.args.poolSize, math.MaxInt32, tt.args.standard); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newScalingConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestScalingConfig_monitorScalingDown(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
	}
	type args struct {
		stopCh chan struct{}
	}
	mockPoolSize := int32(1)
	mockStandard := standard("pool22")
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "test001", fields: fields{
			elasticScalingCfg: &config.ElasticScalingConfig{
				UsedPodNumsExceedPercentage:      0.5,
				UsedPodNumsExceedFreePercentage:  0.75,
				UsedPodNumsExceedHeavyPercentage: 0.5,
				ScaleUpPodNumsPercentage:         0.25,
				ScaleUpPodNumsFreePercentage:     0.25,
				ScaleUpPodNumsHeavyPercentage:    0.5,
				ExtraScaleUpPodNumsPercentage:    0.1,
				UsedPodNumsBelowPercentage:       0.1,
				ScaleDownPodNumsPercentage:       0.125,
				EffectiveThresholdReplicas:       0,
				MaxScaleUpPodNums:                600,
				ScaleDownInterval:                60,
				ThresholdResourceUsage:           0.75,
			},
			replicas:     mockPoolSize,
			basePoolSize: mockPoolSize,
			standard:     mockStandard,
		}, args: args{stopCh: make(chan struct{})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			go sc.monitorScalingDown(tt.args.stopCh)
			time.Sleep(time.Second)
			close(tt.args.stopCh)
			time.Sleep(time.Second)
		})
	}
}

func TestScalingConfig_monitorScalingDown_noStopCh(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
	}
	type args struct {
		stopCh chan struct{}
	}
	mockPoolSize := int32(1)
	mockStandard := standard("pool22")
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "test001", fields: fields{
			elasticScalingCfg: nil,
			replicas:          mockPoolSize,
			basePoolSize:      mockPoolSize,
			standard:          mockStandard,
		}, args: args{stopCh: nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			sc.monitorScalingDown(tt.args.stopCh)
		})
	}
}

func TestScalingConfig_monitorScalingDown_noCfg(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
	}
	type args struct {
		stopCh chan struct{}
	}
	mockPoolSize := int32(1)
	mockStandard := standard("pool22")
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{name: "test001", fields: fields{
			elasticScalingCfg: nil,
			replicas:          mockPoolSize,
			basePoolSize:      mockPoolSize,
			standard:          mockStandard,
		}, args: args{stopCh: make(chan struct{})}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			sc.monitorScalingDown(tt.args.stopCh)
		})
	}
}

func TestScalingConfig_setScalingCfg(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
	}
	type args struct {
		loadCondition  bool
		ratioCondition bool
	}
	f := fields{
		elasticScalingCfg:         &config.ElasticScalingConfig{},
		standard:                  "",
		replicas:                  0,
		waitingGeneralizedReqNums: 0,
		basePoolSize:              0,
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name:   "case_01_loadCondition_ratioCondition",
			fields: f,
			args:   args{true, true},
		},
		{
			name:   "case_02_loadCondition_not_ratioCondition",
			fields: f,
			args:   args{true, false},
		},
		{
			name:   "case_03_not_loadCondition_ratioCondition",
			fields: f,
			args:   args{false, true},
		},
		{
			name:   "case_04_not_loadCondition_not_ratioCondition",
			fields: f,
			args:   args{false, false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			sc.setScalingCfg(tt.args.loadCondition, tt.args.ratioCondition)
		})
	}
}

func Test_checkCondition(t *testing.T) {
	type args struct {
		req common.PodRequest
	}
	tests := []struct {
		name  string
		args  args
		want  bool
		want1 bool
	}{
		{
			name:  "case_01_currentWorkerNum_zero",
			args:  args{common.PodRequest{CurrentWorkerNum: 0, ScalingWorkersNum: 0}},
			want:  false,
			want1: false,
		}, {
			name:  "case_02_more_than_half",
			args:  args{common.PodRequest{CurrentWorkerNum: 1, ScalingWorkersNum: 3}},
			want:  true,
			want1: false,
		}, {
			name:  "case_03_other_scenario",
			args:  args{common.PodRequest{CurrentWorkerNum: 2, ScalingWorkersNum: -1}},
			want:  false,
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := checkCondition(tt.args.req)
			if got != tt.want {
				t.Errorf("checkCondition() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("checkCondition() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestScalingConfig_getScaleUpNums(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
	}
	tests := []struct {
		name   string
		fields fields
		want   int32
	}{
		{
			name:   "case_01_smaller_or_equal",
			fields: fields{&config.ElasticScalingConfig{UsedPodNumsExceedPercentage: 0.7}, "500-500-python3", 2, 1, 100},
			want:   0,
		}, {
			name:   "case_02_other_scenario",
			fields: fields{&config.ElasticScalingConfig{UsedPodNumsExceedPercentage: 0.7, ScaleUpPodNumsPercentage: 0.8}, "500-500-python3", 2, 5, 100},
			want:   2,
		}, {
			name:   "case_03_pool_scale_up_from_zero",
			fields: fields{&config.ElasticScalingConfig{UsedPodNumsExceedPercentage: 0.7, ScaleUpPodNumsPercentage: 0.8, MaxTransitionScaleUpPercentage: 1.0}, "500-500-python3", 0, 5, 0},
			want:   5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			if got := sc.getScaleUpNums(); got != tt.want {
				t.Errorf("getScaleUpNums() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScalingConfig_getScaleDownNums(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
	}
	tests := []struct {
		name   string
		fields fields
		want   int32
	}{
		{
			name:   "case_01_greater_or_equal",
			fields: fields{&config.ElasticScalingConfig{UsedPodNumsBelowPercentage: 0.7}, "500-500-python3", 2, 4, 100},
			want:   0,
		}, {
			name:   "case_02_other_scenario",
			fields: fields{&config.ElasticScalingConfig{UsedPodNumsBelowPercentage: 0.7, ScaleDownPodNumsPercentage: 0.8}, "500-500-python3", 2, 1, 100},
			want:   2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			if got := sc.getScaleDownNums(); got != tt.want {
				t.Errorf("getScaleDownNums() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScalingConfig_checkScaleDown(t *testing.T) {
	type fields struct {
		replicas     int32
		basePoolSize int32
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 scaleDownNums is zero",
			fields: fields{replicas: 3, basePoolSize: 10},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ScalingConfig).getScaleDownNums,
						func(_ *ScalingConfig) int32 {
							return 0
						}),
				})
				return patches
			},
		}, {
			name:   "case_02 no need to scale down",
			fields: fields{replicas: 3, basePoolSize: 3},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ScalingConfig).getScaleDownNums,
						func(_ *ScalingConfig) int32 {
							return 1
						}),
				})
				return patches
			},
		}, {
			name:   "case_03 other scenario",
			fields: fields{replicas: 4, basePoolSize: 10},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ScalingConfig).getScaleDownNums,
						func(_ *ScalingConfig) int32 {
							return 1
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sc := &ScalingConfig{
				replicas:     tt.fields.replicas,
				basePoolSize: tt.fields.basePoolSize,
			}
			sc.checkScaleDown()
			patches.ResetAll()
		})
	}
}

func TestScalingConfig_scaleUp(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
		poolSizeLimit             int32
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 no need to scale up",
			fields: fields{elasticScalingCfg: &config.ElasticScalingConfig{MaxScaleUpPodNums: 2}, replicas: 3, basePoolSize: 10, poolSizeLimit: 0},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name:   "case_02 scaleUpNums zero",
			fields: fields{elasticScalingCfg: &config.ElasticScalingConfig{MaxScaleUpPodNums: 4}, replicas: 3, waitingGeneralizedReqNums: 4, basePoolSize: 10, poolSizeLimit: 0},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ScalingConfig).getScaleUpNums,
						func(_ *ScalingConfig) int32 {
							return 0
						}),
				})
				return patches
			},
		}, {
			name:   "case_03 reach scaleUpNums limits",
			fields: fields{elasticScalingCfg: &config.ElasticScalingConfig{MaxScaleUpPodNums: 4}, replicas: 3, waitingGeneralizedReqNums: 4, basePoolSize: 2, poolSizeLimit: 3},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ScalingConfig).getScaleUpNums,
						func(_ *ScalingConfig) int32 {
							return 0
						}),
				})
				return patches
			},
		}, {
			name:   "case_04 other scenario",
			fields: fields{elasticScalingCfg: &config.ElasticScalingConfig{MaxScaleUpPodNums: 4, ExtraScaleUpPodNumsPercentage: 0.6}, replicas: 3, waitingGeneralizedReqNums: 4, basePoolSize: 10},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*ScalingConfig).getScaleUpNums,
						func(_ *ScalingConfig) int32 {
							return 10
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
				poolSizeLimit:             tt.fields.poolSizeLimit,
			}
			sc.scaleUp()
			patches.ResetAll()
		})
	}
}

func TestScalingConfig_considerScaleUp(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		replicas                  int32
		waitingGeneralizedReqNums int32
	}
	type args struct {
		req    common.PodRequest
		labels map[string]string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 elasticScalingCfg nil",
			fields: fields{elasticScalingCfg: nil},
			args:   args{req: common.PodRequest{}, labels: make(map[string]string, 1)},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name:   "case_02 cluster average usage exceeded limit",
			fields: fields{elasticScalingCfg: &config.ElasticScalingConfig{ThresholdResourceUsage: 0.7}, replicas: 3},
			args:   args{req: common.PodRequest{}, labels: make(map[string]string, 1)},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(cache.IsOverload,
						func(threshold float64, filters ...cache.Filter) bool {
							return true
						}),
				})
				return patches
			},
		}, {
			name:   "case_03 other scenario",
			fields: fields{elasticScalingCfg: &config.ElasticScalingConfig{ThresholdResourceUsage: 0.7}, replicas: 3},
			args:   args{req: common.PodRequest{}, labels: make(map[string]string, 1)},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(cache.IsOverload,
						func(threshold float64, filters ...cache.Filter) bool {
							return false
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
			}
			sc.considerScaleUp(tt.args.req, tt.args.labels)
			patches.ResetAll()
		})
	}
}

func TestPool_updateReplicasWithExpect(t *testing.T) {
	type fields struct {
		scalingConfig *ScalingConfig
	}
	type args struct {
		expectReplicas int32
		oldReplicas    int32
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        int32
		want1       int32
	}{
		{
			name:   "case_01 expectReplicas equal to oldReplicas",
			fields: fields{scalingConfig: &ScalingConfig{replicas: 3}},
			args:   args{2, 2},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			want:  3,
			want1: 2,
		}, {
			name:   "case_02 expectReplicas not equal to oldReplicas, but update pool replicas failed",
			fields: fields{scalingConfig: &ScalingConfig{replicas: 3}},
			args:   args{3, 2},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PoolMgr).updateDeployReplicas,
						func(_ *PoolMgr, pool *Pool, replicas int32) error {
							return fmt.Errorf("error")
						}),
				})
				return patches
			},
			want:  3,
			want1: 2,
		}, {
			name:   "case_03 expectReplicas not equal to oldReplicas, and update pool replicas success",
			fields: fields{scalingConfig: &ScalingConfig{replicas: 3}},
			args:   args{3, 2},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PoolMgr).updateDeployReplicas,
						func(_ *PoolMgr, pool *Pool, replicas int32) error {
							return nil
						}),
				})
				return patches
			},
			want:  3,
			want1: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &Pool{
				scalingConfig: tt.fields.scalingConfig,
			}
			got, got1 := p.updateReplicasWithExpect(tt.args.expectReplicas, tt.args.oldReplicas)
			if got != tt.want {
				t.Errorf("updateReplicasWithExpect() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("updateReplicasWithExpect() got1 = %v, want %v", got1, tt.want1)
			}
			patches.ResetAll()
		})
	}
}

func TestPool_monitorPoolReplicas(t *testing.T) {
	type fields struct {
		scalingConfig *ScalingConfig
	}
	type args struct {
		stopCh chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 stopCh nil",
			fields: fields{scalingConfig: &ScalingConfig{basePoolSize: 10, replicas: 3}},
			args:   args{stopCh: nil},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name:   "case_02 stopCh not nil",
			fields: fields{scalingConfig: &ScalingConfig{basePoolSize: 10, replicas: 3}},
			args:   args{stopCh: make(chan struct{}, 1)},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc((*Pool).updateReplicasWithExpect,
						func(_ *Pool, expectReplicas, oldReplicas int32) (int32, int32) {
							return 3, 2
						}),
				})
				return patches
			},
		}, {
			name:   "case_03 replicas is zero, monitor exit",
			fields: fields{scalingConfig: &ScalingConfig{basePoolSize: 0, replicas: 0}},
			args:   args{stopCh: make(chan struct{}, 1)},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
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
			p := &Pool{
				scalingConfig: tt.fields.scalingConfig,
				mux:           &sync.Mutex{},
			}
			if tt.args.stopCh != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
			}
			p.monitorPoolReplicas(tt.args.stopCh)
			patches.ResetAll()
		})
	}
}

func TestResizeReplicaNumberInProactiveMode(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				LowerBoundFactor: 0.3,
				UpperBoundFactor: 1.2,
			},
		}, nil
	}
	type fields struct {
		sc *ScalingConfig
	}
	mockBasePoolSize := int32(100)
	mockPoolSizeLimit := int32(300)
	mockStandard := standard("pool23")
	tests := []struct {
		name        string
		fields      fields
		expected    int32
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 when scaling plan lower than lower bound, resize it to lower bound",
			fields: fields{
				sc: &ScalingConfig{
					scalingPlan:   []int64{20},
					replicas:      mockBasePoolSize,
					basePoolSize:  mockBasePoolSize,
					standard:      mockStandard,
					poolSizeLimit: mockPoolSizeLimit,
				},
			},
			expected: 30,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name: "case 2 when scaling plan larger than upper bound, resize it to upper bound",
			fields: fields{
				sc: &ScalingConfig{
					scalingPlan:   []int64{400},
					replicas:      mockBasePoolSize,
					basePoolSize:  mockBasePoolSize,
					standard:      mockStandard,
					poolSizeLimit: mockPoolSizeLimit,
				},
			},
			expected: 360,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name: "case 3 when scaling plan within lower and upper bound, return its original value",
			fields: fields{
				sc: &ScalingConfig{
					scalingPlan:   []int64{200},
					replicas:      mockBasePoolSize,
					basePoolSize:  mockBasePoolSize,
					standard:      mockStandard,
					poolSizeLimit: mockPoolSizeLimit,
				},
			},
			expected: 200,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			actual := tt.fields.sc.resizeReplicaInProactiveMode()
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("monitor scale down function actual = %d, want %d", actual, tt.expected)
			}
			patches.ResetAll()
		})
	}
}

func TestProactiveScalingUpTriggerCounter(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				CooldownTime: 15,
			},
		}, nil
	}
	type fields struct {
		poolStandard              standard
		cooldownPeriod            int32
		enableFusionPolicy        int32
		scalingUpTriggerCounter   int32
		scalingUpTriggerThreshold int32
		elasticScalingCfg         *config.ElasticScalingConfig
		scalingPlan               []int64
		predictionMode            int32
	}
	type expected struct {
		esc             *config.ElasticScalingConfig
		expectedCounter int32
	}
	mockProactiveElasticConfig := &config.ElasticScalingConfig{}
	mockReactiveElasticConfig := &config.ElasticScalingConfig{}
	var mockExpectedReplica int32
	mockExpectedReplica = 64
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		expected    expected
	}{
		{
			name: "case 1 no need to change counter if in reactive mode",
			fields: fields{
				elasticScalingCfg: mockReactiveElasticConfig,
			},
			expected: expected{
				expectedCounter: 0,
				esc:             mockReactiveElasticConfig,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name: "case 2 no need to change counter if in cooldown time",
			fields: fields{
				cooldownPeriod:    15,
				elasticScalingCfg: mockReactiveElasticConfig,
			},
			expected: expected{
				expectedCounter: 0,
				esc:             mockReactiveElasticConfig,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		},
		{
			name: "case 3 no need to change counter if turning off fusion policy",
			fields: fields{
				enableFusionPolicy: 0,
				elasticScalingCfg:  mockReactiveElasticConfig,
				scalingPlan:        []int64{10, 20},
				predictionMode:     1,
			},
			expected: expected{
				expectedCounter: 0,
				esc:             mockReactiveElasticConfig,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name: "case 4 update scaling up time for every scaling up operation",
			fields: fields{
				enableFusionPolicy:        1,
				cooldownPeriod:            0,
				scalingUpTriggerCounter:   0,
				scalingUpTriggerThreshold: 3,
				elasticScalingCfg:         mockProactiveElasticConfig,
				scalingPlan:               []int64{5, 6},
				predictionMode:            1,
			},
			expected: expected{
				expectedCounter: 1,
				esc:             mockProactiveElasticConfig,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name: "case 5 reset scaling up time if the counter larger than threshold and change into reactive mode",
			fields: fields{
				enableFusionPolicy:        1,
				cooldownPeriod:            0,
				scalingUpTriggerCounter:   3,
				scalingUpTriggerThreshold: 3,
				elasticScalingCfg:         mockProactiveElasticConfig,
				scalingPlan:               []int64{5, 6},
				predictionMode:            1,
			},
			expected: expected{
				expectedCounter: 0,
				esc:             mockReactiveElasticConfig,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								ProactiveElasticScalingConfig: mockProactiveElasticConfig,
								PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
									CooldownTime: 15,
								},
							}, nil
						},
					),
					gomonkey.ApplyFunc(config.GetElasticScalingConfig,
						func() *config.ElasticScalingConfig {
							return mockReactiveElasticConfig
						},
					),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.poolStandard,
				cooldownPeriod:            tt.fields.cooldownPeriod,
				enableFusionPolicy:        tt.fields.enableFusionPolicy,
				scalingUpTriggerCounter:   tt.fields.scalingUpTriggerCounter,
				scalingUpTriggerThreshold: tt.fields.scalingUpTriggerThreshold,
				scalingPlan:               tt.fields.scalingPlan,
				proactiveMode:             tt.fields.predictionMode,
			}
			sc.checkScalingUpCounterIfInProactiveMode(mockExpectedReplica)
			if !reflect.DeepEqual(sc.scalingUpTriggerCounter, tt.expected.expectedCounter) {
				t.Errorf("checkScalingUpCounterIfInProactiveMode, counter: actual %d, want %d",
					sc.scalingUpTriggerCounter, tt.expected.expectedCounter)
			}
			if !reflect.DeepEqual(sc.elasticScalingCfg, tt.expected.esc) {
				t.Errorf("checkScalingUpCounterIfInProactiveMode, scaling config: actual %v, want %v",
					sc.elasticScalingCfg, tt.expected.esc)
			}
			patches.ResetAll()
		})
	}
}

func TestSkippingScalingDown(t *testing.T) {
	type fields struct {
		elasticScalingCfg         *config.ElasticScalingConfig
		standard                  standard
		replicas                  int32
		waitingGeneralizedReqNums int32
		basePoolSize              int32
		ScalingPlanAvailable      bool
	}
	type args struct {
		stopCh chan struct{}
	}
	patch := gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
		ch := make(chan time.Time, 4)
		for i := 0; i < 4; i++ {
			ch <- time.Time{}
		}
		return &time.Ticker{C: ch}
	})
	defer func() {
		patch.Reset()
		scTicker = make(map[standard]*time.Ticker)
	}()
	mockPoolSize := int32(100)
	mockStandard1 := standard("pool23")
	mockStandard2 := standard("pool24")
	tests := []struct {
		name        string
		fields      fields
		args        args
		expected    int32
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 when scaling plan is available, skip scale down check",
			fields: fields{
				elasticScalingCfg: &config.ElasticScalingConfig{
					EffectiveThresholdReplicas: -1,
					ScaleDownInterval:          1,
					UsedPodNumsBelowPercentage: 0.3,
					ScaleDownPodNumsPercentage: 0.3,
				},
				replicas:             mockPoolSize,
				basePoolSize:         mockPoolSize,
				standard:             mockStandard1,
				ScalingPlanAvailable: true,
			},
			args:     args{stopCh: make(chan struct{})},
			expected: 100,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(snapshotmanager.GetSnapshotManagerMode, func() bool {
						return false
					}),
				})
				return patches
			},
		}, {
			name: "case 2 when scaling plan is not available, check if need scale down",
			fields: fields{
				elasticScalingCfg: &config.ElasticScalingConfig{
					EffectiveThresholdReplicas: -1,
					ScaleDownInterval:          1,
					UsedPodNumsBelowPercentage: 0.3,
					ScaleDownPodNumsPercentage: 0.3,
				},
				replicas:             mockPoolSize,
				basePoolSize:         mockPoolSize / 2,
				standard:             mockStandard2,
				ScalingPlanAvailable: false,
			},
			args:     args{stopCh: make(chan struct{})},
			expected: mockPoolSize / 2,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(snapshotmanager.GetSnapshotManagerMode, func() bool {
						return false
					}),
				})
				return patches
			},
		}, {
			name: "case 3 when replica less than EffectiveThresholdReplicas, skip scale down check",
			fields: fields{
				elasticScalingCfg: &config.ElasticScalingConfig{
					EffectiveThresholdReplicas: mockPoolSize + 1,
					ScaleDownInterval:          1,
					UsedPodNumsBelowPercentage: 0.3,
					ScaleDownPodNumsPercentage: 0.3,
				},
				replicas:             mockPoolSize,
				basePoolSize:         mockPoolSize / 2,
				standard:             mockStandard2,
				ScalingPlanAvailable: false,
			},
			args:     args{stopCh: make(chan struct{})},
			expected: 100,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(snapshotmanager.GetSnapshotManagerMode, func() bool {
						return false
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := &ScalingConfig{
				elasticScalingCfg:         tt.fields.elasticScalingCfg,
				standard:                  tt.fields.standard,
				replicas:                  tt.fields.replicas,
				waitingGeneralizedReqNums: tt.fields.waitingGeneralizedReqNums,
				basePoolSize:              tt.fields.basePoolSize,
			}
			patches := tt.patchesFunc()
			go sc.monitorScalingDown(tt.args.stopCh)
			time.Sleep(50 * time.Millisecond)
			close(tt.args.stopCh)
			if !reflect.DeepEqual(sc.replicas, tt.expected) {
				t.Errorf("monitor scale down function actual = %d, want %d",
					sc.replicas, tt.expected)
			}
			time.Sleep(50 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestApplyScalingPlanPeriodically(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				ApplyPredictionResultToPoolInterval: 1,
			},
		}, nil
	}
	tickerFunc := func(d time.Duration) *time.Ticker {
		c := make(chan time.Time, 1)
		t := &time.Ticker{
			C: c,
		}
		c <- time.Now()
		return t
	}
	type fields struct {
		elasticScalingCfg    *config.ElasticScalingConfig
		standard             standard
		replicas             int32
		basePoolSize         int32
		scalingPlanAvailable bool
		scalingPlan          []int64
		cooldownPeriod       int32
		proactiveMode        int32
	}
	type args struct {
		stopCh chan struct{}
	}
	type expected struct {
		esc             *config.ElasticScalingConfig
		expectedReplica int32
	}
	mockPoolSize := int32(100)
	expectedPoolSize := int64(130)
	mockStandard := standard("pool23")
	mockCooldownPeriod := int32(15)
	mockProactiveElasticConfig := &config.ElasticScalingConfig{}
	mockReactiveElasticConfig := &config.ElasticScalingConfig{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		expected    expected
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case 1 check stopCh is nil",
			fields: fields{},
			args:   args{stopCh: nil},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name: "case 2 when scaling plan is nil, keep base size and reactive elastic config",
			fields: fields{
				elasticScalingCfg:    &config.ElasticScalingConfig{},
				replicas:             mockPoolSize,
				basePoolSize:         mockPoolSize,
				standard:             mockStandard,
				scalingPlanAvailable: true,
				scalingPlan:          []int64{},
				proactiveMode:        1,
			},
			args: args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
					gomonkey.ApplyFunc(time.NewTicker, tickerFunc),
					gomonkey.ApplyFunc(config.GetElasticScalingConfig,
						func() *config.ElasticScalingConfig {
							return mockReactiveElasticConfig
						},
					),
				})
				return patches
			},
			expected: expected{
				expectedReplica: mockPoolSize,
				esc:             mockReactiveElasticConfig,
			},
		}, {
			name: "case 3 avoid changing replica and keep reactive elastic config when dropped into cooldown periods",
			fields: fields{
				elasticScalingCfg:    &config.ElasticScalingConfig{},
				replicas:             mockPoolSize,
				basePoolSize:         mockPoolSize,
				standard:             mockStandard,
				scalingPlanAvailable: true,
				scalingPlan:          []int64{},
				cooldownPeriod:       mockCooldownPeriod,
			},
			args: args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
					gomonkey.ApplyFunc(time.NewTicker, tickerFunc),
					gomonkey.ApplyFunc(config.GetElasticScalingConfig,
						func() *config.ElasticScalingConfig {
							return mockReactiveElasticConfig
						},
					),
				})
				return patches
			},
			expected: expected{
				expectedReplica: mockPoolSize,
				esc:             mockReactiveElasticConfig,
			},
		}, {
			name: "case 4 apply scaling plan to replica successfully and use proactive elastic config",
			fields: fields{
				elasticScalingCfg:    &config.ElasticScalingConfig{},
				replicas:             mockPoolSize,
				basePoolSize:         mockPoolSize,
				standard:             mockStandard,
				scalingPlanAvailable: true,
				scalingPlan:          []int64{110, 120, expectedPoolSize, expectedPoolSize, expectedPoolSize},
				proactiveMode:        1,
			},
			args: args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
									ApplyPredictionResultToPoolInterval: 1,
								},
								ProactiveElasticScalingConfig: mockProactiveElasticConfig,
							}, nil
						},
					),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 10)
						t := &time.Ticker{
							C: c,
						}
						for i := 0; i < 5; i++ {
							c <- time.Now()
						}
						return t
					}),
					gomonkey.ApplyFunc((*ScalingConfig).resizeReplicaInProactiveMode,
						func(sc *ScalingConfig) int32 {
							if sc.scalingPlan[0] == expectedPoolSize {
								return int32(expectedPoolSize)
							}
							return int32(sc.scalingPlan[0])
						},
					),
				})
				return patches
			},
			expected: expected{
				expectedReplica: int32(expectedPoolSize),
				esc:             mockProactiveElasticConfig,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sc := &ScalingConfig{
				elasticScalingCfg: tt.fields.elasticScalingCfg,
				standard:          tt.fields.standard,
				replicas:          tt.fields.replicas,
				basePoolSize:      tt.fields.basePoolSize,
				scalingPlan:       tt.fields.scalingPlan,
				cooldownPeriod:    tt.fields.cooldownPeriod,
				proactiveMode:     tt.fields.proactiveMode,
			}
			go sc.applyScalingPlanPeriodically(tt.args.stopCh, mockStandard)
			if tt.args.stopCh != nil {
				timer := time.NewTimer(100 * time.Millisecond)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
			}
			time.Sleep(150 * time.Millisecond)
			if !reflect.DeepEqual(sc.replicas, tt.expected.expectedReplica) {
				t.Errorf("apply prediction result to replica config, reolica: actual = %d, want %d",
					sc.replicas, tt.expected.expectedReplica)
			}
			if !reflect.DeepEqual(sc.elasticScalingCfg, tt.expected.esc) {
				t.Errorf("apply prediction result to replica config, scaling config: actual %v, want %v",
					sc.elasticScalingCfg, tt.expected.esc)
			}
			patches.ResetAll()
		})
	}
}

func TestResetScalingUpCounter(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				ApplyPredictionResultToPoolInterval: 1,
			},
		}, nil
	}
	mockTickerFunc := func(d time.Duration) *time.Ticker {
		c := make(chan time.Time, 1)
		t := &time.Ticker{
			C: c,
		}
		c <- time.Now()
		return t
	}
	type fields struct {
		enableFusionPolicy int32
		counter            int32
		proactiveMode      int32
	}
	type args struct {
		stopCh   chan struct{}
		standard standard
	}
	mockStandard := standard("pool23")
	tests := []struct {
		name        string
		fields      fields
		args        args
		expected    int32
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case 1 check stopCh is nil",
			fields: fields{},
			args: args{
				stopCh:   nil,
				standard: mockStandard,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			name: "case 2 reset reactive scaling up counter successfully",
			fields: fields{
				enableFusionPolicy: 1,
				counter:            1,
				proactiveMode:      1,
			},
			args: args{
				stopCh:   make(chan struct{}),
				standard: mockStandard,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
					gomonkey.ApplyFunc(time.NewTicker, mockTickerFunc),
				})
				return patches
			},
			expected: 0,
		}, {
			name: "case 3 skip resetting when turning off enable-fusion-flag",
			fields: fields{
				enableFusionPolicy: 0,
				proactiveMode:      1,
			},
			args: args{
				stopCh:   make(chan struct{}),
				standard: mockStandard,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
					gomonkey.ApplyFunc(time.NewTicker, mockTickerFunc),
				})
				return patches
			},
			expected: 0,
		}, {
			name: "case 4 skip resetting when within reactive mode",
			fields: fields{
				proactiveMode: 0,
			},
			args: args{
				stopCh:   make(chan struct{}),
				standard: mockStandard,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, mockGetConfigFunc),
					gomonkey.ApplyFunc(time.NewTicker, mockTickerFunc),
				})
				return patches
			},
			expected: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			sc := &ScalingConfig{
				enableFusionPolicy: tt.fields.enableFusionPolicy,
				proactiveMode:      tt.fields.proactiveMode,
			}
			go sc.resetScalingUpCounter(tt.args.stopCh, tt.args.standard)
			if tt.args.stopCh != nil {
				timer := time.NewTimer(100 * time.Millisecond)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
			}
			if !reflect.DeepEqual(sc.scalingUpTriggerCounter, tt.expected) {
				t.Errorf("reset scaling up counter: actual %d, want %d",
					sc.scalingUpTriggerCounter, tt.expected)
			}
			time.Sleep(150 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_hotLoadScaleDownTicker(t *testing.T) {
	convey.Convey("hotload ScaleDownInterval", t, func() {
		oldConfig := &config.FaasConfig{ElasticScalingConfig: &config.ElasticScalingConfig{
			ScaleDownInterval: 60,
		}}
		newConfig := &config.HotLoadConfig{ElasticScalingConfig: &config.ElasticScalingConfig{
			ScaleDownInterval: 1,
		}}
		defer func() {
			scTicker = make(map[standard]*time.Ticker)
		}()
		scTicker = map[standard]*time.Ticker{
			"aaaa": nil,
			"bbb":  time.NewTicker(time.Minute),
		}
		convey.ShouldNotEqual(oldConfig.ElasticScalingConfig.ScaleDownInterval, newConfig.ElasticScalingConfig.ScaleDownInterval)
		hotLoadScaleDownTicker(oldConfig, newConfig)
		convey.ShouldEqual(oldConfig.ElasticScalingConfig.ScaleDownInterval, newConfig.ElasticScalingConfig.ScaleDownInterval)
	})

}

func TestScalingConfig(t *testing.T) {
	sc := &ScalingConfig{
		elasticScalingCfg: &config.ElasticScalingConfig{},
	}
	assert.Equal(t, float64(0), sc.usedPodNumsExceedFreePercentage())
	assert.Equal(t, float64(0), sc.usedPodNumsExceedHeavyPercentage())
	assert.Equal(t, float64(0), sc.scaleUpPodNumsFreePercentage())
	assert.Equal(t, float64(0), sc.scaleUpPodNumsHeavyPercentage())
}
