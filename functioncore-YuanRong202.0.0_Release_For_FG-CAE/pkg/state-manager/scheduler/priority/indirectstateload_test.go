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

package priority

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/scheduler/metadata"
	"functioncore/pkg/state-manager/scheduler/predicate"
	"functioncore/test/utils"
)

func Test_indirectStateLoadScorer_Init(t *testing.T) {
	type fields struct {
		cpuScoreFactor   float32
		memScoreFactor   float32
		stateScoreFactor float32
		affinityFactor   float32
	}
	type args struct {
		conf interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// test config empty
		{
			"case1",
			fields{},
			args{},
			false,
		},
		// test config not empty
		{
			"case2",
			fields{},
			args{
				conf: config.Scorer{
					AffinityFactor: 2.0,
				},
			},
			false,
		},
		// test success
		{
			"case2",
			fields{},
			args{
				conf: config.Scorer{},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &indirectStateLoadScorer{
				cpuScoreFactor:   tt.fields.cpuScoreFactor,
				memScoreFactor:   tt.fields.memScoreFactor,
				stateScoreFactor: tt.fields.stateScoreFactor,
				affinityFactor:   tt.fields.affinityFactor,
			}
			if err := s.Init(tt.args.conf); (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_indirectStateLoadScorer_Score(t *testing.T) {
	type fields struct {
		cpuScoreFactor   float32
		memScoreFactor   float32
		stateScoreFactor float32
		affinityFactor   float32
	}
	type args struct {
		target *metadata.TargetInfo
		infos  []*metadata.NodeInfo
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc utils.PatchesFunc
		wantErr   bool
	}{
		// test empty target
		{
			"case1",
			fields{},
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test
		{
			"case2",
			fields{
				memScoreFactor: 10,
				cpuScoreFactor: 10,
			},
			args{
				target: &metadata.TargetInfo{
					StateType: "test",
				},
				infos: []*metadata.NodeInfo{
					nil,
					{
						ID: "test",
						Cache: predicate.ResourceUsage{
							CPURatio: 0.1,
							MEMRatio: 0.1,
						},
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(metadata.GetAffinityNode, func(dataType string) (map[string]struct{}, error) {
						return map[string]struct{}{
							"test": {},
						}, nil
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &indirectStateLoadScorer{
				cpuScoreFactor:   tt.fields.cpuScoreFactor,
				memScoreFactor:   tt.fields.memScoreFactor,
				stateScoreFactor: tt.fields.stateScoreFactor,
				affinityFactor:   tt.fields.affinityFactor,
			}
			patches := tt.patchFunc()
			_, err := s.Score(tt.args.target, tt.args.infos)
			if (err != nil) != tt.wantErr {
				t.Errorf("Score() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_indirectStateLoadScorer_getMaxIndirectStateLoad(t *testing.T) {
	type fields struct {
		cpuScoreFactor   float32
		memScoreFactor   float32
		stateScoreFactor float32
		affinityFactor   float32
	}
	type args struct {
		infos []*metadata.NodeInfo
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        float32
	}{
		{
			"case1",
			fields{},
			args{
				infos: []*metadata.NodeInfo{
					{
						ID: "test",
					},
					{
						ID: "test2",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(metadata.GetIndirectStateLoad, func(nodeID string) float32 {
						if nodeID == "test" {
							return 10
						}
						return 20
					}),
				})
				return patches
			},
			20,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &indirectStateLoadScorer{
				cpuScoreFactor:   tt.fields.cpuScoreFactor,
				memScoreFactor:   tt.fields.memScoreFactor,
				stateScoreFactor: tt.fields.stateScoreFactor,
				affinityFactor:   tt.fields.affinityFactor,
			}
			patches := tt.patchesFunc()
			if got := s.getMaxIndirectStateLoad(tt.args.infos); got != tt.want {
				t.Errorf("getMaxIndirectStateLoad() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_isZero(t *testing.T) {
	type args struct {
		number float32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// test is zero
		{
			"case1",
			args{
				number: 0.005,
			},
			true,
		},
		// test is not zero
		{
			"case2",
			args{
				number: 0.01,
			},
			false,
		},
		// test is not zero
		{
			"case3",
			args{
				number: 10,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isZero(tt.args.number); got != tt.want {
				t.Errorf("isZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
