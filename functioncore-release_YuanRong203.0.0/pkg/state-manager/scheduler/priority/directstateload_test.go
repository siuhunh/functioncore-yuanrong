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

func Test_checkFactor(t *testing.T) {
	type args struct {
		cpuFactor   float32
		memFactor   float32
		stateFactor float32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		// test add total = 1
		{
			"case1",
			args{
				cpuFactor:   0.2,
				memFactor:   0.5,
				stateFactor: 0.3,
			},
			true,
		},
		// test add total > 1
		{
			"case1",
			args{
				cpuFactor:   0.5,
				memFactor:   0.5,
				stateFactor: 0.3,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkFactor(tt.args.cpuFactor, tt.args.memFactor, tt.args.stateFactor); got != tt.want {
				t.Errorf("checkFactor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_directStateLoadScorer_Init(t *testing.T) {
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
		// test empty config
		{
			"case1",
			fields{},
			args{},
			false,
		},
		// test AffinityFactor > 1
		{
			"case2",
			fields{},
			args{
				conf: config.Scorer{
					AffinityFactor: 1.1,
				},
			},
			false,
		},
		{
			"case3",
			fields{},
			args{
				conf: config.Scorer{},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &directStateLoadScorer{
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

func Test_directStateLoadScorer_Score(t *testing.T) {
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
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
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
		// test success
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
			s := &directStateLoadScorer{
				cpuScoreFactor:   tt.fields.cpuScoreFactor,
				memScoreFactor:   tt.fields.memScoreFactor,
				stateScoreFactor: tt.fields.stateScoreFactor,
				affinityFactor:   tt.fields.affinityFactor,
			}
			_, err := s.Score(tt.args.target, tt.args.infos)
			if (err != nil) != tt.wantErr {
				t.Errorf("Score() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
