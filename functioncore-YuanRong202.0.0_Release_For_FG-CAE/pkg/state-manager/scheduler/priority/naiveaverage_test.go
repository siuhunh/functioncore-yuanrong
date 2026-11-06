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

package priority

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/state-manager/scheduler/metadata"
)

func Test_naiveAverageScorer_Score(t *testing.T) {
	convey.Convey("Filter", t, func() {
		scorerFoo := naiveAverageScorer{}
		target := metadata.TargetInfo{
			FuncName:  "handler",
			StateType: "typeA",
		}
		infos := make([]*metadata.NodeInfo, 4, 4)
		infos[0] = nil
		infos[1] = &metadata.NodeInfo{ID: "node1"}
		infos[2] = &metadata.NodeInfo{ID: "node2"}
		infos[3] = &metadata.NodeInfo{ID: "node3"}

		convey.Convey("test 1", func() {
			patches := [2]*gomonkey.Patches{
				gomonkey.ApplyFunc(metadata.GetAffinityNode,
					func(dataType string) (map[string]struct{}, error) {
						ret := map[string]struct{}{}
						ret["node1"] = struct{}{}
						return ret, nil
					}),
				gomonkey.ApplyFunc(metadata.GetNodeAssignedData,
					func(nodeID string) (map[string]struct{}, error) {
						if nodeID == "node2" {
							return nil, errors.New("error")
						}
						return map[string]struct{}{}, nil
					}),
			}
			defer patches[0].Reset()
			defer patches[1].Reset()
			_, err := scorerFoo.Score(&target, infos)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func Test_naiveAverageScorer_Init(t *testing.T) {
	type args struct {
		conf interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"case1",
			args{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &naiveAverageScorer{}
			if err := f.Init(tt.args.conf); (err != nil) != tt.wantErr {
				t.Errorf("Init() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
