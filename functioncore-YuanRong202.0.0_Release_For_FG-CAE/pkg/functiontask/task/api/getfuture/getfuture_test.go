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

package getfuture

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/utils"
)

func TestRequest_stateless(t *testing.T) {
	type args struct {
		taskCtx   taskcontext.Context
		sncFuture *snfuture.Future
	}
	tests := []struct {
		name        string
		req         Request
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			Request{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&snfuture.Future{}), "GetResponse",
						func(_ *snfuture.Future) *common.Response {
							return &common.Response{}
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, _ := tt.req.stateless(tt.args.taskCtx, tt.args.sncFuture)
			if (got != nil) != tt.want {
				t.Errorf("stateless() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}
