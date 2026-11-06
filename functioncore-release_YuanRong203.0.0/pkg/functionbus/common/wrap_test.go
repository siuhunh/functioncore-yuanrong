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

package common

import (
	"context"
	"fmt"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/types"
	mockutils "functioncore/test/utils"
)

func TestWrapInvokeAdaptor(t *testing.T) {
	type args struct {
		originHandlerFunc func(ctx context.Context, dst *FunctionProperty,
			req *types.TCPRequest)
	}
	isMocked := false
	tests := []struct {
		name         string
		args         args
		patchesFunc  mockutils.PatchesFunc
		expectMocked bool
	}{
		{
			name: "test success",
			args: args{
				originHandlerFunc: func(ctx context.Context, dst *FunctionProperty, req *types.TCPRequest) {
					fmt.Println("mock origin handle function")
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tracer.EnableCommonTracer, func() bool {
						isMocked = true
						return true
					}),
				})
				return patches
			},
			expectMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMocked = false
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			handlerFunc := WrapInvokeAdaptor(tt.args.originHandlerFunc)
			handlerFunc(context.Background(), nil, &types.TCPRequest{
				Extra: make(map[string]string),
			})
			if isMocked != tt.expectMocked {
				t.Errorf("expect %v but found %v", tt.expectMocked, isMocked)
			}
		})
	}
}
