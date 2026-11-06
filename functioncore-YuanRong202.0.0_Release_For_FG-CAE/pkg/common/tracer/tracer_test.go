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

// Package tracer for init trace provider
package tracer

import (
	"context"
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	mockUtils "functioncore/test/utils"
)

func TestInitProvider(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	otelGRPCEndpoint = "mockOtelGRPCEndpoint"
	otelGRPCToken = "mockOtelGRPCToken"
	otelServiceName = "mockOtelServiceName"
	enableOTELTracer = true
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			args: args{
				ctx: context.Background(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, // test success
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, err := InitProvider(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitProvider() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestInitProviderWithEnableCommonTracerIsFalse(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	otelGRPCEndpoint = "mockOtelGRPCEndpoint"
	otelGRPCToken = "mockOtelGRPCToken"
	otelServiceName = "mockOtelServiceName"
	enableOTELTracer = false
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success with EnableCommonTracer is false",
			args: args{
				ctx: context.Background(),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()

				return patches
			},
			wantErr: false,
		}, // test success with EnableCommonTracer is false
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, err := InitProvider(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitProvider() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestInitCommonTracer(t *testing.T) {
	type args struct {
		shutdown    func()
		serviceName string
	}
	isMocked := false
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		isMocked    bool
	}{
		{
			name: "test with error",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				gomonkey.ApplyFunc(InitProvider, func(ctx context.Context) (func(), error) {
					isMocked = true
					return nil, errors.New("mockInitProviderError")
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		isMocked = false
		patches := tt.patchesFunc()
		defer patches.ResetAll()
		t.Run(tt.name, func(t *testing.T) {
			InitCommonTracer(tt.args.shutdown, tt.args.serviceName)
		})
		if tt.isMocked != isMocked {
			t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
		}
	}
}
