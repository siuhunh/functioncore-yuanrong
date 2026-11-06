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
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/valyala/fasthttp"

	mockUtils "functioncore/test/utils"
)

func TestWrapGinHandler(t *testing.T) {
	type args struct {
		originHandlerFunc func(c *gin.Context)
	}
	actualMocked := false
	tests := []struct {
		name         string
		args         args
		patchesFunc  mockUtils.PatchesFunc
		expectMocked bool
	}{
		{
			name: "test success",
			args: args{
				originHandlerFunc: func(c *gin.Context) {
					fmt.Println("mock gin origin handler func")
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				gomonkey.ApplyFunc(EnableCommonTracer, func() bool {
					actualMocked = true
					return true
				})
				return patches
			},
			expectMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualMocked = false
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			handlerFunc := WrapGinHandler(tt.args.originHandlerFunc)
			if handlerFunc == nil {
				t.Errorf("expect handler func is not nil")
				return
			}
			handlerFunc(&gin.Context{
				Request: &http.Request{
					URL: &url.URL{
						Path: "mockURLPath",
					},
					Header: make(http.Header),
				},
			})
			if actualMocked != tt.expectMocked {
				t.Errorf("expect %v but found %v", tt.expectMocked, actualMocked)
				return
			}
		})
	}
}

func TestWrapFastHTTPHandler(t *testing.T) {
	type args struct {
		originHandlerFunc func(ctx *fasthttp.RequestCtx)
	}
	actualMocked := false
	tests := []struct {
		name         string
		args         args
		patchesFunc  mockUtils.PatchesFunc
		expectMocked bool
	}{
		{
			name: "test success",
			args: args{
				originHandlerFunc: func(ctx *fasthttp.RequestCtx) {
					fmt.Println("mock fast http origin handler func")
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				gomonkey.ApplyFunc(EnableCommonTracer, func() bool {
					actualMocked = true
					return true
				})
				return patches
			},
			expectMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actualMocked = false
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			handlerFunc := WrapFastHTTPHandler(tt.args.originHandlerFunc)
			if handlerFunc == nil {
				t.Errorf("expect handler func is not nil")
				return
			}
			handlerFunc(&fasthttp.RequestCtx{})
			if actualMocked != tt.expectMocked {
				t.Errorf("expect %v but found %v", tt.expectMocked, actualMocked)
				return
			}
		})
	}
}
