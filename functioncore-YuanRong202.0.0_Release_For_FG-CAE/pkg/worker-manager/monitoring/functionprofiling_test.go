/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package monitoring

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/redisclient"
	"functioncore/test/utils"
)

func TestGetFunctionProfilingDataByFuncName(t *testing.T) {
	tmp := funcProfilingData
	type args struct {
		funcName          string
		funcProfilingData *FunctionProfilingData
	}
	tests := []struct {
		name    string
		args    args
		wantNil bool
	}{
		{
			name: "test funcProfilingData is nil",
			args: args{
				funcName:          "test",
				funcProfilingData: nil,
			},
			wantNil: true,
		},
		{
			name: "test funcProfilingData is exist want data",
			args: args{
				funcName: "test",
				funcProfilingData: &FunctionProfilingData{
					ProfilingContent: map[string]*ProfilingContent{
						"test": {},
					},
				},
			},
			wantNil: false,
		},
		{
			name: "test funcProfilingData is not exist want data",
			args: args{
				funcName: "test",
				funcProfilingData: &FunctionProfilingData{
					ProfilingContent: map[string]*ProfilingContent{
						"test1": {},
					},
				},
			},
			wantNil: true,
		},
	}
	for _, tt := range tests {
		funcProfilingData = tt.args.funcProfilingData
		profilingContent := GetFunctionProfilingDataByFuncName(tt.args.funcName)
		if tt.wantNil != (profilingContent == nil) {
			t.Errorf("GetFunctionProfilingDataByFuncName() = %v, want %v", profilingContent == nil, tt.wantNil)
		}
	}

	funcProfilingData = tmp
}

func Test_getFunctionProfilingDataFromRedis(t *testing.T) {
	tmp := client
	tests := []struct {
		name      string
		client    *monitoringClient
		patchFunc utils.PatchesFunc
		wantErr   bool
	}{
		{
			"test get redis key error",
			&monitoringClient{
				c: &redisclient.Client{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", errors.New("redis get error")
						}),
				})
				return patches
			},
			true,
		},
		{
			"test get redis key success",
			&monitoringClient{
				c: &redisclient.Client{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "aaa|", nil
						}),
				})
				return patches
			},
			true,
		},
		{
			"test get redis key success",
			&monitoringClient{
				c: &redisclient.Client{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"test\":\"test\"}", nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		client = tt.client
		err := getFunctionProfilingDataFromRedis()
		if tt.wantErr != (err != nil) {
			t.Errorf("getFunctionProfilingDataFromRedis() = %v, want %v", err != nil, tt.wantErr)
		}
		patches.ResetAll()
	}
	client = tmp
}

func Test_monitorFunctionProfiling(t *testing.T) {
	type args struct {
		stopCh chan struct{}
	}

	tests := []struct {
		name      string
		args      args
		patchFunc utils.PatchesFunc
	}{
		{
			"test stopCh is nil",
			args{
				nil,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
		},
		{
			"test ticker apply",
			args{
				stopCh: make(chan struct{}),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						tickerC := make(chan time.Time, 1)
						tickerC <- time.Now()
						ticker := &time.Ticker{
							C: tickerC,
						}
						return ticker
					}),
					gomonkey.ApplyFunc(getFunctionProfilingDataFromRedis, func() error {
						return errors.New("get from redis error")
					}),
				})

				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		if tt.args.stopCh != nil {
			go func() {
				time.Sleep(time.Millisecond)
				tt.args.stopCh <- struct{}{}
			}()
		}
		monitorFunctionProfiling(tt.args.stopCh)
		patches.ResetAll()
	}
}
