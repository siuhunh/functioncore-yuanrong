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

package monitoring

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"

	"functioncore/pkg/common/redisclient"
	mockUtils "functioncore/test/utils"
)

func Test_getAndMonitorActiveFunction(t *testing.T) {
	type args struct {
		stopCh <-chan struct{}
	}
	var a args
	a.stopCh = nil
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getAndMonitorActiveFunction(tt.args.stopCh)
		})
	}
}

func Test_updateInactiveFunctionList(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{"failed to get redis client", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return nil
				}),
			})
			return patches
		}},
		{"failed to get inactive function", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
						res := &redis.MapStringStringCmd{}
						res.SetErr(errors.New("aaa"))
						res.SetVal(map[string]string{})
						return res
					}),
			})
			return patches
		}},
		{"bad format for inactiveFunctionMap", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
						res := &redis.MapStringStringCmd{}
						res.SetErr(nil)
						res.SetVal(map[string]string{})
						return res
					}),
			})
			return patches
		}},
		{"failed to parse lastUpdateTime", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
						res := &redis.MapStringStringCmd{}
						res.SetErr(nil)
						res.SetVal(map[string]string{
							"lastUpdateTime": "aaa",
						})
						return res
					}),
			})
			return patches
		}},
		{"lastUpdateData is one hour ago", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
						res := &redis.MapStringStringCmd{}
						res.SetErr(nil)
						res.SetVal(map[string]string{
							"lastUpdateTime": "1700000000",
						})
						return res
					}),
			})
			return patches
		}},
		{"updated inactiveFunctionList", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
						res := &redis.MapStringStringCmd{}
						res.SetErr(nil)
						res.SetVal(map[string]string{
							"lastUpdateTime": strconv.FormatInt(time.Now().Unix(), 10),
						})
						return res
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			updateInactiveFunctionList()
		})
		patches.ResetAll()
	}
}
