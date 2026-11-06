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

// Package podevent -
package podevent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"

	"functioncore/pkg/common/redisclient"
	testutils "functioncore/test/utils"
)

func TestReportPodErrMsgToRedis(t *testing.T) {
	type args struct {
		c              *redisclient.Client
		URNWithVersion string
		podErrInfo     EventInfo
	}
	tests := []struct {
		name      string
		args      args
		patchFunc testutils.PatchesFunc
	}{
		{
			"test client is nil",
			args{
				c: nil,
			},
			func() testutils.PatchSlice {
				return testutils.InitPatchSlice()
			},
		},
		{
			"failed to Marshal",
			args{
				c: &redisclient.Client{},
				podErrInfo: EventInfo{
					PodName: "test",
					EventStageInfo: EventStageInfo{
						ErrorMessage: "test",
					},
				},
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("test")
					}),
				})
				return patches
			},
		},
		{
			"failed to LLen",
			args{
				c:              &redisclient.Client{},
				URNWithVersion: "test",
				podErrInfo: EventInfo{
					PodName: "test",
					EventStageInfo: EventStageInfo{
						ErrorMessage: "test",
					},
				},
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte("test"), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "LLen",
						func(r *redisclient.Client, ctx context.Context, key string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 0, errors.New("test")
						}),
				})
				return patches
			},
		},
		{
			"failed to LPopCount",
			args{
				c:              &redisclient.Client{},
				URNWithVersion: "test",
				podErrInfo: EventInfo{
					PodName: "test",
					EventStageInfo: EventStageInfo{
						ErrorMessage: "test",
					},
				},
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte("test"), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "LLen",
						func(r *redisclient.Client, ctx context.Context, key string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 0, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
						func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			"failed to RPush",
			args{
				c:              &redisclient.Client{},
				URNWithVersion: "test",
				podErrInfo: EventInfo{
					PodName: "test",
					EventStageInfo: EventStageInfo{
						ErrorMessage: "test",
					},
				},
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte("test"), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "LLen",
						func(r *redisclient.Client, ctx context.Context, key string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 0, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
						func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return errors.New("test")
						}),
				})
				return patches
			},
		},
		{
			"test ok",
			args{
				c:              &redisclient.Client{},
				URNWithVersion: "test",
				podErrInfo: EventInfo{
					PodName: "test",
					EventStageInfo: EventStageInfo{
						ErrorMessage: "test",
					},
				},
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte("test"), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "LLen",
						func(r *redisclient.Client, ctx context.Context, key string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 20, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
						func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "LPop",
						func(r *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		ReportPodEventToRedis(tt.args.c, tt.args.URNWithVersion, 10, tt.args.podErrInfo)
		patches.ResetAll()
	}
}
