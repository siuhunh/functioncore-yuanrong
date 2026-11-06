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

// Package metrics -
package metrics

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	mockUtils "functioncore/test/utils"
)

func TestCreateMonitoringClient(t *testing.T) {
	type args struct {
		redisConfig config.MetricServerConfig
		stopCh      <-chan struct{}
	}
	redisConfig := config.MetricServerConfig{}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case1 test create monitoring client",
			args{
				redisConfig: redisConfig,
				stopCh:      make(<-chan struct{}),
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return &redisclient.Client{}, nil
					}),
				})
				return patches
			},
		},
		{
			"case2 test create monitoring client",
			args{
				redisConfig: redisConfig,
				stopCh:      make(<-chan struct{}),
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"case3 test create monitoring client",
			args{
				redisConfig: redisConfig,
				stopCh:      make(<-chan struct{}),
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return nil, errors.New("error")
					}),
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := CreateMonitoringClient(tt.args.redisConfig, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("CreateMonitoringClient() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
