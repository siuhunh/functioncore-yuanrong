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

// Package utils get config
package utils

import (
	"errors"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/config"
	mockutils "functioncore/test/utils"
)

func TestGetRuntimeManagerCfg(t *testing.T) {
	tests := []struct {
		name        string
		want        *config.Config
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test succeed to get config",
			want: &config.Config{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.PatchSlice{}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{}, nil
					}),
				})
				return patches
			},
		},
		{
			name: "test failed to get config",
			want: nil,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.PatchSlice{}
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return nil, errors.New("mock get runtime-manager config error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		// init global var before test
		runtimeManagerCfgOnce = sync.Once{}
		runtimeManagerCfg = nil
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetRuntimeManagerCfg(), "GetRuntimeManagerCfg()")
			assert.Equalf(t, tt.want, GetRuntimeManagerCfg(), "GetRuntimeManagerCfg()")
		})
		patches.ResetAll()
	}
}

func TestIsRegisterRuntimeType(t *testing.T) {
	type args struct {
		runtimeLanguage string
	}
	actualCfg := &config.Config{
		RuntimeConfigs: map[string]config.RuntimeConfig{
			"python3.6": {},
			"go":        {},
			"java8":     {},
		},
	}
	tests := []struct {
		name  string
		args  args
		rmCfg *config.Config
		want  bool
	}{
		{
			name: "test go is registered",
			args: args{
				runtimeLanguage: "go",
			},
			rmCfg: actualCfg,
			want:  true,
		},
		{
			name: "test python3.6 is registered",
			args: args{
				runtimeLanguage: "python3.6",
			},
			rmCfg: actualCfg,
			want:  true,
		},
		{
			name: "test custom-image is registered",
			args: args{
				runtimeLanguage: "custom-image",
			},
			rmCfg: actualCfg,
			want:  true,
		},
		{
			name: "test custom image is registered",
			args: args{
				runtimeLanguage: "custom image",
			},
			rmCfg: actualCfg,
			want:  true,
		},
		{
			name: "test python3 is unregistered",
			args: args{
				runtimeLanguage: "python3",
			},
			rmCfg: actualCfg,
			want:  false,
		},
		{
			name: "test runtime manager config is nil",
			args: args{
				runtimeLanguage: "python3.6",
			},
			rmCfg: nil,
			want:  false,
		},
		{
			name: "test runtime manager config's map is nil",
			args: args{
				runtimeLanguage: "python3.6",
			},
			rmCfg: &config.Config{
				RuntimeConfigs: nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		runtimeManagerCfg = tt.rmCfg
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsRegisterRuntimeType(tt.args.runtimeLanguage), "IsRegisterRuntimeType(%v)", tt.args.runtimeLanguage)
		})
		runtimeManagerCfg = nil
	}
}
