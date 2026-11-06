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

// Package config contains configuration of debug agent
package config

import (
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/test/utils"
)

func TestGetConfig(t *testing.T) {
	gPatch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
		f()
	})
	defer gPatch.Reset()

	debugAgentConfig := DebugAgentConfig{
		ServerConfig: ServerConfig{
			Port: 8082,
		},
	}
	configBytes, _ := json.Marshal(debugAgentConfig)

	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return string(configBytes), true
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: config key does not exist",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "", false
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case3: failed to unmarshal config",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "aaa", true
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case4: config is invalid",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "{}", true
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		got, err := GetConfig()
		if (err != nil) != tt.wantErr {
			t.Errorf("GetConfig error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		if !tt.wantErr && !assert.NotNil(t, got) {
			t.Errorf("GetConfig got is nil")
		}
		patches.ResetAll()
	}
}
