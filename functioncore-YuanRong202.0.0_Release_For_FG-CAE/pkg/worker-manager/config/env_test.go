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

package config

import (
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/test/utils"
)

func TestGetSWRRegionID(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{
			name: "test get swrRegionID right",
			want: "swr.cn-north-7.myhuaweicloud.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			swrAPI = "swr.cn-north-7.myhuaweicloud.com"
			if got := GetSWRAPI(); got != tt.want {
				t.Errorf("GetSWRAPI() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitEnvs(t *testing.T) {
	tests := []struct {
		name      string
		wantErr   bool
		patchFunc func() utils.PatchSlice
	}{
		{
			name:    "test SWR_API empty",
			wantErr: true,
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "SWR_API" {
							return ""
						}
						return "swr.cn-north-7.myhuaweicloud.com"
					}),
				})
				return patches
			},
		},
		{
			name:    "test all empty",
			wantErr: true,
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			if err := InitEnvs(); (err != nil) != tt.wantErr {
				t.Errorf("InitEnvs() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
