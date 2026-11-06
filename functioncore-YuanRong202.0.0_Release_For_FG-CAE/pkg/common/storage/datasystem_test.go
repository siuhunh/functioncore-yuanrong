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

// Package storage provides common storage interface
package storage

import (
	"clients/common"
	"clients/statecache"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	mockutils "functioncore/test/utils"
)

func TestGetDSClient(t *testing.T) {
	mockStateClient := statecache.StateClient{}
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
		want        *DSClient
		isGetErr    bool
		isSetErr    bool
	}{
		{
			name: "TestGetDSClient with success",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(statecache.CreateClient, func(param common.ConnectArguments) statecache.StateClient {
						return mockStateClient
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Init, func(_ *statecache.StateClient) common.Status {
						return common.Status{}
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Get, func(_ *statecache.StateClient, key string,
						timeoutms ...uint32) (string, common.Status) {
						return "mockGetValue", common.Status{}
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Set, func(_ *statecache.StateClient,
						key string, value string, param statecache.SetParam) common.Status {
						return common.Status{}
					}),
				})
				return patches
			},
			want: &DSClient{
				stateCacheClient: mockStateClient,
			},
			isGetErr: false,
			isSetErr: false,
		}, // TestGetDSClient with success
		{
			name: "TestGetDSClient with get error",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(statecache.CreateClient, func(param common.ConnectArguments) statecache.StateClient {
						return mockStateClient
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Init, func(_ *statecache.StateClient) common.Status {
						return common.Status{}
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Get, func(_ *statecache.StateClient, key string,
						timeoutms ...uint32) (string, common.Status) {
						return "mockGetValue", common.Status{Code: 1, Err: errors.New("get error")}
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Set, func(_ *statecache.StateClient,
						key string, value string, param statecache.SetParam) common.Status {
						return common.Status{}
					}),
				})
				return patches
			},
			want: &DSClient{
				stateCacheClient: mockStateClient,
			},
			isGetErr: true,
			isSetErr: false,
		}, // TestGetDSClient get error
		{
			name: "TestGetDSClient with set error",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(statecache.CreateClient, func(param common.ConnectArguments) statecache.StateClient {
						return mockStateClient
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Init, func(_ *statecache.StateClient) common.Status {
						return common.Status{}
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Get, func(_ *statecache.StateClient, key string,
						timeoutms ...uint32) (string, common.Status) {
						return "mockGetValue", common.Status{}
					}),
					gomonkey.ApplyFunc((*statecache.StateClient).Set, func(_ *statecache.StateClient,
						key string, value string, param statecache.SetParam) common.Status {
						return common.Status{Code: 1, Err: errors.New("set error")}
					}),
				})
				return patches
			},
			want: &DSClient{
				stateCacheClient: mockStateClient,
			},
			isGetErr: false,
			isSetErr: true,
		}, // TestGetDSClient set error
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsClientOnce = sync.Once{}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			if dsClient := GetDSClient(); !reflect.DeepEqual(dsClient, tt.want) {
				t.Errorf("GetDSClient() = %v, want %v", dsClient, tt.want)
			}
			if dsClient == nil {
				return
			}
			_, err := dsClient.Get("hello")
			assert.Equal(t, err != nil, tt.isGetErr)
			err = dsClient.Set("hello", "world")
			assert.Equal(t, err != nil, tt.isSetErr)
		})
	}
}
