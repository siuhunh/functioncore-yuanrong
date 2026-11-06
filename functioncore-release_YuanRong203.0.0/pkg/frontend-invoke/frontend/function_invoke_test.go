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

package frontend

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/model"
)

func Test_setRequestDeadline(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
			if funcName == "normal" {
				return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{Timeout: 10}}, true
			}
			if funcName == "custom" {
				return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{Timeout: 10, IsCustomContainer: true}}, true
			}
			if funcName == "init" {
				return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{Timeout: 10}, ExtendedMetaData: functiontypes.ExtendedMetaData{Initializer: functiontypes.Initializer{InitializerTimeout: 5}}}, true
			}
			if funcName == "longrun" {
				return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{Timeout: 910}}, true
			}
			return nil, false
		}),
	}
	oldCfg := constant.FConfig
	constant.FConfig.E2EMaxDelayTime = 50
	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{WorkerInstanceReadTimeOut: 1200}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
		constant.FConfig = oldCfg
	}()

	tests := []struct {
		name     string
		reqInfo  *model.FunctionRequestInfo
		expected time.Duration
	}{
		{
			name:     "normal",
			reqInfo:  &model.FunctionRequestInfo{Name: "normal"},
			expected: 60 * time.Second,
		},
		{
			name:     "custom",
			reqInfo:  &model.FunctionRequestInfo{Name: "custom"},
			expected: 1200 * time.Second,
		},
		{
			name:     "longrun",
			reqInfo:  &model.FunctionRequestInfo{Name: "longrun"},
			expected: 950 * time.Second,
		},
	}

	getTimeDiff := func(a, b time.Duration) time.Duration {
		if a > b {
			return a - b
		}
		return b - a
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequestDeadline(tt.reqInfo)
			diff := tt.reqInfo.Deadline.Sub(time.Now())
			assert.True(t, getTimeDiff(diff, tt.expected) < 200*time.Millisecond, tt.name)
		})
	}
}

func Test_getInsResource(t *testing.T) {
	data := map[string]*metadata.FunctionMetaData{
		"cae1": {FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: "CAE"}, ResourceMetaData: functiontypes.ResourceMetaData{CPU: 300, Memory: 128}},
	}
	defer gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
		f, ok := data[funcName]
		return f, ok
	}).Reset()

	tests := []struct {
		name    string
		reqInfo *model.FunctionRequestInfo
		req     *fasthttp.Request

		expected types.InstanceResource
	}{
		{
			name:    "cae func",
			reqInfo: &model.FunctionRequestInfo{Name: "cae1"},

			expected: types.InstanceResource{CPU: "0", Memory: "0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, _ := getInsResource(tt.reqInfo, tt.req)
			assert.Equal(t, res, tt.expected, tt.name)
		})
	}
}
