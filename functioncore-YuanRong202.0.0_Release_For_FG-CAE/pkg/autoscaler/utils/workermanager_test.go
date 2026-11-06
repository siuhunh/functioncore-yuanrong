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

// Package utils -
package utils

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/tls"
	mockUtils "functioncore/test/utils"
)

func TestGetWorkerMgrInsByKey(t *testing.T) {
	type args struct {
		key string
	}
	var a args
	a.key = "/sn/worker-manager/instances//10.0.0.0/worker-manager-aaa"
	var a2 args
	a2.key = "/sn/worker-manager/instances/cluster001//worker-manager-aaa"
	var a3 args
	a3.key = "/sn/worker-manager/instances/cluster001/10.0.0.0/"
	tests := []struct {
		name    string
		args    args
		want    *WorkerMgrInstance
		wantErr bool
	}{
		{"case1", a, nil, true},
		{"case2", a2, nil, true},
		{"case3", a3, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetWorkerMgrInsByKey(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetWorkerMgrInsByKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetWorkerMgrInsByKey() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetWorkerManagerGetInstanceClient(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
			return &config.Config{
				HTTPSConfig: &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
				},
			}, nil
		}),
	})
	client := GetWorkerManagerGetInstanceClient()
	if client.Timeout != defaultRequestWorkerManagerInstanceNumTimeout {
		t.Errorf("failed to get workerManagerGetInsatnceClient")
	}

}
