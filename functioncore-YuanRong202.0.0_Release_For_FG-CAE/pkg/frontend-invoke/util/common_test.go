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

package util

import (
	"fmt"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestIPPortConv(t *testing.T) {
	tests := []struct {
		name                 string
		ipPort               string
		expectWorkerInstance *WorkerInstance
		expectError          error
	}{
		{
			name:   "succeed to conv ip:port",
			ipPort: "127.0.0.1:8000",
			expectWorkerInstance: &WorkerInstance{
				IP:   "127.0.0.1",
				Port: "8000",
			},
			expectError: nil,
		},
		{
			name:                 "failed to conv ip:port",
			ipPort:               "127.0.0.1,8000",
			expectWorkerInstance: nil,
			expectError:          fmt.Errorf("wrong ip port : %s", "127.0.0.1,8000"),
		},
	}
	for _, test := range tests {
		actualWorkerInstance, actualError := IPPortConv(test.ipPort)
		assert.Equal(t, test.expectWorkerInstance, actualWorkerInstance, test.name)
		assert.Equal(t, test.expectError, actualError, test.name)
	}
}

func TestWorkerInstanceKeyConv(t *testing.T) {
	tests := []struct {
		name                 string
		key                  string
		expectWorkerInstance *WorkerInstance
		expectError          error
	}{
		{
			name: "succeed to conv workerInstanceKey",
			key:  BuildWorkerInstanceKey("127.0.0.1", "8000", "instanceID"),
			expectWorkerInstance: &WorkerInstance{
				IP:         "127.0.0.1",
				Port:       "8000",
				InstanceID: "instanceID",
			},
			expectError: nil,
		},
		{
			name:                 "failed to conv workerInstanceKey",
			key:                  "127.0.0.1:8000@@",
			expectWorkerInstance: nil,
			expectError:          fmt.Errorf("wrong key : %s", "127.0.0.1:8000@@"),
		},
	}
	for _, test := range tests {
		actualWorkerInstance, actualError := WorkerInstanceKeyConv(test.key)
		assert.Equal(t, test.expectWorkerInstance, actualWorkerInstance, test.name)
		assert.Equal(t, test.expectError, actualError, test.name)
	}
}

func TestInitETCDClient(t *testing.T) {
	convey.Convey("init etcd client err", t, func() {
		stopCh := make(chan struct{}, 1)
		convey.ShouldNotBeNil(InitETCDClient(stopCh))
	})
}
func TestParseFuncKeyWithRes(t *testing.T) {
	convey.Convey("test ParseFuncKeyWithRes ok", t, func() {
		tenantID, funcKey := ParseFuncKeyWithRes("aaa/bbb/ccc/ddd")
		convey.So(tenantID, convey.ShouldEqual, "aaa")
		convey.So(funcKey, convey.ShouldEqual, "aaa/bbb/ccc")
	})
	convey.Convey("test ParseFuncKeyWithRes nok", t, func() {
		tenantID, funcKey := ParseFuncKeyWithRes("aaa/bbb/ccc/ddd/eee")
		convey.So(tenantID, convey.ShouldEqual, "")
		convey.So(funcKey, convey.ShouldEqual, "")
	})
}
