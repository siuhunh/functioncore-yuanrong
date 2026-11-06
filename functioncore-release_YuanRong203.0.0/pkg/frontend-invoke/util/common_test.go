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
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/frontend-invoke/constant"
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

func TestIsHTTPStream(t *testing.T) {
	t.Run("without ContentType", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)
		if IsHTTPStream(req) {
			t.Error("Expected IsHTTPStream to return false when no content type is provided")
		}
	})
	t.Run("not http stream", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set(constants.ContentType, "invalid/contenttype")
		if IsHTTPStream(req) {
			t.Error("Expected IsHTTPStream to return false when content type is not in upStreamContentTypes")
		}
	})
	t.Run("http stream : multipart/form-data", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set(constants.ContentType, "multipart/form-data; boundary=03090d5baa3cef9dbf1334c848eb5cfe96c8dfe0c98c78930f04dd087ad3")
		if !IsHTTPStream(req) {
			t.Error("Expected IsHTTPStream to return true when content type is in upStreamContentTypes")
		}
	})
	t.Run("http stream : application/x-www-form-urlencoded", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/", nil)
		req.Header.Set(constants.ContentType, "application/x-www-form-urlencoded")
		if !IsHTTPStream(req) {
			t.Error("Expected IsHTTPStream to return true when content type is in upStreamContentTypes")
		}
	})
}

func TestPermittedRequest(t *testing.T) {
	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{}
	t.Run("exceeds MaxStreamRequestBodySize", func(t *testing.T) {
		maxRequestBodySizeOnce = sync.Once{}
		constant.FConfig.HTTPConfig.MaxStreamRequestBodySize = 500
		r, _ := http.NewRequest("POST", "http://example.com", nil)
		r.ContentLength = int64(constant.FConfig.HTTPConfig.MaxStreamRequestBodySize*megabytes + 1)
		r.Header.Set(constants.ContentType, "multipart/form-data; boundary=03090d5baa3cef9dbf1334c848eb5cfe96c8dfe0c98c78930f04dd087ad3")
		err := PermittedRequest(r)
		assert.NotNil(t, err)
		assert.Equal(t, snerror.MaxRequestBodySizeErr, err.Code())
	})
	t.Run("not exceeds MaxStreamRequestBodySize", func(t *testing.T) {
		maxRequestBodySizeOnce = sync.Once{}
		constant.FConfig.HTTPConfig.MaxStreamRequestBodySize = 500
		r, _ := http.NewRequest("POST", "http://example.com", nil)
		r.ContentLength = int64(constant.FConfig.HTTPConfig.MaxStreamRequestBodySize * megabytes)
		r.Header.Set(constants.ContentType, "multipart/form-data; boundary=03090d5baa3cef9dbf1334c848eb5cfe96c8dfe0c98c78930f04dd087ad3")
		err := PermittedRequest(r)
		assert.Nil(t, err)
	})
	t.Run("exceeds MaxRequestBodySize", func(t *testing.T) {
		maxRequestBodySizeOnce = sync.Once{}
		constant.FConfig.HTTPConfig.MaxRequestBodySize = 6
		r, _ := http.NewRequest("POST", "http://example.com", nil)
		r.ContentLength = int64(constant.FConfig.HTTPConfig.MaxRequestBodySize*megabytes + 1)
		r.Header.Set(constants.ContentType, constants.ApplicationJSON)
		err := PermittedRequest(r)
		assert.NotNil(t, err)
		assert.Equal(t, snerror.MaxRequestBodySizeErr, err.Code())
	})
	t.Run("not ExceedsMaxRequestBodySize", func(t *testing.T) {
		maxRequestBodySizeOnce = sync.Once{}
		constant.FConfig.HTTPConfig.MaxRequestBodySize = 6
		r, _ := http.NewRequest("POST", "http://example.com", nil)
		r.ContentLength = int64(constant.FConfig.HTTPConfig.MaxRequestBodySize * megabytes)
		r.Header.Set(constants.ContentType, constants.ApplicationJSON)
		err := PermittedRequest(r)
		assert.Nil(t, err)
	})
	t.Run("exceeds default 1GB MaxStreamRequestBodySize", func(t *testing.T) {
		maxRequestBodySizeOnce = sync.Once{}
		constant.FConfig.HTTPConfig.MaxStreamRequestBodySize = 0
		r, _ := http.NewRequest("POST", "http://example.com", nil)
		r.ContentLength = int64(defaultStreamLengthLimit + 1)
		r.Header.Set(constants.ContentType, "multipart/form-data; boundary=03090d5baa3cef9dbf1334c848eb5cfe96c8dfe0c98c78930f04dd087ad3")
		err := PermittedRequest(r)
		assert.NotNil(t, err)
		assert.Equal(t, snerror.MaxRequestBodySizeErr, err.Code())
	})
	t.Run("not exceeds default 1GB MaxStreamRequestBodySize", func(t *testing.T) {
		maxRequestBodySizeOnce = sync.Once{}
		constant.FConfig.HTTPConfig.MaxStreamRequestBodySize = 0
		r, _ := http.NewRequest("POST", "http://example.com", nil)
		r.ContentLength = int64(defaultStreamLengthLimit)
		r.Header.Set(constants.ContentType, "multipart/form-data; boundary=03090d5baa3cef9dbf1334c848eb5cfe96c8dfe0c98c78930f04dd087ad3")
		err := PermittedRequest(r)
		assert.Nil(t, err)
	})
}

func TestInstancesRecord(t *testing.T) {
	funcKeyWithRes := "funcA/300-128"
	instanceA := "instanceA"
	processStatus001 := &schedulerapi.ProcessStatus{
		ProcessorID:  0,
		RestThread:   9,
		LeaseVersion: 1,
	}
	CreateOrUpdateInstancesRecord(funcKeyWithRes, instanceA, processStatus001)

	instancesRecord := GetInstancesRecord()
	assert.Equal(t, funcKeyWithRes, instancesRecord.InstancesRes[funcKeyWithRes].FuncKeyWithRes)
	assert.Equal(t, instanceA, instancesRecord.InstancesRes[funcKeyWithRes].InstancesStatusMap[instanceA].InstanceKey)
	assert.Equal(t, 9, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].RestThread))
	assert.Equal(t, 1, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].LeaseVersion))

	ReleaseInstances(funcKeyWithRes, instanceA, 0)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, funcKeyWithRes, instancesRecord.InstancesRes[funcKeyWithRes].FuncKeyWithRes)
	assert.Equal(t, instanceA, instancesRecord.InstancesRes[funcKeyWithRes].InstancesStatusMap[instanceA].InstanceKey)
	assert.Equal(t, 10, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].RestThread))
	assert.Equal(t, 2, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].LeaseVersion))

	CreateOrUpdateInstancesRecord(funcKeyWithRes, instanceA, processStatus001)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 9, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].RestThread))
	assert.Equal(t, 2, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].LeaseVersion))

	processStatus002 := &schedulerapi.ProcessStatus{
		ProcessorID:  0,
		RestThread:   8,
		LeaseVersion: 3,
	}
	CreateOrUpdateInstancesRecord(funcKeyWithRes, instanceA, processStatus002)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 8, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].RestThread))
	assert.Equal(t, 3, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].LeaseVersion))
	ReleaseInstances(funcKeyWithRes, instanceA, 0)
	ReleaseInstances(funcKeyWithRes, instanceA, 0)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 10, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].RestThread))
	assert.Equal(t, 5, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceA].ProcessesStatusMap[0].LeaseVersion))

	instanceB := "instanceB"
	CreateOrUpdateInstancesRecord(funcKeyWithRes, instanceB, processStatus001)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 9, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceB].ProcessesStatusMap[0].RestThread))
	assert.Equal(t, 1, int(instancesRecord.InstancesRes[funcKeyWithRes].
		InstancesStatusMap[instanceB].ProcessesStatusMap[0].LeaseVersion))
	assert.Equal(t, 2, len(instancesRecord.InstancesRes[funcKeyWithRes].InstancesStatusMap))

	DeleteInstancesRecord(funcKeyWithRes, instanceA)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 1, len(instancesRecord.InstancesRes[funcKeyWithRes].InstancesStatusMap))

	DeleteInstancesRecord(funcKeyWithRes, instanceB)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 0, len(instancesRecord.InstancesRes))

	CreateOrUpdateInstancesRecord(funcKeyWithRes, instanceB, processStatus001)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 1, len(instancesRecord.InstancesRes[funcKeyWithRes].InstancesStatusMap))
	DeleteFuncRecord(funcKeyWithRes)
	instancesRecord = GetInstancesRecord()
	assert.Equal(t, 0, len(instancesRecord.InstancesRes))

	for i := 0; i < 1000; i++ {
		go func() {
			CreateOrUpdateInstancesRecord(funcKeyWithRes, instanceB, processStatus001)
			time.Sleep(1 * time.Millisecond)
			ReleaseInstances(funcKeyWithRes, instanceB, processStatus001.ProcessorID)
			GetInstancesRecord()
		}()
	}
	time.Sleep(1000 * time.Millisecond)
}

func TestGetInstancesRecord(t *testing.T) {
	instancesStatusMap := make(map[string]*schedulerapi.InstancesStatus)
	instancesStatusMap["instanceA"] = &schedulerapi.InstancesStatus{
		InstanceKey:        "instanceA",
		ProcessesStatusMap: map[uint32]*schedulerapi.ProcessStatus{},
	}
	instancesRes.InstancesRes["funcKeyWithRes"] = &schedulerapi.CheckInstanceReq{
		FuncKeyWithRes:     "funcKeyWithRes",
		InstancesStatusMap: instancesStatusMap,
	}

	go func() {
		for i := 0; i < 10000; i++ {
			instancesRecord := GetInstancesRecord()
			_, err := json.Marshal(instancesRecord)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
		}
	}()
	time.Sleep(1 * time.Millisecond)
	go func() {
		for i := 0; i < 10000; i++ {
			instancesLock.Lock()
			instancesStatusMap["instanceB"] = &schedulerapi.InstancesStatus{
				InstanceKey:        "instanceB",
				ProcessesStatusMap: map[uint32]*schedulerapi.ProcessStatus{},
			}
			instancesRes.InstancesRes["funcKeyWithRes"] = &schedulerapi.CheckInstanceReq{
				FuncKeyWithRes:     "funcKeyWithRes",
				InstancesStatusMap: instancesStatusMap,
			}
			instancesLock.Unlock()
		}
	}()
	time.Sleep(1000 * time.Millisecond)
}
