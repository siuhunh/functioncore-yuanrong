/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

package workerdispatcher

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/requestdispatcher"
	"functioncore/pkg/functionbus/servicediscovery"
	mockUtils "functioncore/test/utils"
)

func TestWorkerDispatcher(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc((*servicediscovery.Registry).GetFunctionInstanceMetadata, func(_ *servicediscovery.Registry, functionKey string) (servicediscovery.InstanceMetaData, error) {
		if functionKey == "error_key" {
			return servicediscovery.InstanceMetaData{}, errors.New("failed to get concurrency value")
		}
		return servicediscovery.InstanceMetaData{ConcurrentNum: 1}, nil
	})
	patches.ApplyFunc((*servicediscovery.Registry).GetFunctionMetadata, func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.FunctionMetaInfo, error) {
		if functionKey == "error_key" {
			return nil, errors.New("failed to get concurrency value")
		}
		return &servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: 1}}, nil
	})
	patches.ApplyFunc(requestdispatcher.CreateRequestDispatcher, func(funcKeyWithResource string, _ servicediscovery.FunctionMetaInfo, _ bool) *requestdispatcher.RequestDispatcher {
		return &requestdispatcher.RequestDispatcher{}
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Start, func(*requestdispatcher.RequestDispatcher) {})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Request, func(_ *requestdispatcher.RequestDispatcher, request *types.DispatcherRequest) error {
		return nil
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).OnObserved, func(_ *requestdispatcher.RequestDispatcher, content interface{}, recovering bool) {
	})
	common.NotifyRemote = func(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error { return nil }
	defer patches.Reset()

	wd := CreateWorkerDispatcher(&servicediscovery.Registry{})
	err := wd.Request(&types.DispatcherRequest{
		FunctionKey: "functionA",
		Request:     &types.TCPRequest{CPU: "300", Memory: "128"},
	})
	assert.NoError(t, err)

	wd.busTerminating = true
	err = wd.Request(&types.DispatcherRequest{
		FunctionKey: "functionA",
		Request:     &types.TCPRequest{CPU: "300", Memory: "128"},
	})
	assert.Error(t, err)
	wd.busTerminating = false

	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).SetInstanceMetadata, func(_ *requestdispatcher.RequestDispatcher, _ servicediscovery.FunctionMetaInfo) {
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Stop, func(*requestdispatcher.RequestDispatcher) {})

	funcKeyWithResource := "testID/funcTest/1/500-500"
	functionKey := "testID/funcTest/1"
	wd.workerRequestDispatchers[funcKeyWithResource] = &requestdispatcher.RequestDispatcher{}

	wd.UpdateFunctionDispatcher(functionKey)
	wd.UpdateFunctionDispatcher("error_key")
	rd, err := wd.getFunctionDispatcher(funcKeyWithResource, false)
	assert.Nil(t, err)
	assert.NotNil(t, rd)

	wd.DeleteFunctionDispatcher(functionKey)
	wd.DeleteFunctionDispatcher("functionA/300-128")
	assert.Equal(t, 1, len(wd.workerRequestDispatchers))
	time.Sleep(500 * time.Millisecond)
}

func TestInitFunctionDispatcher_BusinessTypeCAE(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc((*servicediscovery.Registry).GetFunctionMetadata, func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.FunctionMetaInfo, error) {
		return &servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: 1}}, nil
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Start, func(*requestdispatcher.RequestDispatcher) {})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Request, func(_ *requestdispatcher.RequestDispatcher, request *types.DispatcherRequest) error {
		return nil
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).OnObserved, func(_ *requestdispatcher.RequestDispatcher, content interface{}, recovering bool) {
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).UpdateWorkerWillDeleteStatus, func(_ *requestdispatcher.RequestDispatcher, workerID string, status bool) {
		return
	})
	defer patches.Reset()
	wd := CreateWorkerDispatcher(&servicediscovery.Registry{})
	assert.Equal(t, 0, len(wd.workerRequestDispatchers))
	wd.InitFunctionDispatcher("test", "", &servicediscovery.Provider{PreviousWillDeleteState: false,
		WillDelete: true, BusinessType: constants.BusinessTypeCAE}, true)

	assert.Equal(t, 1, len(wd.workerRequestDispatchers))
}

func TestWorkerDispatcher_StopAllDispatchers(t *testing.T) {
	wd := CreateWorkerDispatcher(&servicediscovery.Registry{})
	wd.StopAllDispatchers()
	assert.Equal(t, true, wd.busTerminating)
}

func Test_InitFunctionDispatcher(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc((*servicediscovery.Registry).GetFunctionMetadata, func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.FunctionMetaInfo, error) {
		return &servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: 1}}, nil
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Start, func(*requestdispatcher.RequestDispatcher) {})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).Request, func(_ *requestdispatcher.RequestDispatcher, request *types.DispatcherRequest) error {
		return nil
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).OnObserved, func(_ *requestdispatcher.RequestDispatcher, content interface{}, recovering bool) {
	})
	patches.ApplyFunc((*requestdispatcher.RequestDispatcher).UpdateWorkerWillDeleteStatus, func(_ *requestdispatcher.RequestDispatcher, workerID string, status bool) {
		return
	})
	defer patches.Reset()
	wd := CreateWorkerDispatcher(&servicediscovery.Registry{})
	assert.Equal(t, 0, len(wd.workerRequestDispatchers))
	wd.InitFunctionDispatcher("funcA-300-128", "", &servicediscovery.Provider{PreviousWillDeleteState: false,
		WillDelete: true}, true)

	assert.Equal(t, 1, len(wd.workerRequestDispatchers))
}

func TestWorkerDispatcher_GetPendingRequestsMetrics(t *testing.T) {
	convey.Convey("workerDispatcher is nil", t, func() {
		var wd *WorkerDispatcher
		pendingRequestMetrics := wd.GetPendingRequestsMetrics()
		assert.Equal(t, 0, len(pendingRequestMetrics))
	})

	convey.Convey("workerDispatcher is not nil", t, func() {
		wd := CreateWorkerDispatcher(&servicediscovery.Registry{})
		funcKeyWithResource := "testID/funcTest/1/500-500"
		wd.workerRequestDispatchers[funcKeyWithResource] = &requestdispatcher.RequestDispatcher{}
		patches := gomonkey.NewPatches()
		patches.ApplyFunc((*requestdispatcher.RequestDispatcher).GetRequestQueueLen, func(_ *requestdispatcher.RequestDispatcher) int {
			return 10
		})
		defer patches.Reset()
		pendingRequestMetrics := wd.GetPendingRequestsMetrics()
		assert.Equal(t, 1, len(pendingRequestMetrics))
	})
}

func TestWorkerDispatcher_UpdateKeepAliveTime(t *testing.T) {
	funcKeyWithResource := "testID/funcTest/1/300-128"
	setupWorkerDispatcher := func() *WorkerDispatcher {
		wd := CreateWorkerDispatcher(&servicediscovery.Registry{})
		wd.workerRequestDispatchers[funcKeyWithResource] = requestdispatcher.CreateRequestDispatcher(funcKeyWithResource,
			servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
				ConcurrentNum: 0,
				IdleMode:      false,
			}}, false)
		return wd
	}
	getKeepaliveTime := func(p *WorkerDispatcher, key string) int {
		kaTime := reflect.ValueOf(p.workerRequestDispatchers[key]).Elem().FieldByName("scheduler").Elem().Elem()
		return int(kaTime.FieldByName("keepaliveInterval").Int())
	}
	type args struct {
		keepAliveMap map[string]string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		expected    int
	}{
		{
			name: "case 1 normal case",
			args: args{keepAliveMap: map[string]string{
				funcKeyWithResource: "20",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(urnutils.AnonymizeTenantKey, func(string) string { return funcKeyWithResource }),
				})
				return patches
			},
			expected: 20,
		}, {
			name: "case 2 float type of map keep alive value",
			args: args{keepAliveMap: map[string]string{
				funcKeyWithResource: "20.3",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(urnutils.AnonymizeTenantKey, func(string) string { return funcKeyWithResource }),
				})
				return patches
			},
			expected: 0,
		}, {
			name: "case 3 map includes positive for in keep alive",
			args: args{keepAliveMap: map[string]string{
				funcKeyWithResource: "-9",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(urnutils.AnonymizeTenantKey, func(string) string { return funcKeyWithResource }),
				})
				return patches
			},
			expected: 0,
		}, {
			name: "case 4 value in redis map exceeds the maximum",
			args: args{keepAliveMap: map[string]string{
				funcKeyWithResource: "80",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(urnutils.AnonymizeTenantKey, func(string) string { return funcKeyWithResource }),
				})
				return patches
			},
			expected: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := setupWorkerDispatcher()
			p.UpdateKeepAliveTime(tt.args.keepAliveMap)
			actual := getKeepaliveTime(p, funcKeyWithResource)
			if tt.expected != getKeepaliveTime(p, funcKeyWithResource) {
				t.Errorf("update keep-alive time fiale actual: %d, expected %d", actual, tt.expected)
			}
			patches.ResetAll()
		})
	}
}
