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

package taskdispatcher

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	tenantID        = "TenantID01"
	stateId         = "state01"
	instance        = "worker01"
	functionKey     = "TenantID01/function01/latest"
	functionName    = "function01"
	localHostName   = "127.0.0.1"
	remoteHostName  = "127.0.0.2"
	functionVersion = "latest"
)

var (
	patches        testutils.PatchSlice
	backupHostName string
)

func setup() {
	backupHostName = internal.HostName
	internal.HostName = localHostName

	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetStateValue,
			func(_, _ string) (string, error) {
				return instance, nil
			}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetFunctionTaskNodeValue,
			func(_ string) (string, string, error) {
				return remoteHostName, "", nil
			}),
	})
}

func tearDown() {
	internal.HostName = backupHostName

	patches.ResetAll()
}

func buildTestRequest() *types.DispatcherRequest {
	return &types.DispatcherRequest{
		Request:  &types.TCPRequest{},
		Response: make(chan *types.TCPResponse, 1),
	}
}

func buildTestFunctionProperty() *common.FunctionProperty {
	return &common.FunctionProperty{
		InstanceID:      localHostName,
		TenantID:        tenantID,
		FunctionVersion: functionVersion,
		FunctionName:    functionName,
	}
}

func TestRequestWithLocalTask(t *testing.T) {
	convey.Convey("TestRequestWithLocalTask", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		registry := &servicediscovery.Registry{}
		dispatcher := &TaskDispatcher{registry: registry}
		dst := buildTestFunctionProperty()
		req := buildTestRequest()

		dispatcher.Request(req, dst)
		response := <-req.Response
		convey.So(response.ErrorCode, convey.ShouldEqual, proxyerrors.ForwardCode)
	})
}

func TestRequestWithNoLocalStatelessTask(t *testing.T) {
	convey.Convey("TestRequestWithNoLocalStatelessTask", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		registry := &servicediscovery.Registry{}
		dispatcher := &TaskDispatcher{registry: registry}
		req := buildTestRequest()
		dst := buildTestFunctionProperty()

		dispatcher.Request(req, dst)
		response := <-req.Response
		convey.So(response.ErrorCode, convey.ShouldEqual, proxyerrors.ForwardCode)
		convey.So(req.Position.InstanceID, convey.ShouldEqual, internal.HostName)
	})
}

func TestRequestWithStateInLocalCache(t *testing.T) {
	convey.Convey("TestRequestWithStateInLocalCache", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		registry := &servicediscovery.Registry{
			Providers: make(map[string]map[string]*servicediscovery.Provider, 1),
		}
		m := make(map[string]*servicediscovery.Provider, 1)
		m[instance] = &servicediscovery.Provider{
			NodeIP: remoteHostName,
		}
		registry.Providers[functionKey] = m
		registry.States.Store(stateId, instance)
		dispatcher := &TaskDispatcher{registry: registry}

		req := buildTestRequest()
		dst := buildTestFunctionProperty()
		dst.InstanceID = ""
		dst.StateID = stateId

		dispatcher.Request(req, dst)
		response := <-req.Response
		convey.So(response.ErrorCode, convey.ShouldEqual, proxyerrors.ForwardCode)
		convey.So(req.Position.InstanceID, convey.ShouldEqual, instance)
		convey.So(req.Position.NodeID, convey.ShouldEqual, remoteHostName)
	})
}

func TestRequestWithStateNotInLocalCache(t *testing.T) {
	convey.Convey("TestRequestWithStateNotInLocalCache", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		registry := &servicediscovery.Registry{}
		dispatcher := &TaskDispatcher{registry: registry}
		req := buildTestRequest()
		dst := buildTestFunctionProperty()
		dst.InstanceID = ""
		dst.StateID = stateId

		dispatcher.Request(req, dst)
		response := <-req.Response
		convey.So(response.ErrorCode, convey.ShouldEqual, proxyerrors.ForwardCode)
		convey.So(req.Position.InstanceID, convey.ShouldEqual, instance)
		convey.So(req.Position.NodeID, convey.ShouldEqual, remoteHostName)
	})
}

func TestCreateTaskDispatcher(t *testing.T) {
	convey.Convey("CreateTaskDispatcher", t, func() {
		convey.Convey("taskDispatcher is not nil", func() {
			dispatcher := CreateTaskDispatcher(&servicediscovery.Registry{})
			convey.So(dispatcher, convey.ShouldNotBeNil)
		})
	})
}

func TestRequest(t *testing.T) {
	convey.Convey("Request", t, func() {
		convey.Convey("request error", func() {
			internal.HostName = "test_hostName"
			internal.SelfNodeIP = "test_nodeIP"
			dispatcher := CreateTaskDispatcher(&servicediscovery.Registry{})
			req := &types.DispatcherRequest{
				Response: make(chan *types.TCPResponse, 1),
			}
			err := dispatcher.Request(req, &common.FunctionProperty{
				InstanceID: "test_instanceID",
			})
			convey.So(err, convey.ShouldBeNil)
			convey.So(req.Position.InstanceID, convey.ShouldEqual, "test_hostName")
			convey.So(req.Position.NodeID, convey.ShouldEqual, "test_nodeIP")
			resp := <-req.Response
			convey.So(resp.ErrorCode, convey.ShouldEqual, proxyerrors.ForwardCode)
		})
	})
}

func Test_stateReqToTask(t *testing.T) {
	convey.Convey("Test_stateReqToTask", t, func() {
		convey.Convey("scheduleState error", func() {
			dispatcher := CreateTaskDispatcher(&servicediscovery.Registry{})
			patch := gomonkey.ApplyFunc(utils.GetStateValue, func(stateKey, stateID string) (string, error) {
				return "", errors.New("scheduleState error")
			})
			defer patch.Reset()
			req := &types.DispatcherRequest{
				Response: make(chan *types.TCPResponse, 1),
			}
			dispatcher.stateReqToTask(req, &common.FunctionProperty{
				InstanceID: "test_instanceID",
			})
			resp := <-req.Response
			convey.So(resp.ErrorCode, convey.ShouldEqual, proxyerrors.ScheduleErrCode)
		})
	})
}

func Test_getScheduleRes(t *testing.T) {
	a, b, c := getScheduleRes([]string{"a", "b"})
	assert.Equal(t, "", a)
	assert.Equal(t, "", b)
	assert.Equal(t, "", c)

	a, b, c = getScheduleRes([]string{"a", "b", "c"})
	assert.Equal(t, "a", a)
	assert.Equal(t, "b", b)
	assert.Equal(t, "c", c)
}
