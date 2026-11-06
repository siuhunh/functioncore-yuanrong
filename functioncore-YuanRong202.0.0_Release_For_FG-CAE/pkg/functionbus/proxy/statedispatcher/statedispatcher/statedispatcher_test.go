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

package statedispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/statedispatcher"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	instance    = "worker01"
	stateId     = "state01"
	functionKey = "TenantID01/function01/latest"
	cpu         = "300"
	memory      = "128"
	localIP     = "127.0.0.1"
	remoteIP    = "127.0.0.2"
	retryTimes  = 1
)

var (
	patches          testutils.PatchSlice
	backupSelfNodeIP string
	backupHostName   string
	workerIP         string
)

func mockClientDo(c *http.Client, _ *http.Request) (*http.Response, error) {
	rsp := action.GetWorkerSuccessResponse{
		Worker: &action.Worker{},
		Instance: &types.Instance{
			InstanceID: instance,
			DeployedIP: workerIP,
		},
		Code: 150200,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, nil
}

func setup() {
	stopCh := make(chan struct{}, 1)
	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetFuncKeyFromFuncKeyWithResource, func(funcKeyWithResource string) string {
			return functionKey
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
			info := servicediscovery.FunctionMetaInfo{
				FuncMetaData:     servicediscovery.FuncMetaData{IsBridgeFunction: false},
				InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: 9},
			}
			return json.Marshal(info)
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetFuncKeyWithResource, func(functionKey string, insResource types.InstanceResource) string {
			return functionKey
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(action.ScaleUp, func(scaleUpParam common.ScaleUpParam) (*types.Instance, uint32, string) {
			return &types.Instance{}, 1, ""
		}),
	})
	workerevaluator.CreateInstance(stopCh)

	backupSelfNodeIP = internal.SelfNodeIP
	backupHostName = internal.HostName

	internal.SelfNodeIP = localIP
	internal.HostName = localIP

	client := &http.Client{}

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return client
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockClientDo),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURL, func() string {
			return ""
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(tcpbus.BuildClientConnection,
			func(_ tcpbus.ClientOption) (*tcpbus.Connection, error) {
				return &tcpbus.Connection{}, nil
			}),
	})

	bus := &tcpbus.TCPBus{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "Call",
			func(_ *tcpbus.TCPBus, _ context.Context, _ *common.FunctionProperty, req *types.TCPRequest,
				_ bool) (*types.TCPResponse, error) {
				return &types.TCPResponse{
					RequestID:    req.RequestID,
					ErrorCode:    proxyerrors.StateOKCode,
					ErrorMessage: "",
				}, nil
			}),
	})
}

func tearDown() {
	patches.ResetAll()

	internal.SelfNodeIP = backupSelfNodeIP
	internal.HostName = backupHostName
}

func buildTestRequest() *types.DispatcherRequest {
	return &types.DispatcherRequest{
		FunctionKey: functionKey,
		Request: &types.TCPRequest{
			StateID: stateId,
			CPU:     cpu,
			Memory:  memory,
		},
		Response: make(chan *types.TCPResponse, 1),
	}
}

func buildStateDispatcher() *StateDispatcher {
	registry := &servicediscovery.Registry{
		FuncMeta: make(map[string]*servicediscovery.FunctionMetaInfo),
	}
	return CreateStateDispatcher(registry)
}

// test state function dispatch to local worker
// input:state function req
// output:state function response
func TestRequestWithLocalWorker(t *testing.T) {
	convey.Convey("TestRequestWithLocalWorker", t, func() {
		workerIP = localIP
		setup()
		defer func() {
			tearDown()
		}()

		req := buildTestRequest()
		dispatcher := buildStateDispatcher()

		var rsp *types.TCPResponse
		for i := 0; i < retryTimes; i++ {
			err := dispatcher.Request(req)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
			} else {
				rsp = <-req.Response
				break
			}
		}

		if rsp != nil {
			convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		}

		dispatcher.DeleteStateReqDispatcher(functionKey)
	})
}

func mockNotifyRemote(_ string, _ *common.FunctionProperty, _ *common.NotifyEvent) error {
	return nil
}

func mockSendCallReq(_, _ string, _ *types.TCPRequest, _ *common.FunctionProperty) *types.TCPResponse {
	return &types.TCPResponse{
		RequestID:    1,
		ErrorCode:    proxyerrors.StateOKCode,
		ErrorMessage: "",
	}
}

// test state function dispatch to remote worker
// input:state function req
// output:state function response
func TestRequestWithRemoteWorker(t *testing.T) {
	convey.Convey("TestRequestWithRemoteWorker", t, func() {
		workerIP = remoteIP
		setup()

		backupNotifyRemote := common.NotifyRemote
		backupSendCallReq := common.SendCallReq

		common.NotifyRemote = mockNotifyRemote
		common.SendCallReq = mockSendCallReq
		defer func() {
			tearDown()
			common.NotifyRemote = backupNotifyRemote
			common.SendCallReq = backupSendCallReq
		}()

		req := buildTestRequest()
		dispatcher := buildStateDispatcher()

		var rsp *types.TCPResponse
		for i := 0; i < retryTimes; i++ {
			err := dispatcher.Request(req)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
			} else {
				rsp = <-req.Response
				break
			}
		}

		if rsp != nil {
			convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		}

		dispatcher.DeleteStateReqDispatcher(functionKey)
	})
}

func TestGetFunctionDispatcher(t *testing.T) {
	convey.Convey("TestGetFunctionDispatcher", t, func() {
		registry := servicediscovery.GetRegistry()
		stateReqDispatchers := make(map[string]*statedispatcher.RequestDispatcher, 0)
		stateReqDispatchers[functionKey] = &statedispatcher.RequestDispatcher{}
		stateDispatcher := StateDispatcher{registry: registry,
			stateReqDispatchers: stateReqDispatchers}

		requestDispatcher := stateDispatcher.getFunctionDispatcher(functionKey)
		convey.So(requestDispatcher, convey.ShouldNotEqual, nil)

		registry.FuncMeta = make(map[string]*servicediscovery.FunctionMetaInfo)
		requestDispatcher = stateDispatcher.getFunctionDispatcher(functionKey)
		convey.So(requestDispatcher, convey.ShouldNotEqual, nil)

		registry.FuncMeta = make(map[string]*servicediscovery.FunctionMetaInfo)
		registry.FuncMeta[functionKey] = &servicediscovery.FunctionMetaInfo{}
		requestDispatcher = stateDispatcher.getFunctionDispatcher(functionKey)
		convey.So(requestDispatcher, convey.ShouldNotEqual, nil)

		registry.FuncMeta = make(map[string]*servicediscovery.FunctionMetaInfo)
		registry.FuncMeta[functionKey] = &servicediscovery.FunctionMetaInfo{}
		requestDispatcher = stateDispatcher.getFunctionDispatcher(functionKey)
		convey.So(requestDispatcher, convey.ShouldNotEqual, nil)
	})
}

func Test_InitFunctionDispatcher(t *testing.T) {
	convey.Convey("Test_InitFunctionDispatcher", t, func() {
		registry := servicediscovery.GetRegistry()
		stateReqDispatchers := make(map[string]*statedispatcher.RequestDispatcher, 0)
		stateReqDispatchers[functionKey] = &statedispatcher.RequestDispatcher{}
		stateDispatcher := StateDispatcher{registry: registry,
			stateReqDispatchers: stateReqDispatchers}
		stateDispatcher.InitFunctionDispatcher(functionKey)
	})
}

func TestDeleteInstanceByStateID(t *testing.T) {
	convey.Convey("TestRequestWithLocalWorker", t, func() {
		workerIP = localIP
		setup()
		defer func() {
			tearDown()
		}()

		req := buildTestRequest()
		dispatcher := buildStateDispatcher()

		var rsp *types.TCPResponse
		for i := 0; i < retryTimes; i++ {
			err := dispatcher.Request(req)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
			} else {
				rsp = <-req.Response
				break
			}
		}

		if rsp != nil {
			convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		}

		dispatcher.DeleteInstanceByStateID(functionKey)
	})
}

func TestPutStateQueueMetadataByStateID(t *testing.T) {
	convey.Convey("TestRequestWithLocalWorker", t, func() {
		workerIP = localIP
		setup()
		defer func() {
			tearDown()
		}()

		req := buildTestRequest()
		dispatcher := buildStateDispatcher()

		var rsp *types.TCPResponse
		for i := 0; i < retryTimes; i++ {
			err := dispatcher.Request(req)
			if err != nil {
				time.Sleep(100 * time.Millisecond)
			} else {
				rsp = <-req.Response
				break
			}
		}

		if rsp != nil {
			convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		}

		dispatcher.PutStateQueueMetadataByStateID(functionKey)
	})
}

func TestStateDispatcher_UpdateFunctionDispatcher(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.RequestDispatcher{}), "Stop", func(*statedispatcher.RequestDispatcher) {}),
		gomonkey.ApplyMethod(reflect.TypeOf(&singleflight.SingleFlight{}), "Do", func(*singleflight.SingleFlight, string, func() (interface{}, error)) (interface{}, error) {
			return nil, errors.New("function metadata not found")
		}),
	}
	defer func() {
		time.Sleep(100 * time.Millisecond)
		for i := range patches {
			patches[i].Reset()
		}
	}()
	sd := StateDispatcher{
		stateReqDispatchers: make(map[string]*statedispatcher.RequestDispatcher),
		registry:            &servicediscovery.Registry{FuncMeta: make(map[string]*servicediscovery.FunctionMetaInfo)},
	}

	sd.registry.FuncMeta["12345/0@default@hello/latest"] = &servicediscovery.FunctionMetaInfo{}
	sd.registry.FuncMeta["12345/0@default@called/latest"] = &servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{IsStatefulFunction: true}}

	dispacherFunc := []string{
		"12345/0@default@hello/latest/300-128",
		"12345/0@default@hello/latest/400-256",
		"12345/0@default@called/latest/300-128",
		"12345/0@default@test/latest/300-128",
	}
	for i := range dispacherFunc {
		sd.stateReqDispatchers[dispacherFunc[i]] = &statedispatcher.RequestDispatcher{}
	}

	assert.Equal(t, len(dispacherFunc), len(sd.stateReqDispatchers))

	sd.UpdateFunctionDispatcher("12345/0@default@called/latest")
	assert.Equal(t, len(dispacherFunc), len(sd.stateReqDispatchers))

	sd.UpdateFunctionDispatcher("12345/0@default@hello/latest")
	assert.Equal(t, len(dispacherFunc)-2, len(sd.stateReqDispatchers))

	sd.UpdateFunctionDispatcher("12345/0@default@test/latest")
	assert.Equal(t, len(dispacherFunc)-2, len(sd.stateReqDispatchers))
}


func TestDeleteStateQueueByStateID(t *testing.T) {
	convey.Convey("Test DeleteStateQueueByStateID", t, func() {
		stateRouter := make(map[string]string, 5)
		stateID := "11adfb18-91e5-5f1b-8f1b-4da286e880bd"
		stateRouter["11adfb18-91e5-5f1b-8f1b-4da286e880bd"] = "nodeID"
		stateRouter[constants.TenantIDKey + stateID] = "c53626012ba84727b938ca8bf03108ef"
		stateRouter[constants.ServiceIDKey + stateID]= "0@default"
		stateRouter[constants.BusinessIDKey + stateID] = "yrk"

		ch := make(chan common.StatefulFuncMetaData, 1000)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(statedispatcher.GetStateQueueMetadataChannel, func() chan common.StatefulFuncMetaData {
				return ch
			}),
		}
		defer func() {
			time.Sleep(100 * time.Millisecond)
			for i := range patches {
			patches[i].Reset()
			}
		}()

		DeleteStateQueueByStateID(stateRouter)

		stateQueueMetadataChannel := <- ch
		assert.Equal(t, "11adfb18-91e5-5f1b-8f1b-4da286e880bd", stateQueueMetadataChannel.StateID)
		assert.Equal(t, "c53626012ba84727b938ca8bf03108ef", stateQueueMetadataChannel.TenantID)
		assert.Equal(t, "0@default", stateQueueMetadataChannel.ServiceID)
	})
}