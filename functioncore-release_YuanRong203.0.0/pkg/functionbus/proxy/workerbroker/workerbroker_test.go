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

package workerbroker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/connection"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/functionbus/bus/calls"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/bridgemetrics"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

var (
	resp *types.TCPResponse
)

func mockHandleCancel(m *cancel.Manager, parent context.Context, traceID string, dst *common.CancelProperty,
) *types.TCPResponse {
	return &types.TCPResponse{
		ErrorCode: 0,
	}
}

func mockHandleReq(traceID string, request *types.TCPRequest, dst *common.FunctionProperty) *types.TCPResponse {
	return &types.TCPResponse{
		ErrorCode: 0,
	}
}

func brokerSetup() {
	patches = testutils.InitPatchSlice()
	requestHandler = mockHandleReq

	bus := &tcpbus.TCPBus{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "SetCancelResult",
			func(_ *tcpbus.TCPBus, rsp *types.TCPResponse) error {
				resp = rsp
				return nil
			}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "SetResult",
			func(_ *tcpbus.TCPBus, rsp *types.TCPResponse) error {
				resp = rsp
				return nil
			}),
	})

	manager := &cancel.Manager{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(manager), "HandleCancel", mockHandleCancel),
	})
}

func brokerTeardown() {
	patches.ResetAll()
	resp = nil
}

func TestHandleCancel(t *testing.T) {
	convey.Convey("TestHandleCancel", t, func() {
		brokerSetup()
		defer brokerTeardown()

		broker := workerBroker{
			bus: &tcpbus.TCPBus{},
		}
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, "1234")
		err := broker.handleCancel(ctx, &common.CancelProperty{}, &types.TCPRequest{})
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestHandleRequest(t *testing.T) {
	convey.Convey("TestHandleRequest", t, func() {
		brokerSetup()
		defer brokerTeardown()

		broker := workerBroker{
			bus: &tcpbus.TCPBus{},
		}
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, "1234")
		err := broker.handleRequest(ctx, &common.FunctionProperty{}, &types.TCPRequest{})
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestReportRuntimeMetrics(t *testing.T) {
	convey.Convey("TestReportRuntimeMetrics", t, func() {
		metrics := common.RuntimeMetricsProperty{
			MetricsName: constants.MetricsUsedResourceTimeWindow,
		}
		broker := workerBroker{}
		err := broker.reportRuntimeMetrics(metrics)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func buildTestRequest() *types.DispatcherRequest {
	return &types.DispatcherRequest{
		FunctionKey: funcKey,
		Response:    make(chan *types.TCPResponse, 1),
	}
}

func TestProcessWithRequestCanceled(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}), "GetFunctionMetadata", func(*servicediscovery.Registry, string) (*servicediscovery.FunctionMetaInfo, error) {
		return nil, errors.New("func meta not found")
	})
	defer func() {
		patch.Reset()
	}()
	convey.Convey("TestProcessWithRequestCanceled", t, func() {
		stopCh := make(chan struct{}, 1)
		workerevaluator.CreateInstance(stopCh)
		broker := newWorkerBroker(&BrokerConfig{
			FuncKeyWithResource: funcKeyWithResource,
			Resource: vpa.PodResourceInfo{
				Worker: vpa.ResourceConfig{
					CPULimit:    1000,
					MemoryLimit: 200,
				},
			},
			ViewResource: vpa.ViewResourceInfo{
				Runtime: vpa.ResourceConfig{
					CPULimit:    1000,
					MemoryLimit: 200,
				},
			},
		})
		broker.process(nil)

		broker.readyChan <- struct{}{}
		req := buildTestRequest()
		broker.process(req)
		broker.bus = &tcpbus.TCPBus{}

		req.Canceled = true
		broker.process(req)
		rsp := <-req.Response
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.RequestCanceledCode)

		vpaInfo := broker.getVerticalAutoscalingInfo()
		assert.NotNil(t, vpaInfo, "vpa info should not be nil")
		assert.Equal(t, int64(200*mbToBytes), vpaInfo.workerMemoryLimit)
	})
}

func TestRecoverWithInvalidBus(t *testing.T) {
	convey.Convey("TestRecoverWithInvalidBus", t, func() {
		broker := &workerBroker{}
		rsp := broker.recover(1, "1", false)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func TestCancelWithInvalidBus(t *testing.T) {
	convey.Convey("TestCancelWithInvalidBus", t, func() {
		broker := &workerBroker{}
		rsp := broker.cancel(nil, 1, "1")
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func mockCancel(t *tcpbus.TCPBus, ctx context.Context, dst *common.CancelProperty,
	req *types.TCPRequest) (*types.TCPResponse, error) {
	return nil, errors.New("cancel error")
}

func TestCancelWithBusReturnErr(t *testing.T) {
	convey.Convey("TestCancelWithBusReturnErr", t, func() {
		broker := &workerBroker{}
		bus := &tcpbus.TCPBus{}
		broker.bus = bus
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(bus), "Cancel", mockCancel),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		rsp := broker.cancel(nil, 1, "1")
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func mockRecoverFail(t *tcpbus.TCPBus, ctx context.Context, req *types.TCPRequest) (*types.TCPResponse, error) {
	return nil, errors.New("recover error")
}

func TestRecoverWithBusReturnErr(t *testing.T) {
	convey.Convey("TestRecoverWithBusReturnErr", t, func() {
		broker := &workerBroker{}
		bus := &tcpbus.TCPBus{}
		broker.bus = bus
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(bus), "Recover", mockRecoverFail),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		rsp := broker.recover(1, "1", false)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func TestRecoverWithTimeout(t *testing.T) {
	convey.Convey("TestRecoverWithBusReturnErr", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(pool.NewConnPool, func(opt pool.Options) (*pool.ConnPool, error) {
				return &pool.ConnPool{}, nil
			}),
			gomonkey.ApplyFunc((*pool.ConnPool).Get, func(*pool.ConnPool) (net.Conn, error) {
				return &connection.BufferedConn{}, nil
			}),
			gomonkey.ApplyFunc((*connection.BufferedConn).SetWriteDeadline,
				func(*connection.BufferedConn, time.Time) error {
					return nil
				}),
			gomonkey.ApplyFunc((*bytes.Buffer).WriteTo,
				func(*bytes.Buffer, io.Writer) (n int64, err error) {
					return 1, nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		broker := &workerBroker{}
		tcpBusOpt := &tcpbus.Option{CreateClient: true,
			Client: &tcpbus.Connection{}, ServerAddr: workerAddr, Remote: true}
		tcpBus, err := tcpbus.NewTCPBus(common.BusOption{NewBusOpt: tcpBusOpt})
		convey.So(err, convey.ShouldBeNil)
		broker.bus = tcpBus
		rsp := broker.recover(1, "1", false)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.TimeoutErrCode)
		convey.So(rsp.ErrorMessage, convey.ShouldEqual, proxyerrors.TimeoutErr)
	})
}

func Test_getRequestTimeout(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}),
			"GetDirectFunctionMetadata", func(r *servicediscovery.Registry, key string) (*servicediscovery.FunctionMetaInfo, error) {
				if key == "12345678901234561234567890123456/0@default@hello/latest" {
					return &servicediscovery.FunctionMetaInfo{
						FuncMetaData: servicediscovery.FuncMetaData{Timeout: 3},
					}, nil
				}
				if key == "12345678901234561234567890123456/0@default@image/latest" {
					return &servicediscovery.FunctionMetaInfo{
						FuncMetaData: servicediscovery.FuncMetaData{
							Timeout:           10,
							IsCustomContainer: true,
						},
					}, nil
				}
				if key == "12345678901234561234567890123456/0@default@init/latest" {
					return &servicediscovery.FunctionMetaInfo{
						FuncMetaData: servicediscovery.FuncMetaData{
							Timeout: 10,
						},
						ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 3}},
					}, nil
				}
				if key == "12345678901234561234567890123456/0@default@longrun/latest" {
					return &servicediscovery.FunctionMetaInfo{
						FuncMetaData: servicediscovery.FuncMetaData{
							Timeout: 1000,
						},
					}, nil
				}
				return nil, errors.New("function metadata not found")
			}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	commonDelay := int64(8)
	common.FConfig.MaxWorkerDelay = commonDelay

	tests := []struct {
		name                      string
		funcKeyWithRes            string
		expected                  time.Duration
		expectedDirectCallTimeout time.Duration
		expectedDirectInitTimeout time.Duration
		longRunFunc               bool
	}{
		{
			name:                      "func not exits",
			funcKeyWithRes:            "12345678901234561234567890123456/0@default@notexist/latest/300-128",
			expected:                  time.Duration(defaultRequestTimeout+commonDelay) * time.Second,
			expectedDirectCallTimeout: 900 * time.Second,
			expectedDirectInitTimeout: defaultInitializeTimeout * time.Second,
			longRunFunc:               true,
		},
		{
			name:                      "normal func",
			funcKeyWithRes:            "12345678901234561234567890123456/0@default@hello/latest/300-128",
			expected:                  time.Duration(3+commonDelay) * time.Second,
			expectedDirectCallTimeout: 3 * time.Second,
			expectedDirectInitTimeout: defaultInitializeTimeout * time.Second,
		},
		{
			name:                      "custom image func",
			funcKeyWithRes:            "12345678901234561234567890123456/0@default@image/latest/300-128",
			expected:                  time.Duration(10+commonDelay) * time.Second,
			expectedDirectCallTimeout: 10 * time.Second,
			expectedDirectInitTimeout: defaultInitializeTimeout * time.Second,
		},
		{
			name:                      "init func",
			funcKeyWithRes:            "12345678901234561234567890123456/0@default@init/latest/300-128",
			expected:                  time.Duration(13+commonDelay) * time.Second,
			expectedDirectCallTimeout: 10 * time.Second,
			expectedDirectInitTimeout: 3 * time.Second,
		},
		{
			name:                      "long run",
			funcKeyWithRes:            "12345678901234561234567890123456/0@default@longrun/latest/300-128",
			expected:                  time.Duration(900+commonDelay) * time.Second,
			expectedDirectCallTimeout: 1000 * time.Second,
			expectedDirectInitTimeout: defaultInitializeTimeout * time.Second,
			longRunFunc:               true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetRequestTimeout(tt.funcKeyWithRes), tt.name)
			callTimeout, initTimeout, flag := GetDirectTimeout(tt.funcKeyWithRes)
			assert.Equal(t, tt.expectedDirectCallTimeout, callTimeout, tt.name)
			assert.Equal(t, tt.expectedDirectInitTimeout, initTimeout, tt.name)
			assert.Equal(t, tt.longRunFunc, flag, tt.name)
		})
	}
}

func Test_workerBroker_updateRuntimeConn(t *testing.T) {
	updateCh := make(chan tcpbus.RuntimeReport, 1)
	wb := &workerBroker{
		workerID:          workerID,
		isDirectFunction:  true,
		runtimeConnUpdate: updateCh,
		dstProperty:       &common.FunctionProperty{},
		stopCh:            make(chan struct{}),
		directLock:        new(sync.RWMutex),
	}
	go wb.updateRuntimeConn()

	assert.Nil(t, wb.runtimeConn.Load())
	updateCh <- tcpbus.RuntimeReport{}
	time.Sleep(100 * time.Millisecond)
	assert.NotNil(t, wb.runtimeConn.Load())
	close(wb.stopCh)
}

func Test_workerBroker_backgroundPause(t *testing.T) {
	pauseCh := make(chan time.Time, 1)
	reportCh := make(chan time.Time, 1)

	pauseCount := uint64(0)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(time.NewTicker, func(t time.Duration) *time.Ticker {
			if t == pauseCheckTime {
				return &time.Ticker{C: pauseCh}
			}
			return &time.Ticker{C: reportCh}
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).Pause, func(_ *tcpbus.TCPBus, _ context.Context) (*types.TCPResponse, error) {
			atomic.AddUint64(&pauseCount, 1)
			return &types.TCPResponse{}, nil
		}),
	}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	wb := &workerBroker{
		workerID:         workerID,
		isDirectFunction: true,
		dstProperty:      &common.FunctionProperty{},
		stopCh:           make(chan struct{}),
		reportCache:      &sync.Map{},
		bus:              &tcpbus.TCPBus{},
		directLock:       new(sync.RWMutex),
	}
	wb.reportCache.Store("invalid", "some invalid value")
	wb.reportCache.Store("outdata", common.WorkerReportNotify{ReportTime: time.Now().Add(-time.Hour)})
	wb.reportCache.Store("valid", common.WorkerReportNotify{ReportTime: time.Now()})
	count := 0
	wb.reportCache.Range(func(key, value any) bool {
		count++
		return true
	})

	assert.Equal(t, 3, count)

	go wb.backgroundPause()

	pauseCh <- time.Time{}
	reportCh <- time.Time{}
	time.Sleep(100 * time.Millisecond)
	// last req finished not match, don't pause
	assert.Equal(t, uint64(0), atomic.LoadUint64(&pauseCount))

	wb.lastReqFinish = time.Now().Add(-time.Hour)
	pauseCh <- time.Time{}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(1), atomic.LoadUint64(&pauseCount))
	assert.Equal(t, true, wb.paused)

	count = 0
	wb.reportCache.Range(func(key, value any) bool {
		count++
		return true
	})

	assert.Equal(t, 2, count)
	close(wb.stopCh)
}

func Test_workerBroker_doRequest(t *testing.T) {
	callCtrl := ""
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
			req *types.TCPRequest, isSendingRemoteProxy bool) (*types.TCPResponse, error) {
			if callCtrl == "returnError" {
				return nil, errors.New("some error")
			}
			if callCtrl == "returnClientExitError" {
				return &types.TCPResponse{
					ErrorCode:    proxyerrors.ClientExitErrCode,
					ErrorMessage: "client exit",
				}, nil
			}
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestStart, func(_ *workerevaluator.WorkerEvaluator, _ string) {
			return
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestEnd, func(_ *workerevaluator.WorkerEvaluator, _ string, _ time.Duration) {
			return
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).DeleteWorkerMetrics, func(_ *workerevaluator.WorkerEvaluator, _ string, _ string) {
			return
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	runtimeConn := atomic.Value{}
	runtimeConn.Store(&tcpbus.TCPBus{})
	lock := new(sync.RWMutex)
	wb := &workerBroker{
		workerID:         workerID,
		isDirectFunction: true,
		dstProperty:      &common.FunctionProperty{},
		stopCh:           make(chan struct{}),
		reportCache:      &sync.Map{},
		bus:              &tcpbus.TCPBus{},
		runtimeConn:      runtimeConn,
		directLock:       lock,
		recoverBlock:     sync.NewCond(lock),
	}

	tcpBus := &tcpbus.TCPBus{
		Property: common.FunctionProperty{},
	}
	request := &types.DispatcherRequest{
		TraceID:  "testTraceID",
		Request:  &types.TCPRequest{LogType: constants.RuntimeLogOptTail},
		Response: make(chan *types.TCPResponse, 1),
	}
	callCtrl = "returnError"
	wb.doRequest(request, tcpBus)
	rsp := <-request.Response
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	callCtrl = ""
	wb.doRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.StateOKCode), rsp.ErrorCode)
}

func Test_workerBroker_doDirectRequest(t *testing.T) {
	recoverCtrl := ""
	checkCtrl := ""
	callCtrl := ""
	directRequestCountMatchTime := 0
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*tcpbus.TCPBus).Init, func(_ *tcpbus.TCPBus, _ context.Context) (*types.TCPResponse, error) {
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).CheckRuntimeConnect, func(_ *tcpbus.TCPBus) bool {
			return true
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
			if recoverCtrl == "returnError" {
				return nil, errors.New("some error")
			}
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).CheckRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ string, _ bool) (*types.TCPResponse, error) {
			if checkCtrl == "returnError" {
				return nil, errors.New("check runtime internal error")
			}
			if checkCtrl == "returnErrorResponse" {
				return &types.TCPResponse{
					ErrorCode:    proxyerrors.InternalErrCode,
					ErrorMessage: "client exit",
				}, nil
			}
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
			req *types.TCPRequest, isSendingRemoteProxy bool) (*types.TCPResponse, error) {
			if callCtrl == "returnError" {
				return nil, errors.New("some error")
			}
			if callCtrl == "returnClientExitError" {
				return &types.TCPResponse{
					ErrorCode:    proxyerrors.ClientExitErrCode,
					ErrorMessage: "client exit",
				}, nil
			}
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestStart, func(_ *workerevaluator.WorkerEvaluator, _ string) {
			return
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestEnd, func(_ *workerevaluator.WorkerEvaluator, _ string, _ time.Duration) {
			return
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).DeleteWorkerMetrics, func(_ *workerevaluator.WorkerEvaluator, _ string, _ string) {
			return
		}),
		gomonkey.ApplyFunc((*workerBroker).blockAndRecoverRuntime, func(_ *workerBroker) {
			return
		}),
		gomonkey.ApplyFunc((*calls.CallCollector).AddDirectRequestCount, func(c *calls.CallCollector, count int64) {
			if count == 1 {
				directRequestCountMatchTime++
			}
			if count == -1 && directRequestCountMatchTime == 1 {
				directRequestCountMatchTime++
			}
			return
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	runtimeConn := atomic.Value{}
	runtimeConn.Store(&tcpbus.TCPBus{})
	lock := new(sync.RWMutex)
	wb := &workerBroker{
		workerID:              workerID,
		isDirectFunction:      true,
		dstProperty:           &common.FunctionProperty{},
		stopCh:                make(chan struct{}),
		reportCache:           &sync.Map{},
		asyncDirectRequestMap: &sync.Map{},
		bus:                   &tcpbus.TCPBus{},
		runtimeConn:           runtimeConn,
		directLock:            lock,
		reInitLock:            new(sync.Mutex),
		recoverBlock:          sync.NewCond(lock),
	}
	tcpBus := &tcpbus.TCPBus{
		Property: common.FunctionProperty{},
	}
	request := &types.DispatcherRequest{
		TraceID:  "testTraceID",
		Request:  &types.TCPRequest{LogType: constants.RuntimeLogOptTail},
		Response: make(chan *types.TCPResponse, 1),
	}
	wb.hasInitializer, wb.isInitialized, wb.isMinInstance = true, false, true
	wb.initErrResponse = &types.TCPResponse{ErrorCode: proxyerrors.InternalErrCode, ErrorMessage: "internal error"}
	wb.doDirectRequest(request, tcpBus)
	rsp := <-request.Response
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	wb.hasInitializer, wb.isInitialized, wb.isMinInstance = true, false, false
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.StateOKCode), rsp.ErrorCode)
	assert.Equal(t, directRequestCountMatchTime, 2)
	wb.hasInitializer, wb.paused = false, true
	recoverCtrl = "returnError"
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	recoverCtrl = ""
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.StateOKCode), rsp.ErrorCode)
	callCtrl = "returnError"
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	callCtrl = "returnClientExitError"
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.ClientExitErrCode), rsp.ErrorCode)
	callCtrl = ""
	ch := make(chan common.WorkerReport, 1)
	ch <- common.WorkerReport{Logs: "some log", Summary: "some summary"}
	wb.reportCache.Store("testTraceID", common.WorkerReportNotify{Ch: ch})
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, uint32(proxyerrors.StateOKCode), rsp.ErrorCode)
	assert.Equal(t, "some log", rsp.Logs)
	// use runtime summary instead of report summary
	assert.Equal(t, "", rsp.Summary)
}

func Test_workerBroker_checkRuntime(t *testing.T) {
	checkCount := 0
	checkCtrl := ""
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*tcpbus.TCPBus).CheckRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ string, _ bool) (*types.TCPResponse, error) {
			checkCount++
			if checkCtrl == "returnError" {
				return nil, errors.New("check runtime internal error")
			}
			if checkCtrl == "returnErrorResponse" {
				return &types.TCPResponse{
					ErrorCode:    proxyerrors.InternalErrCode,
					ErrorMessage: "client exit",
				}, nil
			}
			return &types.TCPResponse{}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	runtimeConn := atomic.Value{}
	runtimeConn.Store(&tcpbus.TCPBus{})
	lock := new(sync.RWMutex)
	wb := &workerBroker{
		workerID:         workerID,
		isDirectFunction: true,
		dstProperty:      &common.FunctionProperty{},
		stopCh:           make(chan struct{}),
		reportCache:      &sync.Map{},
		bus:              &tcpbus.TCPBus{},
		runtimeConn:      runtimeConn,
		directLock:       lock,
		recoverBlock:     sync.NewCond(lock),
	}
	report, err := wb.checkRuntime("", false)
	assert.Nil(t, err)
	assert.NotNil(t, report)
	checkCtrl = "returnError"
	report, err = wb.checkRuntime("", false)
	assert.Equal(t, "check runtime internal error", err.Error())
	assert.Nil(t, report)
	checkCtrl = "returnErrorResponse"
	report, err = wb.checkRuntime("", false)
	assert.Nil(t, err)
	assert.Equal(t, proxyerrors.InternalErrCode, int(report.ErrorCode))
}

func Test_workerBroker_doInitialize(t *testing.T) {
	initCount := 0
	recoverCount := 0
	checkCount := 0
	initCtrl := ""
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*tcpbus.TCPBus).Init, func(_ *tcpbus.TCPBus, _ context.Context) (*types.TCPResponse, error) {
			initCount++
			if initCtrl == "returnError" {
				return nil, errors.New("init internal error")
			}
			if initCtrl == "returnClientExitError" {
				return &types.TCPResponse{
					ErrorCode:    proxyerrors.ClientExitErrCode,
					ErrorMessage: "client exit",
				}, nil
			}
			if initCtrl == "returnTimeoutError" {
				return &types.TCPResponse{
					ErrorCode:    proxyerrors.TimeoutErrCode,
					ErrorMessage: "client exit",
				}, nil
			}
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
			recoverCount++
			return &types.TCPResponse{}, nil
		}),
		gomonkey.ApplyFunc((*tcpbus.TCPBus).CheckRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ string, _ bool) (*types.TCPResponse, error) {
			checkCount++
			return &types.TCPResponse{}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	t.Run("no runtime connection available", func(t *testing.T) {
		wb := newTestWorkerBroker()
		rsp := wb.doInitialize("")
		assert.Equal(t, proxyerrors.InternalErrCode, int(rsp.ErrorCode))
	})
	t.Run("init success", func(t *testing.T) {
		wb := newTestWorkerBroker()
		wb.runtimeConn.Store(&tcpbus.TCPBus{})
		wb.isInitialized = true
		rsp := wb.doInitialize("")
		assert.Equal(t, 0, initCount)
		assert.Equal(t, proxyerrors.StateOKCode, int(rsp.ErrorCode))
	})
	t.Run("init internal error", func(t *testing.T) {
		wb := newTestWorkerBroker()
		wb.runtimeConn.Store(&tcpbus.TCPBus{})
		wb.isInitialized = false
		initCtrl = "returnError"
		rsp := wb.doInitialize("")
		assert.Equal(t, 1, initCount)
		assert.Equal(t, proxyerrors.InternalErrCode, int(rsp.ErrorCode))
	})
	t.Run("client exit", func(t *testing.T) {
		wb := newTestWorkerBroker()
		wb.runtimeConn.Store(&tcpbus.TCPBus{})
		initCtrl = "returnClientExitError"
		rsp := wb.doInitialize("")
		defer closeRecoverWaitNewConnCh(wb.recoverWaitNewConnCh)
		time.Sleep(500 * time.Millisecond)
		assert.Equal(t, 2, initCount)
		assert.Equal(t, 1, recoverCount)
		assert.Equal(t, proxyerrors.ClientExitErrCode, int(rsp.ErrorCode))
		assert.Equal(t, proxyerrors.ClientExitErrCode, int(wb.initErrResponse.ErrorCode))
	})
	t.Run("runtime init timeout", func(t *testing.T) {
		wb := newTestWorkerBroker()
		wb.runtimeConn.Store(&tcpbus.TCPBus{})
		initCtrl = "returnTimeoutError"
		rsp := wb.doInitialize("")
		defer closeRecoverWaitNewConnCh(wb.recoverWaitNewConnCh)
		time.Sleep(500 * time.Millisecond)
		assert.Equal(t, 3, initCount)
		assert.Equal(t, 2, checkCount)
		assert.Equal(t, snerror.RuntimeInitTimeoutCode, int(rsp.ErrorCode))
		assert.Equal(t, snerror.RuntimeInitTimeoutCode, int(wb.initErrResponse.ErrorCode))
	})
	t.Run("init success 2", func(t *testing.T) {
		wb := newTestWorkerBroker()
		wb.runtimeConn.Store(&tcpbus.TCPBus{})
		initCtrl = ""
		rsp := wb.doInitialize("")
		assert.Equal(t, 4, initCount)
		assert.Equal(t, proxyerrors.StateOKCode, int(rsp.ErrorCode))
	})
}

func Test_workerBroker_recoverRuntime(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
			return &types.TCPResponse{}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	t.Run("recoverRuntime restart false", func(t *testing.T) {
		wb := newTestWorkerBroker()
		err := wb.recoverRuntime(false)
		assert.Nil(t, wb.recoverWaitNewConnCh)
		assert.NoError(t, err)
	})
	t.Run("recoverRuntime restart false", func(t *testing.T) {
		wb := newTestWorkerBroker()
		err := wb.recoverRuntime(true)
		assert.NotNil(t, wb.recoverWaitNewConnCh)
		assert.NoError(t, err)
	})
}

func closeRecoverWaitNewConnCh(recoverWaitNewConnCh chan struct{}) {
	if recoverWaitNewConnCh != nil {
		close(recoverWaitNewConnCh)
	}
}

func newTestWorkerBroker() *workerBroker {
	lock := new(sync.RWMutex)
	wb := &workerBroker{
		workerID:         workerID,
		isDirectFunction: true,
		dstProperty:      &common.FunctionProperty{},
		stopCh:           make(chan struct{}),
		reportCache:      &sync.Map{},
		bus:              &tcpbus.TCPBus{},
		runtimeConn:      atomic.Value{},
		directLock:       lock,
		recoverBlock:     sync.NewCond(lock),
		reInitLock:       new(sync.Mutex),
	}
	return wb
}

func Test_workerBroker_blockAndRecoverRuntime(t *testing.T) {
	wb := newTestWorkerBroker()
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
			return &types.TCPResponse{}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	wb.blockAndRecoverRuntime()
	closeRecoverWaitNewConnCh(wb.recoverWaitNewConnCh)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, false, wb.blockForRecover)
}

func TestStoreBridgeMetrics(t *testing.T) {
	convey.Convey("TestStoreBridgeMetrics redis client is nil", t, func() {
		wb := &workerBroker{}
		wb.storeBridgeMetrics(common.BridgeFuncMetrics{})
	})
	convey.Convey("TestStoreBridgeMetrics ok", t, func() {
		count := 0
		gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		gomonkey.ApplyFunc(redisclient.ZAddToRedis,
			func(key, value string, expireTime time.Duration) error {
				count++
				return nil
			})
		wb := &workerBroker{}
		wb.storeBridgeMetrics(common.BridgeFuncMetrics{})
		convey.So(count, convey.ShouldEqual, 1)
	})
	convey.Convey("TestStoreBridgeMetrics failed", t, func() {
		count := 0
		gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		gomonkey.ApplyFunc(redisclient.ZAddToRedis,
			func(key, value string, expireTime time.Duration) error {
				count++
				return errors.New("error happened")
			})
		wb := &workerBroker{}
		wb.storeBridgeMetrics(common.BridgeFuncMetrics{})
		convey.So(count, convey.ShouldEqual, 3)
	})
	convey.Convey("TestStoreBridgeMetrics for bridge manager", t, func() {
		metrics := common.BridgeFuncMetrics{ForBridgeManager: true}
		mockHandler := &bridgemetrics.Handler{}
		count := 0

		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(mockHandler, "Store", func(_ *bridgemetrics.Handler, bridgeMetrics common.BridgeFuncMetrics) {
				count++
			}),
			gomonkey.ApplyFunc(bridgemetrics.GetBridgeMetricsHandler, func() *bridgemetrics.Handler {
				return mockHandler
			}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()

		wb := &workerBroker{}
		wb.storeBridgeMetrics(metrics)
		convey.So(count, convey.ShouldEqual, 1)

		// will not call bridgemetrics.GetBridgeMetricsHandler().Store
		metrics = common.BridgeFuncMetrics{ForBridgeManager: false}
		wb.storeBridgeMetrics(metrics)
		convey.So(count, convey.ShouldEqual, 1)
	})
}

func TestWatchSyncRequestTimeout(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*workerBroker).ProcessAsyncResult,
			func(wb *workerBroker, msg *protocol.Message, requestID string, clientExited bool) {
				wb.asyncDirectRequestMap.Delete(requestID)
			}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	wb := &workerBroker{isDirectFunction: true, longRunFunc: true,
		invokeTimeout: 1 * time.Second, asyncDirectRequestMap: &sync.Map{}}
	wb.storeAsyncDirectRequestMap(&types.DispatcherRequest{TraceID: "request001", Request: &types.TCPRequest{}})
	_, ok := wb.asyncDirectRequestMap.Load("request001")
	assert.Equal(t, true, ok)
	go wb.watchSyncRequestTimeout()
	time.Sleep(2 * time.Second)
	_, ok = wb.asyncDirectRequestMap.Load("request001")
	assert.Equal(t, false, ok)
}

func TestProcessAsyncResult(t *testing.T) {
	convey.Convey("TestProcessAsyncResult request id nil", t, func() {
		wb := &workerBroker{asyncDirectRequestMap: &sync.Map{}}
		wb.ProcessAsyncResult(nil, "", false)
		wb.asyncDirectRequestMap.Store("AAA", "AAA")
		wb.ProcessAsyncResult(&protocol.Message{}, "AAA", false)
	})
	convey.Convey("TestProcessAsyncResult runtime exited", t, func() {
		var response asyncResponse
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*workerBroker).checkRuntime,
				func(wb *workerBroker, traceID string, shouldWait bool) (*common.WorkerReport, error) {
					return &common.WorkerReport{Summary: "{\"funcDigest\":\"5db11d486b320e62d929b4bf95e16ea\",\"duration\":46144.000,\"billingDuration\":46144,\"memorySize\":512,\"memoryUsed\":108.297,\"podName\":\"pool24-600-512-fusion-64bdd8757b-x6mxl\",\"gpuMemorySize\":0,\"ephemeralStorage\":0}"}, nil
				}),
			gomonkey.ApplyFunc(workerevaluator.ProcessAsyncResult,
				func(code int, header map[string]string, body []byte, traceID string, instanceID string) {
					response = asyncResponse{
						statusCode: code,
						header:     header,
						body:       body,
						instanceID: instanceID,
					}
				}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()
		wb := &workerBroker{isDirectFunction: true, isConcurrent: true,
			summaryRecorder: &SummaryRecorder{}, directLock: new(sync.RWMutex),
			invokeTimeout: 1 * time.Second, asyncDirectRequestMap: &sync.Map{}}
		request := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa866",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa866", request)
		msg := &protocol.Message{}
		msg.Header.ErrCode = proxyerrors.ClientExitErrCode
		msg.Payload = []byte(proxyerrors.ClientExitErr)
		wb.ProcessAsyncResult(msg, "75215b09-27dd-4e27-93f2-df6e627aa866", false)
		assert.Equal(t, 500, response.statusCode)
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])

		request2 := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa868",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556f8",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa868", request2)
		wb.ProcessAsyncResult(nil, "", true)
		assert.Equal(t, 500, response.statusCode)
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556f8", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])
	})

	convey.Convey("TestProcessAsyncResult runtime timeout", t, func() {
		var response asyncResponse
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*workerBroker).checkRuntime,
				func(wb *workerBroker, traceID string, shouldWait bool) (*common.WorkerReport, error) {
					return &common.WorkerReport{Summary: "{\"funcDigest\":\"5db11d486b320e62d929b4bf95e16ea\",\"duration\":46144.000,\"billingDuration\":46144,\"memorySize\":512,\"memoryUsed\":108.297,\"podName\":\"pool24-600-512-fusion-64bdd8757b-x6mxl\",\"gpuMemorySize\":0,\"ephemeralStorage\":0}"}, nil
				}),
			gomonkey.ApplyFunc(workerevaluator.ProcessAsyncResult,
				func(code int, header map[string]string, body []byte, traceID string, instanceID string) {
					response = asyncResponse{
						statusCode: code,
						header:     header,
						body:       body,
						instanceID: instanceID,
					}
				}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()
		wb := &workerBroker{isDirectFunction: true, isConcurrent: true,
			summaryRecorder: &SummaryRecorder{}, directLock: new(sync.RWMutex),
			invokeTimeout: 300 * time.Second, asyncDirectRequestMap: &sync.Map{}}
		request := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa866",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa866", request)
		msg := &protocol.Message{}
		msg.Header.ErrCode = proxyerrors.TimeoutErrCode
		msg.Payload = []byte(proxyerrors.TimeoutErr)
		wb.ProcessAsyncResult(msg, "75215b09-27dd-4e27-93f2-df6e627aa866", false)
		assert.Equal(t, 500, response.statusCode)
		assert.Equal(t, "{\"code\":4010,\"message\":\"call invoke timeout 5m0s\"}", string(response.body))
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])

	})

	convey.Convey("TestProcessAsyncResult posix runtime execute succeed", t, func() {
		var response asyncResponse
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(workerevaluator.ProcessAsyncResult,
				func(code int, header map[string]string, body []byte, traceID string, instanceID string) {
					response = asyncResponse{
						statusCode: code,
						header:     header,
						body:       body,
						instanceID: instanceID,
					}
				}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()
		wb := &workerBroker{isDirectFunction: true, isConcurrent: true,
			summaryRecorder: &SummaryRecorder{}, directLock: new(sync.RWMutex),
			invokeTimeout: 300 * time.Second, asyncDirectRequestMap: &sync.Map{}}
		request := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa866",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa866", request)

		message := &pb.Message{Type: pb.Message_POSIX_CALL_RESPONSE,
			PosixCallResult: &pb.PosixCallResult{RawData: []byte("hello world")}}
		payload, _ := proto.Marshal(message)
		msg := &protocol.Message{Payload: payload}
		wb.ProcessAsyncResult(msg, "75215b09-27dd-4e27-93f2-df6e627aa866", false)
		assert.Equal(t, 200, response.statusCode)
		assert.Equal(t, "hello world", string(response.body))
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])
	})

	convey.Convey("TestProcessAsyncResult posix runtime execute succeed - notConcurrent", t, func() {
		var response asyncResponse
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(workerevaluator.ProcessAsyncResult,
				func(code int, header map[string]string, body []byte, traceID string, instanceID string) {
					response = asyncResponse{
						statusCode: code,
						header:     header,
						body:       body,
						instanceID: instanceID,
					}
				}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()
		wb := &workerBroker{isDirectFunction: true, summaryRecorder: &SummaryRecorder{}, directLock: new(sync.RWMutex),
			invokeTimeout: 300 * time.Second, asyncDirectRequestMap: &sync.Map{}}
		request := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa866",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa866", request)

		message := &pb.Message{Type: pb.Message_POSIX_CALL_RESPONSE,
			PosixCallResult: &pb.PosixCallResult{RawData: []byte("hello world"),
				RequestID: "75215b09-27dd-4e27-93f2-df6e627aa866"}}
		payload, _ := proto.Marshal(message)
		msg := &protocol.Message{Payload: payload}
		wb.ProcessAsyncResult(msg, "75215b09-27dd-4e27-93f2-df6e627aa866", false)
		assert.Equal(t, 200, response.statusCode)
		assert.Equal(t, "hello world", string(response.body))
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])

		message2 := &pb.Message{Type: pb.Message_POSIX_CALL_RESPONSE,
			PosixCallResult: &pb.PosixCallResult{RawData: []byte("hello world222"),
				RequestID: "75215b09-27dd-4e27-93f2-df6e627aa867"}}
		payload2, _ := proto.Marshal(message2)
		msg2 := &protocol.Message{Payload: payload2}
		request2 := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa867",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa867", request2)
		wb.ProcessAsyncResult(msg2, "", false)

		assert.Equal(t, 200, response.statusCode)
		assert.Equal(t, "hello world222", string(response.body))
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])
	})

	convey.Convey("TestProcessAsyncResult runtime execute succeed", t, func() {
		var response asyncResponse
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(workerevaluator.ProcessAsyncResult,
				func(code int, header map[string]string, body []byte, traceID string, instanceID string) {
					response = asyncResponse{
						statusCode: code,
						header:     header,
						body:       body,
						instanceID: instanceID,
					}
				}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()
		wb := &workerBroker{isDirectFunction: true, isConcurrent: true,
			summaryRecorder: &SummaryRecorder{}, directLock: new(sync.RWMutex),
			invokeTimeout: 300 * time.Second, asyncDirectRequestMap: &sync.Map{}}
		request := asyncRequest{
			traceID:     "75215b09-27dd-4e27-93f2-df6e627aa866",
			instanceID:  "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
			functionKey: "12345678901234561234567890123456/0-default-asynctt/latest/600-512",
			startTime:   time.Now(),
			headers:     make(map[string]string),
		}
		wb.asyncDirectRequestMap.Store("75215b09-27dd-4e27-93f2-df6e627aa866", request)

		message := &pb.Message{Type: pb.Message_CALL_RESPONSE,
			CallResponse: &pb.CallResponse{RawData: []byte("hello world")}}
		payload, _ := proto.Marshal(message)
		msg := &protocol.Message{Payload: payload}
		wb.ProcessAsyncResult(msg, "75215b09-27dd-4e27-93f2-df6e627aa866", false)
		assert.Equal(t, 200, response.statusCode)
		assert.Equal(t, "hello world", string(response.body))
		assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", response.instanceID)
		assert.Equal(t, "12345678901234561234567890123456@default@asynctt@latest@512",
			response.header[constants.HeaderFuncURN])
	})
}

func TestRecoverAsyncRequest(t *testing.T) {
	stopCh := make(chan struct{}, 1)
	workerevaluator.CreateInstance(stopCh)
	wb := &workerBroker{isDirectFunction: true, longRunFunc: true, isConcurrent: true,
		summaryRecorder: &SummaryRecorder{}, directLock: new(sync.RWMutex),
		workerID:            "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz",
		funcKeyWithResource: "c53626012ba84727b938ca8bf03108ef/0@default@java-test/latest/300-128",
		invokeTimeout:       300 * time.Second, asyncDirectRequestMap: &sync.Map{}}
	report := tcpbus.RuntimeReport{RequestMap: make(map[string]*pb.Message_RecoverAsyncRequest)}
	now := time.Now()
	report.RequestMap["requestID001"] = &pb.Message_RecoverAsyncRequest{}
	report.RequestMap["requestID002"] = &pb.Message_RecoverAsyncRequest{StartTime: now.Unix()}
	wb.RecoverAsyncRequest(report)
	_, ok1 := wb.asyncDirectRequestMap.Load("requestID001")
	assert.Equal(t, true, ok1)
	req02, ok2 := wb.asyncDirectRequestMap.Load("requestID002")
	assert.Equal(t, true, ok2)
	req, ok := req02.(asyncRequest)
	assert.Equal(t, true, ok)
	assert.Equal(t, "defaultaz-#-pool24-600-512-fusion-64bdd8757b-556fz", req.instanceID)
	assert.Equal(t, "requestID002", req.traceID)
	assert.Equal(t, "c53626012ba84727b938ca8bf03108ef/0@default@java-test/latest", req.functionKey)
	assert.Equal(t, now.Add(300*time.Second).Second(), req.deadline.Second())
	assert.Equal(t, now.Add(300*time.Second).Minute(), req.deadline.Minute())

	assert.Equal(t, 2, wb.requestCount)
	assert.Equal(t, uint64(2), wb.summaryRecorder.reqNum)
}

func TestWorkerBroker_waitRecoverNewConnReady(t *testing.T) {
	lock := new(sync.RWMutex)
	wb := &workerBroker{recoverBlock: sync.NewCond(lock)}
	now := time.Now()
	wb.recoverWaitNewConnCh = make(chan struct{})
	wb.waitRecoverNewConnReady()
	subTime := time.Now().Sub(now)
	assert.True(t, subTime >= waitNewConnReadyTimeout)
}
