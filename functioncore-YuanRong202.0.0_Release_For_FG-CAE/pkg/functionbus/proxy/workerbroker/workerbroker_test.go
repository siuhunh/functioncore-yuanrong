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

	"functioncore/pkg/common/connection"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
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
		gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}), "GetFunctionMetadata", func(r *servicediscovery.Registry, key string) (*servicediscovery.FunctionMetaInfo, error) {
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
		name                  string
		funcKeyWithRes        string
		expected              time.Duration
		expectedDirectTimeout time.Duration
	}{
		{
			name:                  "func not exits",
			funcKeyWithRes:        "12345678901234561234567890123456/0@default@notexist/latest/300-128",
			expected:              time.Duration(defaultRequestTimeout+commonDelay) * time.Second,
			expectedDirectTimeout: 900 * time.Second,
		},
		{
			name:                  "normal func",
			funcKeyWithRes:        "12345678901234561234567890123456/0@default@hello/latest/300-128",
			expected:              time.Duration(3+commonDelay) * time.Second,
			expectedDirectTimeout: 3 * time.Second,
		},
		{
			name:                  "custom image func",
			funcKeyWithRes:        "12345678901234561234567890123456/0@default@image/latest/300-128",
			expected:              time.Duration(10+commonDelay) * time.Second,
			expectedDirectTimeout: 10 * time.Second,
		},
		{
			name:                  "init func",
			funcKeyWithRes:        "12345678901234561234567890123456/0@default@init/latest/300-128",
			expected:              time.Duration(13+commonDelay) * time.Second,
			expectedDirectTimeout: 10 * time.Second,
		},
		{
			name:                  "long run",
			funcKeyWithRes:        "12345678901234561234567890123456/0@default@longrun/latest/300-128",
			expected:              time.Duration(900+commonDelay) * time.Second,
			expectedDirectTimeout: 900 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, GetRequestTimeout(tt.funcKeyWithRes), tt.name)
			assert.Equal(t, tt.expectedDirectTimeout, GetDirectTimeout(tt.funcKeyWithRes), tt.name)
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
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestEnd, func(_ *workerevaluator.WorkerEvaluator, _ string, _ time.Duration, _ bool) {
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
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).RecordRequestEnd, func(_ *workerevaluator.WorkerEvaluator, _ string, _ time.Duration, _ bool) {
			return
		}),
		gomonkey.ApplyFunc((*workerevaluator.WorkerEvaluator).DeleteWorkerMetrics, func(_ *workerevaluator.WorkerEvaluator, _ string, _ string) {
			return
		}),
		gomonkey.ApplyFunc((*workerBroker).blockAndRecoverRuntime, func(_ *workerBroker) {
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
	wb.hasInitializer, wb.isInitialized, wb.isMinInstance = true, false, true
	wb.initErrResponse = &types.TCPResponse{ErrorCode: proxyerrors.InternalErrCode, ErrorMessage: "internal error"}
	wb.doDirectRequest(request, tcpBus)
	rsp := <-request.Response
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	wb.hasInitializer, wb.isInitialized, wb.isMinInstance = true, false, false
	wb.doDirectRequest(request, tcpBus)
	rsp = <-request.Response
	assert.Equal(t, uint32(proxyerrors.StateOKCode), rsp.ErrorCode)
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
	}
	rsp := wb.doInitialize("")
	assert.Equal(t, proxyerrors.InternalErrCode, int(rsp.ErrorCode))
	wb.runtimeConn.Store(&tcpbus.TCPBus{})
	wb.isInitialized = true
	rsp = wb.doInitialize("")
	assert.Equal(t, 0, initCount)
	assert.Equal(t, proxyerrors.StateOKCode, int(rsp.ErrorCode))
	wb.isInitialized = false
	initCtrl = "returnError"
	rsp = wb.doInitialize("")
	assert.Equal(t, 1, initCount)
	assert.Equal(t, proxyerrors.InternalErrCode, int(rsp.ErrorCode))
	wb.initErrResponse = nil
	initCtrl = "returnClientExitError"
	rsp = wb.doInitialize("")
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 2, initCount)
	assert.Equal(t, 1, recoverCount)
	assert.Equal(t, proxyerrors.ClientExitErrCode, int(rsp.ErrorCode))
	assert.Equal(t, proxyerrors.ClientExitErrCode, int(wb.initErrResponse.ErrorCode))
	wb.initErrResponse = nil
	initCtrl = "returnTimeoutError"
	rsp = wb.doInitialize("")
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 3, initCount)
	assert.Equal(t, 1, checkCount)
	assert.Equal(t, snerror.RuntimeInitTimeoutCode, int(rsp.ErrorCode))
	assert.Equal(t, snerror.RuntimeInitTimeoutCode, int(wb.initErrResponse.ErrorCode))
	wb.initErrResponse = nil
	initCtrl = ""
	rsp = wb.doInitialize("")
	assert.Equal(t, 4, initCount)
	assert.Equal(t, proxyerrors.StateOKCode, int(rsp.ErrorCode))
}

func Test_workerBroker_blockAndRecoverRuntime(t *testing.T) {
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
	}
	wb.blockAndRecoverRuntime()
	assert.Equal(t, false, wb.blockForRecover)
}
