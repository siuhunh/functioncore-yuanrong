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

package tcpbus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/google/uuid"
	. "github.com/smartystreets/goconvey/convey"
	. "github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/snerror"
	commonTypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/functionbus/bus/calls"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
	mockutils "functioncore/test/utils"
)

func TestNewTCPBus(t *testing.T) {
	Convey("TestNewTCPBus", t, func() {
		Convey("bus opt is nil", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = nil
			tcpBus, err := NewTCPBus(busOption)
			So(tcpBus, ShouldBeNil)
			So(err, ShouldBeError)
		})
		Convey("assert type failed", func() {
			busOption := common.BusOption{}
			type Something struct {
				// if using existed client, set client
				Client *Connection
				// using existed client or create client
				CreateClient bool
				// if creating client, set serverAddr
				ServerAddr string
				// true: use socket, false: use channel.
				Remote bool
				// true: single client, false: multi clients
				SingleMode bool
			}
			busOption.NewBusOpt = &Something{
				CreateClient: false,
			}
			tcpBus, err := NewTCPBus(busOption)
			So(tcpBus, ShouldBeNil)
			So(err, ShouldBeError)
		})
		Convey("not create client", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &Option{
				CreateClient: false,
			}
			tcpBus, err := NewTCPBus(busOption)
			So(tcpBus, ShouldNotBeNil)
			So(err, ShouldBeNil)
		})
		Convey("bus is single mode and bus's client is not nil", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &Option{
				CreateClient: true,
				SingleMode:   true,
				ServerAddr:   ":8080",
			}
			patch := gomonkey.ApplyFunc(BuildClientConnection, func(_ ClientOption) (*Connection, error) {
				return &Connection{}, nil
			})
			defer func() {
				patch.Reset()
			}()
			var tcpBus1, tcpBus2 *TCPBus
			var err1, err2 error
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				tcpBus1, err1 = NewTCPBus(busOption)
				wg.Done()
			}()
			go func() {
				tcpBus2, err2 = NewTCPBus(busOption)
				wg.Done()
			}()
			wg.Wait()
			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(tcpBus1, ShouldNotBeNil)
			So(tcpBus2, ShouldNotBeNil)
			So(tcpBus1.collector, ShouldEqual, tcpBus2.collector)
			So(tcpBus1.client, ShouldNotBeNil)
			So(tcpBus2.client, ShouldNotBeNil)
		})
	})
}

func TestTCPBusFunction(t *testing.T) {
	createBusServer()
	busObj, err := createBusClient()
	if err != nil {
		t.Errorf("failed to create bus client %s", err)
	}
	{
		if err = busObj.RegisterBus(common.FunctionProperty{}); err != nil {
			t.Errorf("failed to register bus: %s", err.Error())
		}
	}
	// function call with no error
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		resp, err := busObj.Call(ctx, &common.FunctionProperty{}, &commonTypes.TCPRequest{}, false)
		if err != nil {
			t.Errorf("failed to call request: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get call response, code: %d", resp.ErrorCode)
		}
	}
	// function cancel with no error
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		resp, err := busObj.Cancel(ctx, &common.CancelProperty{}, &commonTypes.TCPRequest{})
		if err != nil {
			t.Errorf("failed to cancel request: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get cancel response, code: %d", resp.ErrorCode)
		}
	}
	// function recover with no error
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		resp, err := busObj.Recover(ctx, &commonTypes.TCPRequest{})
		if err != nil {
			t.Errorf("failed to recover request: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get recover response, code: %d", resp.ErrorCode)
		}
	}
	// function report runtime metrics with no error
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		resp, err := busObj.ReportRuntimeMetrics(ctx, &common.RuntimeMetricsProperty{})
		if err != nil {
			t.Errorf("failed to ReportRuntimeMetrics: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get ReportRuntimeMetrics response, code: %d", resp.ErrorCode)
		}
	}
	// function report error message with no error
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		properties := map[string]string{"test": "test"}
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		resp, err := busObj.ReportErrMsgToBus(ctx, "test", "test", properties)
		if err != nil {
			t.Errorf("failed to ReportRuntimeMetrics: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get ReportRuntimeMetrics response, code: %d", resp.ErrorCode)
		}
	}
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		resp, err := busObj.Pause(context.Background())
		if err != nil {
			t.Errorf("failed to pause: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get pause response, code: %d", resp.ErrorCode)
		}
	}
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		resp, err := busObj.RecoverRuntime(context.Background(), false)
		if err != nil {
			t.Errorf("failed to recover runtime: %s", err.Error())
		}
		if resp.ErrorCode != 0 {
			t.Errorf("failed to get recover runtime response, code: %d", resp.ErrorCode)
		}
	}
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		data, err := proto.Marshal(&pb.Message{Type: pb.Message_WORKER_REPORT, WorkerReport: &pb.Message_WorkerReport{
			TraceID:    "test_report_traceID",
			RuntimeLog: "test some runtime log",
		}})
		Nil(t, err)
		result := busObj.client.request(context.Background(), data, false, false)
		Equal(t, uint32(0), result.ErrorCode)
		report, ok := mockReportCache.Load("test_report_traceID")
		True(t, ok)
		select {
		case r := <-report.(common.WorkerReportNotify).Ch:
			Equal(t, "test some runtime log", r.Logs)
		default:
			Fail(t, "not receive worker report")
		}

		data, err = proto.Marshal(&pb.Message{Type: pb.Message_WORKER_REPORT, WorkerReport: &pb.Message_WorkerReport{
			TraceID:    "",
			RuntimeLog: "log with empty traceID",
		}})
		Nil(t, err)
		result = busObj.client.request(context.Background(), data, false, false)
		Equal(t, uint32(0), result.ErrorCode)
		report, ok = mockReportCache.Load("")
		False(t, ok)

		busObj.client.isConnect = false
		result = busObj.client.request(context.Background(), data, true, false)
		Equal(t, uint32(proxyerrors.IsUpgradingCode), result.ErrorCode)

		result = busObj.client.request(context.Background(), data, false, true)
		Equal(t, uint32(snerror.StatefulFunctionReloading), result.ErrorCode)
		busObj.client.isConnect = true
	}
	{
		result := busObj.CheckHealth()
		if !result {
			t.Error("test check health error")
		}
	}
	// test heartbeat
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		conn, err := busObj.client.netPool.Get()
		if err != nil {
			t.Errorf("failed to get connection from pool: %s", err.Error())
		}
		if err = sendHeartbeat(conn); err != nil {
			t.Errorf("failed to send heart beat: %s", err.Error())
		}
	}
	// test check auth
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt,
				func(_ string) ([]byte, error) {
					return []byte("secret"), nil
				}),
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		result := busObj.CheckAuth("", "")
		if !result {
			t.Errorf("test check auth success error")
		}

		patches = [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.Decrypt,
				func(_ string) ([]byte, error) {
					return nil, fmt.Errorf("error")
				}),
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		result = busObj.CheckAuth("", "")
		if result {
			t.Errorf("test check auth failed error")
		}
	}
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc((*TCPBus).ReportWorkerExitRequest,
				func(_ *TCPBus, ctx context.Context) error {
					return nil
				}),
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err = busObj.WaitRequestsFinished(nil)
		if err != nil {
			t.Error("test WaitRequestsFinished error")
		}
	}
	busObj.Stop()
}

func TestTCPBusFunctionException(t *testing.T) {
	busObj := &TCPBus{}
	// function with proto marshal error
	{
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
				return nil, fmt.Errorf("error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		_, err := busObj.ReportRuntimeMetrics(ctx, &common.RuntimeMetricsProperty{})
		if err == nil {
			t.Error("failed to test ReportRuntimeMetrics proto marshal error")
		}

		_, err = busObj.Recover(ctx, &commonTypes.TCPRequest{})
		if err == nil {
			t.Error("failed to test Recover proto marshal error")
		}

		_, err = busObj.Call(ctx, &common.FunctionProperty{}, &commonTypes.TCPRequest{}, false)
		if err == nil {
			t.Error("failed to test Call proto marshal error")
		}

		_, err = busObj.Cancel(ctx, &common.CancelProperty{}, &commonTypes.TCPRequest{})
		if err == nil {
			t.Error("failed to test Cancel proto marshal error")
		}
	}
	// bus exit and reject request
	{
		busObj.exit = true
		w := func(result types.Result) {
			if result.ErrorCode != proxyerrors.WorkerExitErrCode {
				t.Errorf("test handler request when exit failed, code: %d", result.ErrorCode)
			}
		}
		busObj.HandleRequest(nil, "127.0.0.1", w)
	}
	// handle report error message
	{
		busObj.exit = true
		w := func(result types.Result) {
			if result.ErrorCode != proxyerrors.WorkerExitErrCode {
				t.Errorf("test handler request when exit failed, code: %d", result.ErrorCode)
			}
		}
		msg := &pb.Message_ReportErrorMessage{
			PodName:            "test",
			FuncUrnWithVersion: "test",
			ErrorMessage:       "test",
		}
		data, err := proto.Marshal(&pb.Message{
			Type:               pb.Message_REPORT_ERROR_MESSAGE_REQUEST,
			ReportErrorMessage: msg,
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(poderrormessage.ReportPodErrMsgToRedis, func(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
				podErrInfo poderrormessage.PodErrorInfo) {
				return
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		busObj.HandleRequest(data, "127.0.0.1", w)
	}
}

func TestSetResultRetry8(t *testing.T) {
	connPool, _ := pool.NewConnPool(pool.Options{
		ConnHandler: func(conn net.Conn, opt pool.Options) {
			return
		},
	})
	tcpBus := &TCPBus{
		collector: calls.InitCollectorOnce(common.DefaultConcurrency),
		client:    &Connection{netPool: connPool},
	}
	resp := &commonTypes.TCPResponse{}
	patch1 := gomonkey.ApplyFunc((*tcpBus.collector).AddResponse, func(*commonTypes.TCPResponse) bool {
		return false
	})
	defer patch1.Reset()
	patch2 := gomonkey.ApplyFunc((*tcpBus.client).send, func(*protocol.Message) error {
		return nil
	})
	patch2.ApplyFunc(time.Sleep, func(_ time.Duration) {})
	defer patch2.Reset()

	err := tcpBus.SetResult(resp)
	assert.Equal(t, err, pool.ErrConnectionPoolEmpty)
}

func TestSetWorkerID(t *testing.T) {
	connPool, _ := pool.NewConnPool(pool.Options{
		ConnHandler: func(conn net.Conn, opt pool.Options) {
			return
		},
	})
	tcpBus := &TCPBus{
		collector: calls.InitCollectorOnce(common.DefaultConcurrency),
		client:    &Connection{netPool: connPool},
	}
	tcpBus.SetWorkerID("test")
	assert.Equal(t, "test", tcpBus.client.workerID)
}

func TestCheckWorkerID(t *testing.T) {
	Convey("tcpbus nil", t, func() {
		connPool, _ := pool.NewConnPool(pool.Options{
			ConnHandler: func(conn net.Conn, opt pool.Options) {
				return
			},
		})
		tcpBus := &TCPBus{
			collector: calls.InitCollectorOnce(common.DefaultConcurrency),
			client:    &Connection{netPool: connPool},
		}
		result := tcpBus.CheckWorkerID([]byte("test"))
		assert.Equal(t, true, result)
	})

	Convey("client is nil", t, func() {
		_, _ = pool.NewConnPool(pool.Options{
			ConnHandler: func(conn net.Conn, opt pool.Options) {
				return
			},
		})
		tcpBus := &TCPBus{
			collector: calls.InitCollectorOnce(common.DefaultConcurrency),
			client:    nil,
		}
		result := tcpBus.CheckWorkerID([]byte("test"))
		assert.Equal(t, true, result)
	})
}

func TestTCPBus_ReportResourceUsage(t1 *testing.T) {
	t1.Run("test send error", func(t1 *testing.T) {
		t := &TCPBus{
			client: &Connection{isConnect: false},
		}
		if err := t.ReportResourceUsage(context.Background(), &common.ResourceUsage{}); err == nil {
			t1.Errorf("ReportResourceUsage() error = %v, wantErr %v", err, true)
		}
	})
}

func TestTCPBus_ReportCaeMetrics(t1 *testing.T) {
	t1.Run("test send error", func(t1 *testing.T) {
		t := &TCPBus{
			client: &Connection{isConnect: false},
		}
		if err := t.ReportCaeMetrics(context.Background(), &common.CaeMetrics{}); err == nil {
			t1.Errorf("ReportResourceUsage() error = %v, wantErr %v", err, true)
		}
	})
}

func TestTCPBus_ReportWorkerExitRequest(t1 *testing.T) {
	type fields struct {
		client               *Connection
		protoCachePool       *sync.Pool
		collector            *calls.CallCollector
		Property             common.FunctionProperty
		callback             common.Call
		asyncCallback        common.AsyncCall
		cancel               common.Cancel
		recover              common.Recover
		exit                 bool
		reportRuntimeMetrics common.ReportRuntimeMetrics
		storeResourceUsage   common.StoreResourceUsage
		singleMode           bool
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			fields: fields{
				client: &Connection{},
			},
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*Connection).request,
						func(_ *Connection, ctx context.Context, payload []byte, isSendingRemoteProxy bool,
							isStateful bool) types.Result {
							return types.Result{}
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test proto marshal error",
			fields: fields{
				client: &Connection{},
			},
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
						return nil, errors.New("mock proto marshal error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test connection is nil",
			fields: fields{
				client: nil,
			},
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		},
		{
			name: "test request error",
			fields: fields{
				client: &Connection{},
			},
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*Connection).request,
						func(_ *Connection, ctx context.Context, payload []byte, isSendingRemoteProxy bool,
							isStateful bool) types.Result {
							return types.Result{ErrorCode: 1, ErrorMsg: "mock request error"}
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &TCPBus{
				client:               tt.fields.client,
				protoCachePool:       tt.fields.protoCachePool,
				collector:            tt.fields.collector,
				Property:             tt.fields.Property,
				callback:             tt.fields.callback,
				asyncCallback:        tt.fields.asyncCallback,
				cancel:               tt.fields.cancel,
				recover:              tt.fields.recover,
				exit:                 tt.fields.exit,
				reportRuntimeMetrics: tt.fields.reportRuntimeMetrics,
				storeResourceUsage:   tt.fields.storeResourceUsage,
				singleMode:           tt.fields.singleMode,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			if err := t.ReportWorkerExitRequest(tt.args.ctx); (err != nil) != tt.wantErr {
				t1.Errorf("ReportWorkerExitRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTCPBus_Init(t *testing.T) {
	tcpBus := TCPBus{
		Property: common.FunctionProperty{},
	}
	requestCtrl := ""
	unmarshalCtrl := ""
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
			return nil, nil
		}),
		gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
			msg, _ := m.(*pb.Message)
			msg.InitResponse = &pb.Message_InitResponse{
				ErrorCode:    0,
				ErrorMessage: "",
			}
			if unmarshalCtrl == "returnError" {
				return errors.New("unmarshal error")
			}
			return nil
		}),
		gomonkey.ApplyFunc((*Connection).request, func(_ *Connection, ctx context.Context, payload []byte,
			isSendingRemoteProxy bool, isStateful bool) types.Result {
			if requestCtrl == "returnError" {
				return types.Result{
					ErrorCode: proxyerrors.InternalErrCode,
					ErrorMsg:  "internal error",
				}
			}
			return types.Result{}
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	rsp, err := tcpBus.Init(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, uint32(0), rsp.ErrorCode)
	requestCtrl = "returnError"
	rsp, err = tcpBus.Init(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	requestCtrl = ""
	unmarshalCtrl = "returnError"
	rsp, err = tcpBus.Init(context.Background())
	assert.Equal(t, true, err != nil)
	assert.Equal(t, true, rsp == nil)
}

func TestTCPBus_CheckRuntime(t *testing.T) {
	tcpBus := TCPBus{
		client:   &Connection{},
		Property: common.FunctionProperty{},
	}
	requestCtrl := ""
	unmarshalCtrl := ""
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
			return nil, nil
		}),
		gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
			msg, _ := m.(*pb.Message)
			msg.WorkerReport = &pb.Message_WorkerReport{
				ErrorCode:      0,
				ErrorMessage:   "",
				RuntimeLog:     "some log",
				RuntimeSummary: "some summary",
			}
			if unmarshalCtrl == "returnError" {
				return errors.New("unmarshal error")
			}
			return nil
		}),
		gomonkey.ApplyFunc((*Connection).ctrlRtmRequest, func(_ *Connection, ctx context.Context, payload []byte) types.Result {
			if requestCtrl == "returnError" {
				return types.Result{
					ErrorCode: proxyerrors.InternalErrCode,
					ErrorMsg:  "internal error",
				}
			}
			return types.Result{}
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	rsp, err := tcpBus.CheckRuntime(context.Background(), "", false)
	assert.NilError(t, err)
	assert.Equal(t, uint32(0), rsp.ErrorCode)
	assert.Equal(t, "some log", rsp.Logs)
	assert.Equal(t, "some summary", rsp.Summary)
	requestCtrl = "returnError"
	rsp, err = tcpBus.CheckRuntime(context.Background(), "", false)
	assert.NilError(t, err)
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	requestCtrl = ""
	unmarshalCtrl = "returnError"
	rsp, err = tcpBus.CheckRuntime(context.Background(), "", false)
	assert.Equal(t, true, err != nil)
	assert.Equal(t, true, rsp == nil)
}

func mockCheckRuntime(traceID string, shouldWait bool) ([]byte, error) {
	return []byte{}, nil
}

func mockCheckRuntimeWithError(traceID string, shouldWait bool) ([]byte, error) {
	return []byte{}, errors.New("test")
}

func TestTCPBus_handleCheckRuntime(t *testing.T) {
	Convey("TestTCPBus_handleCheckRuntime", t, func() {
		Convey("failed to checkRuntime", func() {
			t := &TCPBus{}
			msg := &pb.Message{
				CheckRuntime: &pb.Message_CheckRuntime{
					TraceID: "test",
				},
			}
			t.RegisterCheckRuntime(mockCheckRuntimeWithError)
			res := t.handleCheckRuntime(msg)
			So(res.ErrorCode, ShouldNotEqual, 0)
		})
		Convey("succeed to checkRuntime", func() {
			t := &TCPBus{}
			msg := &pb.Message{
				CheckRuntime: &pb.Message_CheckRuntime{
					TraceID: "test",
				},
			}
			t.RegisterCheckRuntime(mockCheckRuntime)
			res := t.handleCheckRuntime(msg)
			So(res.ErrorCode, ShouldEqual, 0)
		})
	})
}

func TestTCPBus_ReportRuntimeInfo(t *testing.T) {
	Convey("TestTCPBus_ReportRuntimeInfo", t, func() {
		Convey("failed to marshal", func() {
			t := &TCPBus{}
			info := &connecttypes.WorkerReportInfo{
				TraceID: "test",
			}
			patch := gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
				return []byte{}, errors.New("test")
			})
			defer func() {
				patch.Reset()
			}()
			_, err := t.ReportRuntimeInfo(context.TODO(), info)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestTCPBus_ReportErrMsgToBus(t *testing.T) {
	Convey("TestTCPBus_ReportErrMsgToBus", t, func() {
		Convey("failed to marshal", func() {
			t := &TCPBus{}
			patch := gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
				return []byte{}, errors.New("test")
			})
			defer func() {
				patch.Reset()
			}()
			properties := map[string]string{"test": "test"}
			_, err := t.ReportErrMsgToBus(context.TODO(), "test", "test", properties)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func Test_handleRequest(t *testing.T) {
	actual := false
	type args struct {
		tcpBus  *TCPBus
		message *pb.Message
		w       func(types.Result)
	}
	tests := []struct {
		name        string
		want        bool
		patchesFunc mockutils.PatchesFunc
		args        args
	}{
		{
			name: "case 1 REPORT_CAE_METRICS_REQUEST",
			args: args{
				tcpBus: &TCPBus{
					storeCaeMetrics: func(metrics common.CaeMetrics) {
						actual = true
					},
				},
				message: &pb.Message{
					Type:                    pb.Message_REPORT_CAE_METRICS_REQUEST,
					ReportCaeMetricsMessage: &pb.Message_ReportCaeMetricsMessage{},
				},
				w: func(result types.Result) {
					if result.ErrorCode == proxyerrors.WorkerExitErrCode {
						t.Errorf("test handler request when exit failed, code: %d", result.ErrorCode)
					}
				},
			},
			want: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			name: "case 2 REPORT_RESOURCE_USAGE_REQUEST",
			args: args{
				tcpBus: &TCPBus{
					storeResourceUsage: func(resourceUsage common.ResourceUsage) {
						actual = true
					},
				},
				message: &pb.Message{
					Type:                       pb.Message_REPORT_RESOURCE_USAGE_REQUEST,
					ReportResourceUsageMessage: &pb.Message_ReportResourceUsageMessage{},
				},
				w: func(result types.Result) {
					if result.ErrorCode == proxyerrors.WorkerExitErrCode {
						t.Errorf("test handler request when exit failed, code: %d", result.ErrorCode)
					}
				},
			},
			want: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
		{
			name: "case 3 REPORT_WORKER_EXIT_REQUEST",
			args: args{
				tcpBus: &TCPBus{
					client: &Connection{
						normalExit: false,
					},
				},
				message: &pb.Message{
					Type: pb.Message_REPORT_WORKER_EXIT_REQUEST,
				},
				w: func(result types.Result) {
					actual = true
				},
			},
			want: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*Connection).clearPendingReq, func(_ *Connection) {}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			tt.args.tcpBus.handleRequest(tt.args.message, tt.args.w)
			assert.Equal(t, tt.want, actual)
			actual = false
			p.ResetAll()
		})
	}
}
