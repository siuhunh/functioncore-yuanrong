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
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	. "github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"gotest.tools/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/podevent"
	"functioncore/pkg/common/snerror"
	commonTypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/common/uuid"
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
		resp, err := busObj.ReportErrMsgToBus(ctx, "test", "test", "INFO", properties)
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
		result := busObj.client.request(context.Background(), data, false, false, 1)
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
		result = busObj.client.request(context.Background(), data, false, false, 1)
		Equal(t, uint32(0), result.ErrorCode)
		report, ok = mockReportCache.Load("")
		False(t, ok)

		busObj.client.isConnect = false
		result = busObj.client.request(context.Background(), data, true, false, 1)
		Equal(t, uint32(proxyerrors.IsUpgradingCode), result.ErrorCode)

		result = busObj.client.request(context.Background(), data, false, true, 1)
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
				func(_ *TCPBus, ctx context.Context) (types.Result, error) {
					return types.Result{}, nil
				}),
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err = busObj.WaitRequestsFinished(true, nil)
		if err != nil {
			t.Error("test WaitRequestsFinished error")
		}
	}
	{
		count := 0
		countEnd := 30
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc((*TCPBus).ReportWorkerExitRequest,
				func(_ *TCPBus, ctx context.Context) (types.Result, error) {
					count++
					if count > countEnd {
						return types.Result{
							ErrorCode: proxyerrors.ClientExitErrCode,
							ErrorMsg:  proxyerrors.ClientExitErr}, errors.New("error")
					}
					return types.Result{
						ErrorCode: proxyerrors.WaitDirectRequestCode,
						ErrorMsg:  proxyerrors.WaitDirectRequestMsg}, errors.New("error")
				}),
			gomonkey.ApplyFunc(time.NewTicker, func() *time.Ticker {
				c := make(chan time.Time)
				go func() {
					for i := 0; i <= countEnd; i++ {
						c <- time.Time{}
					}
				}()
				return &time.Ticker{
					C: c,
				}
			}),
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err = busObj.WaitRequestsFinished(true, nil)
		if count < countEnd {
			t.Error("test WaitRequestsFinished ticker error")
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
		for idx := range patches {
			patches[idx].Reset()
		}
	}
	// bus exit and reject request
	{
		busObj.exit = 1
		w := func(result types.Result) {
			if result.ErrorCode != proxyerrors.WorkerExitErrCode {
				t.Errorf("test handler request when exit failed, code: %d", result.ErrorCode)
			}
		}
		busObj.HandleRequest(nil, "127.0.0.1", w)
	}
	// handle report error message
	{
		busObj.exit = 1
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
			gomonkey.ApplyFunc(podevent.ReportPodEventToRedis, func(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
				podErrInfo podevent.EventInfo) {
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
	// handlePosixCallRequest
	{
		busObj.exit = 1
		w := func(result types.Result) {
			if result.ErrorCode != proxyerrors.WorkerExitErrCode {
				t.Errorf("test handler request when exit failed, code: %d", result.ErrorCode)
			}
		}
		msg := &pb.PosixCallRequest{
			TraceID: "test",
		}
		data, err := proto.Marshal(&pb.Message{
			Type:             pb.Message_POSIX_CALL_REQUEST,
			PosixCallRequest: msg,
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(podevent.ReportPodEventToRedis, func(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
				podErrInfo podevent.EventInfo) {
				return
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		busObj.asyncCallback = func(ctx context.Context, dst *common.FunctionProperty, req *commonTypes.TCPRequest) {
		}
		busObj.HandleRequest(data, "127.0.0.1", w)
	}
	// graceful exit worker can handle spec-instance invoke
	{
		busObj.exit = 1
		w := func(result types.Result) {
			if result.ErrorCode == proxyerrors.WorkerExitErrCode {
				t.Errorf("spec instance invoke should be handle when worker is exiting")
			}
		}
		msg := &pb.PosixCallRequest{
			TraceID:              "test",
			SpecInvokeInstanceID: "instance",
		}
		data, err := proto.Marshal(&pb.Message{
			Type:             pb.Message_POSIX_CALL_REQUEST,
			PosixCallRequest: msg,
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(podevent.ReportPodEventToRedis, func(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
				podErrInfo podevent.EventInfo) {
				return
			}),
			gomonkey.ApplyPrivateMethod(busObj, "handleRequest", func(_ *TCPBus, _ *pb.Message, _ func(types.Result)) {}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		busObj.asyncCallback = func(ctx context.Context, dst *common.FunctionProperty, req *commonTypes.TCPRequest) {
		}
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

func TestSetResultRetry9(t *testing.T) {
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
	patch2 := gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
		return nil, errors.New("mock proto marshal error")
	})
	defer patch2.Reset()

	err := tcpBus.SetResult(resp)
	assert.Error(t, err, "mock proto marshal error")
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
		exit                 uint32
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
							isStateful bool, concurrency uint32) types.Result {
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
							isStateful bool, concurrency uint32) types.Result {
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
			if _, err := t.ReportWorkerExitRequest(tt.args.ctx); (err != nil) != tt.wantErr {
				t1.Errorf("ReportWorkerExitRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestTCPBus_handleWorkerExitRequest(t1 *testing.T) {
	type fields struct {
		client               *Connection
		protoCachePool       *sync.Pool
		collector            *calls.CallCollector
		Property             common.FunctionProperty
		callback             common.Call
		asyncCallback        common.AsyncCall
		cancel               common.Cancel
		recover              common.Recover
		exit                 uint32
		reportRuntimeMetrics common.ReportRuntimeMetrics
		storeResourceUsage   common.StoreResourceUsage
		singleMode           bool
	}
	type args struct {
		ctx context.Context
	}
	tests := struct {
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantResult  types.Result
	}{
		fields: fields{
			client: &Connection{},
		},
		args: args{},
		patchesFunc: func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			return patches
		},
	}

	t1.Run("test is reset", func(t1 *testing.T) {
		tests.wantResult = types.Result{ErrorCode: proxyerrors.WaitDirectRequestCode, ErrorMsg: proxyerrors.WaitDirectRequestMsg}
		t := &TCPBus{
			client:               tests.fields.client,
			protoCachePool:       tests.fields.protoCachePool,
			collector:            &calls.CallCollector{},
			Property:             tests.fields.Property,
			callback:             tests.fields.callback,
			asyncCallback:        tests.fields.asyncCallback,
			cancel:               tests.fields.cancel,
			recover:              tests.fields.recover,
			exit:                 tests.fields.exit,
			reportRuntimeMetrics: tests.fields.reportRuntimeMetrics,
			storeResourceUsage:   tests.fields.storeResourceUsage,
			singleMode:           tests.fields.singleMode,
		}
		t.collector.SetResetFlag(true)
		patches := tests.patchesFunc()
		defer patches.ResetAll()
		if result := t.handleWorkerExitRequest(); result.ErrorCode != tests.wantResult.ErrorCode || result.ErrorMsg != tests.wantResult.ErrorMsg {
			t1.Errorf("handleWorkerExitRequest() error = %v, wantErr %v", result.ErrorMsg, tests.wantResult.ErrorMsg)
		}
	})

	t1.Run("test DirectRequestCount not zero", func(t1 *testing.T) {
		tests.wantResult = types.Result{ErrorCode: proxyerrors.WaitDirectRequestCode, ErrorMsg: proxyerrors.WaitDirectRequestMsg}
		t := &TCPBus{
			client:               tests.fields.client,
			protoCachePool:       tests.fields.protoCachePool,
			collector:            &calls.CallCollector{},
			Property:             tests.fields.Property,
			callback:             tests.fields.callback,
			asyncCallback:        tests.fields.asyncCallback,
			cancel:               tests.fields.cancel,
			recover:              tests.fields.recover,
			exit:                 tests.fields.exit,
			reportRuntimeMetrics: tests.fields.reportRuntimeMetrics,
			storeResourceUsage:   tests.fields.storeResourceUsage,
			singleMode:           tests.fields.singleMode,
		}
		t.collector.AddDirectRequestCount(1)
		patches := tests.patchesFunc()
		defer patches.ResetAll()
		if result := t.handleWorkerExitRequest(); result.ErrorCode != tests.wantResult.ErrorCode || result.ErrorMsg != tests.wantResult.ErrorMsg {
			t1.Errorf("handleWorkerExitRequest() error = %v, wantErr %v", result.ErrorMsg, tests.wantResult.ErrorMsg)
		}
	})

	t1.Run("test handleWorkerExitRequest success", func(t1 *testing.T) {
		tests.wantResult = types.Result{}
		t := &TCPBus{
			client:               &Connection{pending: &sync.Map{}},
			protoCachePool:       tests.fields.protoCachePool,
			collector:            &calls.CallCollector{},
			Property:             tests.fields.Property,
			callback:             tests.fields.callback,
			asyncCallback:        tests.fields.asyncCallback,
			cancel:               tests.fields.cancel,
			recover:              tests.fields.recover,
			exit:                 tests.fields.exit,
			reportRuntimeMetrics: tests.fields.reportRuntimeMetrics,
			storeResourceUsage:   tests.fields.storeResourceUsage,
			singleMode:           tests.fields.singleMode,
		}
		t.client.pending.Store(1, "")
		patches := tests.patchesFunc()
		defer patches.ResetAll()
		if result := t.handleWorkerExitRequest(); result.ErrorCode != tests.wantResult.ErrorCode || result.ErrorMsg != tests.wantResult.ErrorMsg {
			t1.Errorf("handleWorkerExitRequest() error = %v, wantErr %v", result.ErrorMsg, tests.wantResult.ErrorMsg)
		}
	})
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
			isSendingRemoteProxy bool, isStateful bool, concurrency uint32) types.Result {
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
	rsp, err := tcpBus.Init(context.Background(), 1)
	assert.NilError(t, err)
	assert.Equal(t, uint32(0), rsp.ErrorCode)
	requestCtrl = "returnError"
	rsp, err = tcpBus.Init(context.Background(), 1)
	assert.NilError(t, err)
	assert.Equal(t, uint32(proxyerrors.InternalErrCode), rsp.ErrorCode)
	requestCtrl = ""
	unmarshalCtrl = "returnError"
	rsp, err = tcpBus.Init(context.Background(), 1)
	assert.Equal(t, true, err != nil)
	assert.Equal(t, true, rsp == nil)
}

func TestTCPBus_Init2(t *testing.T) {
	Convey("TestTCPBus_Init", t, func() {
		Convey("failed to Marshal", func() {
			tcpBus := TCPBus{
				Property: common.FunctionProperty{},
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return nil, errors.New("test")
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			_, err := tcpBus.Init(context.Background(), 1)
			assert.Error(t, err, "init error: test")
		})
		Convey("InitResponse is nil", func() {
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
					msg.InitResponse = nil
					if unmarshalCtrl == "returnError" {
						return errors.New("unmarshal error")
					}
					return nil
				}),
				gomonkey.ApplyFunc((*Connection).request, func(_ *Connection, ctx context.Context, payload []byte,
					isSendingRemoteProxy bool, isStateful bool, concurrency uint32) types.Result {
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
			_, err := tcpBus.Init(context.Background(), 1)
			assert.Error(t, err, "bus init response is nil")
		})
	})
}

func TestTCPBus_Pause(t *testing.T) {
	Convey("TestTCPBus_Init", t, func() {
		Convey("failed to Marshal", func() {
			tcpBus := TCPBus{
				Property: common.FunctionProperty{},
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return nil, errors.New("test")
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			_, err := tcpBus.Pause(context.Background())
			assert.Error(t, err, "test")
		})
		Convey("failed to request", func() {
			tcpBus := TCPBus{
				Property: common.FunctionProperty{},
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc((*Connection).request, func(_ *Connection, ctx context.Context, payload []byte,
					isSendingRemoteProxy bool, isStateful bool, concurrency uint32) types.Result {
					return types.Result{
						ErrorCode: proxyerrors.InternalErrCode,
						ErrorMsg:  "internal error",
					}
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			_, err := tcpBus.Pause(context.Background())
			assert.Error(t, err, "internal error")
		})
		Convey("failed to Unmarshal", func() {
			tcpBus := TCPBus{
				Property: common.FunctionProperty{},
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return nil, nil
				}),
				gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
					return errors.New("unmarshal error")
				}),
				gomonkey.ApplyFunc((*Connection).request, func(_ *Connection, ctx context.Context, payload []byte,
					isSendingRemoteProxy bool, isStateful bool, concurrency uint32) types.Result {
					return types.Result{}
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			_, err := tcpBus.Pause(context.Background())
			assert.Error(t, err, "unmarshal error")
		})
	})
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

func TestReportBridgeFuncMetrics(t *testing.T) {
	Convey("Test ReportBridgeFuncMetrics proto.Marshal failed", t, func() {
		tcpBus := &TCPBus{client: &Connection{}}
		count := 0
		err := tcpBus.ReportBridgeFuncMetrics(context.Background(), &common.BridgeFuncMetrics{
			AverageTimeCost: float64(1) / float64(count),
		})
		So(strings.Contains(err.Error(), "failed to report bridgeFunc metrics usage,errCode"), ShouldBeTrue)
	})
	Convey("Test ReportBridgeFuncMetrics ok", t, func() {
		client := &Connection{}
		tcpBus := &TCPBus{client: client}
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(client), "request", func(c *Connection,
				ctx context.Context, payload []byte, isSendingRemoteProxy bool, isStateful bool, concurrency uint32) types.Result {
				return types.Result{}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := tcpBus.ReportBridgeFuncMetrics(context.Background(), &common.BridgeFuncMetrics{
			AverageTimeCost: float64(1) / float64(count),
		})
		So(err, ShouldBeNil)
	})
}

func TestHandleBridgeFuncMetricsRequest(t *testing.T) {
	Convey("TestHandleBridgeFuncMetricsRequest ok", t, func() {
		tcpBus := &TCPBus{
			reportBridgeFuncMetrics: func(metrics common.BridgeFuncMetrics) {},
			client:                  &Connection{}}
		res := tcpBus.handleBridgeFuncMetricsRequest(&pb.Message{
			ReportBridgeFuncMetricsMessage: &pb.Message_ReportBridgeFuncMetricsMessage{},
		})
		So(res.ErrorCode, ShouldEqual, 0)
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
			_, err := t.ReportErrMsgToBus(context.TODO(), "test", "test", "INFO", properties)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestTCPBus_handleCallRequest(t *testing.T) {
	Convey("TestTCPBus_handleCallRequest", t, func() {
		count := 0
		collector := calls.InitCollector(100)
		bus := &TCPBus{
			collector: collector,
			callback: func(ctx context.Context, dst *common.FunctionProperty, req *commonTypes.TCPRequest) error {
				collector.AddResponse(&commonTypes.TCPResponse{
					RequestID: 1,
					RawData:   []byte("hello world"),
				})
				count++
				return nil
			},
			protoCachePool: &sync.Pool{
				New: func() interface{} { return &cache{} },
			},
		}
		w := func(result types.Result) {
			So(strings.Contains(string(result.RspData), "hello world"), ShouldBeTrue)
		}
		bus.handleCallRequest(&pb.Message{
			CallMessage: &pb.Message_CallMessage{
				TraceID: "traceID",
				Dst:     &pb.FunctionProperty{},
				Req:     &pb.CallRequest{},
			},
		}, w)
		So(count == 1, ShouldBeTrue)
	})
}

func TestTCPBus_handleCallResult(t *testing.T) {
	Convey("TestTCPBus_handleCallResult", t, func() {
		Convey("case1: pb.Message_CALL_RESPONSE CallResponse != nil ", func() {
			t := &TCPBus{}
			callMsg := &pb.CallResponse{
				Logs:    "log",
				Summary: "summary",
			}

			msg := &pb.Message{
				Type:         pb.Message_CALL_RESPONSE,
				CallResponse: callMsg,
			}
			_, err := t.handleCallResult(msg, "test", 100)
			So(err, ShouldEqual, nil)
		})

		Convey("case2: pb.Message_CALL_RESPONSE CallResponse == nil ", func() {
			t := &TCPBus{}
			msg := &pb.Message{
				Type:         pb.Message_CALL_RESPONSE,
				CallResponse: nil,
			}
			_, err := t.handleCallResult(msg, "test", 100)
			So(err, ShouldNotEqual, nil)
		})

		Convey("case3: pb.Message_POSIX_CALL_RESPONSE PosixCallResult != nil ", func() {
			t := &TCPBus{}
			callResult := &pb.PosixCallResult{
				Logs:    "log",
				Summary: "summary",
			}

			msg := &pb.Message{
				Type:            pb.Message_POSIX_CALL_RESPONSE,
				PosixCallResult: callResult,
			}
			_, err := t.handleCallResult(msg, "test", 100)
			So(err, ShouldEqual, nil)
		})

		Convey("case4: pb.Message_POSIX_CALL_RESPONSE PosixCallResult == nil ", func() {
			t := &TCPBus{}
			msg := &pb.Message{
				Type:            pb.Message_POSIX_CALL_RESPONSE,
				PosixCallResult: nil,
			}
			_, err := t.handleCallResult(msg, "test", 100)
			So(err, ShouldNotEqual, nil)
		})
	})
}
