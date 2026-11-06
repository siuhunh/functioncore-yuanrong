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

package invoke

import (
	"context"
	"errors"
	"os/exec"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functiontask/task/adaptor"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
)

func TestServer_CallByBus(t *testing.T) {
	convey.Convey("Test Server CallByBus", t, func() {
		s := &Server{}
		request := &runtime.CallRequest{
			FunctionInfo: utils.FunctionInfo{
				TenantID: "TenantID",
				FuncName: "funcname",
				Version:  "1",
			},
		}
		convey.Convey("test 1", func() {
			response := s.CallByBus(context.Background(), request)
			convey.So(response.Error, convey.ShouldNotBeNil)
		})
		convey.Convey("test 2", func() {
			s.bus = &tcpbus.TCPBus{}
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, cctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			})
			defer patch.Reset()
			response := s.CallByBus(context.Background(), request)
			convey.So(response.Error, convey.ShouldBeNil)
		})
		convey.Convey("Unmarshal failed", func() {
			s.bus = &tcpbus.TCPBus{}
			patch := gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
				return errors.New("BUG: internal error (unknown)")
			})
			defer patch.Reset()
			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "FutureID",
			})
			convey.So(response.Error, convey.ShouldNotBeNil)
		})
		convey.Convey("checkCanceled failed", func() {
			s.bus = &tcpbus.TCPBus{}
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, cctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			})
			defer patch.Reset()
			event, _ := proto.Marshal(&adaptor.CallRequestPb{
				Method:    0,
				Source:    0,
				ReqIDList: nil,
				Data:      nil,
			})
			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "FutureID",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldNotBeNil)
		})
		convey.Convey("bus is nil", func() {
			s.bus = nil
			callCount := 0
			patch := gomonkey.ApplyFunc(status.Get, func() status.Status {
				callCount++
				if callCount == 3 {
					return status.Waiting
				}
				return status.Initializing
			})
			defer patch.Reset()
			response := s.CallByBus(context.Background(), request)
			convey.So(response.Error, convey.ShouldNotBeNil)
		})
	})
}

func TestClient_CheckCanceled(t *testing.T) {
	convey.Convey("Test Client checkCanceled", t, func() {
		s := &Server{}
		s.bus = &tcpbus.TCPBus{}
		patch := gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
			return true, nil
		})
		defer patch.Reset()

		s.checkCanceled("id", &adaptor.CallRequestPb{}, &runtime.CallResponse{})
	})
}

func TestClient_CallByBus2(t *testing.T) {
	convey.Convey("Test Client CallByBus", t, func() {
		s := &Server{}
		s.bus = &tcpbus.TCPBus{}
		event, _ := proto.Marshal(&adaptor.CallRequestPb{
			Method:    0,
			Source:    0,
			ReqIDList: nil,
			Data:      nil,
		})
		convey.Convey("test3", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
				return false, nil
			}).ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			}).Reset()

			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "1",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldBeNil)
		})

		convey.Convey("test4", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
				return true, nil
			}).ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, errors.New("BUG: internal error (unknown)")
			}).Reset()

			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "1",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldNotBeNil)
		})

		convey.Convey("test5", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
				return true, nil
			}).ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, errors.New("BUG: internal error (unknown)")
			}).Reset()

			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldNotBeNil)
		})

		convey.Convey("test6", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
				return false, nil
			}).ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{
					ErrorMessage: "tcp error",
				}, nil
			}).Reset()

			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldNotBeNil)
		})

		convey.Convey("test7", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
				return true, nil
			}).ApplyFunc((*tcpbus.TCPBus).Call, func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, errors.New("BUG: internal error (unknown)")
			}).Reset()

			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "function-task",
					Version:  "1",
				},
				FutureID: "1",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldNotBeNil)
		})

		convey.Convey("process init call", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "IsCanceled", func(_ *runtime.Metrics, futureID string) (bool, error) {
				return false, nil
			}).ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return &config.Configuration{}, nil
			}).ApplyFunc(tcpbus.BuildClientConnection, func(opt tcpbus.ClientOption) (*tcpbus.Connection, error) {
				return &tcpbus.Connection{}, nil
			}).ApplyFunc((*tcpbus.TCPBus).Call, func(bus *tcpbus.TCPBus, ctx context.Context, dst *common.FunctionProperty,
				req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			}).Reset()

			status.Set(status.Initializing)
			s.bus = nil
			response := s.CallByBus(context.Background(), &runtime.CallRequest{
				FunctionInfo: utils.FunctionInfo{
					TenantID: "TenantID",
					FuncName: "funcname",
					Version:  "1",
				},
				FutureID: "",
				Event:    event,
			})
			convey.So(response.Error, convey.ShouldBeNil)
			s.bus = nil
		})
	})
}

func TestServer_CancelByBus(t *testing.T) {
	convey.Convey("test Server CancelByBus", t, func() {
		s := &Server{}
		ctx := context.Background()
		request := &runtime.CancelRequest{
			InvokeID:  "invokeID",
			TraceID:   "TranceId",
			FutureID:  "FutureID",
			Force:     false,
			Recursive: false,
		}
		s.bus = &tcpbus.TCPBus{}
		convey.Convey("test1", func() {
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).Cancel, func(_ *tcpbus.TCPBus, ctx context.Context,
				dst *common.CancelProperty, req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			})
			defer patch.Reset()
			response := s.CancelByBus(ctx, request)
			convey.So(response.Error, convey.ShouldEqual, nil)
		})

		convey.Convey("test2", func() {
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).Cancel, func(_ *tcpbus.TCPBus, ctx context.Context,
				dst *common.CancelProperty, req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{
					ErrorCode: 400,
				}, nil
			})
			defer patch.Reset()
			response := s.CancelByBus(ctx, request)
			convey.So(response.Error, convey.ShouldNotBeNil)
		})
	})
}

func TestClearConn(t *testing.T) {
	s := &Server{
		readyChan: make(chan struct{}),
	}
	s.clearConn()
	close(s.readyChan)
	s.clearConn()
	assert.NotNil(t, s)
	convey.Convey("WaitRequestsFinished err", t, func() {
		count := 0
		s.initBus = &tcpbus.TCPBus{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(s.initBus), "WaitRequestsFinished",
				func(t *tcpbus.TCPBus, needReport bool, directExit <-chan struct{}) error {
					count++
					return errors.New("waitRequestsFinished err")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		s.clearConn()
		convey.So(count == 0, convey.ShouldBeFalse)
	})
	convey.Convey("cmd run err", t, func() {
		count := 0
		s.initBus = &tcpbus.TCPBus{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(s.initBus), "WaitRequestsFinished",
				func(t *tcpbus.TCPBus, needReport bool, directExit <-chan struct{}) error {
					return nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(s.initBus), "Stop",
				func(t *tcpbus.TCPBus) {}),
			gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(c *exec.Cmd) error {
				count++
				return errors.New("cmd run error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		s.clearConn()
		convey.So(count == 0, convey.ShouldBeFalse)
	})
	convey.Convey("clearConn ok", t, func() {
		count := 0
		s.initBus = &tcpbus.TCPBus{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(s.initBus), "WaitRequestsFinished",
				func(t *tcpbus.TCPBus, needReport bool, directExit <-chan struct{}) error {
					return nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(s.initBus), "Stop",
				func(t *tcpbus.TCPBus) {}),
			gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(c *exec.Cmd) error {
				count++
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		s.clearConn()
		convey.So(count == 0, convey.ShouldBeFalse)
	})
}

func TestServer_ReportSummaryLogsToBus(t *testing.T) {
	convey.Convey("test Server ReportSummaryLogsToBus", t, func() {
		s := &Server{}
		ctx := context.Background()
		info := &connecttypes.WorkerReportInfo{
			TraceID: "TranceId",
		}
		s.bus = &tcpbus.TCPBus{}
		convey.Convey("succeed to ReportSummaryLogsToBus", func() {
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportRuntimeInfo, func(_ *tcpbus.TCPBus, ctx context.Context,
				info *connecttypes.WorkerReportInfo) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			})
			defer patch.Reset()
			err := s.ReportSummaryLogsToBus(ctx, info)
			convey.So(err, convey.ShouldEqual, nil)
		})

		convey.Convey("failed to ReportSummaryLogsToBus", func() {
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportRuntimeInfo, func(_ *tcpbus.TCPBus, ctx context.Context,
				info *connecttypes.WorkerReportInfo) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, errors.New("test")
			})
			defer patch.Reset()
			err := s.ReportSummaryLogsToBus(ctx, info)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestServer_ReportErrMsgToBus(t *testing.T) {
	convey.Convey("test Server ReportErrMsgToBus", t, func() {
		s := &Server{}
		ctx := context.Background()
		s.bus = &tcpbus.TCPBus{}
		convey.Convey("succeed to ReportErrMsgToBus", func() {
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportErrMsgToBus, func(_ *tcpbus.TCPBus, ctx context.Context,
				_, _ string) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, nil
			})
			defer patch.Reset()
			err := s.ReportErrMsgToBus(ctx, "test", "test", "INFO", map[string]string{})
			convey.So(err, convey.ShouldEqual, nil)
		})

		convey.Convey("failed to ReportErrMsgToBus", func() {
			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportErrMsgToBus, func(_ *tcpbus.TCPBus, ctx context.Context, URNWithVersion, errMsg, level string,
				properties map[string]string) (*commontypes.TCPResponse, error) {
				return &commontypes.TCPResponse{}, errors.New("test")
			})
			defer patch.Reset()
			err := s.ReportErrMsgToBus(ctx, "test", "test", "INFO", map[string]string{})
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestServer_waitForInitBusClient(t *testing.T) {
	type fields struct {
		conf               *config.Configuration
		functionLogger     *functionlog.FunctionLogger
		billingLog         *functionlog.BillingLogger
		bus                *tcpbus.TCPBus
		initBus            *tcpbus.TCPBus
		rt                 runtime.Runtime
		invokeTimeout      time.Duration
		isExiting          bool
		client             *tcpbus.Connection
		funcUrn            urnutils.BaseURN
		NetworkController  commontypes.NetworkController
		IsVPCFunc          bool
		readyChan          chan struct{}
		readyChanCloseOnce sync.Once
		initBusClientOnce  sync.Once
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			fields: fields{
				bus: nil,
			},
		},
	}
	for _, tt := range tests {
		status.Set(status.Waiting)
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			s := &Server{
				conf:              tt.fields.conf,
				functionLogger:    tt.fields.functionLogger,
				billingLog:        tt.fields.billingLog,
				bus:               tt.fields.bus,
				initBus:           tt.fields.initBus,
				rt:                tt.fields.rt,
				invokeTimeout:     tt.fields.invokeTimeout,
				isExiting:         tt.fields.isExiting,
				client:            tt.fields.client,
				funcUrn:           tt.fields.funcUrn,
				NetworkController: tt.fields.NetworkController,
				IsVPCFunc:         tt.fields.IsVPCFunc,
				readyChan:         tt.fields.readyChan,
			}
			s.waitForInitBusClient()
			cost := time.Since(start)
			if cost > 4*time.Second {
				t.Errorf("expect 4 seconds but found %v", cost)
			}
		})
	}
}
