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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/common"
	buscommon "functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
	busutils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/deploy"
	"functioncore/pkg/worker/functionenv"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/network"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/fusion"
	"functioncore/pkg/worker/runtime/rpc"
	cc "functioncore/pkg/worker/runtime/rpc/connect"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

var mockConf = &config.Configuration{
	StartArgs: config.StartArgs{
		LogDir:   "/home/sn/logs",
		IsLoaded: false,
	},
	HeartbeatConfig: config.HeartbeatConfig{
		FailureThreshold:   2,
		CheckPeriodSeconds: 2,
	},
}

func TestInitServer(t *testing.T) {
	convey.Convey("Test InitServer", t, func() {
		var mockConf = &config.Configuration{}
		serverAddr := "127.0.0.1:1234"
		convey.Convey("test tcp", func() {
			_, err := InitServer(mockConf, serverAddr)
			convey.So(err, convey.ShouldEqual, nil)
			mockConf.InvokeServer.Protocol = "udp"
			_, err = InitServer(mockConf, serverAddr)
			convey.So(err, convey.ShouldNotEqual, nil)
		})

		convey.Convey("test tls", func() {
			mockConf.ServerConfig.HTTPSConfig.HTTPSEnable = true
			_, err := InitServer(mockConf, serverAddr)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestServer_LoadAndServe(t *testing.T) {
	convey.Convey("test Server LoadAndServer", t, func() {
		// s := &Server{}
		utils.LocalFuncURN.ParseFrom("sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest")
		rpc.RegisterRPCRuntime("cpp")
		rtConf := config.RuntimeConfig{
			Type:    "cpp",
			RPCPort: 31539,
			Timeout: 900,
			ResourceMetaData: config.ResourceMetaData{
				CPU: 10,
				Mem: 10,
			},
			EnvKey:  "key",
			EnvInfo: "info",
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(cc.Build, func(receiver cc.AbstractReceiver, opts cc.BuildOptions) (*cc.TinyComm, error) {
				tinyComm := &cc.TinyComm{}
				return tinyComm, nil
			}),
			gomonkey.ApplyFunc(functionlog.GetFunctionLogger,
				func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
					return functionlog.NewFunctionLogger(nil, "", ""), nil
				}),
			gomonkey.ApplyFunc((*cc.TinyComm).Request, func(_ *cc.TinyComm,
				timeout time.Duration, metaData []byte, payload []byte) ([]byte, []byte, error) {
				return []byte(nil), []byte(nil), nil
			}),
			gomonkey.ApplyFunc(functionenv.FetchEnvFromConfig, func(_ *config.Configuration, runProcessParams *runtimemanagerrequest.
				RunProcessParams) (*utils.Environment, error) {
				return nil, errors.New("envs errors")
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return &config.Configuration{}, nil
			}),
			gomonkey.ApplyFunc(functionlog.NewBillingLogger, func(_ *config.Configuration) (*functionlog.BillingLogger, error) {
				return &functionlog.BillingLogger{}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		conf := &config.Configuration{
			RuntimeConfig: rtConf,
		}
		conf.StartArgs.LogDir = "/home/sn/logs"
	})
}

func TestSendWithRetryCallBack(t *testing.T) {
	callCount := 0
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		server      *Server
		count       int
	}{
		{
			name: "case1 client is nil",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&tcpbus.Connection{}), "SendWithoutRetry",
						func(c *tcpbus.Connection, msg *protocol.Message) error {
							if len(msg.Payload) == 0 {
								return errors.New("msg payload is not valid")
							}
							return nil
						}),
				})
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(isTimeout, func(startTime time.Time) bool {
						if callCount == 0 {
							callCount = 1
							return false
						} else {
							return true
						}
					}),
				})
				return patches
			},
			server: &Server{},
			count:  1,
		},
		{
			name: "case2 client is not nil",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&tcpbus.Connection{}), "SendWithoutRetry",
						func(c *tcpbus.Connection, msg *protocol.Message) error {
							if len(msg.Payload) == 0 {
								return errors.New("msg payload is not valid")
							}
							callCount = 1
							return nil
						}),
				})
				return patches
			},
			server: &Server{client: &tcpbus.Connection{}},
			count:  1,
		},
		{
			name: "case3 SendWithoutRetry failed 3 times",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&tcpbus.Connection{}), "SendWithoutRetry",
						func(c *tcpbus.Connection, msg *protocol.Message) error {
							if len(msg.Payload) == 0 {
								return errors.New("msg payload is not valid")
							}
							callCount++
							if callCount >= 3 {
								return nil
							}
							return errors.New("test error")
						}),
				})
				return patches
			},
			server: &Server{client: &tcpbus.Connection{}},
			count:  3,
		},
		{
			name: "case4 is timeout",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&tcpbus.Connection{}), "SendWithoutRetry",
						func(c *tcpbus.Connection, msg *protocol.Message) error {
							callCount++
							return errors.New("test error")
						}),
				})
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(isTimeout, func(startTime time.Time) bool {
						return true
					}),
				})
				return patches
			},
			server: &Server{client: &tcpbus.Connection{}},
			count:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			callCount = 0
			msg := &protocol.Message{Payload: make([]byte, 10)}
			tt.server.SendWithRetryCallBack(msg)
			if tt.count != callCount {
				t.Errorf("SendWithRetryCallBack test failed, actual %d, expected: %d", callCount, tt.count)
			}
			if len(msg.Payload) != 0 {
				t.Errorf("msg reset failed")
			}
			patches.ResetAll()
		})
	}
}

func TestServer_PythonTimeoutJudger(t *testing.T) {
	convey.Convey("test Server PythonTimeoutJudger", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(getEventCtx, func(invokectx.InvokeCtx) *eventCtx {
				return &eventCtx{
					concurrency: 1,
				}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		s := &Server{
			isExiting: true,
		}
		ic := &invokectx.InvokeCtx{}
		conf := config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				Language: "python3.6",
			},
		}
		code := int32(snerror.RuntimeInvokeTimeoutCode)
		resultTrue := s.PythonTimeoutJudger(*ic, &conf, code)
		convey.So(resultTrue, convey.ShouldEqual, true)
		confJava := config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				Language: "java8",
			},
		}
		resultConfFalse := s.PythonTimeoutJudger(*ic, &confJava, code)
		convey.So(resultConfFalse, convey.ShouldEqual, false)

		resultCodeFalse := s.PythonTimeoutJudger(*ic, &conf, 1)
		convey.So(resultCodeFalse, convey.ShouldEqual, false)
	})
}

func Test_HandleRouterReady(t *testing.T) {
	convey.Convey("Test_HandleRouterReady", t, func() {
		s := &Server{isExiting: true}
		s.rt = &rpc.AbstractRPC{}
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(s.rt), "NotifyRouterReady",
				func(r *rpc.AbstractRPC) error {
					count++
					return errors.New("notifyRouterReady err")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		s.HandleRouterReady()
		convey.So(count == 0, convey.ShouldBeFalse)
	})
}

func Test_HandleUpdateAgencyToRuntime(t *testing.T) {
	convey.Convey("Test_HandleUpdateAgencyToRuntime", t, func() {
		s := &Server{isExiting: true}
		s.rt = &rpc.AbstractRPC{}
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(s.rt), "RequestUpdateAgency",
				func(r *rpc.AbstractRPC) error {
					count++
					return errors.New("requestUpdateAgency err")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		s.HandleUpdateAgencyToRuntime()
		convey.So(count == 0, convey.ShouldBeFalse)
	})
}

func TestServer_RegisterService(t *testing.T) {
	convey.Convey("test Server RegisterService", t, func() {
		s := &Server{
			isExiting: true,
		}
		client := &tcpbus.Connection{}
		bus := &tcpbus.TCPBus{}
		convey.Convey("test 1", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(bus), "WaitRequestsFinished",
					func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
						return nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := s.RegisterService(client)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("test 2", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "CHECK"
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := s.RegisterService(client)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test 3", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.NewTCPBus, func(busOption common.BusOption) (*tcpbus.TCPBus, error) {
					return &tcpbus.TCPBus{}, errors.New("error test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := s.RegisterService(client)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test 4", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "CHECK"
				}),
				gomonkey.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
					return "key", nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(bus), "CheckAuth", func(_ *tcpbus.TCPBus, key string, source string) bool {
					return false
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := s.RegisterService(client)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test 5", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(bus), "WaitRequestsFinished",
					func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
						return errors.New("test error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := s.RegisterService(client)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestServer_CheckHealth(t *testing.T) {
	convey.Convey("test Server CheckHealth", t, func() {
		s := &Server{}
		ok := s.CheckHealth()
		convey.So(ok, convey.ShouldEqual, false)
	})
}

func TestServer_GracefulShutdown(t *testing.T) {
	convey.Convey("test Server GracefulShutdown", t, func() {
		s := &Server{
			conf: &config.Configuration{},
			rt:   &rpc.AbstractRPC{},
		}
		convey.Convey("test 1", func() {
			err := s.GracefulShutdown()
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test 1.1 CAE function, need send term", func() {
			utils.LocalFuncURN.ParseFrom("sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest")
			executed := false
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(commUtils.IsCAEFunc, func(_ string) bool {
					return true
				}),
				gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout,
					func(_ *tcpmsg.SocketConn, _ *tcpmsg.Message, _ time.Duration) tcpmsg.RuntimeManagerRSP {
						executed = true
						return tcpmsg.RuntimeManagerRSP{
							Code: tcpmsg.MessageWithSuccessResponse,
						}
					}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitRequestsFinished, func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
					// will not call this method
					executed = false
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := s.GracefulShutdown()
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(executed, convey.ShouldEqual, true)
		})
		convey.Convey("test 2", func() {
			utils.LocalFuncURN.ParseFrom("sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest")
			runtime.PodApplier = constants.WorkerManagerApplier
			s.bus = &tcpbus.TCPBus{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitRequestsFinished, func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
					return nil
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).Stop, func(_ *tcpbus.TCPBus) {
					return
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportRuntimeMetrics, func(_ *tcpbus.TCPBus, ctx context.Context,
					metricsProperty *buscommon.RuntimeMetricsProperty) (*types.TCPResponse, error) {
					return &types.TCPResponse{}, nil
				}),

				gomonkey.ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
					func(_ *tcpbus.TCPBus, ctx context.Context,
						_ *common.RuntimeMetricsProperty) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
					func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
						return
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.rt), "PreStop",
					func(*rpc.AbstractRPC) error {
						return nil
					}),
			}
			err := s.GracefulShutdown()
			convey.So(err, convey.ShouldEqual, nil)
			for idx := range patches {
				patches[idx].Reset()
			}

			patch := gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitRequestsFinished, func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
				return errors.New("create error")
			})
			defer patch.Reset()
			err = s.GracefulShutdown()
			convey.So(err, convey.ShouldNotEqual, nil)
			runtime.PodApplier = ""
		})
		convey.Convey("test 3: send term msg to runtime-manager", func() {
			utils.LocalFuncURN.ParseFrom("sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest")
			runtime.PodApplier = constants.WorkerManagerApplier
			s.bus = &tcpbus.TCPBus{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitRequestsFinished, func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
					return nil
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).Stop, func(_ *tcpbus.TCPBus) {
					return
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportRuntimeMetrics, func(_ *tcpbus.TCPBus, ctx context.Context,
					metricsProperty *buscommon.RuntimeMetricsProperty) (*types.TCPResponse, error) {
					return &types.TCPResponse{}, nil
				}),

				gomonkey.ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
					func(_ *tcpbus.TCPBus, ctx context.Context,
						_ *common.RuntimeMetricsProperty) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
					func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
						return
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.rt), "PreStop",
					func(*rpc.AbstractRPC) error {
						return nil
					}),
				gomonkey.ApplyFunc(commUtils.IsCAEFunc, func(_ string) bool {
					return true
				}),
				gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout,
					func(_ *tcpmsg.SocketConn, _ *tcpmsg.Message, _ time.Duration) tcpmsg.RuntimeManagerRSP {
						return tcpmsg.RuntimeManagerRSP{}
					}),
			}
			err := s.GracefulShutdown()
			convey.So(err, convey.ShouldEqual, nil)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("test 4: send term msg to runtime-manager with MessageWithTermError", func() {
			utils.LocalFuncURN.ParseFrom("sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest")
			runtime.PodApplier = constants.WorkerManagerApplier
			s.bus = &tcpbus.TCPBus{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitRequestsFinished, func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
					return nil
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).Stop, func(_ *tcpbus.TCPBus) {
					return
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportRuntimeMetrics, func(_ *tcpbus.TCPBus, ctx context.Context,
					metricsProperty *buscommon.RuntimeMetricsProperty) (*types.TCPResponse, error) {
					return &types.TCPResponse{}, nil
				}),

				gomonkey.ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
					func(_ *tcpbus.TCPBus, ctx context.Context,
						_ *common.RuntimeMetricsProperty) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
					func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
						return
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.rt), "PreStop",
					func(*rpc.AbstractRPC) error {
						return nil
					}),
				gomonkey.ApplyFunc(commUtils.IsCAEFunc, func(_ string) bool {
					return true
				}),
				gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout,
					func(_ *tcpmsg.SocketConn, _ *tcpmsg.Message, _ time.Duration) tcpmsg.RuntimeManagerRSP {
						return tcpmsg.RuntimeManagerRSP{
							Code: tcpmsg.MessageWithTermError,
						}
					}),
			}
			err := s.GracefulShutdown()
			convey.So(err, convey.ShouldEqual, nil)
			for idx := range patches {
				patches[idx].Reset()
			}
		})

		convey.Convey("test 5: send term msg to runtime-manager with MessageWithTermTimeout", func() {
			utils.LocalFuncURN.ParseFrom("sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest")
			runtime.PodApplier = constants.WorkerManagerApplier
			s.bus = &tcpbus.TCPBus{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitRequestsFinished, func(_ *tcpbus.TCPBus, needReport bool, _ <-chan struct{}) error {
					return nil
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).Stop, func(_ *tcpbus.TCPBus) {
					return
				}),
				gomonkey.ApplyFunc((*tcpbus.TCPBus).ReportRuntimeMetrics, func(_ *tcpbus.TCPBus, ctx context.Context,
					metricsProperty *buscommon.RuntimeMetricsProperty) (*types.TCPResponse, error) {
					return &types.TCPResponse{}, nil
				}),

				gomonkey.ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
					func(_ *tcpbus.TCPBus, ctx context.Context,
						_ *common.RuntimeMetricsProperty) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
					func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
						return
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(s.rt), "PreStop",
					func(*rpc.AbstractRPC) error {
						return nil
					}),
				gomonkey.ApplyFunc(commUtils.IsCAEFunc, func(_ string) bool {
					return true
				}),
				gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout,
					func(_ *tcpmsg.SocketConn, _ *tcpmsg.Message, _ time.Duration) tcpmsg.RuntimeManagerRSP {
						return tcpmsg.RuntimeManagerRSP{
							Code: tcpmsg.MessageWithTermTimeout,
						}
					}),
				gomonkey.ApplyFunc((*Server).ReportErrMsgToBus, func(_ *Server, traceCtx context.Context, URNWithVersion, errMsg, level string,
					properties map[string]string) error {
					return nil
				}),
			}

			err := s.GracefulShutdown()
			convey.So(err, convey.ShouldEqual, nil)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func TestServer_Cancel(t *testing.T) {
	convey.Convey("test Cancel", t, func() {
		s := &Server{}
		busOption := buscommon.BusOption{}
		busOption.NewBusOpt = &tcpbus.Option{
			CreateClient: false,
		}
		tcpBus, _ := tcpbus.NewTCPBus(busOption)
		s.bus = tcpBus
		ctx := busutils.ContextWithValueString(context.Background(), constants.HeaderTraceID, "traceID")
		dst := &buscommon.CancelProperty{
			FutureID:  "futureID",
			Force:     false,
			Recursive: false,
		}
		req := &types.TCPRequest{
			RequestID: 1,
		}
		rpc.RegisterRPCRuntime("cpp")
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(cc.Build, func(receiver cc.AbstractReceiver, opts cc.BuildOptions) (*cc.TinyComm, error) {
				tinyComm := &cc.TinyComm{}
				return tinyComm, nil
			}),
			gomonkey.ApplyFunc((*cc.TinyComm).Request, func(_ *cc.TinyComm,
				timeout time.Duration, metaData []byte, payload []byte) ([]byte, []byte, error) {
				return []byte(nil), []byte(nil), nil
			}),
			gomonkey.ApplyFunc((*tcpbus.TCPBus).SetResult,
				func(_ *tcpbus.TCPBus, _ *types.TCPResponse) error {
					return nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("canceled", func() {
			s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
			convey.So(s.Cancel(ctx, dst, req), convey.ShouldBeNil)
			runtime.RunMetrics.AddTraceRecord("futureID", "callee")
			convey.So(s.Cancel(ctx, dst, req), convey.ShouldBeNil)
		})
		convey.Convey("no canceled", func() {
			s.rt = runtime.FromLagacy(fakeRuntime{
				cancelResponse: &runtime.CancelResponse{
					Error: &runtime.Error{
						Code:    0,
						Message: "Message",
					},
				},
			}, runtime.Handlers{})
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "Cancel", func(_ *runtime.Metrics, futureID string) ([]string, bool) {
				return []string{"callee"}, false
			}).Reset()
			convey.So(s.Cancel(ctx, dst, req), convey.ShouldBeNil)
		})

		convey.Convey("no canceled and response nil", func() {
			s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
			defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RunMetrics), "Cancel", func(_ *runtime.Metrics, futureID string) ([]string, bool) {
				return []string{"callee"}, false
			}).Reset()
			convey.So(s.Cancel(ctx, dst, req), convey.ShouldBeNil)
		})
	})
}

func TestServer_CancelRecursive(t *testing.T) {
	convey.Convey("test cancelRecursive", t, func() {
		s := &Server{}
		busOption := buscommon.BusOption{}
		busOption.NewBusOpt = &tcpbus.Option{
			CreateClient: false,
		}
		tcpBus, _ := tcpbus.NewTCPBus(busOption)
		s.bus = tcpBus
		dst := &buscommon.CancelProperty{
			FutureID:  "0",
			Force:     false,
			Recursive: false,
		}

		convey.Convey("cancelRecursive", func() {
			callee, _ := runtime.RunMetrics.Cancel(dst.FutureID)
			s.cancelRecursive(callee, dst.FutureID, dst)
			convey.So(callee, convey.ShouldBeNil)
		})

		patches := gomonkey.ApplyMethod(reflect.TypeOf(s.bus), "Cancel",
			func(_ *tcpbus.TCPBus, ctx context.Context, dst *common.CancelProperty, req *types.TCPRequest) (*types.TCPResponse, error) {
				return &types.TCPResponse{
					RequestID:    req.RequestID,
					ErrorCode:    proxyerrors.StateOKCode,
					ErrorMessage: "",
				}, errors.New("BUG: internal error (unknown)")
			})
		t.Cleanup(func() {
			patches.Reset()
		})

		convey.Convey("cancelRecursive2", func() {
			callee2 := []string{"hello", "world"}
			s.cancelRecursive(callee2, "id", dst)
		})
	})
}

func TestNeedToSendMessage(t *testing.T) {
	convey.Convey("test needToSendMessage", t, func() {
		domainName1 := "{\"testdomainName1\":\"192.168.0.3\"}"
		domainName2 := "{\"testdomainName2\":\"192.168.0.4\"}"
		domainName3 := "{\"testdomainName2\":\"192.168.0.5\"}"
		for i := 0; i < 10; i++ {
			if i == 0 {
				needToSend := dnCache.needToSendMessage(domainName1)
				convey.So(needToSend, convey.ShouldEqual, true)
			} else if i == 1 {
				needToSend := dnCache.needToSendMessage(domainName2)
				convey.So(needToSend, convey.ShouldEqual, true)
			} else if i == 2 {
				needToSend := dnCache.needToSendMessage(domainName3)
				convey.So(needToSend, convey.ShouldEqual, true)
			} else {
				needToSend := dnCache.needToSendMessage(domainName3)
				convey.So(needToSend, convey.ShouldEqual, false)
			}

		}
	})
}

func Test_generateResult(t *testing.T) {
	server := &Server{}
	result := &types.TCPResponse{}
	tests := []struct {
		name     string
		Setup    func(t *testing.T)
		response runtime.InvokeResponse
	}{
		{
			name:     "test1 without error",
			response: runtime.InvokeResponse{},
		},
		{
			name: "test2 response error",
			response: runtime.InvokeResponse{Error: &runtime.Error{
				Code:    snerror.RuntimeUnHealthy,
				Message: snerror.ErrText(snerror.RuntimeUnHealthy),
			}},
		},
		{
			name:     "test3 ProcessExitError",
			response: runtime.InvokeResponse{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			if tt.name == "test3 ProcessExitError" {
				runtime.ProcessExitError = snerror.New(snerror.WorkerPingVpcGatewayError, snerror.ErrText(snerror.
					WorkerPingVpcGatewayError))
				defer func() { runtime.ProcessExitError = nil }()
			}
			server.generateResultBody(result, tt.response)
		})
	}
}

func Test_LoadAndServe(t *testing.T) {
	s := &Server{
		conf: mockConf,
	}
	tests := []struct {
		name  string
		Setup func(t *testing.T)
	}{
		{
			name: "test",
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(fusion.StopRuntimeProcess,
					func(conf *config.Configuration) tcpmsg.RuntimeManagerRSP {
						return tcpmsg.RuntimeManagerRSP{
							Code: len("10"),
							Err:  "xxxxxx",
						}
					})
				patches.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput,
					func(name string) (int, string, error) {
						return 0, "", nil
					})
				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			_, err := s.LoadAndServe(context.Background(), mockConf, nil, nil)
			if err == nil {
				t.Error("Not as expectedg")
			}
		})
	}
	runtime.RuntimeManagerClient = nil
}

type fakeDeploy struct{}

func (d *fakeDeploy) Deploy(conf *config.Configuration,
	getCodeFinishCh chan struct{}, language string) (*deploy.DurationInfo, error) {
	return &deploy.DurationInfo{DeployFuncTime: 1 * time.Millisecond}, nil
}

func (d *fakeDeploy) DeploySnapshot(conf *config.Configuration, ch chan struct{}, language string) (*deploy.DurationInfo, error) {
	return &deploy.DurationInfo{DeployFuncTime: 1 * time.Millisecond}, nil
}

func (d *fakeDeploy) UploadSnapshot(conf *config.Configuration, bucketID, ObjectID string) (int64, string, error) {
	return 0, "", nil
}

func (d *fakeDeploy) DownloadSnapshotWithRetry(conf *config.Configuration, snapshotConfig config.SnapshotConfig,
	language string, traceID string) (bool, error) {
	return false, nil
}

func (d *fakeDeploy) DeleteSnapshot(conf *config.Configuration, bucketID, ObjectID string,
	needDeleteImageTags []string) error {
	return nil
}

func (d *fakeDeploy) SetCacheManagerServiceIP(conf *config.Configuration) {
	return
}
func (d *fakeDeploy) UploadDumpFile(conf *config.Configuration, dumpFileDir string) error {
	return nil
}

type fakeRuntime struct {
	hotLoadErr     error
	cancelResponse *runtime.CancelResponse
}

func (rt fakeRuntime) RequestUpdateAgency() error {
	return nil
}

func (rt fakeRuntime) NotifyRouterReady() error {
	return nil
}

func (rt fakeRuntime) SendProcessEvent(ic invokectx.InvokeCtx, message runtime.InvokeRequest, conf *config.Configuration) {
}

func (rt fakeRuntime) HotLoad(ctx context.Context, conf *config.Configuration) error {
	return rt.hotLoadErr
}

func (rt fakeRuntime) Healthz(path string, timeout int) (bool, error) {
	return false, nil
}

// StartupHealthz return rpc server startup health
func (rt fakeRuntime) StartupHealthz(path string, timeout int) (bool, error) {
	return true, nil
}

func (rt fakeRuntime) ReportCustomMetrics(timeout int) ([]byte, error) {
	return []byte{}, nil
}

func (rt fakeRuntime) ProcessEvent(ic invokectx.InvokeCtx, message *runtime.InvokeRequest) *runtime.InvokeResponse {
	return &runtime.InvokeResponse{
		Error: nil,
		Summary: snerror.SummaryInfo{
			FuncDigest:      "",
			Duration:        0,
			BillingDuration: 0,
			MemorySize:      0,
			MemoryUsed:      0,
		},
		Result: nil,
		Logs:   "Logs",
	}
}

func (rt fakeRuntime) ProcessCancelEvent(message *runtime.CancelRequest) *runtime.CancelResponse {
	if rt.cancelResponse != nil {
		return rt.cancelResponse
	}
	return nil
}

func (rt fakeRuntime) PreStop() error {
	return nil
}

func (rt fakeRuntime) GetException(shouldWait bool) *runtime.Error {
	return nil
}

func Test_LoadAndServe1(t *testing.T) {
	s := &Server{conf: mockConf}
	runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
	defer gomonkey.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "IsConnected",
		func(c *tcpmsg.SocketConn) bool { return true }).Reset()
	convey.Convey("deloy error", t, func() {
		deployMessageChan := make(chan *deploy.Message, 1)
		deployMessageChan <- &deploy.Message{Err: errors.New("")}
		s.DeployCodeFinishedChan = make(chan struct{})
		info, err := s.LoadAndServe(context.Background(), mockConf, deployMessageChan, nil)
		convey.So(info, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("create runtime error", t, func() {
		deployMessageChan := make(chan *deploy.Message, 1)
		deployMessageChan <- &deploy.Message{}
		s.DeployCodeFinishedChan = make(chan struct{})
		info, err := s.LoadAndServe(context.Background(), mockConf, deployMessageChan, nil)
		convey.So(info, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("hotload error", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(&runtime.Reg{}), "NewRuntime",
			func(_ *runtime.Reg, _ string, _ *config.Configuration, hdr runtime.Handlers) (runtime.Runtime, error) {
				return runtime.FromLagacy(fakeRuntime{hotLoadErr: errors.New("")}, hdr), nil
			}).ApplyFunc(functionlog.GetFunctionLogger,
			func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return functionlog.NewFunctionLogger(nil, "", ""), nil
			}).ApplyMethod(reflect.TypeOf(&functionlog.FunctionLogger{}), "RefreshFileModTime",
			func(*functionlog.FunctionLogger, chan struct{}) {
				return
			}).ApplyFunc(functionlog.NewBillingLogger,
			func(cfg *config.Configuration) (*functionlog.BillingLogger, error) {
				return &functionlog.BillingLogger{}, nil
			}).Reset()
		deployMessageChan := make(chan *deploy.Message, 1)
		deployMessageChan <- &deploy.Message{}
		s.DeployCodeFinishedChan = make(chan struct{})
		info, err := s.LoadAndServe(context.Background(), mockConf, deployMessageChan, nil)
		convey.So(info, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(s.functionLogger, convey.ShouldNotBeNil)
		convey.So(s.billingLog, convey.ShouldNotBeNil)
	})
	convey.Convey("get user logger error", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(&runtime.Reg{}), "NewRuntime",
			func(_ *runtime.Reg, _ string, _ *config.Configuration, hdr runtime.Handlers) (runtime.Runtime, error) {
				return runtime.FromLagacy(fakeRuntime{}, hdr), nil
			}).ApplyFunc(functionlog.GetFunctionLogger,
			func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return nil, errors.New("")
			}).Reset()
		deployMessageChan := make(chan *deploy.Message, 1)
		deployMessageChan <- &deploy.Message{}
		s.DeployCodeFinishedChan = make(chan struct{})
		info, err := s.LoadAndServe(context.Background(), mockConf, deployMessageChan, nil)
		convey.So(info, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(s.functionLogger, convey.ShouldBeNil)
	})
	convey.Convey("get billing logger error", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(&runtime.Reg{}), "NewRuntime",
			func(_ *runtime.Reg, _ string, _ *config.Configuration, hdr runtime.Handlers) (runtime.Runtime, error) {
				return runtime.FromLagacy(fakeRuntime{}, hdr), nil
			}).ApplyFunc(functionlog.GetFunctionLogger,
			func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return functionlog.NewFunctionLogger(nil, "", ""), nil
			}).ApplyMethod(reflect.TypeOf(&functionlog.FunctionLogger{}), "RefreshFileModTime",
			func(*functionlog.FunctionLogger, chan struct{}) {
				return
			}).ApplyFunc(functionlog.NewBillingLogger,
			func(cfg *config.Configuration) (*functionlog.BillingLogger, error) {
				return nil, errors.New("")
			}).Reset()
		deployMessageChan := make(chan *deploy.Message, 1)
		deployMessageChan <- &deploy.Message{}
		s.DeployCodeFinishedChan = make(chan struct{})
		info, err := s.LoadAndServe(context.Background(), mockConf, deployMessageChan, nil)
		convey.So(info, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(s.billingLog, convey.ShouldBeNil)
	})
	convey.Convey("success to LoadAndServe", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(&runtime.Reg{}), "NewRuntime",
			func(_ *runtime.Reg, _ string, _ *config.Configuration, hdr runtime.Handlers) (runtime.Runtime, error) {
				return runtime.FromLagacy(fakeRuntime{}, hdr), nil
			}).ApplyFunc(functionlog.GetFunctionLogger,
			func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return functionlog.NewFunctionLogger(nil, "", ""), nil
			}).ApplyMethod(reflect.TypeOf(&functionlog.FunctionLogger{}), "RefreshFileModTime",
			func(*functionlog.FunctionLogger, chan struct{}) {
				return
			}).ApplyFunc(functionlog.NewBillingLogger,
			func(cfg *config.Configuration) (*functionlog.BillingLogger, error) {
				return &functionlog.BillingLogger{}, nil
			}).Reset()
		deployMessageChan := make(chan *deploy.Message, 1)
		deployMessageChan <- &deploy.Message{}
		s.DeployCodeFinishedChan = make(chan struct{})
		info, err := s.LoadAndServe(context.Background(), mockConf, deployMessageChan, nil)
		time.Sleep(100 * time.Millisecond)
		convey.So(info, convey.ShouldBeNil)
		convey.So(err, convey.ShouldBeNil)
		runtime.ProcessExitSignal <- struct{}{}
	})
	runtime.RuntimeManagerClient = nil
}

func Test_StopFusionRuntimeProcess(t *testing.T) {
	s := &Server{conf: mockConf}
	convey.Convey("test stopFusionRuntimeProcess", t, func() {
		conf := &config.Configuration{
			StartArgs: config.StartArgs{},
			RuntimeConfig: config.RuntimeConfig{
				FunctionType:       "",
				EnableAuthInHeader: false,
				Type:               "fusion",
			},
		}
		patches := gomonkey.ApplyFunc(fusion.StopRuntimeProcess,
			func(conf *config.Configuration) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{
					Code: len("10"),
					Err:  "xxxxxx",
				}
			})
		t.Cleanup(func() {
			patches.Reset()
		})

		err := s.stopFusionRuntimeProcess(conf)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("test stopFusionRuntimeProcess2", t, func() {
		conf := &config.Configuration{
			StartArgs: config.StartArgs{},
			RuntimeConfig: config.RuntimeConfig{
				FunctionType:       "",
				EnableAuthInHeader: false,
				Type:               "fusion",
			},
		}
		patches := gomonkey.ApplyFunc(fusion.StopRuntimeProcess,
			func(conf *config.Configuration) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{
					Code: len("10"),
					Err:  "",
				}
			})
		t.Cleanup(func() {
			patches.Reset()
		})

		err := s.stopFusionRuntimeProcess(conf)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("test stopFusionRuntimeProcess3", t, func() {
		conf := &config.Configuration{
			StartArgs: config.StartArgs{},
			RuntimeConfig: config.RuntimeConfig{
				FunctionType:       "",
				EnableAuthInHeader: false,
				Type:               "fusion",
			},
		}
		patches := gomonkey.ApplyFunc(fusion.StopRuntimeProcess,
			func(conf *config.Configuration) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{
					Code: 0,
					Err:  "xx",
				}
			})
		t.Cleanup(func() {
			patches.Reset()
		})

		err := s.stopFusionRuntimeProcess(conf)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestServer_Invoke(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	patches := gomonkey.NewPatches()
	s := &Server{conf: &config.Configuration{}, bus: &tcpbus.TCPBus{}}
	patches.ApplyPrivateMethod(reflect.TypeOf(s), "writeResponse",
		func(s *Server, ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {})
	s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{
		RecvProcessEventHandler: func(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
			s.RecvProcessEvent(ic, resp)
			wg.Done()
		},
	})
	s.functionLogger = functionlog.NewFunctionLogger(nil, "", "")
	s.conf.RuntimeConfig = config.RuntimeConfig{Language: "custom-runtime"}
	dst := &buscommon.FunctionProperty{}
	recorder := functionlog.NewLogRecorder()
	patches.ApplyMethod(reflect.TypeOf(&runtime.ProcessLimit{}), "RecoverRuntime",
		func(*runtime.ProcessLimit, *config.Configuration, *tcpmsg.SocketConn) {})
	patches.ApplyMethod(reflect.TypeOf(s.bus), "SetResult",
		func(*tcpbus.TCPBus, *types.TCPResponse) error {
			return nil
		})
	patches.ApplyMethod(reflect.TypeOf(s.functionLogger), "GetLogRecorder",
		func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
			return recorder
		})
	patches.ApplyMethod(reflect.TypeOf(recorder), "Finish",
		func(*functionlog.LogRecorder, invokectx.InvokeCtx) {
		})
	patches.ApplyFunc(slice.ContainsString,
		func(slice []string, s string, modifier func(s string) string) bool {
			return true
		})
	patches.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	defer patches.Reset()

	request := &types.TCPRequest{
		Extra:   map[string]string{constants.HeaderDomainName: "HeaderDomainName"},
		LogType: constants.RuntimeLogOptTail}
	s.Adaptor(context.TODO(), dst, request)
	wg.Wait()
}

func TestServer_WriteResponse(t *testing.T) {
	bus := &tcpbus.TCPBus{}
	s := &Server{conf: &config.Configuration{}, bus: bus}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(getEventCtx, func(ic invokectx.InvokeCtx) *eventCtx {
			return &eventCtx{
				summarySpan: &runtime.Summary{SummaryInfo: snerror.SummaryInfo{}},
			}
		}),
		gomonkey.ApplyFunc((*Server).generateResultBody, func(_ *Server, result *types.TCPResponse, response runtime.InvokeResponse) {}),
		gomonkey.ApplyFunc(generateResultInfos, func(result *types.TCPResponse, dst *common.FunctionProperty,
			extra map[string]string, concurrency uint32, billingDuration int) {
		}),
		gomonkey.ApplyFunc(invokectx.GetTimeTraceContext, func(ic invokectx.InvokeCtx) *invokectx.TimeTraceContext {
			return &invokectx.TimeTraceContext{}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "SetResult",
			func(t *tcpbus.TCPBus, resp *types.TCPResponse) error { return nil }),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	s.writeResponse(invokectx.InvokeCtx{}, runtime.InvokeResponse{})
}

func TestServer_InvokeWorkerContentInconsistent(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	s := &Server{conf: &config.Configuration{}, bus: &tcpbus.TCPBus{}}
	s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{
		RecvProcessEventHandler: func(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
			s.RecvProcessEvent(ic, resp)
			wg.Done()
		},
	})
	s.functionLogger = functionlog.NewFunctionLogger(nil, "", "")
	s.conf.RuntimeConfig = config.RuntimeConfig{Language: "custom-runtime"}
	dst := &buscommon.FunctionProperty{}
	recorder := functionlog.NewLogRecorder()
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(&runtime.ProcessLimit{}), "RecoverRuntime",
		func(*runtime.ProcessLimit, *config.Configuration, *tcpmsg.SocketConn) {})
	patches.ApplyMethod(reflect.TypeOf(s.bus), "SetResult",
		func(*tcpbus.TCPBus, *types.TCPResponse) error {
			return nil
		})
	patches.ApplyMethod(reflect.TypeOf(s.functionLogger), "GetLogRecorder",
		func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
			return recorder
		})
	patches.ApplyMethod(reflect.TypeOf(recorder), "Finish",
		func(*functionlog.LogRecorder, invokectx.InvokeCtx) {
		})
	patches.ApplyFunc(slice.ContainsString,
		func(slice []string, s string, modifier func(s string) string) bool {
			return true
		})
	patches.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	patches.ApplyFunc(os.Getenv, func(key string) string {
		if key == "CONTENT_SIGNATURE_CHECK" {
			return "CHECK"
		}
		return ""
	})
	defer patches.Reset()

	request := &types.TCPRequest{
		Extra:         map[string]string{constants.HeaderDomainName: "HeaderDomainName", constants.HeaderContentSignature: "aaa"},
		LogType:       constants.RuntimeLogOptTail,
		RawData:       []byte("bbb"),
		DirectRequest: true,
	}
	s.Adaptor(context.TODO(), dst, request)
}

func Test_checkContentSignature(t *testing.T) {
	convey.Convey("test checkContentSignature", t, func() {
		convey.Convey("not direct request", func() {
			request := runtime.InvokeRequest{
				DirectRequest: false,
			}
			convey.So(checkContentSignature(request, "funcName"), convey.ShouldBeTrue)
		})
		convey.Convey("content consistent", func() {
			request := runtime.InvokeRequest{
				DirectRequest: true,
				Event:         []byte("aaa"),
				Extra:         map[string]string{constants.HeaderContentSignature: base64.StdEncoding.EncodeToString([]byte("aaa3"))},
			}
			convey.So(checkContentSignature(request, "funcName"), convey.ShouldBeTrue)
		})
		convey.Convey("content inconsistent", func() {
			request := runtime.InvokeRequest{
				DirectRequest: true,
				Event:         []byte("aaa"),
				Extra:         map[string]string{constants.HeaderContentSignature: "bbb"},
			}
			convey.So(checkContentSignature(request, "funcName"), convey.ShouldBeFalse)
		})
	})
}

func TestServer_InvokeWithInitError(t *testing.T) {
	s := &Server{conf: &config.Configuration{}, bus: &tcpbus.TCPBus{}}
	s.functionLogger = functionlog.NewFunctionLogger(nil, "", "")
	s.conf.RuntimeConfig = config.RuntimeConfig{Language: "custom-runtime"}
	dst := &buscommon.FunctionProperty{}
	recorder := functionlog.NewLogRecorder()
	mq := cache.GetMessageQueue()
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(&runtime.ProcessLimit{}), "RecoverRuntime",
		func(*runtime.ProcessLimit, *config.Configuration, *tcpmsg.SocketConn) {})
	patches.ApplyMethod(reflect.TypeOf(s.bus), "SetResult",
		func(*tcpbus.TCPBus, *types.TCPResponse) error {
			return errors.New("")
		})
	patches.ApplyFunc(cache.GetMessageQueue, func() *cache.MessageQueue {
		return mq
	})
	patches.ApplyMethod(reflect.TypeOf(mq), "WaitUnblock",
		func(queue *cache.MessageQueue, concurrency uint32, directRequest bool) error {
			return errors.New("")
		})
	patches.ApplyMethod(reflect.TypeOf(s.functionLogger), "NewLogRecorder",
		func(*functionlog.FunctionLogger, invokectx.InvokeCtx, string, string,
			...functionlog.LogRecorderOption) *functionlog.LogRecorder {
			return recorder
		})
	patches.ApplyMethod(reflect.TypeOf(recorder), "Finish",
		func(*functionlog.LogRecorder, invokectx.InvokeCtx) {
		})
	patches.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	defer patches.Reset()

	request := &types.TCPRequest{
		Extra:   map[string]string{constants.HeaderDomainName: "HeaderDomainName"},
		LogType: constants.RuntimeLogOptTail}
	s.Adaptor(context.TODO(), dst, request)
}

func TestServer_InvokeWithDomainName(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	rt := runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{
		RecvProcessEventHandler: func(ic invokectx.InvokeCtx, ir runtime.InvokeResponse) {
			wg.Done()
		},
	})
	s := &Server{
		conf:           &config.Configuration{},
		rt:             rt,
		functionLogger: functionlog.NewFunctionLogger(nil, "", ""),
	}
	dst := &buscommon.FunctionProperty{}
	request := &types.TCPRequest{
		Extra:   map[string]string{constants.HeaderDomainName: "HeaderDomainName"},
		LogType: constants.RuntimeLogOptTail}
	patches := gomonkey.NewPatches()
	patches.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	defer patches.Reset()
	s.Adaptor(context.TODO(), dst, request)
	wg.Wait()
}

func TestServer_InvokeWithNotConsistentFunctionInfo(t *testing.T) {
	s := &Server{
		conf: &config.Configuration{},
		bus:  &tcpbus.TCPBus{},
		funcUrn: urnutils.BaseURN{
			TenantID: "1234434",
			Name:     "test",
			Version:  "1",
		},
	}
	s.functionLogger = functionlog.NewFunctionLogger(nil, "", "")
	s.conf.RuntimeConfig = config.RuntimeConfig{Language: "custom-runtime"}
	dst := &buscommon.FunctionProperty{
		TenantID:        "123",
		FunctionName:    "test",
		FunctionVersion: "1",
	}
	recorder := functionlog.NewLogRecorder()
	mq := cache.GetMessageQueue()
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(&runtime.ProcessLimit{}), "RecoverRuntime",
		func(*runtime.ProcessLimit, *config.Configuration, *tcpmsg.SocketConn) {})
	patches.ApplyMethod(reflect.TypeOf(s.bus), "SetResult",
		func(*tcpbus.TCPBus, *types.TCPResponse) error {
			return errors.New("")
		})
	patches.ApplyFunc(cache.GetMessageQueue, func() *cache.MessageQueue {
		return mq
	})
	patches.ApplyMethod(reflect.TypeOf(mq), "WaitUnblock",
		func(queue *cache.MessageQueue, concurrency uint32, directRequest bool) error {
			return errors.New("")
		})
	patches.ApplyMethod(reflect.TypeOf(s.functionLogger), "NewLogRecorder",
		func(*functionlog.FunctionLogger, invokectx.InvokeCtx, string, string,
			...functionlog.LogRecorderOption) *functionlog.LogRecorder {
			return recorder
		})
	patches.ApplyMethod(reflect.TypeOf(recorder), "Finish",
		func(*functionlog.LogRecorder, invokectx.InvokeCtx) {
		})
	patches.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	patches.ApplyFunc(runtime.CloseExitChannel, func() {
		return
	})
	defer patches.Reset()
	request := &types.TCPRequest{
		Extra:   map[string]string{constants.HeaderDomainName: "HeaderDomainName"},
		LogType: constants.RuntimeLogOptTail}
	s.Adaptor(context.TODO(), dst, request)
}

func TestServer_Recover(t *testing.T) {
	rt := runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
	s := &Server{conf: mockConf, rt: rt, bus: &tcpbus.TCPBus{}}
	convey.Convey("failed to Recover for SetRecoverResult error", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(s.bus), "SetRecoverResult",
			func(*tcpbus.TCPBus, *types.TCPResponse) error {
				return errors.New("")
			}).Reset()
		request := &types.TCPRequest{Extra: make(map[string]string)}
		convey.So(s.Recover(request, &tcpbus.TCPBus{}), convey.ShouldNotBeNil)
	})
}

func TestIsPrintUserErrorLog(t *testing.T) {
	convey.Convey("test isPrintUserErrorLog", t, func() {
		convey.Convey("test_is_not_function_consistent", func() {
			result := isPrintUserErrorLog(true, nil)
			convey.So(result, convey.ShouldEqual, false)
		})
		convey.Convey("test_isBlock_cause_inodeusageError", func() {
			result := isPrintUserErrorLog(true, errors.New(snerror.ErrText(snerror.INodeUsageExceed)))
			convey.So(result, convey.ShouldEqual, false)
		})
		convey.Convey("test_function_consistent_and_not_block", func() {
			result := isPrintUserErrorLog(false, nil)
			convey.So(result, convey.ShouldEqual, true)
		})
		convey.Convey("test_function_consistent_and_other_error", func() {
			result := isPrintUserErrorLog(false, errors.New("other error"))
			convey.So(result, convey.ShouldEqual, true)
		})
	})
}

func TestServer_HandleDomainName(t *testing.T) {
	convey.Convey("test handleDomainName", t, func() {
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}

		convey.Convey("test1", func() {
			dnCache.domainNameValue = "domainName"
			handleDomainName(map[string]string{
				constants.HeaderDomainName: "domainName",
			}, false)
		})
	})

	convey.Convey("test handleDomainName needToSend", t, func() {
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}

		convey.Convey("test2", func() {
			patches := gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{}
			})
			defer patches.Reset()
			dnCache.domainNameValue = "domainName1"
			handleDomainName(map[string]string{
				constants.HeaderDomainName: "domainName",
			}, false)
		})
	})

	convey.Convey("test handleDomainName CustomContainer unmarshal error", t, func() {
		runtime.RuntimeManagerClient = nil
		convey.Convey("test3", func() {
			patches := gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{}
			})
			defer patches.Reset()
			dnCache.domainNameValue = "domainName1"
			handleDomainName(map[string]string{
				constants.HeaderDomainName: "domainName",
			}, true)
		})
	})

	convey.Convey("test handleDomainName CustomContainer ReplaceByDNS error", t, func() {
		runtime.RuntimeManagerClient = nil
		convey.Convey("test4", func() {
			patches := gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{}
			})
			defer patches.Reset()
			inputDomain, _ := json.Marshal(map[string]string{"abc": "123.1.1.1"})
			dnCache.domainNameValue = "domainName1"
			handleDomainName(map[string]string{
				constants.HeaderDomainName: string(inputDomain),
			}, true)
		})
	})
}

func TestMain(m *testing.M) {
	os.MkdirAll(mockConf.StartArgs.LogDir, 0750)
	functionlog.GetFunctionLogger(mockConf)
	m.Run()
	os.RemoveAll(mockConf.StartArgs.LogDir)
}

func TestCheckpointRuntimeProcess(t *testing.T) {
	conf := &config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			BucketID:            "bucketID",
			ObjectID:            "objectID",
			LayerInfo:           "layer",
			DeployDir:           "/opt/function/code",
			StorageType:         "s3",
			CodePackage:         config.CodePackage{},
			S3Config:            config.S3Config{},
			AuthConfig:          localauth.AuthConfig{},
			InternalHTTPSConfig: config.InternalHTTPSConfig{HTTPSEnable: true},
		},
		SnapshotConfig: config.SnapshotConfig{
			SnapshotBucketID: "snapshotBucketID",
			SnapshotObjectID: "snapshotObjectID",
			SnapshotSize:     120,
		},
		RuntimeConfig: config.RuntimeConfig{
			URN: "urn",
		},
		ServerConfig: config.ServerConfig{IOReadTimeout: 3},
	}

	s := &Server{conf: conf}

	deployer := &fakeDeploy{}

	convey.Convey("test CheckpointRuntimeProcess", t, func() {
		convey.Convey("test checkpoint ready", func() {
			s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(fusion.SendCheckpointCommand, func() error {
				return nil
			})
			t.Cleanup(func() {
				patches.Reset()
			})

			status.Set(status.Checkpointing)
			s.CheckpointRuntimeProcess(deployer, "")
			convey.So(status.Get(), convey.ShouldEqual, status.CheckpointReady)
		})

		convey.Convey("test SendCheckpointCommand failed", func() {
			s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
			patches := gomonkey.NewPatches()

			patches.ApplyFunc(fusion.SendCheckpointCommand, func() error {
				return errors.New("SendCheckpointCommand error")
			})
			t.Cleanup(func() {
				patches.Reset()
			})
			status.Set(status.Checkpointing)
			s.CheckpointRuntimeProcess(deployer, "")
			convey.So(status.Get(), convey.ShouldEqual, status.CheckpointFailed)
		})

		convey.Convey("test funcBucketID empty", func() {
			s.rt = runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
			patches := gomonkey.NewPatches()

			patches.ApplyFunc(fusion.SendCheckpointCommand, func() error {
				return nil
			})
			t.Cleanup(func() {
				patches.Reset()
			})
			status.Set(status.Checkpointing)
			s.conf.SnapshotBucketID = ""
			s.CheckpointRuntimeProcess(deployer, "")
			convey.So(status.Get(), convey.ShouldEqual, status.CheckpointFailed)
		})
	})
}

func Test_writeLoadInitLog2(t *testing.T) {
	traceID := commUtils.UniqueID()
	summarySpan := runtime.NewSummarySpan(constants.InitializeStage, traceID, &config.Configuration{}, 3*time.Second)
	summarySpan.SetErr(&runtime.Error{
		Code:    snerror.FunctionExceptionCode,
		Message: "function invocation exception",
	})
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	functionLog := functionlog.NewFunctionLogger(nil, "", "")
	logRecorder := functionLog.NewLogRecorder(ic, traceID, constants.InitializeStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	log := &functionlog.FunctionLog{
		Level:   "INFO",
		Message: bytes.NewBufferString(strings.Repeat("a", 90*1024)),
	}
	logRecorder.WriteLimit(log)
	logRecorder.FinishCopy(ic)
	traceID2 := commUtils.UniqueID()
	summarySpan2 := runtime.NewSummarySpan(constants.LoadStage, traceID2, &config.Configuration{}, 3*time.Second)
	summarySpan2.SetErr(&runtime.Error{
		Code:    snerror.DiskUsageExceed,
		Message: "disk not usage exceed",
	})
	ic2 := invokectx.DefaultMetrics.AcquireInvokeCtx()
	logRecorder2 := functionLog.NewLogRecorder(ic, traceID2, constants.InitializeStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	log2 := &functionlog.FunctionLog{
		Level:   "INFO",
		Message: bytes.NewBufferString(strings.Repeat("a", 90*1024)),
	}
	logRecorder2.WriteLimit(log2)
	logRecorder2.FinishCopy(ic2)
	invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)

	type args struct {
		traceID    string
		livedataID string
	}
	tests := []struct {
		name string
		args args
		want *runtime.Error
	}{
		{"case1 with error", args{traceID: "traceID", livedataID: "livedataID"}, &runtime.Error{
			Code:    snerror.FunctionExceptionCode,
			Message: "function invocation exception",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtime.WriteLoadInitLog(tt.args.traceID, tt.args.livedataID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("writeLoadInitLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_writeLoadInitLog(t *testing.T) {
	traceID := commUtils.UniqueID()
	summarySpan := runtime.NewSummarySpan(constants.InitializeStage, traceID, &config.Configuration{}, 3*time.Second)
	summarySpan.SetErr(&runtime.Error{
		Code:    snerror.FunctionExceptionCode,
		Message: "function invocation exception",
	})
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	functionLog := functionlog.NewFunctionLogger(nil, "", "")
	logRecorder := functionLog.NewLogRecorder(ic, traceID, constants.InitializeStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	log := &functionlog.FunctionLog{
		Level:   "INFO",
		Message: bytes.NewBufferString(strings.Repeat("a", 90*1024)),
	}
	logRecorder.WriteLimit(log)
	logRecorder.FinishCopy(ic)
	invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)

	type args struct {
		traceID    string
		livedataID string
	}
	tests := []struct {
		name string
		args args
		want *runtime.Error
	}{
		{"case1 without error", args{traceID: "traceID", livedataID: "livedataID"}, &runtime.Error{
			Code:    snerror.FunctionExceptionCode,
			Message: "function invocation exception",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := runtime.WriteLoadInitLog(tt.args.traceID, tt.args.livedataID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("writeLoadInitLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_BuildRuntimeDoneAndReportEvent(t *testing.T) {
	type args struct {
		runtimeDoneEvent string
		reportEvent      string
		summarySpan      *runtime.Summary
		traceID          string
	}
	tests := []struct {
		name        string
		args        args
		want        bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test001",
			args: args{
				runtimeDoneEvent: constants.TelemetryInitRuntimeDone,
				reportEvent:      constants.TelemetryInitReport,
				summarySpan:      runtime.NewSummarySpan("stage", "traceID", &config.Configuration{}, 10*time.Second),
				traceID:          "traceID",
			},
			want: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			buildRuntimeDoneAndReportEvent(tt.args.runtimeDoneEvent, tt.args.reportEvent, tt.args.summarySpan, tt.args.traceID)
			patches.ResetAll()
		})
	}
}

func Test_afterLoadHealthCheck(t *testing.T) {
	rt := runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{})
	s := &Server{conf: mockConf, rt: rt, bus: &tcpbus.TCPBus{}}
	convey.Convey("test afterLoadHealthCheck", t, func() {
		convey.Convey("failed to afterLoadHealthCheck", func() {
			convey.So(s.afterLoadHealthCheck(), convey.ShouldBeNil)
		})
		convey.Convey("test afterLoadHealthCheck failed", func() {
			fr := fakeRuntime{}
			s1 := &Server{conf: mockConf, rt: fr, bus: &tcpbus.TCPBus{}}
			patch := gomonkey.ApplyMethod(reflect.TypeOf(fr), "Healthz",
				func(rt fakeRuntime, path string, timeout int) (bool, error) {
					return false, fmt.Errorf("status nok")
				})
			defer patch.Reset()
			err := s1.afterLoadHealthCheck()
			convey.So(strings.Contains(err.Error(), "health check failed after load function"), convey.ShouldBeTrue)
		})
	})
}

func TestServer_ReportResourceUsage(t *testing.T) {
	type fields struct {
		bus *tcpbus.TCPBus
	}
	type args struct {
		resourceUsage *common.ResourceUsage
		reportTimeout time.Duration
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockUtils.PatchesFunc
		wantErr   bool
	}{
		{
			"test server health check error",
			fields{bus: nil}, args{}, func() mockUtils.PatchSlice { return mockUtils.InitPatchSlice() }, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				bus: tt.fields.bus,
			}
			err := s.ReportResourceUsage(tt.args.resourceUsage, tt.args.reportTimeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReportResourceUsage() meet err: %s, want %t", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestServer_ReportCaeMetrics(t *testing.T) {
	type fields struct {
		bus *tcpbus.TCPBus
	}
	type args struct {
		resourceUsage *common.CaeMetrics
		reportTimeout time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test server health check error",
			fields{bus: nil}, args{}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				bus: tt.fields.bus,
			}
			err := s.ReportCaeMetrics(tt.args.resourceUsage, tt.args.reportTimeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReportResourceUsage() meet err: %s, want %t", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestServer_ReportBridgeFuncMetrics(t *testing.T) {
	type fields struct {
		bus *tcpbus.TCPBus
	}
	type args struct {
		resourceUsage *common.BridgeFuncMetrics
		reportTimeout time.Duration
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test server health check error",
			fields{bus: nil}, args{}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Server{
				bus: tt.fields.bus,
			}
			err := s.ReportBridgeFuncMetrics(tt.args.resourceUsage, tt.args.reportTimeout)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReportResourceUsage() meet err: %s, want %t", err.Error(), tt.wantErr)
			}
		})
	}
	convey.Convey("test RecoverRuntime", t, func() {
		tcpBus := &tcpbus.TCPBus{}
		s := &Server{bus: tcpBus}
		gomonkey.ApplyMethod(reflect.TypeOf(s), "CheckHealth", func(s *Server) bool { return true })
		gomonkey.ApplyMethod(reflect.TypeOf(tcpBus), "ReportBridgeFuncMetrics", func(t *tcpbus.TCPBus,
			ctx context.Context, bridgeFuncMetrics *common.BridgeFuncMetrics) error {
			return errors.New("failed to report bridgeFunc metrics usage,errCode")
		})
		err := s.ReportBridgeFuncMetrics(&common.BridgeFuncMetrics{}, 10)
		convey.So(strings.Contains(err.Error(),
			"failed to report bridgeFunc metrics usage,errCode"), convey.ShouldBeTrue)
	})
}

func TestServerPause(t *testing.T) {
	s := &Server{
		conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{Language: worker.CustomContainerLanguage}},
	}
	assert.Nil(t, s.PauseRuntime())
}

func TestServer_RuntimeCheck(t *testing.T) {
	convey.Convey("test RuntimeCheck", t, func() {
		rt := runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{
			RecvProcessEventHandler: func(ic invokectx.InvokeCtx, ir runtime.InvokeResponse) {
			},
		})
		s := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890123",
					CodeSha512: "123456789012345678901234567890123",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			},
			rt: rt,
		}
		convey.Convey("failed to generateLogAndSummary", func() {
			patch := gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
				return runtime.TraceIDFunctionLogger{}, errors.New("test")
			})
			defer patch.Reset()
			_, err := s.RuntimeCheck("test", false)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("failed to proto.Marshal", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
					return runtime.TraceIDFunctionLogger{
						LogRecorder: &functionlog.LogRecorder{},
						Ic:          invokectx.InvokeCtx{},
						Info:        &functionlog.InfoRecoder{},
					}, nil
				}),
				gomonkey.ApplyFunc(runtime.GenerateSummaryInfo, func(conf *config.Configuration, CPUUsedNs, TotalTimestampNs uint64,
					err error) snerror.SummaryInfo {
					return snerror.SummaryInfo{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "SetFooter", func(_ *functionlog.LogRecorder, footer *functionlog.FunctionLog, summaryInfo functionlog.LogHeaderFooterGenerator) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "FinishFooter", func(_ *functionlog.LogRecorder, ic invokectx.InvokeCtx) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "MarshalAll", func(_ *functionlog.LogRecorder) string {
					return "test"
				}),
				gomonkey.ApplyFunc(runtime.CreateRuntimeLoggerCtx, func(conf *config.Configuration, funcLog *functionlog.FunctionLog, isReversed bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(runtime.DestroyRuntimeLoggerCtx, func(funcLog *functionlog.FunctionLog) error {
					return nil
				}),
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return []byte{}, errors.New("test")
				}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := s.RuntimeCheck("test", false)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("succeed to RuntimeCheck", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
					return runtime.TraceIDFunctionLogger{
						LogRecorder: &functionlog.LogRecorder{},
						Ic:          invokectx.InvokeCtx{},
						Info:        &functionlog.InfoRecoder{},
					}, nil
				}),
				gomonkey.ApplyFunc(runtime.GenerateSummaryInfo, func(conf *config.Configuration, CPUUsedNs, TotalTimestampNs uint64,
					err error) snerror.SummaryInfo {
					return snerror.SummaryInfo{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "SetFooter", func(_ *functionlog.LogRecorder, footer *functionlog.FunctionLog, summaryInfo functionlog.LogHeaderFooterGenerator) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "FinishFooter", func(_ *functionlog.LogRecorder, ic invokectx.InvokeCtx) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "MarshalAll", func(_ *functionlog.LogRecorder) string {
					return "test"
				}),
				gomonkey.ApplyFunc(runtime.CreateRuntimeLoggerCtx, func(conf *config.Configuration, funcLog *functionlog.FunctionLog, isReversed bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(runtime.DestroyRuntimeLoggerCtx, func(funcLog *functionlog.FunctionLog) error {
					return nil
				}),
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(runtime.GetSummary, func(traceID string) *runtime.Summary {
					return &runtime.Summary{SummaryInfo: snerror.SummaryInfo{PodName: "test"}}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := s.RuntimeCheck("test", false)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestServer_generateLogAndSummary(t *testing.T) {
	convey.Convey("test generateLogAndSummary", t, func() {
		rt := runtime.FromLagacy(fakeRuntime{}, runtime.Handlers{
			RecvProcessEventHandler: func(ic invokectx.InvokeCtx, ir runtime.InvokeResponse) {
			},
		})
		s := &Server{
			conf: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890123",
					CodeSha512: "123456789012345678901234567890123",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			},
			rt: rt,
		}
		convey.Convey("failed to GetRuntimeLogRecorder", func() {
			patch := gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
				return runtime.TraceIDFunctionLogger{}, errors.New("test")
			})
			defer patch.Reset()
			_, err := s.generateLogAndSummary("test", false)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("summary span is nil", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
					return runtime.TraceIDFunctionLogger{
						LogRecorder: &functionlog.LogRecorder{},
						Ic:          invokectx.InvokeCtx{},
						Info:        &functionlog.InfoRecoder{},
					}, nil
				}),
				gomonkey.ApplyFunc(runtime.GenerateSummaryInfo, func(conf *config.Configuration, CPUUsedNs, TotalTimestampNs uint64,
					err error) snerror.SummaryInfo {
					return snerror.SummaryInfo{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "SetFooter", func(_ *functionlog.LogRecorder, footer *functionlog.FunctionLog, summaryInfo functionlog.LogHeaderFooterGenerator) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "FinishFooter", func(_ *functionlog.LogRecorder, ic invokectx.InvokeCtx) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "MarshalAll", func(_ *functionlog.LogRecorder) string {
					return "test"
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "MarshalAll", func(_ *functionlog.LogRecorder) string {
					return "test"
				}),
				gomonkey.ApplyFunc(runtime.GetSummary, func(traceID string) *runtime.Summary {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := s.generateLogAndSummary("test", false)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("failed to json.Marshal", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
					return runtime.TraceIDFunctionLogger{
						LogRecorder: &functionlog.LogRecorder{},
						Ic:          invokectx.InvokeCtx{},
						Info:        &functionlog.InfoRecoder{},
					}, nil
				}),
				gomonkey.ApplyFunc(runtime.GenerateSummaryInfo, func(conf *config.Configuration, CPUUsedNs, TotalTimestampNs uint64,
					err error) snerror.SummaryInfo {
					return snerror.SummaryInfo{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "SetFooter", func(_ *functionlog.LogRecorder, footer *functionlog.FunctionLog, summaryInfo functionlog.LogHeaderFooterGenerator) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "FinishFooter", func(_ *functionlog.LogRecorder, ic invokectx.InvokeCtx) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "MarshalAll", func(_ *functionlog.LogRecorder) string {
					return "test"
				}),
				gomonkey.ApplyFunc(runtime.DestroyRuntimeLoggerCtx, func(funcLog *functionlog.FunctionLog) error {
					return nil
				}),
				gomonkey.ApplyFunc(runtime.GetSummary, func(traceID string) *runtime.Summary {
					return &runtime.Summary{}
				}),
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return []byte{}, errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := s.generateLogAndSummary("test", false)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("succeed to generateFinishLogAndSummaryInfo", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
					return runtime.TraceIDFunctionLogger{
						LogRecorder: &functionlog.LogRecorder{},
						Ic:          invokectx.InvokeCtx{},
						Info:        &functionlog.InfoRecoder{},
					}, nil
				}),
				gomonkey.ApplyFunc(runtime.GenerateSummaryInfo, func(conf *config.Configuration, CPUUsedNs, TotalTimestampNs uint64,
					err error) snerror.SummaryInfo {
					return snerror.SummaryInfo{}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "SetFooter", func(_ *functionlog.LogRecorder, footer *functionlog.FunctionLog, summaryInfo functionlog.LogHeaderFooterGenerator) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "FinishFooter", func(_ *functionlog.LogRecorder, ic invokectx.InvokeCtx) {
					return
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "MarshalAll", func(_ *functionlog.LogRecorder) string {
					return "test"
				}),
				gomonkey.ApplyFunc(runtime.DestroyRuntimeLoggerCtx, func(funcLog *functionlog.FunctionLog) error {
					return nil
				}),
				gomonkey.ApplyFunc(runtime.GetSummary, func(traceID string) *runtime.Summary {
					return &runtime.Summary{SummaryInfo: snerror.SummaryInfo{PodName: "test"}}
				}),
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return []byte{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := s.generateLogAndSummary("test", false)
			convey.So(err, convey.ShouldBeNil)
			_, err = s.generateLogAndSummary("test", true)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestServer_RecoverRuntime(t *testing.T) {
	convey.Convey("test RecoverRuntime", t, func() {
		s := &Server{}
		convey.Convey("succeed to RecoverRuntime 1", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyMethod(reflect.TypeOf(&runtime.ProcessLimit{}), "RecoverRuntime",
				func(*runtime.ProcessLimit, *config.Configuration, *tcpmsg.SocketConn) {})
			defer patches.Reset()
			err := s.RecoverRuntime(false)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("succeed to RecoverRuntime 2", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
					msg *tcpmsg.Message) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&runtime.ProcessStatus{}), "Get", func(_ *runtime.ProcessStatus) tcpmsg.RuntimeProcessStatus {
					return tcpmsg.RuntimeProcessStatusRunning
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Cond{}), "Wait", func(_ *sync.Cond) {
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			runtime.NewRuntimeState(true)
			err := s.RecoverRuntime(true)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("failed to RecoverRuntime", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
					msg *tcpmsg.Message) error {
					return errors.New("test")
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&runtime.ProcessStatus{}), "Get", func(_ *runtime.ProcessStatus) tcpmsg.RuntimeProcessStatus {
					return tcpmsg.RuntimeProcessStatusRunning
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Cond{}), "Wait", func(_ *sync.Cond) {
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			runtime.NewRuntimeState(true)
			err := s.RecoverRuntime(true)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestServer_setResponseLogs(t *testing.T) {
	s := &Server{conf: &config.Configuration{
		RuntimeConfig: config.RuntimeConfig{Language: worker.GOV1RuntimeLanguage}},
		bus: &tcpbus.TCPBus{},
	}
	r := &functionlog.LogRecorder{}
	runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp",
		func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
			return errors.New("err")
		})
	invokeClearSync := 0
	patch.ApplyMethod(reflect.TypeOf(r), "ClearSync", func(_ *functionlog.LogRecorder) {
		invokeClearSync++
	})
	defer func() {
		patch.Reset()
	}()
	resp := &runtime.InvokeResponse{}
	s.setResponseLogs(resp, r)
	assert.Equal(t, 1, invokeClearSync)
}

func TestServer_setDurationInfo(t *testing.T) {
	convey.Convey("test setDurationInfo", t, func() {
		convey.Convey("case1: s.conf is nil", func() {
			requestID := uint64(2)
			server := &Server{}
			response := &runtime.InvokeResponse{}
			server.setDurationInfo(response, requestID)
			convey.So(response.Summary.SpecifyPodDuration, convey.ShouldEqual, 0)
			convey.So(response.Summary.LoadFuncDuration, convey.ShouldEqual, 0)
			convey.So(response.Summary.InitFuncDuration, convey.ShouldEqual, 0)
			convey.So(response.Summary.GetCodeAndDecompressDuration, convey.ShouldEqual, 0)
		})
		convey.Convey("case1: not cold start request", func() {
			requestID := uint64(2)
			server := &Server{
				conf: &config.Configuration{
					DurationInfo: config.DurationInfo{
						ColdStartRequestID: uint64(3),
					},
				},
			}
			response := &runtime.InvokeResponse{}
			server.setDurationInfo(response, requestID)
			convey.So(response.Summary.SpecifyPodDuration, convey.ShouldEqual, 0)
			convey.So(response.Summary.LoadFuncDuration, convey.ShouldEqual, 0)
			convey.So(response.Summary.InitFuncDuration, convey.ShouldEqual, 0)
			convey.So(response.Summary.GetCodeAndDecompressDuration, convey.ShouldEqual, 0)
		})
		convey.Convey("case3: set duration", func() {
			requestID := uint64(2)
			server := &Server{
				conf: &config.Configuration{
					DurationInfo: config.DurationInfo{
						ColdStartRequestID:           requestID,
						SpecifyPodDuration:           10 * time.Millisecond,
						LoadFuncDuration:             5 * time.Millisecond,
						InitFuncDuration:             2 * time.Millisecond,
						GetCodeAndDecompressDuration: 1 * time.Millisecond,
					},
				},
			}
			response := &runtime.InvokeResponse{}
			server.setDurationInfo(response, requestID)
			convey.So(response.Summary.SpecifyPodDuration, convey.ShouldEqual, server.conf.DurationInfo.SpecifyPodDuration)
			convey.So(response.Summary.LoadFuncDuration, convey.ShouldEqual, server.conf.DurationInfo.LoadFuncDuration)
			convey.So(response.Summary.InitFuncDuration, convey.ShouldEqual, server.conf.DurationInfo.InitFuncDuration)
			convey.So(response.Summary.GetCodeAndDecompressDuration, convey.ShouldEqual, server.conf.DurationInfo.GetCodeAndDecompressDuration)
		})
	})
}

func Test_addDefaultDestination(t *testing.T) {
	defer gomonkey.ApplyFunc(os.ExpandEnv, func(s string) string {
		return "127.0.0.1"
	}).Reset()
	defer gomonkey.ApplyFunc(network.GetIntfnameAndGatewayByIPAddr, func(ip string) (string, string, error) {
		return "", "8.8.8.8", nil
	}).Reset()
	defer gomonkey.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
		return 0, "", nil
	}).Reset()
	err := addDefaultDestination()
	assert.Nil(t, err)

}
