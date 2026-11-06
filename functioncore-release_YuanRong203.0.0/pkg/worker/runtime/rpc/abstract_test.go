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

package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionenv"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/rpc/connect"
	"functioncore/pkg/worker/runtime/rpc/connect/message"
	"functioncore/pkg/worker/runtime/rpc/pb"
	"functioncore/pkg/worker/telemetry"
	"functioncore/pkg/worker/utils"
	mockutils "functioncore/test/utils"
)

func runServer() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	msgPool := &sync.Pool{
		New: func() interface{} {
			return &message.Message{}
		},
	}
	bufPool := &sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
	process := func(conn net.Conn) {
		for {
			msg := msgPool.Get().(*message.Message)
			err := message.Decode(msg, conn)
			if err != nil {
				panic(err)
			}
			go func() {
				msg.Header.PacketType = message.NormalResp
				buf := bufPool.Get().(*bytes.Buffer)
				defer bufPool.Put(buf)
				err := message.Encode(msg, buf)
				if err != nil {
					panic(err)
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					panic(err)
				}
				msgPool.Put(msg)
			}()
		}
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			go process(conn)
		}
	}()

	return listener.Addr().String(), nil
}

func TestPrestart(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal("run server error.")
	}
	Convey("test pre start", t, func() {
		Convey("test pre start fail", func() {
			patch := ApplyFunc(NewClient, func(address string, timeout int64, options ...func(*BuildOption)) (*Client, error) {
				return nil, fmt.Errorf("fail to new client")
			})
			defer patch.Reset()
			c := &creator{}
			serverPort, err := strconv.ParseInt(strings.Split(addr, ":")[1], 10, 32)
			if err != nil {
				t.Fatal("get server port error.")
			}
			err = c.Prestart(&config.RuntimeConfig{
				RPCPort: int32(serverPort),
			})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test pre start success", func() {
			c := &creator{}
			serverPort, err := strconv.ParseInt(strings.Split(addr, ":")[1], 10, 32)
			if err != nil {
				t.Fatal("get server port error.")
			}
			err = c.Prestart(&config.RuntimeConfig{
				RPCPort: int32(serverPort),
			})
			So(err, ShouldEqual, nil)
		})
	})
}

func TestNewAbstractRPC(t *testing.T) {
	configuration := &config.Configuration{ReconnectTimes: 1}
	handlers := runtime.Handlers{}
	Convey("test new abstract prc", t, func() {
		Convey("test new abstract prc new abstract runtime fail", func() {
			patch := ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
				return nil, fmt.Errorf("fail to new abstract runtime")
			})
			defer patch.Reset()
			_, err := NewAbstractRPC(configuration, handlers)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test new abstract prc CallHandler not exist fail", func() {
			patch := ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
				return &runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				}, nil
			})
			defer patch.Reset()
			_, err := NewAbstractRPC(configuration, handlers)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test new abstract prc CancelHandler not exist fail", func() {
			patch := ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
				return &runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				}, nil
			})
			defer patch.Reset()
			handlers.CallHandler = func(ctx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
				return nil
			}
			_, err := NewAbstractRPC(configuration, handlers)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test new abstract prc success", func() {
			patch := ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
				return &runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
				}, nil
			})
			defer patch.Reset()
			handlers.CallHandler = func(ctx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
				return nil
			}
			handlers.CancelHandler = func(ctx context.Context, request *runtime.CancelRequest) *runtime.CancelResponse {
				return nil
			}
			_, err := NewAbstractRPC(configuration, handlers)
			So(err, ShouldEqual, nil)
		})
	})
}

func TestStartClient(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal("run server error.")
	}
	serverPort, err := strconv.ParseInt(strings.Split(addr, ":")[1], 10, 32)
	if err != nil {
		t.Fatal("get server port error.")
	}
	r := &AbstractRPC{}
	r.Configuration = &config.Configuration{}
	r.Configuration.RPCPort = int32(serverPort)
	c := &Client{conn: &connect.TinyComm{}, receiver: &Receiver{}}
	Convey("test start client", t, func() {
		Convey("success with runtimeClient is nil", func() {
			err = r.StartClient(nil)
			So(err, ShouldEqual, nil)
		})
		Convey("success with conn of runtimeClient is broken", func() {
			patch := ApplyMethod(reflect.TypeOf(c.conn), "IsBroken", func(*connect.TinyComm) bool {
				return true
			})
			defer patch.Reset()
			err = r.StartClient(c)
			So(err, ShouldEqual, nil)
			So(r.rpcClient, ShouldNotResemble, c)
		})
		Convey("success with runtimeClient is not nil", func() {
			patch := ApplyMethod(reflect.TypeOf(c.conn), "IsBroken", func(*connect.TinyComm) bool {
				return false
			})
			defer patch.Reset()
			err = r.StartClient(c)
			So(err, ShouldEqual, nil)
			So(r.rpcClient, ShouldResemble, c)
		})
	})
}

func TestFetchEnvFromConfig(t *testing.T) {
	Convey("test fetch env from config", t, func() {
		Convey("test fetch env fail", func() {
			patch := ApplyFunc(functionenv.FetchEnvFromConfig, func(c *config.Configuration, runProcessParams *runtimemanagerrequest.
				RunProcessParams) (*utils.Environment, error) {
				return nil, fmt.Errorf("fetch env from config error")
			})
			defer patch.Reset()
			env := fetchEnvFromConfig(nil)
			So(env.FuncEnvs, ShouldNotEqual, nil)
			So(env.RuntimeEnvs, ShouldNotEqual, nil)
		})
		Convey("test fetch env success", func() {
			patch := ApplyFunc(functionenv.FetchEnvFromConfig, func(c *config.Configuration, runProcessParams *runtimemanagerrequest.
				RunProcessParams) (*utils.Environment, error) {
				return &utils.Environment{}, nil
			})
			defer patch.Reset()
			env := fetchEnvFromConfig(nil)
			So(env.FuncEnvs, ShouldEqual, nil)
			So(env.RuntimeEnvs, ShouldEqual, nil)
		})
	})
}

func TestHandleLoadFailure(t *testing.T) {
	Convey("test handle load failure", t, func() {
		Convey("test handle load failure getExceptionHandler is nil", func() {
			rpc := AbstractRPC{
				handlers: runtime.Handlers{},
			}
			errC := fmt.Errorf("test handle load failure")
			err := rpc.handleLoadFailure(errC)
			So(err.Error(), ShouldEqual, errC.Error())
		})
		Convey("test handle load failure getExceptionHandler exist", func() {
			rpc := AbstractRPC{
				handlers: runtime.Handlers{
					GetExceptionHandler: func() *runtime.Error {
						return &runtime.Error{
							Code:    snerror.CallFunctionErrorCode,
							Message: "test handle load failure",
						}
					},
				},
			}
			errC := fmt.Errorf("test handle load failure")
			err := rpc.handleLoadFailure(errC)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestProcessInitFunction(t *testing.T) {
	Convey("test process init function", t, func() {
		rpc := AbstractRPC{protocol: &ProtoProtocol{}}
		Convey("test process init function fail", func() {
			patch := ApplyFunc((*AbstractRPC).processInitializer, func(a *AbstractRPC, c *config.Configuration) error {
				return fmt.Errorf("process initializer error")
			})
			defer patch.Reset()
			err := rpc.processInitFunction(&config.Configuration{})
			So(err, ShouldNotEqual, nil)
		})
		Convey("test process pb init function success", func() {
			patch := ApplyFunc((*AbstractRPC).processInitializer, func(a *AbstractRPC, c *config.Configuration) error {
				return nil
			})
			defer patch.Reset()
			err := rpc.processInitFunction(&config.Configuration{})
			So(err, ShouldEqual, nil)
		})
		rpc = AbstractRPC{protocol: &JSONProtocol{}}
		Convey("test process json init function success", func() {
			patch := ApplyFunc((*AbstractRPC).processInitializer, func(a *AbstractRPC, c *config.Configuration) error {
				return nil
			})
			defer patch.Reset()
			err := rpc.processInitFunction(&config.Configuration{})
			So(err, ShouldEqual, nil)
		})
	})
}

func TestProcessCancelEvent(t *testing.T) {
	Convey("test process cancel event", t, func() {
		r := &AbstractRPC{protocol: &JSONProtocol{}}
		cancelResp := r.ProcessCancelEvent(nil)
		So(cancelResp.Error.Code, ShouldEqual, snerror.WorkerInternalErrorCode)
	})
}

func TestBuildRuntimeError(t *testing.T) {
	Convey("test build runtime error", t, func() {
		rpc := AbstractRPC{
			rpcClient: &Client{
				invokeTimeout:      10 * time.Second,
				initializerTimeout: 10 * time.Second,
			},
			handlers: runtime.Handlers{},
			protocol: &JSONProtocol{},
		}
		Convey("test build runtime error internal error", func() {
			runtimeErr := &runtime.Error{
				Code:    snerror.CallFunctionErrorCode,
				Message: "test handle load failure",
			}
			patch := ApplyGlobalVar(&rpc.handlers.GetExceptionHandler, func() *runtime.Error {
				return runtimeErr
			})
			defer patch.Reset()
			err := rpc.buildRuntimeError(snerror.New(snerror.InternalError, snerror.InternalErrorMsg), constants.InitializeStage)
			So(err, ShouldEqual, runtimeErr)
		})
		Convey("test build runtime error FuncInsExceptionCode", func() {
			killFlag := false
			patches := [...]*Patches{
				ApplyGlobalVar(&rpc.handlers.GetExceptionHandler, func() *runtime.Error {
					return nil
				}),
				ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
					msg *tcpmsg.Message) error {
					killFlag = true
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := rpc.buildRuntimeError(snerror.New(snerror.FuncInsExceptionCode, "test"), constants.InvokeStage)
			So(err.Code, ShouldEqual, snerror.FuncInsExceptionCode)
			So(err.Message, ShouldEqual, snerror.ErrText(snerror.FuncInsExceptionCode))
			So(killFlag, ShouldBeTrue)
		})
		Convey("test build runtime error not sn error", func() {
			errC := fmt.Errorf("not sn error")
			err := rpc.buildRuntimeError(errC, constants.InitializeStage)
			So(err.Code, ShouldEqual, snerror.WorkerInternalErrorCode)
			So(err.Message, ShouldEqual, errC.Error())
		})
		Convey("test build runtime error normal error stage is invoke stage", func() {
			err := rpc.buildRuntimeError(snerror.New(snerror.InternalError, snerror.InternalErrorMsg), constants.InvokeStage)
			So(err.Code, ShouldEqual, snerror.InternalError)
			So(err.Message, ShouldEqual, snerror.InternalErrorMsg)
		})
		Convey("test build runtime error normal error stage is initialize stage", func() {
			err := rpc.buildRuntimeError(snerror.New(snerror.InternalError, snerror.InternalErrorMsg), constants.InitializeStage)
			So(err.Code, ShouldEqual, snerror.InternalError)
			So(err.Message, ShouldEqual, snerror.FunctionInitFailErrPrefix+snerror.InternalErrorMsg)
		})
		Convey("test build runtime error normal error invoke timeout", func() {
			err := rpc.buildRuntimeError(snerror.New(snerror.RuntimeInvokeTimeoutCode, snerror.InternalErrorMsg), constants.InitializeStage)
			So(err.Code, ShouldEqual, snerror.RuntimeInvokeTimeoutCode)
			So(err.Message, ShouldEqual, fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), rpc.rpcClient.initializerTimeout))
		})
		Convey("test build runtime error normal error load timeout", func() {
			err := rpc.buildRuntimeError(snerror.New(snerror.RuntimeLoadTimeoutCode, snerror.InternalErrorMsg), constants.InitializeStage)
			So(err.Code, ShouldEqual, snerror.RuntimeLoadTimeoutCode)
			So(err.Message, ShouldEqual, fmt.Sprintf(snerror.ErrText(snerror.RuntimeLoadTimeoutCode), rpc.rpcClient.initializerTimeout))
		})
	})
}

func TestConvertErrorFromLoadResponse(t *testing.T) {
	Convey("test convert error from load response", t, func() {
		Convey("test convert error from load response error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			err := r.convertErrorFromLoadResponse(fmt.Errorf("test convert error"), nil)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test convert error from proto load response error rsp error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			rsp := &pb.LoadResponse{Error: &pb.Error{Code: snerror.InternalError}}
			response, _ := proto.Marshal(rsp)
			err := r.convertErrorFromLoadResponse(nil, response)
			So(err.Code, ShouldEqual, rsp.Error.Code)
		})
		Convey("test convert error from json load response error rsp error", func() {
			r := &AbstractRPC{protocol: &JSONProtocol{}}
			rsp := LoadResponse{Error: &Error{Code: snerror.InternalError}}
			response, _ := json.Marshal(rsp)
			err := r.convertErrorFromLoadResponse(nil, response)
			So(err.Code, ShouldEqual, rsp.Error.Code)
		})
	})
}

func TestConvertErrorFromInitializerResponse(t *testing.T) {
	Convey("test convert error from initializer response", t, func() {
		Convey("test convert error from initializer response error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			err := r.convertErrorFromInitializerResponse(fmt.Errorf("test convert error"), nil)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test convert error from initializer response rsp error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			rsp := &pb.InitializerResponse{
				Error: &pb.Error{Code: snerror.InternalError},
			}
			response, _ := proto.Marshal(rsp)
			patch := ApplyMethod(reflect.TypeOf(rsp), "GetError",
				func(_ *pb.InitializerResponse) *pb.Error {
					return &pb.Error{
						Code:    snerror.InternalError,
						Message: snerror.InternalErrorMsg,
					}
				})
			defer patch.Reset()
			err := r.convertErrorFromInitializerResponse(nil, response)
			So(err.Code, ShouldEqual, rsp.GetError().GetCode())
			So(err.Message, ShouldEqual, rsp.GetError().GetMessage())
		})
		Convey("test convert error from json initializer response rsp error", func() {
			r := &AbstractRPC{protocol: &JSONProtocol{}}
			rsp := &InitializerResponse{
				Error: &Error{Code: snerror.InternalError},
			}
			response, _ := json.Marshal(rsp)
			err := r.convertErrorFromInitializerResponse(nil, response)
			So(err.Code, ShouldEqual, rsp.Error.Code)
			So(err.Message, ShouldEqual, rsp.Error.Message)
		})
	})
}

func TestSetResultFromRsp(t *testing.T) {
	Convey("test set result from response", t, func() {
		Convey("test set result from proto response error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			result := &runtime.InvokeResponse{}
			r.setResultFromRsp(result, fmt.Errorf("test convert error"), nil)
			So(result.Error, ShouldNotEqual, nil)
		})
		Convey("test set result from response get error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			result := &runtime.InvokeResponse{}
			rsp := &pb.InvokeResponse{
				Error: &pb.Error{Code: snerror.InternalError},
			}
			response, _ := proto.Marshal(rsp)
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(rsp), "GetError",
					func(_ *pb.InvokeResponse) *pb.Error {
						return &pb.Error{
							Code:    snerror.InternalError,
							Message: snerror.InternalErrorMsg,
						}
					}),
				ApplyMethod(reflect.TypeOf(rsp), "GetResult",
					func(_ *pb.InvokeResponse) []byte {
						return []byte("test rsp error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			r.setResultFromRsp(result, nil, response)
			So(result.Error, ShouldNotEqual, nil)
			So(result.Error.Code, ShouldEqual, rsp.GetError().Code)
			So(result.Error.Message, ShouldEqual, string(rsp.GetResult()))
		})
		Convey("test set result from pb response get result", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			result := &runtime.InvokeResponse{}
			rsp := &pb.InvokeResponse{}
			response, _ := proto.Marshal(rsp)
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(rsp), "GetResult",
					func(_ *pb.InvokeResponse) []byte {
						return []byte("test rsp error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			r.setResultFromRsp(result, nil, response)
			So(string(result.Result), ShouldEqual, string(rsp.GetResult()))
		})
		Convey("test set result from json response get result", func() {
			r := &AbstractRPC{protocol: &JSONProtocol{}}
			funcRes := []byte("Hello World")
			buf := bytes.NewBuffer([]byte{})
			if err := binary.Write(buf, binary.BigEndian, uint32(len(funcRes))); err != nil {
				fmt.Println("failed to build invoke response" + err.Error())
				return
			}
			buf.Write(funcRes)
			rsp := &InvokeResponse{}
			response, _ := json.Marshal(rsp)
			buf.Write(response)
			result := &runtime.InvokeResponse{}
			r.setResultFromRsp(result, nil, buf.Bytes())
			So(string(result.Result), ShouldEqual, string(funcRes))
		})
	})
}

func TestProcessEvent(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}

	r := &AbstractRPC{protocol: &ProtoProtocol{}}
	r.rpcClient = &Client{
		receiver:      &Receiver{},
		invokeTimeout: 60 * time.Second,
	}
	c, err := connect.Build(r.rpcClient.receiver, connect.BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
	if err != nil {
		t.Fatal(err)
	}
	r.rpcClient.SetProcessor(r)
	r.rpcClient.conn = c

	var wg sync.WaitGroup

	metrics := invokectx.DefaultMetrics

	r.handlers.RecvProcessEventHandler = func(ic invokectx.InvokeCtx, ir runtime.InvokeResponse) {
		metrics.ReleaseInvokeCtx(ic)
		wg.Done()
	}

	conf := &config.Configuration{}
	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)
	wg.Add(1)
	r.SendProcessEvent(metrics.AcquireInvokeCtx(), runtime.InvokeRequest{}, conf)
	wg.Wait()

	r.protocol = &JSONProtocol{}
	wg.Add(1)
	r.SendProcessEvent(metrics.AcquireInvokeCtx(), runtime.InvokeRequest{}, conf)
	wg.Wait()

	wg.Add(1)
	patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("s") })
	r.SendProcessEvent(metrics.AcquireInvokeCtx(), runtime.InvokeRequest{}, conf)
	wg.Wait()
	patch.Reset()
}

func TestProcessCall(t *testing.T) {
	Convey("test process call", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
		}
		defer ApplyMethod(reflect.TypeOf(r.FuncLogger), "GetLogRecorder",
			func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
				return functionlog.NewLogRecorder()
			}).Reset()
		cr := &pb.CallRequest{}
		Convey("CallResponse is nil", func() {
			r.handlers = runtime.Handlers{
				CallHandler: func(ctx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
					return nil
				},
			}
			_, err := r.ProcessCall(cr)
			So(err, ShouldBeNil)
		})

		Convey("CallResponse is not nil", func() {
			r.handlers = runtime.Handlers{
				CallHandler: func(ctx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
					return &runtime.CallResponse{}
				},
			}
			_, err := r.ProcessCall(cr)
			So(err, ShouldBeNil)
		})
	})
}

func TestProcessCancelRequest(t *testing.T) {
	Convey("test Process Cancel Request", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
		}
		cr := &pb.CancelRequest{}
		Convey("CallResponse is nil", func() {
			r.handlers = runtime.Handlers{
				CancelHandler: func(ctx context.Context, request *runtime.CancelRequest) *runtime.CancelResponse {
					return nil
				},
			}
			_, err := r.ProcessCancelRequest(cr)
			So(err, ShouldBeNil)
		})

		Convey("CallResponse is not nil", func() {
			r.handlers = runtime.Handlers{
				CancelHandler: func(ctx context.Context, request *runtime.CancelRequest) *runtime.CancelResponse {
					return &runtime.CancelResponse{}
				},
			}
			_, err := r.ProcessCancelRequest(cr)
			So(err, ShouldBeNil)
		})
	})
}

func TestProcessLog(t *testing.T) {
	Convey("test pb process Log", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
			protocol: &ProtoProtocol{},
		}
		logRecorder := functionlog.NewLogRecorder()
		defer ApplyMethod(reflect.TypeOf(r.AbstractRuntime.FuncLogger), "GetLogRecorder",
			func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
				return logRecorder
			}).Reset()
		log := &pb.FunctionLog{
			Timestamp: time.Now().String(),
		}
		logByte, _ := proto.Marshal(log)
		r.ProcessLog(logByte)
		logs := logRecorder.MarshalAll()
		So(len(logs), ShouldNotEqual, 0)
	})
	Convey("test json process Log", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
			protocol: &JSONProtocol{},
		}
		logRecorder := functionlog.NewLogRecorder()
		defer ApplyMethod(reflect.TypeOf(r.AbstractRuntime.FuncLogger), "GetLogRecorder",
			func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
				return logRecorder
			}).Reset()
		log := FunctionLog{
			Timestamp: time.Now().String(),
		}
		logByte, _ := json.Marshal(log)
		r.ProcessLog(logByte)
		logs := logRecorder.MarshalAll()
		So(len(logs), ShouldNotEqual, 0)
	})
	Convey("test json process Log with standard print", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
			protocol: &JSONProtocol{},
		}
		logRecorder := functionlog.NewLogRecorder()
		defer ApplyMethod(reflect.TypeOf(r.AbstractRuntime.FuncLogger), "GetLogRecorder",
			func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
				return logRecorder
			}).Reset()
		log := FunctionLog{
			Timestamp: time.Now().String(),
			Content:   "2022-12-10 10:15:12.125 1d5e992b-28f3-4c13-b268-e6a975550e9e log-test",
		}
		logByte, _ := json.Marshal(log)
		r.ProcessLog(logByte)
		logs := logRecorder.MarshalAll()
		So(len(logs), ShouldNotEqual, 0)
	})
	Convey("test pb process Log with standard print", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
			protocol: &ProtoProtocol{},
		}
		logRecorder := functionlog.NewLogRecorder()
		defer ApplyMethod(reflect.TypeOf(r.AbstractRuntime.FuncLogger), "GetLogRecorder",
			func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
				return logRecorder
			}).Reset()
		log := &pb.FunctionLog{
			Timestamp: time.Now().String(),
			Content:   "2022-12-10 10:15:12.125 1d5e992b-28f3-4c13-b268-e6a975550e9e log-test",
		}
		logByte, _ := proto.Marshal(log)
		r.ProcessLog(logByte)
		logs := logRecorder.MarshalAll()
		So(len(logs), ShouldNotEqual, 0)
	})

	Convey("test pb process Log with context logger print", t, func() {
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
			protocol: &ProtoProtocol{},
		}
		logRecorder := functionlog.NewLogRecorder()
		defer ApplyMethod(reflect.TypeOf(r.AbstractRuntime.FuncLogger), "GetLogRecorder",
			func(*functionlog.FunctionLogger, invokectx.InvokeCtx) *functionlog.LogRecorder {
				return logRecorder
			}).Reset()
		ApplyMethod(reflect.TypeOf(&ProtoProtocol{}), "ParseLog",
			func(_ *ProtoProtocol, l *functionlog.FunctionLog, message []byte) (string, string, error) {
				return "100", "traceID", nil
			})
		log := &pb.FunctionLog{
			Timestamp: time.Now().String(),
			Content:   "2022-12-10 10:15:12.125 1d5e992b-28f3-4c13-b268-e6a975550e9e log-test",
		}
		logByte, _ := proto.Marshal(log)
		r.ProcessLog(logByte)
		logs := logRecorder.MarshalAll()
		So(len(logs), ShouldNotEqual, 0)
	})
}

func TestGetRuntimeAddress(t *testing.T) {
	Convey("TestGetRuntimeAddress", t, func() {
		Convey("test true", func() {
			addr := GetRuntimeAddress(8080)
			So(addr, ShouldEqual, "127.0.0.1:8080")
		})
	})

}

func TestGetStorageInfo(t *testing.T) {
	Convey("test get storage info", t, func() {
		storageInfo := config.StorageConfig{
			Addr:     "127.0.0.1",
			Password: "123",
		}
		c := &config.Configuration{
			StorageConfig: storageInfo,
		}
		c.HandlerName = "handler"
		info := getStorageInfo(c)
		if info != nil {
			t.Fatal("this is not finance function")
		}

		c.HandlerName = ""
		c.StorageConfig.Password = ""
		info = getStorageInfo(c)
		if len(info) != 1 {
			t.Fatal("storage pwd is empty")
		}
		c.StorageConfig.Password = "123"
		info = getStorageInfo(c)
		if info == nil {
			t.Fatal("failed to get storage info")
		}
	})
}

func TestAbstractRPC_HotLoad(t *testing.T) {
	Convey("TestAbstractRPC_HotLoad", t, func() {
		Convey("hot load success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).LoadFunction, func(c *Client, request []byte,
					timeout time.Duration) ([]byte, error) {
					return nil, nil
				}),
				ApplyFunc((*AbstractRPC).processInitFunction, func(a *AbstractRPC, c *config.Configuration) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
				protocol: &ProtoProtocol{},
			}
			ctx := context.Background()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Initializer: config.Initializer{
						InitializerHandler: "handler",
					},
				},
			}
			err := rpc.HotLoad(ctx, cfg)
			So(err, ShouldEqual, nil)
		})
		Convey("load function error", func() {
			exceptedErrorMsg := "failed to marshal load request"
			patches := [...]*Patches{
				ApplyFunc((*Client).LoadFunction, func(c *Client, request []byte,
					timeout time.Duration) ([]byte, error) {
					return nil, snerror.New(http.StatusBadRequest, exceptedErrorMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
				protocol: &JSONProtocol{},
			}
			ctx := context.Background()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{},
			}
			err := rpc.HotLoad(ctx, cfg)
			So(err, ShouldNotEqual, nil)
			So(err.Error(), ShouldEqual, exceptedErrorMsg)
		})
		Convey("hot load restore snapshot success", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).processRestoreHook, func(a *AbstractRPC, c *config.Configuration) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
				protocol: &ProtoProtocol{},
			}
			ctx := context.Background()
			cfg := &config.Configuration{
				SnapshotConfig: config.SnapshotConfig{
					RestoreHook: config.RestoreHook{
						Handler: "handler",
					},
				},
				RuntimeConfig: config.RuntimeConfig{
					RunningMode: constants.RunningModeRestore,
				},
			}
			err := rpc.HotLoad(ctx, cfg)
			So(err, ShouldEqual, nil)
		})
		Convey("hot load success with direct Function", func() {
			var initFlag bool
			tmpDirectFlag := utils.IsDirectRuntime
			patches := [...]*Patches{
				ApplyFunc((*Client).LoadFunction, func(c *Client, request []byte,
					timeout time.Duration) ([]byte, error) {
					return nil, nil
				}),
				ApplyFunc((*AbstractRPC).processInitFunction, func(a *AbstractRPC, c *config.Configuration) error {
					initFlag = true
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
				protocol: &ProtoProtocol{},
			}
			ctx := context.Background()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Initializer: config.Initializer{
						InitializerHandler: "handler",
					},
				},
			}
			utils.IsDirectRuntime = true
			err := rpc.HotLoad(ctx, cfg)
			So(err, ShouldEqual, nil)
			So(initFlag, ShouldBeFalse)
			utils.IsDirectRuntime = tmpDirectFlag

		})
	})
}

func TestAbstractRPC_Healthz(t *testing.T) {
	Convey("TestAbstractRPC_Healthz", t, func() {
		Convey("check health rpc client is nil", func() {
			r := AbstractRPC{}
			b, err := r.Healthz("", 0)
			So(b, ShouldEqual, false)
			So(err, ShouldNotEqual, nil)
		})
		Convey("check health success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).CheckHeath, func(_ *Client, _ time.Duration) (*pb.HealthResponse, error) {
					return &pb.HealthResponse{
						Status: "OK",
					}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			healthz, err := rpc.Healthz("", 0)
			So(err, ShouldEqual, nil)
			So(healthz, ShouldEqual, true)
		})
		Convey("check health error", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).GetConnInfo, func(rpc *AbstractRPC) string {
					return ""
				}),
				ApplyFunc((*Client).CheckHeath, func(_ *Client, _ time.Duration) (*pb.HealthResponse, error) {
					return nil, errors.New("test error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			healthz, err := rpc.Healthz("", 0)
			So(err, ShouldNotEqual, nil)
			So(err.Error(), ShouldEqual, "test error")
			So(healthz, ShouldEqual, false)
		})
		Convey("check health status not ok", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).CheckHeath, func(_ *Client, _ time.Duration) (*pb.HealthResponse, error) {
					return &pb.HealthResponse{
						Status: "NOT OK",
					}, nil
				}),
				ApplyFunc((*AbstractRPC).GetConnInfo, func(rpc *AbstractRPC) string {
					return ""
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			healthz, err := rpc.Healthz("", 0)
			So(err, ShouldNotEqual, nil)
			So(healthz, ShouldEqual, false)
		})
	})
}

func TestAbstractRPC_CheckStartupHeath(t *testing.T) {
	Convey("TestAbstractRPC_Healthz", t, func() {
		Convey("check health rpc client is nil", func() {
			r := AbstractRPC{}
			b, err := r.StartupHealthz("", 0)
			So(b, ShouldEqual, false)
			So(err, ShouldNotEqual, nil)
		})
		Convey("check health success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).CheckStartupHeath, func(_ *Client, _ time.Duration) (*pb.HealthResponse, error) {
					return &pb.HealthResponse{
						Status: "OK",
					}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			healthz, err := rpc.StartupHealthz("", 0)
			So(err, ShouldEqual, nil)
			So(healthz, ShouldEqual, true)
		})
		Convey("check health error", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).GetConnInfo, func(rpc *AbstractRPC) string {
					return ""
				}),
				ApplyFunc((*Client).CheckStartupHeath, func(_ *Client, _ time.Duration) (*pb.HealthResponse, error) {
					return nil, errors.New("test error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			healthz, err := rpc.StartupHealthz("", 0)
			So(err, ShouldNotEqual, nil)
			So(err.Error(), ShouldEqual, "test error")
			So(healthz, ShouldEqual, false)
		})
		Convey("check health status not ok", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).CheckStartupHeath, func(_ *Client, _ time.Duration) (*pb.HealthResponse, error) {
					return &pb.HealthResponse{
						Status: "NOT OK",
					}, nil
				}),
				ApplyFunc((*AbstractRPC).GetConnInfo, func(rpc *AbstractRPC) string {
					return ""
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			healthz, err := rpc.StartupHealthz("", 0)
			So(err, ShouldNotEqual, nil)
			So(healthz, ShouldEqual, false)
		})
	})
}

func Test_ReportCustomMetrics(t *testing.T) {
	Convey("TestAbstractRPC_ReportCustomMetrics", t, func() {
		Convey("check Test_ReportCustomMetrics rpc client is nil", func() {
			r := AbstractRPC{}
			result, err := r.ReportCustomMetrics(10)
			So(result, ShouldNotEqual, nil)
			So(err, ShouldNotEqual, nil)
		})
		Convey("check report Custom Metrics success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).ReportCustomMetrics, func(_ *Client, _ int) ([]byte, error) {
					return []byte("test"), nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			result, err := rpc.ReportCustomMetrics(10)
			So(err, ShouldEqual, nil)
			So(result, ShouldNotEqual, nil)
		})
		Convey("check report Custom Metrics fail", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).ReportCustomMetrics, func(_ *Client, _ int) ([]byte, error) {
					return []byte("fail"), errors.New("test fail")
				}),
				ApplyFunc((*AbstractRPC).GetConnInfo, func(rpc *AbstractRPC) string {
					return ""
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				rpcClient: &Client{},
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{
						RuntimeConfig: config.RuntimeConfig{
							Timeout: 10,
						},
					},
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				handlers: runtime.Handlers{},
			}
			result, err := rpc.ReportCustomMetrics(10)
			So(err, ShouldNotEqual, nil)
			So(result, ShouldNotEqual, nil)
		})
	})
}

func Test_creator_NewRuntime(t *testing.T) {
	type fields struct {
		runtimeClient *Client
	}
	type args struct {
		configuration *config.Configuration
		handlers      runtime.Handlers
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      runtime.Runtime
		wantErr   bool
		patchFunc mockutils.PatchesFunc
	}{
		{
			name:   "test01",
			fields: fields{runtimeClient: &Client{}},
			args: args{
				configuration: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
					URN: "",
				}},
				handlers: runtime.Handlers{},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "test02",
			fields: fields{runtimeClient: &Client{receiver: &Receiver{}, conn: &connect.TinyComm{}}},
			args: args{
				configuration: &config.Configuration{},
				handlers: runtime.Handlers{
					CallHandler: func(ctx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
						return nil
					},
					CancelHandler: func(ctx context.Context, request *runtime.CancelRequest) *runtime.CancelResponse {
						return nil
					}},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(runtime.NewAbstractRuntime,
						func(*config.Configuration) (*runtime.AbstractRuntime, error) {
							return &runtime.AbstractRuntime{
								FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
								Configuration: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
									URN: "",
								}},
							}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "test03",
			fields: fields{runtimeClient: nil},
			args: args{
				configuration: &config.Configuration{ReconnectTimes: 1},
				handlers: runtime.Handlers{
					CallHandler: func(ctx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
						return nil
					},
					CancelHandler: func(ctx context.Context, request *runtime.CancelRequest) *runtime.CancelResponse {
						return nil
					}},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(runtime.NewAbstractRuntime,
						func(*config.Configuration) (*runtime.AbstractRuntime, error) {
							return &runtime.AbstractRuntime{
								FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
								Configuration: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
									URN: "",
								}, ReconnectTimes: 1},
							}, nil
						}),
					ApplyFunc(NewClient,
						func(address string, timeout int64, options ...func(*BuildOption)) (*Client, error) {
							return &Client{receiver: &Receiver{}}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &creator{
				runtimeClient: tt.fields.runtimeClient,
			}
			patches := tt.patchFunc()
			got, err := c.NewRuntime(tt.args.configuration, tt.args.handlers)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRuntime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.name != "test01" && got == tt.want {
				t.Errorf("NewRuntime() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getInvokeCtx(t *testing.T) {
	r := AbstractRPC{}
	_, exist := r.getInvokeCtx(functionlog.InitializerInvokeID)
	assert.False(t, exist)

	r.initCtx = &invokectx.InvokeCtx{ID: 10}
	ic, exist := r.getInvokeCtx(functionlog.InitializerInvokeID)
	assert.True(t, exist)
	assert.Equal(t, r.initCtx.ID, ic.ID)
}

func Test_BuildTelemetryFunctionEvent(t *testing.T) {
	type args struct {
		message   string
		timestamp string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{"case1", args{message: "[Function]aaaaa",
			timestamp: strconv.FormatInt(time.Now().UnixNano(), 10)}, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
					return false
				}),
			})
			return patches
		}},
		{"case2", args{message: "[Function]aaaaa",
			timestamp: strconv.FormatInt(time.Now().UnixNano(), 10)}, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
					return true
				}),
			})
			return patches
		}},
		{"case3", args{message: "[Function]aaaaa",
			timestamp: "111"}, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
					return true
				}),
			})
			return patches
		}},
		{"case4", args{message: "aaaaaaaaaaaaaaa",
			timestamp: strconv.FormatInt(time.Now().UnixNano(), 10)}, func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
					return true
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			buildTelemetryFunctionEvent(tt.args.message, tt.args.timestamp)
			patches.ResetAll()
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
	var nameIsOK bool
	tests := []struct {
		name        string
		args        args
		want        bool
		patchesFunc mockutils.PatchesFunc
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
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
					ApplyFunc(telemetry.SendEvent, func(eventData telemetry.Event) {
						if eventData.Name == constants.TelemetryInitRuntimeDone {
							nameIsOK = true
						}
						return
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
			assert.Equal(t, nameIsOK, true)
			patches.ResetAll()
		})
	}
}

func TestPreStop(t *testing.T) {
	Convey("test process init function", t, func() {
		rpc := AbstractRPC{protocol: &ProtoProtocol{}, rpcClient: &Client{}}
		rpc.Configuration = &config.Configuration{}
		rpc.Configuration.RuntimeConfig.PreStop.Handler = "test"
		rpc.Configuration.RuntimeConfig.PreStop.Handler = "0"
		Convey("test process init function fail", func() {
			patch := ApplyFunc((*AbstractRPC).processPreStopFunction, func(a *AbstractRPC) error {
				return fmt.Errorf("process preStop error")
			})
			defer patch.Reset()
			err := rpc.PreStop()
			So(err, ShouldNotEqual, nil)
		})
		Convey("test process pb init function success", func() {
			patch := ApplyFunc((*AbstractRPC).processPreStopFunction, func(a *AbstractRPC) error {
				return nil
			})
			defer patch.Reset()
			err := rpc.PreStop()
			So(err, ShouldEqual, nil)
		})
		rpc = AbstractRPC{protocol: &JSONProtocol{}, rpcClient: &Client{}}
		rpc.Configuration = &config.Configuration{}
		rpc.Configuration.RuntimeConfig.PreStop.Handler = "test"
		rpc.Configuration.RuntimeConfig.PreStop.Handler = "0"
		Convey("test process json init function success", func() {
			patch := ApplyFunc((*AbstractRPC).processPreStopFunction, func(a *AbstractRPC) error {
				return nil
			})
			defer patch.Reset()
			err := rpc.PreStop()
			So(err, ShouldEqual, nil)
		})
	})
}

func TestConvertErrorFromPreStopResponse(t *testing.T) {
	Convey("test convert error from initializer response", t, func() {
		Convey("test convert error from initializer response error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			err := r.convertErrorFromPreStopResponse(fmt.Errorf("test convert error"), nil)
			So(err, ShouldNotEqual, nil)
		})
		Convey("test convert error from initializer response rsp error", func() {
			r := &AbstractRPC{protocol: &ProtoProtocol{}}
			rsp := &pb.PreStopResponse{
				Error: &pb.Error{Code: snerror.InternalError},
			}
			response, _ := proto.Marshal(rsp)
			patch := ApplyMethod(reflect.TypeOf(rsp), "GetError",
				func(_ *pb.PreStopResponse) *pb.Error {
					return &pb.Error{
						Code:    snerror.InternalError,
						Message: snerror.InternalErrorMsg,
					}
				})
			defer patch.Reset()
			err := r.convertErrorFromPreStopResponse(nil, response)
			So(err.Code, ShouldEqual, rsp.GetError().GetCode())
			So(err.Message, ShouldEqual, rsp.GetError().GetMessage())
		})
		Convey("test convert error from json initializer response rsp error", func() {
			r := &AbstractRPC{protocol: &JSONProtocol{}}
			rsp := &PreStopResponse{
				Error: &Error{Code: snerror.InternalError},
			}
			response, _ := json.Marshal(rsp)
			err := r.convertErrorFromPreStopResponse(nil, response)
			So(err.Code, ShouldEqual, rsp.Error.Code)
			So(err.Message, ShouldEqual, rsp.Error.Message)
		})
	})
}

func TestConvertErrorFromRuntimeManagerResponse(t *testing.T) {
	r := &AbstractRPC{}
	t.Run("TimeoutError", func(t *testing.T) {
		r.rpcClient = &Client{preStopTimeout: 10 * time.Second}
		resp := tcpmsg.RuntimeManagerRSP{Err: "read response timeout"}
		err := r.convertErrorFromRuntimeManagerResponse(resp)
		assert.NotNil(t, err)
		assert.Equal(t, snerror.RuntimeInvokeTimeoutCode, int(err.Code))
		assert.Equal(t, fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), "10s"), err.Message)
	})
	t.Run("some error", func(t *testing.T) {
		resp := tcpmsg.RuntimeManagerRSP{Err: "some error"}
		err := r.convertErrorFromRuntimeManagerResponse(resp)
		assert.NotNil(t, err)
		assert.Equal(t, snerror.IllegalArgumentErrorCode, int(err.Code))
		assert.Equal(t, "some error", err.Message)
	})
	t.Run("no error", func(t *testing.T) {
		resp := tcpmsg.RuntimeManagerRSP{}
		err := r.convertErrorFromRuntimeManagerResponse(resp)
		assert.Nil(t, err)
	})
}

func TestProcessPreStopFunction(t *testing.T) {
	Convey("test process preStop function", t, func() {
		rpc := AbstractRPC{protocol: &ProtoProtocol{}, rpcClient: &Client{}}
		rpc.Configuration = &config.Configuration{}
		rpc.Configuration.RuntimeConfig.PreStop.Handler = "test"
		rpc.Configuration.RuntimeConfig.PreStop.Handler = "0"
		Convey("test process preStopFunction fail", func() {
			patch := ApplyFunc((*AbstractRPC).processPreStop, func(a *AbstractRPC) error {
				return fmt.Errorf("process preStop error")
			})
			defer patch.Reset()
			err := rpc.processPreStopFunction()
			So(err, ShouldNotEqual, nil)
		})
		Convey("test process preStopFunction success", func() {
			patch := ApplyFunc((*AbstractRPC).processPreStop, func(a *AbstractRPC) error {
				return nil
			})
			defer patch.Reset()
			err := rpc.processPreStopFunction()
			So(err, ShouldEqual, nil)
		})
	})
}

func TestRuntimeLogRecorder(t *testing.T) {
	Convey("TestGetRuntimeLogRecorder", t, func() {
		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				CodeSha256: "123456789012345678901234567890",
				CodeSha512: "123456789012345678901234567890",
				ResourceMetaData: config.ResourceMetaData{
					Mem: 100,
				},
			},
		}
		Convey("TestGetRuntimeLogRecorder_Success", func() {
			funcLog := &functionlog.FunctionLog{
				TraceID: "TEST",
				Message: new(bytes.Buffer),
			}
			patch := ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return &functionlog.FunctionLogger{}, nil
			})
			defer patch.Reset()
			functionlog.InitRuntimeLoggerRecoder(conf)
			err := runtime.CreateRuntimeLoggerCtx(conf, funcLog, false)
			So(err, ShouldEqual, nil)
			_, err = runtime.GetRuntimeLogRecorder("TEST")
			So(err, ShouldBeNil)
			err = runtime.DestroyRuntimeLoggerCtx(funcLog)
			So(err, ShouldEqual, nil)
		})
		Convey("TestGetRuntimeLogRecorder_Failure", func() {
			patch := ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return &functionlog.FunctionLogger{}, nil
			})
			defer patch.Reset()
			functionlog.InitRuntimeLoggerRecoder(conf)
			funcLog := &functionlog.FunctionLog{
				TraceID: "TEST-1",
				Message: new(bytes.Buffer),
			}
			err := runtime.CreateRuntimeLoggerCtx(conf, funcLog, false)
			_, err = runtime.GetRuntimeLogRecorder("TEST3")
			So(err, ShouldNotBeNil)
			err = runtime.DestroyRuntimeLoggerCtx(funcLog)
			So(err, ShouldEqual, nil)
			funcLog2 := &functionlog.FunctionLog{
				TraceID: "TEST2",
				Message: new(bytes.Buffer),
			}
			err = runtime.DestroyRuntimeLoggerCtx(funcLog2)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestRuntimeStartAndFinishLog(t *testing.T) {
	Convey("TestRuntimeStartAndFinishLog", t, func() {
		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				CodeSha256: "123456789012345678901234567890",
				CodeSha512: "123456789012345678901234567890",
				ResourceMetaData: config.ResourceMetaData{
					Mem: 100,
				},
			},
		}
		Convey("TestRuntimeStartAndFinishLog_Success with load log", func() {
			funcLog := &functionlog.FunctionLog{
				TraceID: "TEST-3",
				Message: new(bytes.Buffer),
				Stage:   "load",
			}
			rpc := AbstractRPC{protocol: &ProtoProtocol{}, rpcClient: &Client{}, conf: conf}
			patch := ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return &functionlog.FunctionLogger{}, nil
			})
			defer patch.Reset()
			functionlog.InitRuntimeLoggerRecoder(conf)
			err := handleStartLog(conf, funcLog, false)
			So(err, ShouldEqual, nil)
			err = rpc.handleFinishLog(conf, funcLog, false)
			So(err, ShouldEqual, nil)
		})
		Convey("TestRuntimeStartAndFinishLog_Success with invoke log", func() {
			rpc := AbstractRPC{protocol: &ProtoProtocol{}, rpcClient: &Client{}, conf: conf}
			funcLog := &functionlog.FunctionLog{
				TraceID: "TEST-5",
				Message: new(bytes.Buffer),
				Stage:   "invoke",
			}
			patches := [...]*Patches{
				ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
				ApplyFunc(runtime.WriteLoadInitLog, func(traceID string, livedataID string) *runtime.Error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			functionlog.InitRuntimeLoggerRecoder(conf)
			err := handleStartLog(conf, funcLog, false)
			So(err, ShouldEqual, nil)
			err = rpc.handleFinishLog(conf, funcLog, false)
			So(err, ShouldEqual, nil)
		})
		Convey("TestRuntimeStartAndFinishLog_Failure with load log", func() {
			funcLog := &functionlog.FunctionLog{
				TraceID: "TEST-6",
				Message: new(bytes.Buffer),
				Stage:   "load",
			}
			rpc := AbstractRPC{protocol: &ProtoProtocol{}, rpcClient: &Client{}, conf: conf}
			patch := ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return &functionlog.FunctionLogger{}, nil
			})
			defer patch.Reset()
			functionlog.InitRuntimeLoggerRecoder(conf)
			funcLog2 := &functionlog.FunctionLog{
				TraceID: "TEST-7",
				Message: new(bytes.Buffer),
				Stage:   "load",
			}
			err := handleStartLog(conf, funcLog, false)
			So(err, ShouldEqual, nil)
			err = rpc.handleFinishLog(conf, funcLog2, false)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestHandleDuringLog(t *testing.T) {
	Convey("TestHandleDuringLog", t, func() {
		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				CodeSha256: "123456789012345678901234567890",
				CodeSha512: "123456789012345678901234567890",
				ResourceMetaData: config.ResourceMetaData{
					Mem: 100,
				},
			},
		}
		r := &AbstractRPC{
			AbstractRuntime: runtime.AbstractRuntime{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
			},
			protocol: &ProtoProtocol{},
			conf:     conf,
		}
		funcLog := &functionlog.FunctionLog{
			TraceID: "TEST-8",
			Message: new(bytes.Buffer),
			Stage:   "load",
		}
		patch := ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
			return &functionlog.FunctionLogger{}, nil
		})
		defer patch.Reset()
		functionlog.InitRuntimeLoggerRecoder(conf)
		err := handleStartLog(conf, funcLog, false)
		So(err, ShouldEqual, nil)
		r.handleDuringLog(funcLog, "TEST-7")
		r.handleDuringLog(funcLog, "TEST-8")
		err = r.handleFinishLog(conf, funcLog, false)
		So(err, ShouldEqual, nil)
	})
}

func TestProcessRuntimeLog(t *testing.T) {
	Convey("ProcessRuntimeLog", t, func() {
		Convey("ProcessRuntimeLog successful", func() {
			conf := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890",
					CodeSha512: "123456789012345678901234567890",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			}
			r := &AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				protocol: &ProtoProtocol{},
				conf:     conf,
			}
			patch := ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
				return &functionlog.FunctionLogger{}, nil
			})
			defer patch.Reset()
			functionlog.InitRuntimeLoggerRecoder(conf)

			startlog := &pb.FunctionLog{
				Timestamp: time.Now().String(),
				Stage:     "load",
				TraceID:   "Load",
				IsStart:   true,
			}
			startLogByte, _ := proto.Marshal(startlog)
			err := r.ProcessRuntimeLog(startLogByte)
			So(err, ShouldEqual, nil)
			finishlog := &pb.FunctionLog{
				Timestamp: time.Now().String(),
				Stage:     "load",
				TraceID:   "Load",
				IsFinish:  true,
			}
			finishLogByte, _ := proto.Marshal(finishlog)
			err = r.ProcessRuntimeLog(finishLogByte)
			So(err, ShouldEqual, nil)
			err = r.ProcessRuntimeLog([]byte{})
			So(err, ShouldEqual, nil)
		})
		Convey("failed to ParseRuntimeLog", func() {
			r := &AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				protocol: &ProtoProtocol{},
			}
			patch := ApplyFunc(proto.Unmarshal, func([]byte, proto.Message) error {
				return errors.New("test")
			})
			defer patch.Reset()
			err := r.ProcessRuntimeLog([]byte{})
			So(err, ShouldNotEqual, nil)
		})
		Convey("failed to handleStartLog", func() {
			conf := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890",
					CodeSha512: "123456789012345678901234567890",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			}
			r := &AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				protocol: &ProtoProtocol{},
				conf:     conf,
			}
			patch := ApplyFunc(handleStartLog, func(*config.Configuration, *functionlog.FunctionLog, bool) error {
				return errors.New("test")
			})
			defer patch.Reset()
			testlog := &pb.FunctionLog{
				Timestamp: time.Now().String(),
				Stage:     "invoke",
				TraceID:   "invoke-1",
				IsStart:   true,
			}
			testLogByte, _ := proto.Marshal(testlog)
			err := r.ProcessRuntimeLog(testLogByte)
			So(err, ShouldNotEqual, nil)
		})
		Convey("failed to handleFinishLog", func() {
			conf := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890",
					CodeSha512: "123456789012345678901234567890",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			}
			r := &AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				},
				protocol: &ProtoProtocol{},
				conf:     conf,
			}
			patch := ApplyFunc(runtime.GetRuntimeLogRecorder, func(traceID string) (runtime.TraceIDFunctionLogger, error) {
				return runtime.TraceIDFunctionLogger{}, errors.New("test")
			})
			defer patch.Reset()

			testlog := &pb.FunctionLog{
				Timestamp: time.Now().String(),
				Stage:     "invoke",
				TraceID:   "invoke-2",
				IsFinish:  true,
			}
			testLogByte, _ := proto.Marshal(testlog)
			err := r.ProcessRuntimeLog(testLogByte)
			So(err, ShouldNotEqual, nil)
		})
	})
}

func TestAbstractRPC_RequestUpdateAgency(t *testing.T) {
	type fields struct {
		rpcClient *Client
		protocol  DataProtocol
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{name: "test 001", fields: fields{&Client{}, &ProtoProtocol{}}, wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &AbstractRPC{
				rpcClient: tt.fields.rpcClient,
				protocol:  tt.fields.protocol,
			}
			patch := ApplyFunc((*Client).ProcessUpdateAgency, func(c *Client, request []byte) ([]byte, error) {
				return nil, nil
			})
			defer patch.Reset()
			err := r.RequestUpdateAgency()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRuntime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestAbstractRPC_NotifyRouterReady(t *testing.T) {
	tests := []struct {
		name        string
		patchFunc   mockutils.PatchesFunc
		expectedErr bool
	}{
		{
			name: "failed to notify",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&connect.TinyComm{}), "Request", func(_ *connect.TinyComm, _ time.Duration, _, _ []byte) ([]byte, []byte, error) {
						return nil, nil, errors.New("failed to send")
					}),
				})
				return patches
			},
			expectedErr: true,
		},
		{
			name: "succeed to notify",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&connect.TinyComm{}), "Request", func(_ *connect.TinyComm, _ time.Duration, _, _ []byte) ([]byte, []byte, error) {
						return nil, nil, nil
					}),
				})
				return patches
			},
		},
	}

	client := &AbstractRPC{
		rpcClient: &Client{},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			err := client.NotifyRouterReady()
			patches.ResetAll()
			if tt.expectedErr {
				assert.Error(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
			}
		})
	}
}

func TestRefreshSpecialAgency(t *testing.T) {
	t.Run("refresh", func(t *testing.T) {
		extra := map[string]string{constants.HeaderAccessKey: "123"}
		refreshAgencyInExtra(extra)
		value, ok := extra[constants.HeaderAccessKey]
		assert.True(t, ok)
		assert.Equal(t, "123", value)
		_, ok = extra[constants.HeaderSecretKey]
		assert.False(t, ok)
	})
	t.Run("special agency key not exists in extra", func(t *testing.T) {
		extra := map[string]string{}
		refreshSpecialAgency(extra, constants.HeaderAccessKey)
		_, ok := extra[constants.HeaderAccessKey]
		assert.False(t, ok)
	})
	t.Run("no refresh when agency key not in userAgency", func(t *testing.T) {
		extra := map[string]string{constants.HeaderAccessKey: "123"}
		patch := ApplyFunc(utils.GetUserAgency, func(key string) string {
			return ""
		})
		defer patch.Reset()
		refreshSpecialAgency(extra, constants.HeaderAccessKey)
		value, ok := extra[constants.HeaderAccessKey]
		assert.True(t, ok)
		assert.Equal(t, "123", value)
	})
	t.Run("no refresh when failed to decrypt agency value", func(t *testing.T) {
		extra := map[string]string{constants.HeaderAccessKey: "123"}
		patch := ApplyFunc(utils.GetUserAgency, func(key string) string {
			return "456"
		})
		patch.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			return nil, errors.New("failed")
		})
		defer patch.Reset()
		refreshSpecialAgency(extra, constants.HeaderAccessKey)
		value, ok := extra[constants.HeaderAccessKey]
		assert.True(t, ok)
		assert.Equal(t, "123", value)
	})
	t.Run("refresh success", func(t *testing.T) {
		extra := map[string]string{constants.HeaderAccessKey: "123"}
		patch := ApplyFunc(utils.GetUserAgency, func(key string) string {
			return "456"
		})
		patch.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			return []byte("456"), nil
		})
		defer patch.Reset()
		refreshSpecialAgency(extra, constants.HeaderAccessKey)
		value, ok := extra[constants.HeaderAccessKey]
		assert.True(t, ok)
		assert.Equal(t, "456", value)
	})
}
