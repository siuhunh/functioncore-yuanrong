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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/proto"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	cc "functioncore/pkg/worker/runtime/rpc/connect"
	"functioncore/pkg/worker/runtime/rpc/pb"
	"functioncore/pkg/worker/utils"
)

func TestSetInitializerTimeout(t *testing.T) {
	convey.Convey("SetInitializerTimeout", t, func() {
		convey.Convey("test true", func() {
			client := &Client{
				conn:               nil,
				receiver:           nil,
				invokeTimeout:      0,
				initializerTimeout: 0,
			}
			client.SetInitializerTimeout(10)
			convey.So(client.initializerTimeout, convey.ShouldEqual, 10)
		})
	})
}

func TestNewClient(t *testing.T) {
	convey.Convey("TestNewClient", t, func() {
		convey.Convey("Test NewClient success", func() {
			patches := [...]*Patches{
				ApplyFunc(cc.Build, func(receiver cc.AbstractReceiver, opts cc.BuildOptions) (*cc.TinyComm, error) {
					tinyComm := &cc.TinyComm{}
					return tinyComm, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			client, err := NewClient(GetRuntimeAddress(8080), 60)
			if err != nil {
				fmt.Println(err)
				return
			}
			convey.So(client.invokeTimeout, convey.ShouldEqual, 60*time.Second)
		})
	})
}

func TestProcessInitializer(t *testing.T) {
	convey.Convey("TestProcessInitializer", t, func() {
		convey.Convey("test false", func() {
			patches := [...]*Patches{
				ApplyFunc((*cc.TinyComm).Request, func(t *cc.TinyComm,
					timeout time.Duration, metaData []byte, payload []byte) ([]byte, []byte, error) {
					initializerResponse := pb.InitializerResponse{}
					return nil, []byte(initializerResponse.String()), nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			client := &Client{
				conn:               &cc.TinyComm{},
				receiver:           nil,
				invokeTimeout:      60,
				initializerTimeout: 1 * time.Millisecond,
			}
			extra := make(map[string]string, 3)
			extra[constants.HeaderAccessKey] = "ak"
			extra[constants.HeaderSecretKey] = "sk"
			extra[constants.HeaderAuthToken] = "token"

			req := &pb.InitializerRequest{
				Extra: extra,
			}
			request, _ := proto.Marshal(req)

			response, err := client.ProcessInitializer(request)
			if err != nil {
				fmt.Println(err)
				return
			}
			rsp := &pb.InitializerResponse{}
			proto.Unmarshal(response, rsp)
			convey.So(rsp.Error, convey.ShouldEqual, nil)
		})
	})
}

type AbstractFake struct {
}

func (a AbstractFake) ProcessCall(request *pb.CallRequest) (*pb.CallResponse, error) {
	return nil, nil
}

func (a AbstractFake) ProcessLog(log []byte) error {
	return errors.New("failed to unmarshal")
}

func (a AbstractFake) ProcessRuntimeLog(log []byte) error {
	return errors.New("failed to unmarshal")
}

func (a AbstractFake) ProcessMetrics(log *pb.Metrics) {
	return
}

func (a AbstractFake) ProcessCancelRequest(request *pb.CancelRequest) (*pb.CancelResponse, error) {
	return nil, nil
}

func (a AbstractFake) RecvInvoke(ic invokectx.InvokeCtx, rsp []byte, err error) {}

func TestHandlePost(t *testing.T) {
	convey.Convey("HandlePost", t, func() {
		convey.Convey("default", func() {
			abstract := AbstractFake{}
			r := &Receiver{processor: abstract}
			rawMeta := []byte{}
			message := []byte{}
			err := r.HandlePost(rawMeta, message)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("FunctionLogType", func() {
			abstract := AbstractFake{}
			r := &Receiver{processor: abstract}
			rawMeta := []byte{FunctionLogType}
			message := []byte("test")
			err := r.HandlePost(rawMeta, message)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("MetricsType", func() {
			abstract := AbstractFake{}
			r := &Receiver{processor: abstract}
			rawMeta := []byte{MetricsType}
			message := []byte("test")
			err := r.HandlePost(rawMeta, message)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestLoadFunction(t *testing.T) {
	convey.Convey("LoadFunction", t, func() {
		timeout := time.Duration(1) * time.Second
		convey.Convey("failed case 1", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := &pb.LoadRequest{}
			request, _ := proto.Marshal(req)
			c := &Client{conn: tinyComm}
			_, err := c.LoadFunction(request, timeout)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 2", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, snerror.New(4010, "test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := &pb.LoadRequest{}
			request, _ := proto.Marshal(req)
			c := &Client{conn: tinyComm}
			_, err := c.LoadFunction(request, timeout)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 3", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						err := &pb.Error{
							Code:    100,
							Message: "test",
						}
						loadResponse := pb.LoadResponse{
							Error: err,
						}
						resp, _ := proto.Marshal(&loadResponse)
						return nil, resp, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := &pb.LoadRequest{FuncName: "test"}
			request, _ := proto.Marshal(req)
			c := &Client{conn: tinyComm}
			_, err := c.LoadFunction(request, timeout)
			response, err := c.LoadFunction(request, timeout)
			resp := &pb.LoadResponse{}
			proto.Unmarshal(response, resp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(resp.Error.Code, convey.ShouldEqual, 100)
			convey.So(resp.Error.Message, convey.ShouldEqual, "test")
		})
	})
}

func TestProcessCancel(t *testing.T) {
	convey.Convey("ProcessCancel", t, func() {
		convey.Convey("failed case 1", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := &pb.CancelRequest{}
			c := &Client{conn: tinyComm}
			_, err := c.ProcessCancel(req)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 2", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, snerror.New(4010, "test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := &pb.CancelRequest{}
			c := &Client{conn: tinyComm}
			_, err := c.ProcessCancel(req)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("successful case", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						cancelResponse := pb.CancelResponse{
							CallID: "test",
							Error: &pb.Error{
								Code:    100,
								Message: "test",
							},
						}
						resp, _ := proto.Marshal(&cancelResponse)
						return nil, resp, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			req := &pb.CancelRequest{CallID: "test"}
			c := &Client{conn: tinyComm}
			resp, err := c.ProcessCancel(req)
			convey.So(err, convey.ShouldBeNil)
			convey.So(resp.Error.Code, convey.ShouldEqual, 100)
			convey.So(resp.CallID, convey.ShouldEqual, "test")
		})
	})
}

func TestCheckHeath(t *testing.T) {
	convey.Convey("ProcessCancel", t, func() {
		convey.Convey("failed case 1", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.CheckHeath(time.Duration(3) * time.Second)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 2", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, snerror.New(4010, "test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.CheckHeath(time.Duration(3) * time.Second)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("successful case", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						healthResponse := pb.HealthResponse{
							Status: "test",
						}
						resp, _ := proto.Marshal(&healthResponse)
						return nil, resp, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			resp, err := c.CheckHeath(time.Duration(3) * time.Second)
			convey.So(err, convey.ShouldBeNil)
			convey.So(resp.Status, convey.ShouldEqual, "test")
		})
	})
}

func TestCheckStartupHeath(t *testing.T) {
	convey.Convey("ProcessCancel", t, func() {
		convey.Convey("failed case 1", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.CheckStartupHeath(time.Duration(3) * time.Second)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 2", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, snerror.New(4010, "test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.CheckStartupHeath(time.Duration(3) * time.Second)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("successful case", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						healthResponse := pb.HealthResponse{
							Status: "test",
						}
						resp, _ := proto.Marshal(&healthResponse)
						return nil, resp, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			resp, err := c.CheckStartupHeath(time.Duration(3) * time.Second)
			convey.So(err, convey.ShouldBeNil)
			convey.So(resp.Status, convey.ShouldEqual, "test")
		})
	})
}

func TestReportCustomMetrics(t *testing.T) {
	convey.Convey("ProcessCancel", t, func() {
		convey.Convey("failed case 1", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.ReportCustomMetrics(10)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 2", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, nil, snerror.New(4010, "test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.ReportCustomMetrics(10)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed case 3", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						return nil, []byte("test"), nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.ReportCustomMetrics(10)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("successful case", func() {
			tinyComm := &cc.TinyComm{}
			patches := [...]*Patches{
				ApplyMethod(reflect.TypeOf(tinyComm), "Request",
					func(_ *cc.TinyComm, timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
						customReportMetricsResponse := pb.CustomMetricsReportResponse{
							Result: []byte("test"),
							Error:  nil,
						}
						resp, _ := proto.Marshal(&customReportMetricsResponse)
						return nil, resp, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			c := &Client{conn: tinyComm}
			_, err := c.ReportCustomMetrics(10)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestReceiver_HandleRequest(t *testing.T) {
	convey.Convey("TestReceiver_HandleRequest", t, func() {
		convey.Convey("handle call request type message success", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).ProcessCall, func(a *AbstractRPC, callRequest *pb.CallRequest) (*pb.CallResponse, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			r := Receiver{
				processor: rpcRuntime{},
			}
			rawMeta := make([]byte, 1)
			rawMeta[0] = CallRequestType
			var message []byte
			_, _, err := r.HandleRequest(rawMeta, message)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("handle cancel request type message success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Receiver).handleCancelRequest, func(r *Receiver, message []byte) ([]byte, []byte, error) {
					return nil, nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			r := Receiver{
				processor: rpcRuntime{},
			}
			rawMeta := make([]byte, 1)
			rawMeta[0] = CancelRequestType
			var message []byte
			_, _, err := r.HandleRequest(rawMeta, message)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("failed to process call request", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).ProcessCall, func(a *AbstractRPC, callRequest *pb.CallRequest) (*pb.CallResponse, error) {
					return nil, errors.New("test error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			r := Receiver{
				processor: rpcRuntime{},
			}
			rawMeta := make([]byte, 1)
			rawMeta[0] = CallRequestType
			var message []byte
			_, _, err := r.HandleRequest(rawMeta, message)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("fail to handle cancel request type message", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Receiver).handleCancelRequest, func(r *Receiver, message []byte) ([]byte, []byte, error) {
					return nil, nil, errors.New(exceptedErrMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			r := Receiver{
				processor: rpcRuntime{},
			}
			rawMeta := make([]byte, 1)
			rawMeta[0] = CancelRequestType
			var message []byte
			_, _, err := r.HandleRequest(rawMeta, message)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, exceptedErrMsg)
		})
		convey.Convey("unknown message type", func() {
			exceptedErrMsg := "unknown message type"
			r := Receiver{}
			var rawMeta []byte
			var message []byte
			_, _, err := r.HandleRequest(rawMeta, message)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, exceptedErrMsg)
		})
	})
}

func TestClient_processInitializer(t *testing.T) {
	convey.Convey("TestClient_ProcessInitializer", t, func() {
		convey.Convey("process initializer success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessInitializer, func(c *Client, request []byte) ([]byte, error) {
					return nil, nil
				}),
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			cfg := &config.Configuration{}
			err := rpc.processInitializer(cfg)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("process initializer normal error", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessInitializer, func(c *Client, request []byte) ([]byte, error) {
					return nil, errors.New(exceptedErrMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			cfg := &config.Configuration{}
			err := rpc.processInitializer(cfg)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, exceptedErrMsg)
		})
		convey.Convey("process initializer sn error", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessInitializer, func(c *Client, request []byte) ([]byte, error) {
					return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode, exceptedErrMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			cfg := &config.Configuration{}
			err := rpc.processInitializer(cfg)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestClient_processRestoreHook(t *testing.T) {
	convey.Convey("TestClient_ProcessRestoreHook", t, func() {
		convey.Convey("process restore hook success", func() {
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessRestoreHook, func(c *Client, request []byte) ([]byte, error) {
					return nil, nil
				}),
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			cfg := &config.Configuration{}
			err := rpc.processRestoreHook(cfg)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("process restore hook normal error", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessRestoreHook, func(c *Client, request []byte) ([]byte, error) {
					return nil, errors.New(exceptedErrMsg)
				}),
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			cfg := &config.Configuration{}
			err := rpc.processRestoreHook(cfg)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, exceptedErrMsg)
		})
		convey.Convey("process restore hook sn error", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessRestoreHook, func(c *Client, request []byte) ([]byte, error) {
					return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode, exceptedErrMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			cfg := &config.Configuration{}
			err := rpc.processRestoreHook(cfg)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestReceiver_handleCancelRequest(t *testing.T) {
	convey.Convey("TestReceiver_handleCancelRequest", t, func() {
		convey.Convey("handle cancel request success", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).ProcessCancelRequest, func(rpc *AbstractRPC, request *pb.CancelRequest) (*pb.CancelResponse, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			r := Receiver{
				processor: rpcRuntime{},
			}
			var msg []byte
			_, _, err := r.handleCancelRequest(msg)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("handle cancel request fail", func() {
			patches := [...]*Patches{
				ApplyFunc((*AbstractRPC).ProcessCancelRequest, func(rpc *AbstractRPC, request *pb.CancelRequest) (*pb.CancelResponse, error) {
					return nil, errors.New("test error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			r := Receiver{
				processor: rpcRuntime{},
			}
			var msg []byte
			_, _, err := r.handleCancelRequest(msg)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, "test error")
		})
	})
}

func TestConnectType(t *testing.T) {
	type args struct {
		language   string
		socketType string
	}
	tests := []struct {
		name string
		args args
		want *BuildOption
	}{
		{
			name: "test001",
			args: args{
				language:   "java8",
				socketType: worker.DefaultSocketType,
			},
			want: &BuildOption{
				ConnectType: worker.DefaultSocketType,
			},
		},
		{
			name: "test002",
			args: args{
				language:   "java8",
				socketType: worker.UnixDomainSocketType,
			},
			want: &BuildOption{
				Address:     "/home/snuser/socket/java8.sock",
				ConnectType: worker.UnixDomainSocketType,
			},
		},
		{
			name: "test003",
			args: args{
				language:   "java11",
				socketType: worker.UnixDomainSocketType,
			},
			want: &BuildOption{
				Address:     "/home/snuser/socket/java11.sock",
				ConnectType: worker.UnixDomainSocketType,
			},
		},
		{
			name: "test004",
			args: args{
				language:   "javax",
				socketType: worker.UnixDomainSocketType,
			},
			want: &BuildOption{
				Address:     "/home/snuser/socket/javax.sock",
				ConnectType: worker.UnixDomainSocketType,
			},
		},
		{
			name: "test005",
			args: args{
				language:   "java17",
				socketType: worker.UnixDomainSocketType,
			},
			want: &BuildOption{
				Address:     "/home/snuser/socket/java17.sock",
				ConnectType: worker.UnixDomainSocketType,
			},
		},
		{
			name: "test006",
			args: args{
				language:   "java21",
				socketType: worker.UnixDomainSocketType,
			},
			want: &BuildOption{
				Address:     "/home/snuser/socket/java21.sock",
				ConnectType: worker.UnixDomainSocketType,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFunc := ConnectType(tt.args.language, tt.args.socketType)
			s := &BuildOption{}
			gotFunc(s)
			if s.ConnectType != tt.want.ConnectType || s.Address != tt.want.Address {
				t.Errorf("ConnectType() = %+v, want %+v", s, tt.want)
			}
		})
	}
}

func TestCheckJavaxLanguage(t *testing.T) {
	type args struct {
		language string
	}
	tests := []struct {
		name string
		args args
		want *BuildOption
	}{
		{
			name: "test002",
			args: args{
				language: "java8",
			},
			want: &BuildOption{
				IsDirectFunc: false,
			},
		},
		{
			name: "test001",
			args: args{
				language: "javax",
			},
			want: &BuildOption{
				IsDirectFunc: true,
			},
		},
		{
			name: "test003",
			args: args{
				language: "cpp",
			},
			want: &BuildOption{
				IsDirectFunc: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFunc := IsDirectRuntime(tt.args.language)
			s := &BuildOption{}
			gotFunc(s)
			if s.IsDirectFunc != tt.want.IsDirectFunc {
				t.Errorf("ConnectType() = %+v, want %+v", s, tt.want)
			}
		})
	}
}

func TestClient_processPreStop(t *testing.T) {
	convey.Convey("TestClient_ProcessInitializer", t, func() {
		convey.Convey("process preStop success", func() {
			utils.SetUserAgency(constants.HeaderAccessKey, "ak")
			utils.SetUserAgency(constants.HeaderSecretKey, "sk")
			utils.SetUserAgency(constants.HeaderAuthToken, "token")
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessPreStop, func(c *Client, request []byte) ([]byte, error) {
					return nil, nil
				}),
				ApplyFunc(slice.ContainsString,
					func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			err := rpc.processPreStop()
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("process preStop normal error", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessPreStop, func(c *Client, request []byte) ([]byte, error) {
					return nil, errors.New(exceptedErrMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			err := rpc.processPreStop()
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, exceptedErrMsg)
		})
		convey.Convey("process preStop sn error", func() {
			exceptedErrMsg := "test error"
			patches := [...]*Patches{
				ApplyFunc((*Client).ProcessPreStop, func(c *Client, request []byte) ([]byte, error) {
					return nil, snerror.New(snerror.RuntimeInvokeTimeoutCode, exceptedErrMsg)
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			err := rpc.processPreStop()
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("process command preStop", func() {
			patches := [...]*Patches{
				ApplyFunc((*tcpmsg.SocketConn).SendMessageWithTimeout,
					func(*tcpmsg.SocketConn, *tcpmsg.Message, time.Duration) tcpmsg.RuntimeManagerRSP {
						return tcpmsg.RuntimeManagerRSP{Response: tcpmsg.RuntimeRSP{Result: "test"}}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			rpc := AbstractRPC{
				AbstractRuntime: runtime.AbstractRuntime{
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
					Configuration: &config.Configuration{},
				},
				rpcClient: &Client{},
				protocol:  &ProtoProtocol{},
			}
			runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}
			rpc.Configuration.PreStop = types.PreStop{
				Type: "command",
			}
			err := rpc.processPreStop()
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}
