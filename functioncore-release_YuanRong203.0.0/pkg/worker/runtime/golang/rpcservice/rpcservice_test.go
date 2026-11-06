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

package rpcservice

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/rpc"
	"reflect"
	"sync"
	"testing"
	"time"

	"functioncore/pkg/worker/runtime"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/snerror"
)

var (
	once sync.Once
)

type Function struct {
}

func NewFunction() *Function {
	return &Function{}
}

func (fn *Function) Invoke(req *InvokeRequest, resp *InvokeResponse) error {
	return nil
}

func (fn *Function) HealthCheck(req *HealthCheckRequest, resp *HealthCheckResponse) error {
	*resp = HealthCheckResponse{}
	return nil
}

type InvokeError struct {
	ErrorCode int    `json:"error_code"`
	ErrorMsg  string `json:"error_msg"`
}

func (e *InvokeError) Error() string {
	return e.ErrorMsg
}

func startServer(fn *Function) (*httptest.Server, error) {
	if err := rpc.Register(fn); err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	server := httptest.NewServer(nil)
	httpServerAddr := server.Listener.Addr().String()
	log.Println("Test HTTP RPC server listening on", httpServerAddr)
	return server, nil
}

func TestRPCService_HealthCheck(t *testing.T) {
	convey.Convey("Test HealthCheck", t, func() {
		convey.Convey("Test 1", func() {
			once.Do(func() {
				f := NewFunction()
				s, err := startServer(f)
				if err != nil {
					t.Fatal(err)
				}
				for {
					rpcService = GetRPCService(s.Listener.Addr().String())
					if rpcService != nil {
						break
					}
					rpcService.client = &rpc.Client{}
				}
			})
			fmt.Println()
			patches := []*Patches{
				ApplyFunc(rpcService.client.Call, func(serviceMethod string, args any, reply any) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := rpcService.HealthCheck()
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestRPCService_Invoke(t *testing.T) {

	once.Do(func() {
		f := NewFunction()
		s, err := startServer(f)
		if err != nil {
			t.Fatal(err)
		}
		for {
			rpcService = GetRPCService(s.Listener.Addr().String())
			if rpcService != nil {
				break
			}
		}
	})

	testCases := []struct {
		invokeError *InvokeError
		name        string
		sleep       time.Duration
		wantErr     snerror.SNError
	}{
		{
			name: "succeeded to invoke",
		},
		{
			name: "invoke return err",
			invokeError: &InvokeError{
				ErrorCode: http.StatusInternalServerError,
				ErrorMsg:  "return err",
			},
			wantErr: snerror.New(snerror.FunctionExceptionCode, "return err"),
		},
		{
			name:  "invoke timeout",
			sleep: 20 * time.Millisecond,
			wantErr: snerror.New(snerror.RuntimeInvokeTimeoutCode, fmt.Sprintf(snerror.ErrText(snerror.
				RuntimeInvokeTimeoutCode), 10*time.Millisecond)),
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			patch := ApplyMethod(reflect.TypeOf(&Function{}), "Invoke",
				func(_ *Function, req *InvokeRequest, resp *InvokeResponse) error {
					if tt.sleep > 0 {
						time.Sleep(tt.sleep)
					}
					if tt.invokeError != nil {
						return tt.invokeError
					}
					return nil
				})
			defer patch.Reset()

			req := &InvokeRequest{}
			resp := &InvokeResponse{}
			call := rpcService.Invoke(req)
			actualErr := WaitResp(10*time.Millisecond, resp, call)
			if tt.wantErr == nil {
				assert.Nil(t, actualErr)
			} else {
				assert.EqualError(t, actualErr, tt.wantErr.Error())
			}
		})
	}
}

func Test_WaitResp(t *testing.T) {
	convey.Convey("Test_WaitResp time out", t, func() {
		resp := WaitResp(time.Millisecond, &InvokeResponse{}, &rpc.Call{})
		convey.So(resp.Code(), convey.ShouldEqual, snerror.RuntimeInvokeTimeoutCode)
	})
	convey.Convey("Test_WaitResp runtime exit", t, func() {
		close(runtime.ProcessExitSignal)
		resp := WaitResp(time.Second, &InvokeResponse{}, &rpc.Call{})
		convey.So(resp.Code(), convey.ShouldEqual, snerror.RuntimeProcessExit)
		runtime.ProcessExitSignal = make(chan struct{})
	})
	convey.Convey("Test_WaitResp call.Done closed", t, func() {
		doneChan := make(chan *rpc.Call)
		close(doneChan)
		resp := WaitResp(time.Second, &InvokeResponse{}, &rpc.Call{
			Done: doneChan,
		})
		convey.So(resp.Code(), convey.ShouldEqual, snerror.FuncInsExceptionCode)
	})
	convey.Convey("Test_WaitResp result is nil", t, func() {
		doneChan := make(chan *rpc.Call, 1)
		doneChan <- nil
		resp := WaitResp(time.Second, &InvokeResponse{}, &rpc.Call{
			Done: doneChan,
		})
		convey.So(resp.Code(), convey.ShouldEqual, snerror.FuncInsExceptionCode)
	})
	convey.Convey("Test_WaitResp result is error", t, func() {
		doneChan := make(chan *rpc.Call, 1)
		doneChan <- &rpc.Call{Error: errors.New("error happened")}
		resp := WaitResp(time.Second, &InvokeResponse{}, &rpc.Call{
			Done: doneChan,
		})
		convey.So(resp.Code(), convey.ShouldEqual, snerror.FuncInsExceptionCode)
	})
	convey.Convey("Test_WaitResp result Reply assert failed", t, func() {
		doneChan := make(chan *rpc.Call, 1)
		doneChan <- &rpc.Call{}
		resp := WaitResp(time.Second, &InvokeResponse{}, &rpc.Call{
			Done: doneChan,
		})
		convey.So(resp.Code(), convey.ShouldEqual, snerror.FuncInsExceptionCode)
	})
}

func Test_RenewRPCService(t *testing.T) {
	convey.Convey("Test RenewRPCService", t, func() {
		convey.Convey("Test 1", func() {
			rpcRes := RenewRPCService("127.0.0.1:8080")
			fmt.Println(rpcRes)
			convey.So(rpcRes, convey.ShouldBeNil)
		})
	})
}

func Test_isInvokeError(t *testing.T) {
	convey.Convey("Test isInvokeError", t, func() {
		convey.Convey("Test 1", func() {
			err := errors.New("errorMessage")
			res := isInvokeError(err)
			convey.So(res, convey.ShouldEqual, false)
		})
		convey.Convey("Test 2", func() {
			err := errors.New(`{"Message":"test err"}`)
			res := isInvokeError(err)
			convey.So(res, convey.ShouldEqual, false)
		})
		convey.Convey("Test 3", func() {
			err := errors.New(`{"errorMessage":"test err"}`)
			res := isInvokeError(err)
			convey.So(res, convey.ShouldEqual, true)
		})
	})
}

func Test_newRPCService(t *testing.T) {
	convey.Convey("Test RenewRPCService", t, func() {
		convey.Convey("Test 1", func() {
			testClient := &rpc.Client{}
			patches := []*Patches{
				ApplyFunc(rpc.DialHTTPPath, func(network, address, path string) (*rpc.Client, error) {
					return testClient, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			rpcRes := newRPCService("127.0.0.1")
			convey.So(rpcRes.client, convey.ShouldEqual, testClient)
		})
		convey.Convey("Test 2", func() {
			patches := []*Patches{
				ApplyFunc(rpc.DialHTTPPath, func(network, address, path string) (*rpc.Client, error) {
					return nil, errors.New("DialHTTPPath err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			rpcRes := newRPCService("127.0.0.1")
			convey.So(rpcRes, convey.ShouldBeNil)
		})
	})
}
