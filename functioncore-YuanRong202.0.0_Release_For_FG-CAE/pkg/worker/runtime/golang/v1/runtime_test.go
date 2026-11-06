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

package v1

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/golang/rpcservice"
)

var once sync.Once
var formatLogger log.FormatLogger

func consoleLogger() *gomonkey.Patches {
	patches := gomonkey.ApplyFunc(log.GetLogger,
		func() log.FormatLogger {
			once.Do(func() {
				formatLogger = log.NewConsoleLogger()
			})
			return formatLogger
		})
	return patches
}

func TestRegisterRuntime(t *testing.T) {
	RegisterRuntime()
	c := runtime.Registry.GetCreator(worker.GOV1RuntimeLanguage)
	assert.NotNil(t, c)
}

func getGoRuntimeConfig() *config.Configuration {
	runtimeManagerConfig := rmconfig.RuntimeConfig{
		Port:       8001,
		Enable:     false,
		CustomArgs: nil,
		Addition:   make(map[string]string, 2),
	}
	runtimeManagerConfig.Addition["initTimeout"] = "30"
	runtimeManagerConfig.Addition["maxResponseBodySize"] = "6"
	workerConfig := &config.Configuration{
		RuntimeConfig: config.RuntimeConfig{
			URN:      "sn:cn:yrk:****:function:0@default@go:latest",
			Timeout:  30,
			RPCPort:  8000,
			Language: "go1.x",
			RMConfig: rmconfig.Config{
				RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
			},
		},
	}
	workerConfig.RMConfig.RuntimeConfigs[worker.GOV1RuntimeLanguage] = runtimeManagerConfig
	return workerConfig
}

func TestCreator_NewRuntime(t *testing.T) {
	c := &creator{}
	rpcService := &rpcservice.RPCService{}
	p := gomonkey.ApplyFunc(rpcservice.RenewRPCService, func(remoteAddress string) *rpcservice.RPCService {
		return rpcService
	})
	p.ApplyFunc(rpcservice.GetRPCService, func(remoteAddress string) *rpcservice.RPCService {
		return rpcService
	})
	p.ApplyMethod(reflect.TypeOf(rpcService), "HealthCheck",
		func(_ *rpcservice.RPCService) error {
			return nil
		})
	defer p.Reset()
	_, err := c.NewRuntime(getGoRuntimeConfig(), runtime.Handlers{})
	assert.NotNil(t, err)
}

func TestGolangV1_ProcessCancelEvent(t *testing.T) {
	g := &golangV1{}
	req := &runtime.CancelRequest{}
	resp := g.ProcessCancelEvent(req)
	assert.NotNil(t, resp)
}

func TestGolangV1_HotLoad(t *testing.T) {
	c := &creator{}
	rpcService := &rpcservice.RPCService{}
	p := gomonkey.ApplyFunc(rpcservice.RenewRPCService, func(remoteAddress string) *rpcservice.RPCService {
		return rpcService
	})
	p.ApplyFunc(rpcservice.GetRPCService, func(remoteAddress string) *rpcservice.RPCService {
		return rpcService
	})
	p.ApplyMethod(reflect.TypeOf(rpcService), "HealthCheck",
		func(_ *rpcservice.RPCService) error {
			return nil
		})
	p.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
		return functionlog.NewFunctionLogger(nil, "", ""), nil
	})
	defer p.Reset()
	configuration := getGoRuntimeConfig()
	configuration.StartArgs.LogDir = "/home/sn/logs"
	g, err := c.NewRuntime(configuration, runtime.Handlers{})
	assert.Nil(t, err)
	// context cancel
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NotNil(t, g.HotLoad(ctx, configuration))
	assert.Nil(t, g.HotLoad(context.Background(), configuration))
}

func newRuntime() (*golangV1, *gomonkey.Patches) {
	p := consoleLogger()
	rpcService := &rpcservice.RPCService{}
	p.ApplyFunc(rpcservice.RenewRPCService, func(remoteAddress string) *rpcservice.RPCService {
		return rpcService
	})
	p.ApplyFunc(rpcservice.GetRPCService, func(remoteAddress string) *rpcservice.RPCService {
		return rpcService
	})
	p.ApplyMethod(reflect.TypeOf(rpcService), "HealthCheck",
		func(_ *rpcservice.RPCService) error {
			return nil
		})
	g := &golangV1{
		maxResponseBodyLength: 6 * megabyte,
		invokeChan:            make(chan *recvMsgCall, 10),
	}
	return g, p
}

func TestGolangV1_ProcessEvent(t *testing.T) {
	rpcService := &rpcservice.RPCService{}
	message := &runtime.InvokeRequest{}
	respTemp := &runtime.InvokeResponse{}
	conf := &config.Configuration{}
	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)
	Convey("TestRunProcess", t, func() {
		g, _ := newRuntime()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go g.recvInvokeMsg(ctx)
		time.Sleep(time.Second * 1)
		defer gomonkey.ApplyMethod(reflect.TypeOf(rpcService), "Invoke",
			func(service *rpcservice.RPCService, req *rpcservice.InvokeRequest) *rpc.Call {
				return nil
			}).Reset()
		defer gomonkey.ApplyFunc((*golangV1).recvInvoke, func(_ *golangV1, ic invokectx.InvokeCtx,
			resp []byte, err *runtime.Error) {
			response := &runtime.InvokeResponse{}
			response.Result, response.Error = resp, err
			respTemp = response
		}).Reset()

		Convey("success", func() {
			defer gomonkey.ApplyFunc(rpcservice.WaitResp, func(timeout time.Duration, resp *rpcservice.InvokeResponse,
				call *rpc.Call) snerror.SNError {
				resp.Payload = []byte(`{"result":"success"}`)
				return nil
			}).Reset()

			g.SendProcessEvent(invokectx.InvokeCtx{}, *message, conf)
			//
			time.Sleep(1 * time.Second)
			t.Log(respTemp)
			So(respTemp.Result, ShouldResemble, []byte(`{"result":"success"}`))
		})
		Convey("invoke failed", func() {
			defer gomonkey.ApplyFunc(rpcservice.WaitResp, func(timeout time.Duration, resp *rpcservice.InvokeResponse,
				call *rpc.Call) snerror.SNError {
				return snerror.New(snerror.FunctionExceptionCode, "failed")
			}).Reset()
			g.SendProcessEvent(invokectx.InvokeCtx{}, *message, conf)
			time.Sleep(1 * time.Second)
			So(respTemp.Error, ShouldResemble, &runtime.Error{Code: snerror.FunctionExceptionCode, Message: "failed"})
		})
		Convey("too max response body", func() {
			defer gomonkey.ApplyFunc(rpcservice.WaitResp, func(timeout time.Duration, resp *rpcservice.InvokeResponse,
				call *rpc.Call) snerror.SNError {
				resp.Payload = []byte(strings.Repeat("1", 6*megabyte+1))
				return nil
			}).Reset()
			g.SendProcessEvent(invokectx.InvokeCtx{}, *message, conf)
			time.Sleep(3 * time.Second)
			errorMessage := fmt.Sprintf(snerror.ErrText(snerror.IllegalArgumentErrorCode),
				6291457, 6291456)
			So(respTemp.Error, ShouldResemble, &runtime.Error{
				Code:    snerror.IllegalArgumentErrorCode,
				Message: errorMessage,
			})
		})
		Convey("invoke timeout", func() {
			g.invokeTimeout = 3 * time.Second
			defer gomonkey.ApplyFunc(rpcservice.WaitResp, func(timeout time.Duration, resp *rpcservice.InvokeResponse,
				call *rpc.Call) snerror.SNError {
				return snerror.New(snerror.RuntimeInvokeTimeoutCode, "")
			}).Reset()
			g.SendProcessEvent(invokectx.InvokeCtx{}, *message, conf)
			time.Sleep(1 * time.Second)
			So(respTemp.Error, ShouldResemble, &runtime.Error{Code: snerror.RuntimeInvokeTimeoutCode, Message: "call invoke timeout 3s"})
		})
	})
}

func TestRunProcess(t *testing.T) {
	conf := &config.Configuration{}
	logReporter := func(message []byte) {}
	Convey("TestRunProcess", t, func() {
		Convey("success", func() {
			p := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, nil
			})
			cmd := &exec.Cmd{}
			p.ApplyMethod(reflect.TypeOf(cmd), "Start",
				func(_ *exec.Cmd) error {
					return nil
				})
			defer p.Reset()
			_, err := RunProcess(1, 1, conf, logReporter)
			So(err, ShouldBeNil)
		})
		Convey("runtimeBootstrapNotFound", func() {
			p := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, os.ErrNotExist
			})
			defer p.Reset()
			_, err := RunProcess(1, 1, conf, logReporter)
			So(err, ShouldResemble,
				snerror.New(snerror.RuntimeBootstrapNotFound, snerror.ErrText(snerror.RuntimeBootstrapNotFound)))
		})
		Convey("failed to start the go1.x server", func() {
			expectErr := errors.New("failed to start the go1.x server")
			p := gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, nil
			})
			cmd := &exec.Cmd{}
			p.ApplyMethod(reflect.TypeOf(cmd), "Start",
				func(_ *exec.Cmd) error {
					return expectErr
				})
			defer p.Reset()
			_, err := RunProcess(1, 1, conf, logReporter)
			So(err, ShouldResemble, snerror.New(snerror.RuntimeProcessExit, err.Error()))
		})
	})
}

func Test_recvInvoke(t *testing.T) {
	Convey("Test_recvInvoke", t, func() {
		count := 0
		f := func(invokectx.InvokeCtx, runtime.InvokeResponse) {
			count++
		}
		g := &golangV1{
			handlers: runtime.Handlers{
				RecvProcessEventHandler: f,
			},
		}
		g.recvInvoke(invokectx.InvokeCtx{}, make([]byte, 0), nil)
		So(count, ShouldEqual, 1)
	})
}

func Test_StartupHealthz(t *testing.T) {
	g := &golangV1{}
	ok, err := g.StartupHealthz("/healthcheck", 5)
	assert.Nil(t, err)
	assert.True(t, ok)
}
