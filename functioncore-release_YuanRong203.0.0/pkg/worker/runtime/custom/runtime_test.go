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

package custom

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	customConfig "functioncore/pkg/worker/runtime/custom/config"
	"functioncore/pkg/worker/runtime/rpc/pb"
)

func TestInitTimerByInvokeTimeout(t *testing.T) {
	convey.Convey("TestInitTimerByInvokeTimeout", t, func() {
		convey.Convey("Test 1", func() {
			c := &Custom{
				initTimer: nil,
			}
			c.InitTimerByInvokeTimeout()
			convey.So(c.initTimer, convey.ShouldNotBeNil)
		})
	})
}

func TestRegisterRuntime(t *testing.T) {
	convey.Convey("test custom RegisterRuntime", t, func() {
		creator := runtime.Registry.GetCreator("custom")
		convey.So(creator, convey.ShouldEqual, nil)
		RegisterRuntime()
		creator = runtime.Registry.GetCreator("custom")
		convey.So(creator, convey.ShouldNotEqual, nil)
	})
}

func TestCustom_NewRuntime(t *testing.T) {
	convey.Convey("test custom NewRuntime", t, func() {
		convey.Convey("succeed to NewRuntime", func() {
			patches := [...]*Patches{
				ApplyFunc(runtime.NewAbstractRuntime,
					func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
						return &runtime.AbstractRuntime{}, nil
					}),
				ApplyFunc(getServerAddress, func(port int32) string {
					return "127.0.0.1:8080"
				}),
				ApplyFunc(getServer, func(address string) (*server, error) {
					return &server{}, nil
				}),
				ApplyFunc((*Custom).RunWrapper, func(c *Custom) (*os.Process, error) {
					return &os.Process{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			custom := creator{}
			process, err := custom.NewRuntime(&config.Configuration{}, runtime.Handlers{})
			assert.NotNil(t, process)
			assert.Nil(t, err)
		})
	})
}

func TestCustom_RunWrapperByID(t *testing.T) {
	convey.Convey("test custom RunWrapperByID", t, func() {
		convey.Convey("failed to run wrapper by id", func() {
			patch := ApplyFunc(filepath.Abs, func(path string) (string, error) {
				return "", nil
			})
			defer patch.Reset()
			custom := Custom{AbstractRuntime: runtime.AbstractRuntime{Configuration: &config.Configuration{}}}
			process, err := custom.RunWrapperByID(1003, 1003)
			assert.Nil(t, process)
			assert.NotNil(t, err)
		})
		convey.Convey("succeed to run wrapper by id", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),

				ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
			}
			_, err := custom.RunWrapperByID(1003, 1003)
			assert.Nil(t, err)
		})
	})
}

func TestCustom_RunWrapper(t *testing.T) {
	convey.Convey("test custom RunWrapper", t, func() {
		convey.Convey("succeed to run Wrapper 1", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),
				ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return nil
				}),
				ApplyFunc(customConfig.GetServerConfig, func() (*customConfig.ServerConfiguration, error) {
					return &customConfig.ServerConfiguration{}, nil
				}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
			}
			_, err := custom.RunWrapper()
			assert.Nil(t, err)
			time.Sleep(1 * time.Second)

		})
		convey.Convey("succeed to run Wrapper 2", func() {
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
			}
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc((*Custom).buildCmd, func(c *Custom, deployFuncPath string) (*exec.Cmd, error) {
					return nil, errors.New("build Cmd err")
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return nil
				}),
				ApplyFunc(customConfig.GetServerConfig, func() (*customConfig.ServerConfiguration, error) {
					return &customConfig.ServerConfiguration{}, nil
				}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := custom.RunWrapper()
			convey.So(err.Error(), convey.ShouldEqual, "build Cmd err")
			time.Sleep(1 * time.Second)
		})
		convey.Convey("succeed to run Wrapper 3", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),
				ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return errors.New("exec Cmd err")
				}),
				ApplyFunc(customConfig.GetServerConfig, func() (*customConfig.ServerConfiguration, error) {
					return &customConfig.ServerConfiguration{}, nil
				}),
			}

			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
			}
			_, err := custom.RunWrapper()
			assert.Nil(t, err)
			time.Sleep(1 * time.Second)

		})
	})
}

func TestCustom_GetServiceID(t *testing.T) {
	id1 := getServiceID("")
	assert.Equal(t, "", id1)

	id2 := getServiceID("a-b-c")
	assert.Equal(t, "b", id2)

	id3 := getServiceID("a-b")
	assert.Equal(t, "", id3)
}

func TestCustom_HotLoad(t *testing.T) {
	convey.Convey("test custom HotLoad", t, func() {
		convey.Convey("the runtime exited before ready", func() {
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
				InvokeTimeout: 1 * time.Second,
			}
			conf := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Timeout: 1,
				},
			}
			ctx, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := custom.HotLoad(ctx, conf)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("inited custom runtime timeout", func() {
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
				InvokeTimeout: 1 * time.Second,
			}
			conf := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Timeout: 1,
				},
			}
			ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)
			err := custom.HotLoad(ctx, conf)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("success", func() {
			custom := Custom{
				AbstractRuntime: runtime.AbstractRuntime{
					Configuration: &config.Configuration{},
					FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
				},
				InvokeTimeout: 1 * time.Second,
			}
			ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
			var err error
			conf := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					Timeout: 1,
				},
			}
			go func() {
				err = custom.HotLoad(ctx, conf)
			}()
			close(clientReadyChan)
			time.Sleep(2 * time.Second)
			convey.So(err, convey.ShouldBeNil)
			convey.So(conf.DurationInfo.LoadFuncDuration, convey.ShouldNotEqual, 0)
		})
	})
}

func TestCustom_WatchRuntimeProcessStatus(t *testing.T) {
	convey.Convey("TestCustom_WatchRuntimeProcessStatus", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyGlobalVar(&runtime.ProcessExitSignal, make(chan struct{})),
			gomonkey.ApplyGlobalVar(&clientReadyChan, make(chan struct{})),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		defer gomonkey.ApplyFunc(invokectx.GetTimeTraceContext,
			func(ic invokectx.InvokeCtx) *invokectx.TimeTraceContext {
				return invokectx.NewTimeTraceContext()
			}).Reset()

		custom := Custom{
			AbstractRuntime: runtime.AbstractRuntime{
				Configuration: &config.Configuration{},
				FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
			},
			initTimer: &time.Timer{},
			server:    &server{eventManager: NewEventManager()},
		}
		traceID := "traceID001"
		rspChan := make(chan *runtime.InvokeResponse, 1)
		custom.eventManager.PutRequest(invokectx.InvokeCtx{ID: 123}, &runtime.InvokeRequest{TraceID: traceID}, rspChan)
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		go custom.WatchRuntimeProcessStatus(ctx, runtime.Handlers{
			CheckRuntimeStatusHandler: mockCheckRuntimeStatusHandler,
			GetExceptionHandler:       mockGetExceptionHandler,
		})
		time.Sleep(time.Second)
		rsp := custom.eventManager.GetResponse(traceID)
		assert.NotNil(t, rsp.Error)
		assert.Equal(t, int32(4000), rsp.Error.Code)
	})
}

func mockCheckRuntimeStatusHandler() *runtime.Error {
	return &runtime.Error{4000, "runtime error", nil}
}

func mockGetExceptionHandler() *runtime.Error {
	return &runtime.Error{4000, "runtime error", nil}
}

func TestCustom_SendProcessEvent(t *testing.T) {
	custom := Custom{
		AbstractRuntime: runtime.AbstractRuntime{
			Configuration: &config.Configuration{},
			FuncLogger:    functionlog.NewFunctionLogger(nil, "", ""),
		},
		initTimer:     &time.Timer{},
		server:        &server{eventManager: NewEventManager()},
		invokeChan:    make(chan *recvMsgCall, invokeReqNotifyChNum),
		InvokeTimeout: time.Microsecond * 1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go custom.recvInvokeMsg(ctx)

	respTemp := &runtime.InvokeResponse{}
	defer gomonkey.ApplyFunc((*Custom).recvInvoke, func(c *Custom, call *recvMsgCall) {
		response, ok := <-call.rspChan
		if ok {
			respTemp = response
		}
	}).Reset()

	conf := &config.Configuration{}
	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)
	convey.Convey("success", t, func() {
		defer gomonkey.ApplyFunc((*EventManager).PutRequest, func(_ *EventManager,
			ic invokectx.InvokeCtx, request *runtime.InvokeRequest, rspChan chan *runtime.InvokeResponse) error {
			rspChan <- &runtime.InvokeResponse{Result: []byte(`{"result":"success"}`)}
			return nil
		}).Reset()
		custom.SendProcessEvent(invokectx.InvokeCtx{}, runtime.InvokeRequest{TraceID: "success"}, conf)
		time.Sleep(time.Second * 1)
		convey.So(respTemp.Result, convey.ShouldResemble, []byte(`{"result":"success"}`))
	})
	convey.Convey("put error", t, func() {
		defer gomonkey.ApplyFunc((*EventManager).PutRequest, func(_ *EventManager,
			ic invokectx.InvokeCtx, request *runtime.InvokeRequest, rspChan chan *runtime.InvokeResponse) error {
			return errors.New("failed to put request")
		}).Reset()
		custom.SendProcessEvent(invokectx.InvokeCtx{}, runtime.InvokeRequest{TraceID: "put error"}, conf)
		time.Sleep(time.Second * 1)
		convey.So(respTemp.Error, convey.ShouldResemble, &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"event manager put request error"),
		})
	})
}

func Test_setMapValIfKeyExist(t *testing.T) {
	convey.Convey("Test setMapValIfKeyExist", t, func() {
		convey.Convey("Test 1", func() {
			var envMap map[string]string
			key := ""
			value := ""
			res := setMapValIfKeyExist(envMap, key, value)
			convey.So(res, convey.ShouldBeNil)
		})
		convey.Convey("Test 2", func() {
			envMap := make(map[string]string)
			envMap["key 1"] = "test 1"
			envMap["key 2"] = "test 2"
			key := ""
			value := ""
			res := setMapValIfKeyExist(envMap, key, value)
			convey.So(res["key 1"], convey.ShouldEqual, "test 1")
		})
		convey.Convey("Test 3", func() {
			envMap := make(map[string]string)
			envMap["key 1"] = "test 1"
			envMap["key 2"] = "test 2"
			key := "key 3"
			value := "test 3"
			res := setMapValIfKeyExist(envMap, key, value)
			convey.So(res["key 3"], convey.ShouldEqual, "test 3")
		})
	})
}

func TestInspectProcess(t *testing.T) {
	convey.Convey("InspectProcess", t, func() {
		convey.Convey("Test 1", func() {
			c := &Custom{}
			runtime.ProcessExitSignal = make(chan struct{})
			c.InspectProcess()
			_, ok := <-runtime.ProcessExitSignal
			convey.So(ok, convey.ShouldEqual, false)
		})
		convey.Convey("Test 2", func() {
			c := &Custom{
				Process: &os.Process{},
			}
			runtime.ProcessExitSignal = make(chan struct{})
			c.InspectProcess()
			_, ok := <-runtime.ProcessExitSignal
			convey.So(ok, convey.ShouldEqual, false)
		})
	})
}

func Test_Healthz(t *testing.T) {
	convey.Convey("test Healthz", t, func() {
		c := &Custom{
			server: &server{
				healthy: true,
			},
		}
		res, err := c.Healthz("", 0)
		convey.So(res, convey.ShouldEqual, true)
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_ProcessCancelEvent(t *testing.T) {
	convey.Convey("Test ProcessCancelEvent", t, func() {
		c := &Custom{}
		request := &runtime.CancelRequest{}
		res := c.ProcessCancelEvent(request)
		convey.So(res.Error.Code, convey.ShouldEqual, snerror.WorkerInternalErrorCode)
	})
}

func Test_newStatefulResult(t *testing.T) {
	convey.Convey("Test newStatefulResult", t, func() {
		convey.Convey("Test 1", func() {
			patches := []*Patches{
				ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			event := make([]byte, 0)
			state := &pb.State{}
			property := ""
			res := newStatefulResult(event, state, property)
			convey.So(res, convey.ShouldBeNil)
		})
		convey.Convey("Test 2", func() {
			patches := []*Patches{
				ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
					return nil, errors.New("proto message marshal error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			event := make([]byte, 0)
			event = append(event, 'a')
			state := &pb.State{}
			property := ""
			res := newStatefulResult(event, state, property)
			convey.So(res[0], convey.ShouldEqual, 'a')
		})
	})
}

func Test_watchResponse(t *testing.T) {
	convey.Convey("Test watchResponse", t, func() {
		convey.Convey("Test 1", func() {
			c := &Custom{server: &server{eventManager: NewEventManager()}}
			traceID := "Test 1"
			fromTask := true
			res := c.watchResponse(nil, traceID, fromTask)
			convey.So(res, convey.ShouldNotBeNil)
		})
		convey.Convey("Test 2", func() {
			c := &Custom{server: &server{eventManager: NewEventManager()}}
			rspChan := make(chan *runtime.InvokeResponse, 1)
			traceID := "Test 1"
			fromTask := true
			testResponse := &runtime.InvokeResponse{}
			rspChan <- testResponse
			res := c.watchResponse(rspChan, traceID, fromTask)
			convey.So(string(res.Result), convey.ShouldEqual, "")
		})
		convey.Convey("Test 3", func() {
			c := &Custom{server: &server{eventManager: NewEventManager()}}
			rspChan := make(chan *runtime.InvokeResponse, 1)
			traceID := "Test 1"
			fromTask := true
			close(rspChan)
			res := c.watchResponse(rspChan, traceID, fromTask)
			convey.So(res.Error.Code, convey.ShouldEqual, snerror.WorkerInternalErrorCode)
		})
	})
}

func Test_parseStatefulEvent(t *testing.T) {
	convey.Convey("Test parseStatefulEvent", t, func() {
		convey.Convey("Test 1", func() {
			event := make([]byte, 0)
			event = append(event, 't')
			_, err := parseStatefulEvent(event)
			convey.So(err, convey.ShouldEqual, false)
		})
	})
}

func Test_newInvokeTimer(t *testing.T) {
	convey.Convey("Test parseStatefulEvent", t, func() {
		convey.Convey("Test 1", func() {
			c := &Custom{
				InvokeTimeout: 6,
			}
			runtime.StartupDuration = 1
			time := c.newInvokeTimer()
			convey.So(time, convey.ShouldNotBeNil)
		})
	})
}

func Test_appendEnv(t *testing.T) {
	convey.Convey("Test appendEnv", t, func() {
		c := &Custom{
			AbstractRuntime: struct {
				FuncLogger    *functionlog.FunctionLogger
				Configuration *config.Configuration
			}{
				Configuration: &config.Configuration{
					DeploymentConfig: config.DeploymentConfig{
						DeployDir: "Test_Dir",
					},
					RuntimeConfig: config.RuntimeConfig{
						HandlerName: "Test_Handler_Name",
					},
				},
			},
		}
		env := make([]string, 0)
		env = c.appendEnv(env)
		convey.So(env, convey.ShouldNotBeNil)
	})
}

func Test_buildCmd(t *testing.T) {
	convey.Convey("Test buildCmd has PATH env", t, func() {
		c := &Custom{
			AbstractRuntime: struct {
				FuncLogger    *functionlog.FunctionLogger
				Configuration *config.Configuration
			}{
				FuncLogger: functionlog.NewFunctionLogger(nil, "", ""),
				Configuration: &config.Configuration{
					DeploymentConfig: config.DeploymentConfig{
						DeployDir: "Test_Dir",
					},
					RuntimeConfig: config.RuntimeConfig{
						HandlerName:  "Test_Handler_Name",
						FuncLogLevel: "INFO",
					},
				},
			},
		}
		patches := []*Patches{
			ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, nil
			}),
			ApplyFunc(os.Getenv, func(key string) string {
				return "/bin/bash"
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		cmd, err := c.buildCmd("deployFuncCode")
		convey.So(err, convey.ShouldBeNil)
		convey.So(cmd.Args[0], convey.ShouldEqual, "deployFuncCode/bootstrap")
		convey.So(cmd.Env, convey.ShouldContain, "LD_LIBRARY_PATH=/lib64:/usr/lib64:Test_Dir:Test_Dir/lib:/usr/local/nvidia/lib64")
	})
}

func Test_appendCustomRuntimeEnv(t *testing.T) {
	convey.Convey("Test appendEnv", t, func() {
		c := &Custom{
			AbstractRuntime: struct {
				FuncLogger    *functionlog.FunctionLogger
				Configuration *config.Configuration
			}{
				Configuration: &config.Configuration{
					DeploymentConfig: config.DeploymentConfig{
						DeployDir: "Test_Dir",
					},
					RuntimeConfig: config.RuntimeConfig{
						HandlerName: "Test_Handler_Name",
					},
				},
			},
		}
		patches := []*Patches{
			ApplyFunc(customConfig.GetServerConfig, func() (*customConfig.ServerConfiguration, error) {
				cfg := &customConfig.ServerConfiguration{
					ServerUrl:     customConfig.ServerUrl{},
					ServerHeaders: customConfig.ServerHeaders{},
					ServerEnvKey:  customConfig.ServerEnvKey{},
				}
				cfg.ServerEnvKey.RuntimeFuncName = "functionName"
				return cfg, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		env := make([]string, 0)
		env = c.appendEnv(env)

		c.Configuration.URN = "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-service-dsaf:$latest"
		env = c.appendCustomRuntimeEnv(env)
		convey.So(env, convey.ShouldNotBeNil)
	})
}

func Test_StartupHealthz(t *testing.T) {
	c := &Custom{}
	ok, err := c.StartupHealthz("/healthcheck", 5)
	assert.Nil(t, err)
	assert.True(t, ok)
}
