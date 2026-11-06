/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package adaptor

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/core-sdk/config"
	"functioncore/pkg/functionbus/proxy/fasthttpserver"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
	taskutil "functioncore/pkg/functiontask/task/util"
	"functioncore/test/functiontask/test/testcontext"
	"functioncore/test/utils"
)

func TestDoForwardReq(t *testing.T) {
	convey.Convey("success", t, func() {
		httpClient := &fasthttp.Client{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		requestPb := &invoke.RequestPb{}
		requestPb.FuncName = "hello-world"
		requestPb.Version = "latest"
		requestPb.Payload = "hello world payload"
		endpoint := "7.218.74.110:31220"
		config.FConfigs = &config.Configs{}
		config.FConfigs.LocalAuth = &localauth.AuthConfig{}
		config.FConfigs.LocalAuth.AKey = "AKey"
		config.FConfigs.LocalAuth.SKey = "SKey"
		config.FConfigs.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: true}

		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.ClusterFrontends["cluster002"] = []string{"7.218.74.120"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttpserver.GetFastHTTPClient, func() *fasthttp.Client {
			return httpClient
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "DoTimeout",
			func(c *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
				// add traceID assertions, not from requestIDList
				if string(req.Header.Peek(constants.HeaderTraceID)) != "traceID" {
					resp.Header.Set(constants.HeaderInnerCode, "500")
					return nil
				}
				resp.Header.Set(constants.HeaderInnerCode, "0")
				resp.Header.Set(constants.HeaderLogResult, "forward log")
				return nil
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		resp := doForwardReq(taskCtx, requestPb, []string{endpoint})
		convey.So(resp.Code, convey.ShouldEqual, 0)
	})
	convey.Convey("forward timeout", t, func() {
		httpClient := &fasthttp.Client{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		requestPb := &invoke.RequestPb{}
		requestPb.FuncName = "hello-world"
		requestPb.Version = "latest"
		requestPb.Payload = "hello world payload"
		endpoint := "7.218.74.110:31220"
		config.FConfigs = &config.Configs{}
		config.FConfigs.LocalAuth = &localauth.AuthConfig{}
		config.FConfigs.LocalAuth.AKey = "AKey"
		config.FConfigs.LocalAuth.SKey = "SKey"
		config.FConfigs.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: true}

		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.ClusterFrontends["cluster002"] = []string{"7.218.74.120"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttpserver.GetFastHTTPClient, func() *fasthttp.Client {
			return httpClient
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "DoTimeout",
			func(c *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
				resp.Header.Set(constants.HeaderInnerCode, "0")
				resp.Header.Set(constants.HeaderLogResult, "forward log")
				return errors.New("do request timeout")
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		resp := doForwardReq(taskCtx, requestPb, []string{endpoint})
		convey.So(resp.Code, convey.ShouldEqual, snerror.InternalError)
	})
	convey.Convey("response code not integer, return InternalError", t, func() {
		httpClient := &fasthttp.Client{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		requestPb := &invoke.RequestPb{}
		requestPb.FuncName = "hello-world"
		requestPb.Version = "latest"
		requestPb.Payload = "hello world payload"
		endpoint := "7.218.74.110:31220"
		config.FConfigs = &config.Configs{}
		config.FConfigs.LocalAuth = &localauth.AuthConfig{}
		config.FConfigs.LocalAuth.AKey = "AKey"
		config.FConfigs.LocalAuth.SKey = "SKey"
		config.FConfigs.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: true}

		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.ClusterFrontends["cluster002"] = []string{"7.218.74.120"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttpserver.GetFastHTTPClient, func() *fasthttp.Client {
			return httpClient
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "DoTimeout",
			func(c *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
				resp.Header.Set(constants.HeaderInnerCode, "xx")
				resp.Header.Set(constants.HeaderLogResult, "forward log")
				return nil
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		resp := doForwardReq(taskCtx, requestPb, []string{endpoint})
		convey.So(resp.Code, convey.ShouldEqual, snerror.InternalError)
	})
	convey.Convey("response code nok", t, func() {
		httpClient := &fasthttp.Client{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		requestPb := &invoke.RequestPb{}
		requestPb.FuncName = "hello-world"
		requestPb.Version = "latest"
		requestPb.Payload = "hello world payload"
		endpoint := "7.218.74.110:31220"
		config.FConfigs = &config.Configs{}
		config.FConfigs.LocalAuth = &localauth.AuthConfig{}
		config.FConfigs.LocalAuth.AKey = "AKey"
		config.FConfigs.LocalAuth.SKey = "SKey"
		config.FConfigs.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: true}
		resp := &fasthttp.Response{}

		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.ClusterFrontends["cluster002"] = []string{"7.218.74.120"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttpserver.GetFastHTTPClient, func() *fasthttp.Client {
			return httpClient
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttp.AcquireResponse, func() *fasthttp.Response {
			return resp
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(resp), "Body",
			func(resp *fasthttp.Response) []byte {
				return []byte("{ \"code\": 500, \"message\": \"func meta not found\" }")
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "DoTimeout",
			func(c *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
				resp.Header.Set(constants.HeaderInnerCode, "500")
				resp.Header.Set(constants.HeaderLogResult, "forward log")
				return nil
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		resp1 := doForwardReq(taskCtx, requestPb, []string{endpoint})
		convey.So(resp1.Code, convey.ShouldEqual, 500)
		convey.So(resp1.Message, convey.ShouldEqual, "func meta not found")
	})
	convey.Convey("response code nok, message error", t, func() {
		httpClient := &fasthttp.Client{}
		funcArgs := &FuncArgs{}
		taskCtx := testcontext.New(common.MethodInvoke)
		requestPb := &invoke.RequestPb{}
		requestPb.FuncName = "hello-world"
		requestPb.Version = "latest"
		requestPb.Payload = "hello world payload"
		endpoint := "7.218.74.110:31220"
		config.FConfigs = &config.Configs{}
		config.FConfigs.LocalAuth = &localauth.AuthConfig{}
		config.FConfigs.LocalAuth.AKey = "AKey"
		config.FConfigs.LocalAuth.SKey = "SKey"
		config.FConfigs.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: true}
		resp := &fasthttp.Response{}

		var patches []*gomonkey.Patches
		patches = append(patches, gomonkey.ApplyFunc(servicediscovery.GetRegistry, func() *servicediscovery.Registry {
			registry := &servicediscovery.Registry{
				FuncAvailableClusters: make(map[string][]string, mapInitSize),
				ClusterFrontends:      make(map[string][]string, mapInitSize),
				FuncMeta:              make(map[string]*servicediscovery.FunctionMetaInfo),
			}
			registry.ClusterFrontends["cluster001"] = []string{"7.218.74.110"}
			registry.ClusterFrontends["cluster002"] = []string{"7.218.74.120"}
			registry.FuncAvailableClusters["sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world"] =
				[]string{"cluster002"}
			registry.FuncMeta["580943580943580943/0@debugservice@hello-world/latest"] =
				&servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{
					FunctionVersionURN: "sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world:latest",
				}}
			return registry
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttpserver.GetFastHTTPClient, func() *fasthttp.Client {
			return httpClient
		}))
		patches = append(patches, gomonkey.ApplyFunc(fasthttp.AcquireResponse, func() *fasthttp.Response {
			return resp
		}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
			func(x *CallRequestPb) *invoke.RequestPb {
				requestPb := &invoke.RequestPb{}
				requestPb.FuncName = "hello-world"
				requestPb.Version = "latest"
				requestPb.Payload = "hello world payload"
				return requestPb
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "TenantID",
			func(c *testcontext.Context) string {
				return "580943580943580943"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(taskCtx), "ServiceID",
			func(c *testcontext.Context) string {
				return "0@debugservice"
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(resp), "Body",
			func(resp *fasthttp.Response) []byte {
				return []byte("{ \"code\": 500, \"message\": \"func meta not found\" }x")
			}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "DoTimeout",
			func(c *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
				resp.Header.Set(constants.HeaderInnerCode, "500")
				resp.Header.Set(constants.HeaderLogResult, "forward log")
				return nil
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		resp1 := doForwardReq(taskCtx, requestPb, []string{endpoint})
		convey.So(resp1.Code, convey.ShouldEqual, snerror.InternalError)
	})
}

func TestInvoke(t *testing.T) {
	convey.Convey("test invoke", t, func() {
		convey.Convey("case1: failed to get real funcName", func() {
			adaptor := &Adaptor{}
			funcArgs := &FuncArgs{}
			taskCtx := testcontext.New(common.MethodInvoke)
			patches := utils.InitPatchSlice()
			isFuncCalled := false
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
					func(_ *CallRequestPb) *invoke.RequestPb {
						return &invoke.RequestPb{
							FuncName:        "funcA",
							Version:         "v1",
							CountOfFuncName: 1,
						}
					}),
				gomonkey.ApplyFunc(taskutil.GetRealFuncName, func(_, _, _, _ string) (string, bool) {
					return "", false
				}),
				gomonkey.ApplyFunc(processNoSuchFunctionError, func(_ taskcontext.Context) {
					isFuncCalled = true
					return
				}),
				gomonkey.ApplyFunc(judgeForwardToOtherCluster, func(_ taskcontext.Context, _ *invoke.RequestPb) (bool, []string) {
					return false, []string{}
				}),
			})
			defer patches.ResetAll()
			adaptor.invoke(taskCtx, funcArgs)
			convey.So(isFuncCalled, convey.ShouldBeTrue)
		})
		convey.Convey("case2: function does not exist", func() {
			adaptor := &Adaptor{}
			funcArgs := &FuncArgs{}
			taskCtx := testcontext.New(common.MethodInvoke)
			patches := utils.InitPatchSlice()
			isFuncCalled := false
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
					func(_ *CallRequestPb) *invoke.RequestPb {
						return &invoke.RequestPb{
							FuncName:        "funcA",
							Version:         "v1",
							CountOfFuncName: 1,
						}
					}),
				gomonkey.ApplyFunc(taskutil.GetRealFuncName, func(_, _, _, _ string) (string, bool) {
					return "funcA:v1", true
				}),
				gomonkey.ApplyFunc((*servicediscovery.Registry).IsFunctionExist,
					func(_ *servicediscovery.Registry, _, _, _, _ string) bool {
						return false
					}),
				gomonkey.ApplyFunc(processNoSuchFunctionError, func(_ taskcontext.Context) {
					isFuncCalled = true
					return
				}),
				gomonkey.ApplyFunc(judgeForwardToOtherCluster, func(_ taskcontext.Context, _ *invoke.RequestPb) (bool, []string) {
					return false, []string{}
				}),
			})
			defer patches.ResetAll()
			adaptor.invoke(taskCtx, funcArgs)
			convey.So(isFuncCalled, convey.ShouldBeTrue)
		})
		convey.Convey("case3: need forward", func() {
			adaptor := &Adaptor{}
			funcArgs := &FuncArgs{}
			taskCtx := testcontext.New(common.MethodInvoke)
			patches := utils.InitPatchSlice()
			isFuncCalled := false
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(funcArgs), "GetInvokeReq",
					func(_ *CallRequestPb) *invoke.RequestPb {
						return &invoke.RequestPb{
							FuncName:        "funcA",
							Version:         "v1",
							CountOfFuncName: 1,
						}
					}),
				gomonkey.ApplyFunc(taskutil.GetRealFuncName, func(_, _, _, _ string) (string, bool) {
					isFuncCalled = true
					return "funcA:v1", true
				}),
				gomonkey.ApplyFunc(judgeForwardToOtherCluster, func(_ taskcontext.Context, _ *invoke.RequestPb) (bool, []string) {
					return true, []string{"endpoint"}
				}),
				gomonkey.ApplyFunc(forwardReq, func(_ taskcontext.Context, _ *FuncArgs, _ *invoke.Request, _ []string) *invoke.Response {
					return &invoke.Response{
						Code: common.EOK,
					}
				}),
			})
			defer patches.ResetAll()
			adaptor.invoke(taskCtx, funcArgs)
			convey.So(isFuncCalled, convey.ShouldBeFalse)
		})
	})
}
