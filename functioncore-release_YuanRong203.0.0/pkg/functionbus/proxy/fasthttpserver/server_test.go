/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package fasthttpserver

import (
	"bytes"
	cryptotls "crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/vpa"
	"functioncore/pkg/functionbus/proxyerrors"
	testutils "functioncore/test/utils"
)

var (
	urn               = "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:Function:1"
	rawData           = []byte{1, 2, 3}
	backupHTTPSConfig *tls.InternalHTTPSConfig
	patches           testutils.PatchSlice
	rspChan           chan int
)

func mockHandleReq(_ string,
	_ *commontypes.TCPRequest, _ *common.FunctionProperty) *commontypes.TCPResponse {
	return &commontypes.TCPResponse{
		RequestID:    1,
		ErrorCode:    proxyerrors.StateOKCode,
		ErrorMessage: "",
		RawData:      rawData,
	}
}

func mockHandleReqWorkerContentInconsistent(_ string,
	_ *commontypes.TCPRequest, _ *common.FunctionProperty) *commontypes.TCPResponse {
	return &commontypes.TCPResponse{
		RequestID:    1,
		ErrorCode:    snerror.ContentInconsistentErrorCode,
		ErrorMessage: "worker content is inconsistent",
		RawData:      rawData,
	}
}

func mockListenAndServe(_ *fasthttp.Server, _ string) error {
	rspChan <- 1
	return nil
}

func mockDoTimeout(_ *fasthttp.Client, _ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
	resp.AppendBody(rawData)
	return nil
}

func setup() {
	backupHTTPSConfig = common.FConfig.HTTPSConfig
	common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	common.FConfig.MemoryEvaluatorConfig = &common.MemoryEvaluatorConfig{
		RequestMemoryEvaluator: 1.0,
	}

	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(tls.FastHTTPListenAndServeTLS, func(_ string, _ *fasthttp.Server) error {
			return nil
		}),
	})

	server := &fasthttp.Server{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(server), "ListenAndServe", mockListenAndServe),
	})

	client := &fasthttp.Client{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "DoTimeout", mockDoTimeout),
	})

	rspChan = make(chan int, 1)

}

func tearDown() {
	common.FConfig.HTTPSConfig = backupHTTPSConfig
	patches.ResetAll()
	close(rspChan)
}

func TestStart(t *testing.T) {
	convey.Convey("TestStart", t, func() {
		setup()
		defer tearDown()
		common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
		backup := common.FConfig.VPAConfig
		common.FConfig.VPAConfig = &common.VerticalAutoscalingConfig{Enable: 1}
		server := CreateFastServer("127.0.0.1:88888", mockHandleReq)
		convey.So(server.Start(), convey.ShouldBeNil)
		rsp := <-rspChan
		convey.So(rsp, convey.ShouldEqual, 1)
		rsp = <-rspChan
		convey.So(rsp, convey.ShouldEqual, 1)
		common.FConfig.VPAConfig = backup
	})
}

func buildRequest() *fasthttp.RequestCtx {
	header := fasthttp.RequestHeader{}
	header.Add(constants.HeaderInvokeURN, urn)
	return &fasthttp.RequestCtx{
		Request: fasthttp.Request{
			Header: header,
		},
	}
}

func buildRequestHeaderContentSignatureConsistent() *fasthttp.RequestCtx {
	header := fasthttp.RequestHeader{}
	header.Add(constants.HeaderInvokeURN, urn)
	header.Add(constants.HeaderContentSignature, base64.StdEncoding.EncodeToString([]byte("1233")))
	return &fasthttp.RequestCtx{
		Request: fasthttp.Request{
			Header: header,
		},
	}
}

func buildRequestHeaderContentSignatureInconsistent() *fasthttp.RequestCtx {
	header := fasthttp.RequestHeader{}
	header.Add(constants.HeaderInvokeURN, urn)
	header.Add(constants.HeaderContentSignature, "aaa")
	return &fasthttp.RequestCtx{
		Request: fasthttp.Request{
			Header: header,
		},
	}
}

func TestHandleReq(t *testing.T) {
	convey.Convey("TestHandleReq", t, func() {
		req := buildRequest()
		server := CreateFastServer("127.0.0.1:88888", mockHandleReq)
		server.handleReq(req)
		body := req.Response.Body()
		convey.So(bytes.Equal(body, rawData), convey.ShouldBeTrue)
	})
}

func TestHandleReqWorkerContentInconsistent(t *testing.T) {
	convey.Convey("TestHandleReqWorkerContentInconsistent: busproxy content consistent", t, func() {
		req := buildRequestHeaderContentSignatureConsistent()
		server := CreateFastServer("127.0.0.1:88888", mockHandleReqWorkerContentInconsistent)
		server.handleReq(req)
		body := req.Response.Body()
		convey.So(bytes.Equal(body, []byte("{\"code\":161911,\"message\":\"worker content is inconsistent\"}")), convey.ShouldBeTrue)
	})
	convey.Convey("TestHandleReqWorkerContentInconsistent: busproxy content inconsistent", t, func() {
		req := buildRequestHeaderContentSignatureInconsistent()
		server := CreateFastServer("127.0.0.1:88888", mockHandleReqWorkerContentInconsistent)
		server.handleReq(req)
		body := req.Response.Body()
		convey.So(bytes.Equal(body, []byte("{\"code\":161911,\"message\":\"worker content is inconsistent\"}")), convey.ShouldBeTrue)
	})
}

// TestBuildRequest -
func TestBuildRequest(t *testing.T) {
	convey.Convey("TestBuildRequest_parseTaskReq", t, func() {
		ctx := &fasthttp.RequestCtx{
			Request:  fasthttp.Request{},
			Response: fasthttp.Response{},
		}
		ctx.Request.Header.Set(constants.HeaderStateID, "stateID1")
		ctx.Request.Header.Set(constants.HeaderInvokeURN, "sn:cn:yrk:tenant1:function:0@service1@funcName1:latest")
		extraMap := make(map[string]string)
		extraMap[constants.HeaderStateID] = "stateID2"
		extraMap["Test-Extral-Map"] = "test"
		extraMapJSON, _ := json.Marshal(extraMap)
		ctx.Request.Header.Set(constants.ExtraMap, string(extraMapJSON))
		traceID := "test_traceID"
		urnutils.Separator = "@"
		req, dst, _ := buildInternalReq(ctx, traceID)
		convey.So(req.IsStateFul, convey.ShouldBeTrue)
		convey.So(dst.InstanceID, convey.ShouldEqual, internal.HostName)
		convey.So(dst.FunctionName, convey.ShouldEqual, "funcName1")
		convey.So(dst.FutureID, convey.ShouldEqual, traceID)
		convey.So(req.UserData[constants.SNServiceIDFlag], convey.ShouldEqual, "0@service1")
		convey.So(req.UserData[constants.FrontendCallFlag], convey.ShouldEqual, "Frontend")
		convey.So(req.UserData[constants.SrcTenantFlag], convey.ShouldEqual, "tenant1")
		convey.So(req.UserData[constants.StateIDFlag], convey.ShouldEqual, "stateID1")
	})
}

func TestComponentHealth(t *testing.T) {
	convey.Convey("TestComponentHealth", t, func() {
		setup()
		defer tearDown()

		req := buildRequest()
		server := CreateFastServer("127.0.0.1:88888", mockHandleReq)
		server.componentHealth(req)
		body := req.Response.Body()
		convey.So(bytes.Equal(body, rawData), convey.ShouldBeTrue)
	})
}

func serveRecord(handler fasthttp.RequestHandler, req *fasthttp.Request, res *fasthttp.Response) error {
	ln := fasthttputil.NewInmemoryListener()
	defer ln.Close()

	go func() {
		err := fasthttp.Serve(ln, handler)
		if err != nil {
			panic(err)
		}
	}()

	client := fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
	}

	return client.Do(req, res)
}

func Test_FastServer_routerHandler_NotFound(t *testing.T) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.SetRequestURI("http://127.0.0.1" + "/ddd")
	req.Header.SetMethod("POST")

	server := CreateFastServer("", nil)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	err := serveRecord(server.routerHandler, req, resp)
	assert.Nil(t, err, "should no error")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode(), "status code should be not found")
}

func Test_FastServer_componentHealth(t *testing.T) {
	common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	common.FConfig.LocalAuth = &localauth.AuthConfig{}
	common.FConfig.AuthenticationEnable = true
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
			if timestamp == "" {
				return errors.New("no auth check info")
			}
			if timestamp == "test" {
				return nil
			}
			return nil
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
		common.FConfig.AuthenticationEnable = false
	}()
	requestURL := "http://127.0.0.1/componenthealth"
	server := CreateFastServer("", nil)

	tests := []struct {
		name      string
		timestamp string
		targetURL string

		exceptedCode int
	}{
		{
			name:         "no auth header",
			exceptedCode: http.StatusUnauthorized,
		},
		{
			name:         "request workermanager failed timeout",
			timestamp:    strconv.Itoa(int(time.Now().Unix())),
			targetURL:    "http://127.0.0.3:1234",
			exceptedCode: http.StatusInternalServerError,
		},
		{
			name:         "request workermanager failed",
			timestamp:    "test",
			targetURL:    "http://127.0.0.3:1234",
			exceptedCode: http.StatusInternalServerError,
		},
	}

	for _, test := range tests {
		if test.name == "request workermanager failed" {
			client := &fasthttp.Client{}
			patch := gomonkey.ApplyMethod(reflect.TypeOf(client), "DoTimeout", func(_ *fasthttp.Client,
				_ *fasthttp.Request, resp *fasthttp.Response, _ time.Duration) error {
				resp.Header.SetStatusCode(http.StatusInternalServerError)
				resp.AppendBody(rawData)
				return nil
			})
			defer patch.Reset()
		}
		req := fasthttp.AcquireRequest()
		req.SetRequestURI(requestURL)
		req.Header.Set(constants.HeaderAuthTimestamp, test.timestamp)
		resp := fasthttp.AcquireResponse()

		server.workerManagerURI = test.targetURL

		err := serveRecord(server.routerHandler, req, resp)
		assert.Nil(t, err, test.name)
		assert.Equal(t, test.exceptedCode, resp.StatusCode(), test.name)

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}

}

func fakeHandler(traceID string, request *commontypes.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	switch string(request.RawData) {
	case "accepted":
		return &commontypes.TCPResponse{ErrorCode: proxyerrors.StateAcceptedCode}
	case "4002":
		return &commontypes.TCPResponse{ErrorCode: 4002}
	default:
		return &commontypes.TCPResponse{ActualCallStartTime: time.Now()}
	}
}

func Test_FastServer_handleReq(t *testing.T) {
	common.FConfig.LocalAuth = &localauth.AuthConfig{}
	common.FConfig.AuthenticationEnable = true
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
			if timestamp == "" {
				return errors.New("no auth check info")
			}
			return nil
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
		common.FConfig.AuthenticationEnable = false
		common.FConfig.MemoryEvaluatorConfig = &common.MemoryEvaluatorConfig{
			RequestMemoryEvaluator: 1.0,
		}

	}()
	requestURL := "http://127.0.0.1/invoke"
	server := CreateFastServer("", fakeHandler)

	tests := []struct {
		name   string
		header map[string]string
		body   []byte

		exceptedCode int
	}{
		{
			name: "request ok",
			header: map[string]string{
				constants.HeaderInvokeURN:       "sn:cn:yrk:123456:function:0@default@test_func:latest",
				constants.HeaderAuthTimestamp:   strconv.Itoa(int(time.Now().Unix())),
				constants.HeaderBackpressure:    "true",
				constants.HeaderRetryFlag:       "true",
				constants.HeaderPersistResponse: "true",
			},
			exceptedCode: http.StatusOK,
		},
		{
			name: "no auth info",
			header: map[string]string{
				constants.HeaderInvokeURN: "sn:cn:yrk:123456:function:0@default@test_func:latest",
			},

			exceptedCode: http.StatusInternalServerError,
		},
		{
			name: "no function urn header",
			header: map[string]string{
				constants.HeaderAuthTimestamp: strconv.Itoa(int(time.Now().Unix())),
			},

			exceptedCode: http.StatusInternalServerError,
		},
		{
			name: "request accepted",
			header: map[string]string{
				constants.HeaderInvokeURN:     "sn:cn:yrk:123456:function:0@default@test_func:latest",
				constants.HeaderAuthTimestamp: strconv.Itoa(int(time.Now().Unix())),
			},
			body:         []byte("accepted"),
			exceptedCode: http.StatusAccepted,
		},
		{
			name: "function code error",
			header: map[string]string{
				constants.HeaderInvokeURN:     "sn:cn:yrk:123456:function:0@default@test_func:latest",
				constants.HeaderAuthTimestamp: strconv.Itoa(int(time.Now().Unix())),
			},
			body:         []byte("4002"),
			exceptedCode: http.StatusInternalServerError,
		},
	}

	for _, test := range tests {
		req := fasthttp.AcquireRequest()
		req.SetRequestURI(requestURL)
		for k, v := range test.header {
			req.Header.Set(k, v)
		}
		req.SetBody(test.body)

		resp := fasthttp.AcquireResponse()

		err := serveRecord(server.routerHandler, req, resp)
		assert.Nil(t, err, test.name)
		assert.Equal(t, test.exceptedCode, resp.StatusCode(), test.name)

		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}
}

func TestCalculateCost(t *testing.T) {
	convey.Convey("TestCalculateCost", t, func() {
		rsp := &commontypes.TCPResponse{}

		start := time.Now()
		time.Sleep(10 * time.Millisecond)
		cost := calculateCost(start, rsp)
		convey.So(cost.totalCost, convey.ShouldBeGreaterThanOrEqualTo, 10*time.Millisecond)
		convey.So(cost.busCost, convey.ShouldBeGreaterThanOrEqualTo, 10*time.Millisecond)
		convey.So(cost.workerCost, convey.ShouldEqual, 0)
		convey.So(cost.scheduleCost, convey.ShouldEqual, 0)

		rsp.ActualCallCost = 15 * time.Millisecond
		rsp.ActualCallStartTime = time.Now()
		time.Sleep(10 * time.Millisecond)

		cost = calculateCost(start, rsp)
		convey.So(cost.totalCost, convey.ShouldBeGreaterThanOrEqualTo, 20*time.Millisecond)
		convey.So(cost.busCost, convey.ShouldBeGreaterThanOrEqualTo, 5*time.Millisecond)
		convey.So(cost.workerCost, convey.ShouldEqual, rsp.ActualCallCost)
		convey.So(cost.scheduleCost, convey.ShouldBeGreaterThanOrEqualTo, 10*time.Millisecond)
	})
}

func TestGetFastHTTPClient(t *testing.T) {
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(tls.GetClientTLSConfig, func() *cryptotls.Config {
			return &cryptotls.Config{}
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
	}()
	httpsConfig := &tls.InternalHTTPSConfig{}
	httpsConfig.HTTPSEnable = true
	common.FConfig.HTTPSConfig = httpsConfig
	fastHttpCli := GetFastHTTPClient()
	assert.NotNil(t, fastHttpCli)
}

func Test_FastServer_routerHandler_VPACreate(t *testing.T) {
	server := CreateFastServer("", nil)
	tests := []struct {
		name               string
		body               string
		header             map[string]string
		expectedStatusCode int
		vpaConfig          *common.VerticalAutoscalingConfig
		patchesFunc        testutils.PatchesFunc
	}{
		{
			name: "close vpa config then return 404",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: 404,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name: "test nil body then return 500",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: 500,
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name: "test failed to auth then return 401",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: 401,
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{gomonkey.ApplyFunc(auth, func(ctx *fasthttp.RequestCtx) bool {
					return false
				})})
				return p
			},
		}, {
			name: "normal case then return 200200",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: http.StatusOK,
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			body: `{"action":"create","businessType":"test","funcName":"test","podName":"test","podUID":"test",
					"containerIDs":{"worker":"test","runtime":"test"},"resource":{"cpuLimit":1000,"cpuRequest":1000,
					"memoryLimit":1000,"memoryRequest":1000},"initResource":null}`,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(vpa.RegisterFunctionInstanceMonitorInSLOAgent, func(payload []byte, traceID string) snerror.SNError {
						return nil
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			req := fasthttp.AcquireRequest()
			req.SetRequestURI("http://127.0.0.1" + "/vpa/create")
			req.Header.SetMethod("POST")
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			resp := fasthttp.AcquireResponse()

			common.FConfig.VPAConfig = tt.vpaConfig
			req.SetBody([]byte(tt.body))
			err := serveRecord(server.routerHandler, req, resp)

			assert.Nil(t, err, "should no error")
			assert.Equal(t, tt.expectedStatusCode, resp.StatusCode(), "status code should be not found")

			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			p.ResetAll()
		})
	}
}

func Test_FastServer_routerHandler_VPARecover(t *testing.T) {
	server := CreateFastServer("", nil)
	tests := []struct {
		name               string
		header             map[string]string
		expectedStatusCode int
		expectedInnerCode  interface{}
		vpaConfig          *common.VerticalAutoscalingConfig
		patchesFunc        testutils.PatchesFunc
	}{
		{
			name: "close vpa config then return 404",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: 404,
			expectedInnerCode:  nil,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name: "failed to recover function instance status then return 404",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: 500,
			expectedInnerCode:  proxyerrors.VPARecoverFunctionInstanceCacheErrCode,
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(vpa.RecoverFunctionInstanceMonitorInSLOAgent, func(ctx *fasthttp.RequestCtx, traceID string) snerror.SNError {
						return snerror.New(proxyerrors.VPARecoverFunctionInstanceCacheErrCode, "")
					}),
				})
				return p
			},
		}, {
			name: "normal case return 200200",
			header: map[string]string{
				constants.HeaderTraceID: uuid.New().String(),
			},
			expectedStatusCode: http.StatusOK,
			expectedInnerCode:  nil,
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(vpa.RecoverFunctionInstanceMonitorInSLOAgent, func(ctx *fasthttp.RequestCtx, traceID string) snerror.SNError {
						return nil
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			req := fasthttp.AcquireRequest()
			req.SetRequestURI("http://127.0.0.1:8890" + "/vpa/recover")
			req.Header.SetMethod("POST")
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			resp := fasthttp.AcquireResponse()

			common.FConfig.VPAConfig = tt.vpaConfig
			err := serveRecord(server.routerVPAHandler, req, resp)

			assert.Nil(t, err, "should no error")
			assert.Equal(t, tt.expectedStatusCode, resp.StatusCode(), "status code should be not found")
			if tt.expectedInnerCode != nil {
				errCodeStr := strconv.Itoa(tt.expectedInnerCode.(int))
				assert.Equal(t, errCodeStr, string(resp.Header.Peek(constants.HeaderInnerCode)), "status code should be not found")
			}
			fasthttp.ReleaseRequest(req)
			fasthttp.ReleaseResponse(resp)
			p.ResetAll()
		})
	}
}
