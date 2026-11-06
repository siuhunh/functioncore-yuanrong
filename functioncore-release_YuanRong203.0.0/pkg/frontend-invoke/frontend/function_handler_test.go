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

package frontend

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	commonStream "functioncore/pkg/common/stream"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/grpcconn"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/stream"
	"functioncore/pkg/frontend-invoke/trafficlimit"
	mockutils "functioncore/test/utils"
)

func testMockBusServer(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func setupTestEnv() {
	go func() {
		http.HandleFunc("/invoke", testMockBusServer)
		http.ListenAndServe(":22424", nil)
	}()
	time.Sleep(10 * time.Millisecond)

	trafficlimit.TenantBuf.ProcessUpdate(&etcd3.Event{
		Key:   "/sn/qos/business/yrk/tenant/01234567890123456789012345678901",
		Value: []byte(`{"quota":100000}`),
	})
	trafficlimit.TenantBuf.ProcessUpdate(&etcd3.Event{
		Key:   "/sn/qos/business/yrk/tenant/1234",
		Value: []byte(`{"quota":0}`),
	})

	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{
		MaxRequestBodySize: 1,
	}
	constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	urnutils.Separator = "@"
	constant.NativeAz = constants.DefaultAZ
	constant.FConfig.InvokeMaxRetryTimes = 3
	constant.FConfig.HTTPConfig.WorkerInstanceReadTimeOut = 60
	processAddEvent(&etcd3.Event{
		Key:   "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/testnode12345",
		Value: []byte(`{"ip":"","port":"8080","status":"ready","p2pPort":"32568","nodeIP":"127.0.0.1","nodePort":"22424"}`),
	})
	processAddEvent(&etcd3.Event{
		Key:   "/sn/workers/business/yrk/tenant/01234567890123456789012345678901/function/0@default@test_func/version/latest/defaultaz/defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk",
		Value: []byte(`{"InstanceId":"defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk","ip":"127.0.0.1","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"127.0.0.1","nodePort":"22424","nodeName":"testnode12345","applier":"functiontask-926cp","ownerIP":"127.0.0.1","cpu":300,"memory":128}`),
	})
}

func tearDownEnv() {
	trafficlimit.TenantBuf.ProcessDelete(&etcd3.Event{
		Key:   "/sn/qos/business/yrk/tenant/01234567890123456789012345678901",
		Value: []byte(`{"quota":100000}`),
	})
	trafficlimit.TenantBuf.ProcessDelete(&etcd3.Event{
		Key:   "/sn/qos/business/yrk/tenant/1234",
		Value: []byte(`{"quota":0}`),
	})

	processDeleteEvent(&etcd3.Event{
		Key:   "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/testnode12345",
		Value: []byte(`{"ip":"","port":"8080","status":"ready","p2pPort":"32568","nodeIP":"127.0.0.1","nodePort":"22424"}`),
	})
	processDeleteEvent(&etcd3.Event{
		Key:   "/sn/workers/business/yrk/tenant/01234567890123456789012345678901/function/0@default@test_func/version/latest/defaultaz/defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk",
		Value: []byte(`{"InstanceId":"defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk","ip":"127.0.0.1","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"127.0.0.1","nodePort":"22424","nodeName":"testnode12345","applier":"functiontask-926cp","ownerIP":"127.0.0.1","cpu":300,"memory":128}`),
	})
}

func Test_getURNWithVersion(t *testing.T) {
	tests := []struct {
		name            string
		version         string
		plainURN        string
		exceptedURN     string
		exceptedVersion string
	}{
		{
			name:     "empty version",
			version:  "",
			plainURN: "sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func",

			exceptedURN:     "sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest",
			exceptedVersion: "latest",
		},
		{
			name:     "latest version",
			version:  "latest",
			plainURN: "sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest",

			exceptedURN:     "sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest",
			exceptedVersion: "latest",
		},
		{
			name:     "other version",
			version:  "ax",
			plainURN: "sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:ax",

			exceptedURN:     "sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:ax",
			exceptedVersion: "ax",
		},
	}

	for _, test := range tests {
		urn, version := getURNWithVersion(test.version, test.plainURN)
		assert.Equal(t, test.exceptedURN, urn, test.name)
		assert.Equal(t, test.exceptedVersion, version, test.name)
	}
}

func Test_requestAuthCheck_Fail(t *testing.T) {
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(authCheck, func(r *http.Request) error {
			return errors.New("failed to check auth")
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
	}()
	reqInfo := &model.FunctionRequestInfo{}
	w := httptest.NewRecorder()
	assert.Equal(t, true, requestAuthCheck(nil, w, reqInfo), "request auth check should fail")
}

func Test_authCheck(t *testing.T) {
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	constant.FConfig.AuthenticationEnable = true
	defer func() {
		constant.FConfig.AuthenticationEnable = false
	}()
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
	}()

	tests := []struct {
		name      string
		timestamp string

		exceptedError bool
	}{
		{
			name:      "normal request",
			timestamp: strconv.Itoa(int(time.Now().Unix())),

			exceptedError: false,
		},
		{
			name: "no timestamp",

			exceptedError: true,
		},
	}

	for _, test := range tests {
		req, _ := http.NewRequest(http.MethodPost, "http://127.0.0.1:8080", nil)
		req.Header.Set(constants.HeaderAuthTimestamp, test.timestamp)
		err := authCheck(req)
		if test.exceptedError {
			assert.Errorf(t, err, test.name)
		} else {
			assert.Nil(t, err, test.name)
		}
	}
}

func TestInvokeHandler(t *testing.T) {
	now := time.Now().String()
	constant.InitDefaultConfig()
	constant.FConfig.LoadBalanceConfig.ConcurrencyThreshold = 10
	_ = os.Setenv("VIRTUAL_NODE_NUM", "50")
	remotescheduler.CreateInstance()
	mockClient, _ := grpcconn.CreateGrpcClient(common.RPCClientConfig{})
	mockResHeader := map[string]string{
		constants.HeaderMemorySize: "128",
		constants.HeaderCPUSize:    "300",
	}
	mockStreamHeader := map[string]string{
		constants.HeaderStreamToken: "token",
		constants.HeaderStreamRunID: "request001",
		constants.HeaderStreamAddr:  "127.0.0.1",
	}
	mockFaasSchedulerNormalResponse := &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  "default-#-pool300xxx",
		ProcessorID:  0,
		InstanceIP:   "127.0.0.2",
		InstancePort: "8888",
		NodeIP:       "127.0.0.3",
		NodePort:     "22423",
		Error:        schedulerapi.Error{},
	}
	mockWebSocketRequestBody := []byte(`{"instanceIP":"127.0.0.1","instancePort":"8443"}`)
	websocketURI := "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/websocket"
	etcdClient := &etcd3.EtcdClient{}
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", now
		}),
		gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
			return etcdClient
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return nil, errors.New("test err")
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
	}()

	setupTestEnv()
	defer tearDownEnv()
	router := gin.New()
	router.GET("/serverless/v1/componentshealth", func(context *gin.Context) {
		ClusterHealth(context.Writer, context.Request)
	})
	router.GET("/healthz", func(context *gin.Context) {
		healthz(context.Writer, context.Request)
	})
	router.POST("/serverless/v1/functions/:function-name/invocations", InvokeHandler)
	router.POST("/serverless/v1/functions/:function-name/websocket", WebSocketInstanceCreateHandler)
	router.DELETE("/serverless/v1/functions/:function-name/websocket", WebSocketInstanceDeleteHandler)
	router.GET("/serverless/v1/stream/instance", getStreamInstanceHandler)
	router.POST("/serverless/v1/functions/:function-name/stream-invoke", streamInvokeHandler)
	router.POST("/serverless/v1/functions/instances/recover", instancesRecoverHandler)

	tests := []struct {
		name         string
		method       string
		body         []byte
		header       map[string]string
		url          string
		exceptedCode int
		patchesFunc  mockutils.PatchesFunc
	}{
		{
			name:         "normal request",
			url:          "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations",
			exceptedCode: http.StatusOK,
			body:         make([]byte, 2*1024, 2*1024),
		},
		{
			name:         "invalid urn, all empty",
			url:          "/serverless/v1/functions/::::::/invocations",
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "invalid urn",
			url:          "/serverless/v1/functions/test/invocations",
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "request body too large",
			url:          "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations",
			body:         make([]byte, 2*1024*1024, 2*1024*1024),
			exceptedCode: http.StatusInternalServerError,
		},
		{
			name:         "reach traffic limit",
			url:          "/serverless/v1/functions/sn:cn:yrk:1234:function:0@default@test_func:latest/invocations",
			exceptedCode: http.StatusInternalServerError,
		},
		{
			name: "invalid appID of HeaderAuthorization",
			url:  "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations",
			header: map[string]string{
				constants.HeaderAuthorization: "invalid Authorization",
			},
			exceptedCode: http.StatusOK,
		},
		{
			name: "valid appID of HeaderAuthorization",
			url:  "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations",
			header: map[string]string{
				constants.HeaderAuthorization: "appid=xxx, authvalue=xxxx",
			},
			exceptedCode: http.StatusOK,
		},
		{
			name:         "empty tenantID",
			url:          "/serverless/v1/functions/:::::0@default@test_func:latest/invocations",
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "websocket acquiring request normal case",
			url:          websocketURI,
			exceptedCode: http.StatusOK,
			header:       mockResHeader,
			body:         mockWebSocketRequestBody,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
						req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return mockFaasSchedulerNormalResponse, nil
					}),
					gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(
						_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
						return mockClient
					}),
					gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
						return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeWebSocket}}, true
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket is not enable",
			url:          websocketURI,
			exceptedCode: http.StatusInternalServerError,
			header:       mockResHeader,
			body:         mockWebSocketRequestBody,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
						return &metadata.FunctionMetaData{
							FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeCAE},
							ExtendedMetaData:  functiontypes.ExtendedMetaData{EnableWebsocket: false}}, true
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket acquiring request get failed response from faas scheduler",
			url:          websocketURI,
			exceptedCode: http.StatusInternalServerError,
			header:       mockResHeader,
			body:         mockWebSocketRequestBody,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
						req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return &schedulerapi.AcquireInstanceRsp{
							Error: schedulerapi.Error{
								ErrorCode: 500,
							},
						}, errors.New("mock error")
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket acquiring request get bad request from rdispatcher",
			url:          websocketURI,
			exceptedCode: http.StatusInternalServerError,
			header:       mockResHeader,
			body:         []byte("{"),
		},
		{
			name:         "websocket acquiring request transferHTTPRequestToFastHTTP failed",
			url:          websocketURI,
			exceptedCode: http.StatusInternalServerError,
			header:       mockResHeader,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(transferHTTPRequestToFastHTTP, func(req *fhttp.Request, r *http.Request) error {
						return errors.New("mock error")
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket deleting request normal case",
			url:          websocketURI,
			method:       http.MethodDelete,
			exceptedCode: http.StatusOK,
			body:         []byte(`{"instanceKey": "default-#-pool30","processorID":1}`),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(doReleaseReq, func(funcKeyWithRes string, reqInfo *model.FunctionRequestInfo,
						releaseInstanceInfo *schedulerapi.ReleaseInstanceReqFromRdispatcher) error {
						return nil
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket deleting request do release returned err",
			url:          websocketURI,
			method:       http.MethodDelete,
			exceptedCode: http.StatusInternalServerError,
			body:         []byte(`{"instanceKey": "default-#-pool30","processorID":1}`),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(doReleaseReq, func(funcKeyWithRes string, reqInfo *model.FunctionRequestInfo,
						releaseInstanceInfo *schedulerapi.ReleaseInstanceReqFromRdispatcher) error {
						return errors.New("mock error")
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket deleting request bad request body",
			url:          websocketURI,
			method:       http.MethodDelete,
			exceptedCode: http.StatusInternalServerError,
			body:         []byte(`{`),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				return patches
			},
		},
		{
			name:         "websocket deleting request transferHTTPRequestToFastHTTP failed",
			url:          websocketURI,
			method:       http.MethodDelete,
			exceptedCode: http.StatusInternalServerError,
			header:       mockResHeader,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(transferHTTPRequestToFastHTTP, func(req *fhttp.Request, r *http.Request) error {
						return errors.New("mock error")
					}),
				})
				return patches
			},
		},
		{
			name:         "websocket request with specific websocket path of runtime",
			url:          websocketURI,
			exceptedCode: http.StatusOK,
			method:       http.MethodPost,
			header:       mockResHeader,
			body: []byte(`{"instanceIP":"7.218.82.252","instancePort":"8765","requestBody":{"body":"",
						"httpMethod":"GET","pathParameters":{},"queryStringParameters":{},"path":"\/web","isBase64Encoded":true,
						"requestContext":{"apiId":"5906b8c40aaf4e6ba1b4f00528f81949","stage":"RELEASE","requestId":"bcc2cb1f7a0e6e644c01fec2dd4fad5c"},
						"headers":{"user-agent":"Apifox\/1.0.0 (https:\/\/apifox.com)","x-forwarded-for":"10.164.185.156","sec-websocket-version":
						"13","x-forwarded-proto":"https","sec-websocket-key":"NePjcdAvpAdiU4B+0v5IKQ==","connection":"Upgrade","x-forwarded-port":
						"443","sec-websocket-extensions":"permessage-deflate; client_max_window_bits","x-forwarded-host":"100.85.123.59","x-real-ip":
						"10.164.185.156","x-request-id":"bcc2cb1f7a0e6e644c01fec2dd4fad5c","upgrade":"websocket","host":"100.85.123.59"}}}`),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
						req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return mockFaasSchedulerNormalResponse, nil
					}),
					gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(
						_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
						return mockClient
					}),
					gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID,
						version string) (*metadata.FunctionMetaData, bool) {
						return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeWebSocket}}, true
					}),
				})
				return patches
			},
		},
		{
			name:         "get stream failed",
			method:       http.MethodGet,
			url:          "/serverless/v1/stream/instance",
			exceptedCode: http.StatusInternalServerError,
			body:         make([]byte, 2*1024, 2*1024),
		},
		{
			name:         "get stream success",
			method:       http.MethodGet,
			url:          "/serverless/v1/stream/instance",
			exceptedCode: http.StatusOK,
			body:         make([]byte, 2*1024, 2*1024),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(stream.GetRandomStreamsAddr, func() (string, error) {
						return "127.0.0.1", nil
					}),
				})
				return patches
			},
		},
		{
			name:         "invalid stream request",
			method:       http.MethodPost,
			url:          "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/stream-invoke",
			exceptedCode: http.StatusBadRequest,
			body:         make([]byte, 2*1024, 2*1024),
		},
		{
			name:         "normal stream request",
			url:          "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/stream-invoke",
			method:       http.MethodPost,
			header:       mockStreamHeader,
			exceptedCode: http.StatusOK,
			body:         make([]byte, 2*1024, 2*1024),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(commonStream.ReceiveStream, func(option *commonStream.ClientOption) bool {
						option.Writer.WriteHeader(http.StatusOK)
						return true
					}),
				})
				return patches
			},
		},
		{
			name:         "normal instance recover request",
			url:          "/serverless/v1/functions/instances/recover",
			method:       http.MethodPost,
			exceptedCode: http.StatusOK,
		},
		{
			name:         "cae request, startListenFrontendResponseStream success",
			url:          "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations",
			exceptedCode: http.StatusOK,
			body:         make([]byte, 2*1024, 2*1024),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getInsResource, func(_ *model.FunctionRequestInfo, _ *fhttp.Request) (types.InstanceResource, bool) {
						return types.InstanceResource{
							CPU:    "0",
							Memory: "0",
						}, true
					}),
					gomonkey.ApplyFunc(startListenFrontendResponseStream, func(_ string) error {
						return nil
					}),
				})
				return patches
			},
		},
		{
			name:         "cae request, startListenFrontendResponseStream error",
			url:          "/serverless/v1/functions/sn:cn:yrk:01234567890123456789012345678901:function:0@default@test_func:latest/invocations",
			exceptedCode: http.StatusOK,
			body:         make([]byte, 2*1024, 2*1024),
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getInsResource, func(_ *model.FunctionRequestInfo, _ *fhttp.Request) (types.InstanceResource, bool) {
						return types.InstanceResource{
							CPU:    "0",
							Memory: "0",
						}, true
					}),
					gomonkey.ApplyFunc(startListenFrontendResponseStream, func(_ string) error {
						return errors.New("error")
					}),
					gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, _ alarm.Type, _ string) {}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var patches mockutils.PatchSlice
			if tt.patchesFunc != nil {
				patches = tt.patchesFunc()
			}

			r := bytes.NewReader(tt.body)
			var method string
			if tt.method == "" {
				method = http.MethodPost
			} else {
				method = tt.method
			}
			req, _ := http.NewRequest(method, tt.url, r)
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptedCode, w.Result().StatusCode, tt.name)
			if patches != nil {
				patches.ResetAll()
			}
		})
	}
}

func TestGetTraceInfo(t *testing.T) {
	convey.Convey("TestGetTraceInfo", t, func() {
		start := time.Now()

		resp := fhttp.AcquireResponse()
		resp.Header.Set(constants.HeaderCallNode, "1")
		resp.Header.Set(constants.HeaderCallInstance, "2")
		resp.Header.Set(constants.HeaderWorkerCost, "1000")
		traceInfo := getTraceInfo(start, 10*time.Millisecond, resp)

		convey.So(traceInfo.CallNode, convey.ShouldEqual, "1")
		convey.So(traceInfo.CallInstance, convey.ShouldEqual, "2")
		convey.So(traceInfo.workerCost, convey.ShouldEqual, time.Duration(1000)*time.Nanosecond)
	})
}

// Test_processStateID -
func Test_processStateID(t *testing.T) {
	convey.Convey("test: process state id", t, func() {
		req := new(model.FunctionRequestInfo)
		ctx := &gin.Context{
			Request: &http.Request{
				Header: map[string][]string{},
			},
		}
		// state id or stateName is nil
		processStateID(req, ctx)
		convey.So(req.StateID, convey.ShouldEqual, "")
		// stateID exists
		ctx.Request.Header.Set(constants.HeaderStateID, "testID")
		processStateID(req, ctx)
		convey.So(req.StateID, convey.ShouldEqual, "testID")
		ctx.Request.Header.Del(constants.HeaderStateID)
		req.StateID = ""
		// invalid funcName
		req.Name = "service@funcName"
		ctx.Request.Header.Set(constants.HeaderStateKey, "testKey")
		processStateID(req, ctx)
		convey.So(req.StateID, convey.ShouldEqual, "")
		// succeed to process stateID
		req.Name = "0@service@funcName"
		req.TenantID = "tenantID"
		urnutils.Separator = "@"
		processStateID(req, ctx)
		convey.So(req.StateKey, convey.ShouldEqual, "testKey")
		convey.So(req.StateID, convey.ShouldEqual, utils.GenStateIDByKey("tenantID", "0@service",
			"funcName", "testKey"))
	})
}

func Test_websocketFilter(t *testing.T) {
	mockClient, _ := grpcconn.CreateGrpcClient(common.RPCClientConfig{})
	getRequestDeadlineMockFunc := gomonkey.ApplyFunc(getRequestDeadline, func(reqInfo *model.FunctionRequestInfo) time.Duration {
		return time.Second
	})
	getSchedulerMockFunc := gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
		return mockClient
	})
	convey.Convey("test websocket filter", t, func() {
		constant.InitDefaultConfig()
		constant.FConfig.LoadBalanceConfig.ConcurrencyThreshold = 10
		remotescheduler.CreateInstance()
		req := fhttp.AcquireRequest()
		defer fhttp.ReleaseRequest(req)
		insResource := types.InstanceResource{
			CPU:    "0.5",
			Memory: "200M",
		}
		convey.Convey("test get metadata failed", func() {
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return nil, false
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("test not websocket businessType", func() {
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: "other"}}, true
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("test websocket scheduler client not exist", func() {
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeWebSocket}}, true
				}),
				getRequestDeadlineMockFunc,
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return nil
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test websocket businessType but acquire instance failed", func() {
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeWebSocket}}, true
				}),
				getRequestDeadlineMockFunc,
				getSchedulerMockFunc,
				gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
					req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
					return &schedulerapi.AcquireInstanceRsp{
						Error: schedulerapi.Error{
							ErrorCode: 500,
						},
					}, errors.New("mock error")
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test websocket businessType but acquire instance failed 2", func() {
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeWebSocket}}, true
				}),
				getSchedulerMockFunc,
				getRequestDeadlineMockFunc,
				gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
					req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
					return &schedulerapi.AcquireInstanceRsp{
						Error: schedulerapi.Error{
							ErrorCode: 500,
						},
					}, nil
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test websocket businessType and acquire instance successfully", func() {
			mockFaasSchedulerNormalResponse := &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  "default-#-pool300xxx",
				ProcessorID:  0,
				InstanceIP:   "127.0.0.2",
				InstancePort: "8888",
				NodeIP:       "127.0.0.3",
				NodePort:     "22423",
				Error:        schedulerapi.Error{},
			}
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{BusinessType: constants.BusinessTypeWebSocket}}, true
				}),
				getRequestDeadlineMockFunc,
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return mockClient
				}),
				gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
					req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
					return mockFaasSchedulerNormalResponse, nil
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reqInfo.WSReqInfo.InstanceKey, convey.ShouldEqual, "default-#-pool300xxx")
		})
		convey.Convey("cae enable remoteDebug and acquire instance successfully", func() {
			mockFaasSchedulerNormalResponse := &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  "default-#-customcaedebug",
				ProcessorID:  0,
				InstanceIP:   "127.0.0.2",
				InstancePort: "8888",
				NodeIP:       "127.0.0.3",
				NodePort:     "22423",
				Error:        schedulerapi.Error{},
			}
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType:      constants.BusinessTypeCAE,
						EnableRemoteDebug: true,
					}}, true
				}),
				getRequestDeadlineMockFunc,
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return mockClient
				}),
				gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
					req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
					return mockFaasSchedulerNormalResponse, nil
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reqInfo.WSReqInfo.InstanceKey, convey.ShouldEqual, "default-#-customcaedebug")
		})
		convey.Convey("acquire instance successfully when cae both enable remoteDebug and websocket ", func() {
			mockFaasSchedulerNormalResponse := &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  "default-#-customcaedebug",
				ProcessorID:  0,
				InstanceIP:   "127.0.0.2",
				InstancePort: "8888",
				NodeIP:       "127.0.0.3",
				NodePort:     "22423",
				Error:        schedulerapi.Error{},
			}
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType:      constants.BusinessTypeCAE,
						EnableRemoteDebug: true,
					},
						ExtendedMetaData: functiontypes.ExtendedMetaData{EnableWebsocket: true}}, true
				}),
				getRequestDeadlineMockFunc,
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return mockClient
				}),
				gomonkey.ApplyFunc(acquireInstance, func(scheduler common.RPCClient,
					req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
					return mockFaasSchedulerNormalResponse, nil
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reqInfo.WSReqInfo.InstanceKey, convey.ShouldEqual, "default-#-customcaedebug")
		})
		convey.Convey("no need acquire instance when cae only enable websocket", func() {
			reqInfo := &model.FunctionRequestInfo{}
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
					return &metadata.FunctionMetaData{FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType:      constants.BusinessTypeCAE,
						EnableRemoteDebug: false,
					},
						ExtendedMetaData: functiontypes.ExtendedMetaData{EnableWebsocket: true}}, true
				}),
				getRequestDeadlineMockFunc,
				gomonkey.ApplyFunc((*remotescheduler.Manager).GetScheduler, func(_ *remotescheduler.Manager, funcKeyWithRes string) common.RPCClient {
					return mockClient
				}),
			})
			defer patches.ResetAll()
			err := websocketFilter(reqInfo, req, insResource)
			convey.So(err, convey.ShouldBeNil)
			convey.So(reqInfo.WSReqInfo.InstanceKey, convey.ShouldEqual, "")
		})
	})
}

func TestGetRequestContentLength(t *testing.T) {
	t.Run("normal request", func(t *testing.T) {
		request := &http.Request{
			ContentLength: 100,
			Header:        map[string][]string{constants.ContentType: {constants.ApplicationJSON}},
		}
		actual := getRequestContentLength(request)
		assert.Equal(t, int64(100), actual)
	})
	t.Run("upload more than 10MB", func(t *testing.T) {
		request := &http.Request{
			ContentLength: httpStreamContentLength + 1,
			Header:        map[string][]string{constants.ContentType: {"multipart/form-data"}},
		}
		actual := getRequestContentLength(request)
		assert.Equal(t, int64(httpStreamContentLength), actual)
	})
	t.Run("upload not more than 10MB", func(t *testing.T) {
		request := &http.Request{
			ContentLength: httpStreamContentLength - 1,
			Header:        map[string][]string{constants.ContentType: {"multipart/form-data"}},
		}
		actual := getRequestContentLength(request)
		assert.Equal(t, int64(httpStreamContentLength-1), actual)
	})
}

func TestTransferHTTPRequestToFastHTTP(t *testing.T) {
	t.Run("normal request", func(t *testing.T) {
		req := fhttp.AcquireRequest()
		defer fhttp.ReleaseRequest(req)
		request := &http.Request{
			ContentLength: 0,
			Header:        map[string][]string{constants.ContentType: {constants.ApplicationJSON}},
			Body:          MockReadCloser{},
		}
		err := transferHTTPRequestToFastHTTP(req, request)
		assert.Nil(t, err)
	})
	t.Run("http stream request", func(t *testing.T) {
		req := fhttp.AcquireRequest()
		defer fhttp.ReleaseRequest(req)
		request := &http.Request{
			ContentLength: 0,
			Header: map[string][]string{
				constants.ContentType: {"multipart/form-data"},
			},
			Body: MockReadCloser{},
		}
		request.Header.Set(constants.HeaderRequestStreamName, "streamName")
		request.Header.Set(constants.HeaderStreamAPIGEvent, "apigEvent")
		err := transferHTTPRequestToFastHTTP(req, request)
		assert.Nil(t, err)
		assert.Equal(t, []byte("apigEvent"), req.Body())
		assert.Equal(t, constants.ApplicationJSON, string(req.Header.ContentType()))
	})
}

func TestHttpStreamInvokeHandler(t *testing.T) {
	c := &gin.Context{Request: &http.Request{Header: map[string][]string{}}}
	t.Run("NOT enable support stream request", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{TraceID: "testTraceID"}
		responseWriter := &fakeResponseWriter{}
		c.Writer = responseWriter
		constant.FConfig.DataSystem.StreamEnable = false
		c.Request = &http.Request{Header: map[string][]string{constants.ContentType: {"multipart/form-data"}}}
		httpStreamInvokeHandler(c, reqInfo, time.Now())
		expect, _ := json.Marshal(InvokeErrorResponse{Code: statuscode.HTTPStreamNOTEnableError,
			Message: errInter.Error()})
		assert.Equal(t, expect, responseWriter.body)
	})
}

func TestProcessResp(t *testing.T) {
	t.Run("failed to process http stream request", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{}
		writer := &fakeResponseWriter{}
		resp := &fhttp.Response{}
		resp.SetBody([]byte{1})
		// mock requestBodyToStream function set RequestStreamErrorCode
		reqInfo.SetRequestStreamErrorCode(statuscode.QueryStreamCustomerError)
		data, err := json.Marshal(InvokeErrorResponse{
			Code:    statuscode.QueryStreamCustomerError,
			Message: errInter.Error(),
		})
		processResp(reqInfo, resp, writer, nil)
		assert.Nil(t, err)
		assert.Equal(t, data, writer.body)
		assert.Equal(t, 500, writer.statusCode)
	})
	t.Run("process normal response", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{}
		writer := &fakeResponseWriter{header: http.Header{}}
		resp := &fhttp.Response{}
		resp.SetBody([]byte{1})
		resp.SetStatusCode(http.StatusOK)
		processResp(reqInfo, resp, writer, nil)
		assert.Equal(t, []byte{1}, writer.body)
		assert.Equal(t, http.StatusOK, writer.statusCode)
	})
	t.Run("not a stream response", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{}
		writer := &fakeResponseWriter{header: http.Header{}}
		resp := &fhttp.Response{}
		patch := gomonkey.ApplyFunc(CheckIsResponseStream, func(traceID string) bool {
			return false
		})
		defer patch.Reset()
		resp.SetBody([]byte{1})
		resp.SetStatusCode(http.StatusOK)
		processResp(reqInfo, resp, writer, NewStopChan())
		assert.Equal(t, []byte{1}, writer.body)
		assert.Equal(t, http.StatusOK, writer.statusCode)
	})
	t.Run("is a stream response", func(t *testing.T) {
		reqInfo := &model.FunctionRequestInfo{}
		writer := &fakeResponseWriter{header: http.Header{}}
		resp := &fhttp.Response{}
		patch := gomonkey.ApplyFunc(CheckIsResponseStream, func(traceID string) bool {
			return true
		})
		stopChan := NewStopChan()
		defer func() {
			patch.Reset()
		}()
		resp.SetBody([]byte{1})
		resp.SetStatusCode(http.StatusOK)
		go func() {
			time.Sleep(1 * time.Second)
			stopChan.SafeClose()
		}()
		processResp(reqInfo, resp, writer, stopChan)
		assert.Equal(t, 0, writer.statusCode)
	})
}

func TestGenerateWsPath(t *testing.T) {
	t.Run("only path", func(t *testing.T) {
		rdispatcherInfo := &schedulerapi.WebSocketInsAcquireConfig{
			RequestBody: map[string]any{
				apigEventBodyPath: "/path/test",
			},
		}
		wssPath := generateWsPath(rdispatcherInfo)
		assert.Equal(t, "/path/test", wssPath)
	})
	t.Run("path with queryString", func(t *testing.T) {
		rdispatcherInfo := &schedulerapi.WebSocketInsAcquireConfig{
			RequestBody: map[string]any{
				apigEventBodyPath: "/path/test",
				apigEventBodyQueryStringParameters: map[string]interface{}{
					"a": "b",
					"c": "d",
				},
			},
		}
		wssPath := generateWsPath(rdispatcherInfo)
		assert.Equal(t, "/path/test?a=b&c=d", wssPath)
	})
	t.Run("path with queryString is empty", func(t *testing.T) {
		rdispatcherInfo := &schedulerapi.WebSocketInsAcquireConfig{
			RequestBody: map[string]any{
				apigEventBodyPath:                  "/path/test",
				apigEventBodyQueryStringParameters: map[string]interface{}{},
			},
		}
		wssPath := generateWsPath(rdispatcherInfo)
		assert.Equal(t, "/path/test", wssPath)
	})
	t.Run("RequestBody is nil", func(t *testing.T) {
		rdispatcherInfo := &schedulerapi.WebSocketInsAcquireConfig{
			RequestBody: nil,
		}
		wssPath := generateWsPath(rdispatcherInfo)
		assert.Equal(t, "", wssPath)
	})
	t.Run("path with queryInterfaceArr", func(t *testing.T) {
		rdispatcherInfo := &schedulerapi.WebSocketInsAcquireConfig{
			RequestBody: map[string]any{
				apigEventBodyPath: "/path/test",
				apigEventBodyQueryStringParameters: map[string]interface{}{
					"c": []interface{}{"1", "2"},
				},
			},
		}
		wssPath := generateWsPath(rdispatcherInfo)
		assert.Equal(t, "/path/test?c=1&c=2", wssPath)
	})
}

type MockRPCClient struct {
	scheduler   schedulerapi.Scheduler
	retainError error
}

func (c *MockRPCClient) Start() {}

func (c *MockRPCClient) Stop() {}

func (c *MockRPCClient) IsEnable() bool {
	return true
}

func (c *MockRPCClient) ServerKey() string {
	return ""
}

func (c *MockRPCClient) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	rsp, err := c.scheduler.AcquireInstance(req)
	return rsp, err
}

func (c *MockRPCClient) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	return errors.New("failed to releaseInstance")
}

// CheckInstance -
func (c *MockRPCClient) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
	return nil
}

func (c *MockRPCClient) RetainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	if c.retainError != nil {
		return nil, c.retainError
	}
	rsp, err := c.scheduler.RetainInstance(req)
	return rsp, err
}

func Test_releaseInstance(t *testing.T) {
	type args struct {
		scheduler  common.RPCClient
		releaseReq *schedulerapi.ReleaseInstanceReq
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"test release instance",
			args{
				scheduler: &MockRPCClient{},
				releaseReq: &schedulerapi.ReleaseInstanceReq{
					TraceID: "test",
				},
			},
			true,
		},
	}
	for _, tt := range tests {
		err := releaseInstance(tt.args.scheduler, tt.args.releaseReq)
		if (err != nil) != tt.wantErr {
			t.Errorf("failed to test releaseInstance")
		}
	}
}

func TestAddMetricsHeader(t *testing.T) {
	f2 := &fhttp.Response{}
	f2.Header.Set(constants.HeaderCallNode, "127.0.0.1")
	f2.Header.Set(constants.HeaderCallInstance, "defaultaz-#-pod01")
	f2.Header.Set(constants.HeaderPodName, "bus01")

	f3 := &fhttp.Response{}
	f3.Header.Set(constants.HeaderCallNode, "127.0.0.2")
	f3.Header.Set(constants.HeaderCallInstance, "defaultaz-#-pod02")
	f3.Header.Set(constants.HeaderPodName, "bus02")

	f4 := &fhttp.Response{}
	f4.Header.Set(constants.HeaderCallNode, "127.0.0.3")
	f4.Header.Set(constants.HeaderCallInstance, "pod03")
	f4.Header.Set(constants.HeaderPodName, "bus03")

	f5 := &fhttp.Response{}
	f5.Header.Set(constants.HeaderCallNode, "127.0.0.4")
	f5.Header.Set(constants.HeaderCallInstance, "defaultaz-#-pod04")
	f5.Header.Set(constants.HeaderPodName, "bus04")

	tests := []struct {
		c            *gin.Context
		resp         *fhttp.Response
		reqInfo      *model.FunctionRequestInfo
		name         string
		expectedCode string
		expectedNode string
		expectedPod  string
	}{
		{
			name:         "case01",
			c:            gin.CreateTestContextOnly(httptest.NewRecorder(), gin.Default()),
			resp:         &fhttp.Response{},
			reqInfo:      &model.FunctionRequestInfo{InnerCode: 0},
			expectedCode: "",
			expectedNode: "",
			expectedPod:  "",
		},
		{
			name:         "case02",
			c:            gin.CreateTestContextOnly(httptest.NewRecorder(), gin.Default()),
			resp:         f2,
			reqInfo:      &model.FunctionRequestInfo{InnerCode: 161900},
			expectedCode: "161900",
			expectedNode: "127.0.0.1",
			expectedPod:  "pod01",
		},
		{
			name:         "case03",
			c:            gin.CreateTestContextOnly(httptest.NewRecorder(), gin.Default()),
			resp:         f3,
			reqInfo:      &model.FunctionRequestInfo{InnerCode: 4010},
			expectedCode: "4010",
			expectedNode: "127.0.0.2",
			expectedPod:  "pod02",
		},
		{
			name:         "case04",
			c:            gin.CreateTestContextOnly(httptest.NewRecorder(), gin.Default()),
			resp:         f4,
			reqInfo:      &model.FunctionRequestInfo{InnerCode: 211406},
			expectedCode: "211406",
			expectedNode: "127.0.0.3",
			expectedPod:  "bus03",
		},
		{
			name:         "case05",
			c:            gin.CreateTestContextOnly(httptest.NewRecorder(), gin.Default()),
			resp:         f5,
			reqInfo:      &model.FunctionRequestInfo{InnerCode: 150430},
			expectedCode: "150430",
			expectedNode: "",
			expectedPod:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addMetricsHeader(tt.c.Writer, tt.reqInfo, tt.resp)
			assert.Equal(t, tt.expectedCode, tt.c.Writer.Header().Get(constants.HeaderErrorCode))
			assert.Equal(t, tt.expectedNode, tt.c.Writer.Header().Get(constants.HeaderErrorNode))
			assert.Equal(t, tt.expectedPod, tt.c.Writer.Header().Get(constants.HeaderErrorPod))
		})
	}
}
