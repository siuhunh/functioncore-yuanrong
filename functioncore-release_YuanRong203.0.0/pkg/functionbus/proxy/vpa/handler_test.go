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

package vpa

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	mockTraceID = "090b5c14-474e-46f0-97bf-2891d6aa79fd"
)

func TestRegisterFunctionInstanceMonitorInSLOAgent(t *testing.T) {
	tests := []struct {
		name        string
		payload     []byte
		want        snerror.SNError
		patchesFunc testutils.PatchesFunc
	}{
		{
			name:    "unmarshal failed",
			payload: []byte(`{"functionName":`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "failed to unmarshal request body from worker manager"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:    "store vpa info failed when request action is ready",
			payload: []byte(`{"action":"ready"}`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "worker broker SetVPAInfo error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*workerbroker.Manager).SetVPAInfo, func(_ *workerbroker.Manager, workerID string, req *vpa.FuncInstanceInfo) (int, string) {
						return proxyerrors.VPARegisterFunctionInstanceErrCode, "worker broker SetVPAInfo error"
					}),
				})
				return p
			},
		}, {
			name:    "send vpa info to SLO agent failed because of make slo agent request error when request action is create",
			payload: []byte(`{"action":"create"}`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "failed to create SLO agent request, inner error message: make slo agent request error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("make slo agent request error")
					}),
				})
				return p
			},
		}, {
			name:    "send vpa info to SLO agent failed because of sending request error when request action is create",
			payload: []byte(`{"action":"create"}`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "failed to send request to SLO agent, inner error message: sending request error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("sending request error")
					}),
				})
				return p
			},
		}, {
			name:    "receive response from SLO agent because of returning non-200 status code when request action is create",
			payload: []byte(`{"action":"create"}`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "failed to send request to SLO agent, status code: 500, inner error code: "),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("{}"))}, nil
					}),
				})
				return p
			},
		}, {
			name:    "receive response with body from SLO agent because of returning non-200 status code when request action is create",
			payload: []byte(`{"action":"create"}`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "failed to send request to SLO agent, status code: 500, inner error code: "),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("body error message"))}, nil
					}),
				})
				return p
			},
		}, {
			name:    "receive response with inner error code from SLO agent because of returning non-200 status code when request action is create",
			payload: []byte(`{"action":"create"}`),
			want:    snerror.New(proxyerrors.VPARegisterFunctionInstanceErrCode, "failed to send request to SLO agent, status code: 500, inner error code: error code from SLO"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader("body error message")), Header: map[string][]string{constants.HeaderInnerCode: {"error code from SLO"}}}, nil
					}),
				})
				return p
			},
		}, {
			name:    "succeed to handle creating action",
			payload: []byte(`{"action":"create"}`),
			want:    nil,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("body message")), Header: map[string][]string{constants.HeaderInnerCode: {"0"}}}, nil
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			got := RegisterFunctionInstanceMonitorInSLOAgent(tt.payload, mockTraceID)
			if tt.want != got && (got == nil || tt.want == nil) {
				t.Errorf("RegisterFunctionInstanceMonitorInSLOAgent() = %v, want %v", got, tt.want)
			}
			if tt.want != nil && got != nil {
				if got.Code() != tt.want.Code() {
					t.Errorf("RegisterFunctionInstanceMonitorInSLOAgent() error code = %v, want %v", got.Code(), tt.want.Code())
				}
				if got.Error() != tt.want.Error() {
					t.Errorf("RegisterFunctionInstanceMonitorInSLOAgent() error message = %v, want %v", got.Error(), tt.want.Error())
				}
			}
			p.ResetAll()
		})
	}
}

func TestRecoverFunctionInstanceMonitorInSLOAgent(t *testing.T) {
	tests := []struct {
		name        string
		status      bool
		ctx         *fasthttp.RequestCtx
		want        snerror.SNError
		patchesFunc testutils.PatchesFunc
	}{
		{
			name:   "normal case",
			status: true,
			ctx:    &fasthttp.RequestCtx{},
			want:   nil,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*workerbroker.Manager).GetAllInstanceVPAInfo, func(_ *workerbroker.Manager, _ string) *vpa.FuncInstanceInfoWrapper {
						return &vpa.FuncInstanceInfoWrapper{FuncInstanceInfo: make([]*vpa.FuncInstanceInfo, 0)}
					}),
				})
				return p
			},
		}, {
			name:   "sync not ready after restarting",
			status: false,
			ctx:    &fasthttp.RequestCtx{},
			want:   nil,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:   "json marshal failed",
			status: true,
			ctx:    &fasthttp.RequestCtx{},
			want:   snerror.New(proxyerrors.VPARecoverFunctionInstanceCacheErrCode, "failed to marshal vpa info in bus, inner error message: json marshal failed"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*workerbroker.Manager).GetAllInstanceVPAInfo, func(_ *workerbroker.Manager, _ string) *vpa.FuncInstanceInfoWrapper {
						return &vpa.FuncInstanceInfoWrapper{FuncInstanceInfo: make([]*vpa.FuncInstanceInfo, 0)}
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("json marshal failed")
					}),
				})
				return p
			},
		}, {
			name:   "body write failed",
			status: true,
			ctx:    &fasthttp.RequestCtx{},
			want:   snerror.New(proxyerrors.VPARecoverFunctionInstanceCacheErrCode, "failed to write response body, inner error message: body write failed"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*workerbroker.Manager).GetAllInstanceVPAInfo, func(_ *workerbroker.Manager, _ string) *vpa.FuncInstanceInfoWrapper {
						return &vpa.FuncInstanceInfoWrapper{FuncInstanceInfo: make([]*vpa.FuncInstanceInfo, 0)}
					}),
					gomonkey.ApplyFunc((*fasthttp.RequestCtx).Write, func(_ *fasthttp.RequestCtx, p []byte) (int, error) {
						return 0, errors.New("body write failed")
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			SetSyncFinishedStatus(tt.status)
			if got := RecoverFunctionInstanceMonitorInSLOAgent(tt.ctx, mockTraceID); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RecoverFunctionInstanceMonitorInSLOAgent() = %v, want %v", got, tt.want)
			}
			p.ResetAll()
		})
	}
}

func TestGetCurrentResourceView(t *testing.T) {
	tests := []struct {
		name            string
		expectedPayload []byte
		expectedError   snerror.SNError
		patchesFunc     testutils.PatchesFunc
	}{
		{
			name:            "make request failed",
			expectedPayload: nil,
			expectedError:   snerror.New(proxyerrors.VPASyncResourceViewErrCode, "failed to make request to SLO agent when syncing resource view, inner error message: make slo agent request error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("make slo agent request error")
					}),
				})
				return p
			},
		}, {
			name:            "send request failed",
			expectedPayload: nil,
			expectedError:   snerror.New(proxyerrors.VPASyncResourceViewErrCode, "failed to send request to SLO agent when syncing resource view, inner error message: send request error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("send request error")
					}),
				})
				return p
			},
		}, {
			name:            "read nil response body",
			expectedPayload: nil,
			expectedError:   snerror.New(proxyerrors.VPASyncResourceViewErrCode, "failed to request SLO agent, status code: 200, error message: "),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
					}),
				})
				return p
			},
		}, {
			name:            "read response body failed",
			expectedPayload: nil,
			expectedError:   snerror.New(proxyerrors.VPASyncResourceViewErrCode, "failed to read response body, error: mock io read error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(""))}, nil
					}),
					gomonkey.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("mock io read error")
					}),
				})
				return p
			},
		}, {
			name:            "normal case",
			expectedPayload: []byte(`{"mockKey":"mockValue"}`),
			expectedError:   nil,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"mockKey":"mockValue"}`))}, nil
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			payload, err := GetCurrentResourceView(mockTraceID)
			if !reflect.DeepEqual(payload, tt.expectedPayload) {
				t.Errorf("GetCurrentResourceView() got payload = %v, expectedPayload %v", payload, tt.expectedPayload)
			}
			if !reflect.DeepEqual(err, tt.expectedError) {
				t.Errorf("GetCurrentResourceView() got err = %v, expectedError %v", err, tt.expectedError)
			}
			p.ResetAll()
		})
	}
}

func TestSyncResourceView(t *testing.T) {
	tests := []struct {
		name          string
		payload       []byte
		expectedError snerror.SNError
		patchesFunc   testutils.PatchesFunc
	}{
		{
			name:          "make request failed",
			payload:       nil,
			expectedError: snerror.New(proxyerrors.VPASyncResourceViewErrCode, "nil resource view payload got from SLO agent"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:          "make request to worker manager failed",
			payload:       []byte(`{}`),
			expectedError: snerror.New(proxyerrors.VPASyncResourceViewErrCode, "failed to make request when syncing resource view to worker manager, inner error message: make worker manager request error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(wmhealth.GetWorkerManagerBaseURL, func() string {
						return "http://10.77.107.57"
					}),
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("make worker manager request error")
					}),
				})
				return p
			},
		}, {
			name:          "send request to worker manager failed",
			payload:       []byte(`{}`),
			expectedError: snerror.New(proxyerrors.VPASyncResourceViewErrCode, "failed to send request to worker manager server when syncing resource view, inner error message: send request error"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc(wmhealth.GetWorkerManagerBaseURL, func() string {
						return "http://10.77.107.57"
					}),
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("send request error")
					}),
					gomonkey.ApplyFunc(utils.GenerateAuthorization, func() (string, string) {
						return "", ""
					}),
				})
				return p
			},
		}, {
			name:          "send request to worker manager and received non-200 status code",
			payload:       []byte(`{}`),
			expectedError: snerror.New(proxyerrors.VPASyncResourceViewErrCode, "worker manager return non-200 status code, statusCode: 500"),
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc(wmhealth.GetWorkerManagerBaseURL, func() string {
						return "http://10.77.107.57"
					}),
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 500, Body: io.NopCloser(strings.NewReader(`{"mockKey":"mockValue"}`))}, nil
					}),
					gomonkey.ApplyFunc(utils.GenerateAuthorization, func() (string, string) {
						return "", ""
					}),
				})
				return p
			},
		}, {
			name:          "normal case",
			payload:       []byte(`{}`),
			expectedError: nil,
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
						return &http.Client{}
					}),
					gomonkey.ApplyFunc(wmhealth.GetWorkerManagerBaseURL, func() string {
						return "http://10.77.107.57"
					}),
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{Header: map[string][]string{}}, nil
					}),
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						if string(req.Header.Get(constants.Authorization)) == "mock auth info" {
							return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"mockKey":"mockValue"}`))}, nil
						}
						return &http.Response{StatusCode: 401, Body: io.NopCloser(strings.NewReader(`{"mockKey":"mockValue"}`))}, nil
					}),
					gomonkey.ApplyFunc(utils.GenerateAuthorization, func() (string, string) {
						return "mock auth info", ""
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			if got := SyncResourceView(tt.payload, mockTraceID); !reflect.DeepEqual(got, tt.expectedError) {
				t.Errorf("SyncResourceView() error = %v, expectedError %v", got, tt.expectedError)
			}
			p.ResetAll()
		})
	}
}

func TestDeleteFuncInsInSLOAgentCallBack(t *testing.T) {
	tests := []struct {
		name        string
		payload     []byte
		vpaConfig   *common.VerticalAutoscalingConfig
		patchesFunc testutils.PatchesFunc
	}{
		{
			name:      "disable VPA config",
			payload:   []byte(`{"mockKey":"mockValue"}`),
			vpaConfig: &common.VerticalAutoscalingConfig{},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:    "nil payload body",
			payload: nil,
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:    "make request to SLO agent failed",
			payload: []byte(`{}`),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequest, func(method string, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("make request to SLO agent request")
					}),
				})
				return p
			},
		}, {
			name:    "normal case",
			payload: []byte(`{}`),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"mockKey":"mockValue"}`))}, nil
					}),
				})
				return p
			},
		}, {
			name:    "failed to send request to SLO agent",
			payload: []byte(`{}`),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("send request error")
					}),
				})
				return p
			},
		}, {
			name:    "read body error",
			payload: []byte(`{}`),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable: 1,
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*http.Client).Do, func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"mockKey":"mockValue"}`))}, nil
					}),
					gomonkey.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("read body error")
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		p := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			common.FConfig.VPAConfig = tt.vpaConfig
			DeleteFuncInsInSLOAgentCallBack(tt.payload, mockTraceID)
		})
		p.ResetAll()
	}
}

func TestRegisterReadyInstanceToSLOAgent(t *testing.T) {
	r := gin.New()
	ts := httptest.NewServer(r)
	oldURL := vpa.SLOAgentInstanceInfoURL
	defer func() {
		ts.Close()
		vpa.SLOAgentInstanceInfoURL = oldURL
	}()

	r.POST("/ok", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	r.POST("/500", func(c *gin.Context) {
		c.Status(http.StatusInternalServerError)
	})
	r.POST("/already", func(c *gin.Context) {
		c.Data(http.StatusBadRequest, "", msgInstanceAlreadyCreated)
	})

	tests := []struct {
		name string
		url  string

		expectedErr bool
	}{
		{
			name:        "request ok",
			url:         ts.URL + "/ok",
			expectedErr: false,
		},
		{
			name:        "request error",
			url:         ts.URL + "/500",
			expectedErr: true,
		},
		{
			name:        "instance already register",
			url:         ts.URL + "/already",
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vpa.SLOAgentInstanceInfoURL = tt.url
			err := SendInstanceInfoToSLOAgent(vpa.FuncInstanceInfo{})
			if tt.expectedErr {
				assert.Error(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
			}
		})
	}
}

func TestSyncWithSLOAgent(t *testing.T) {
	tests := []struct {
		name string
		data interface{}

		expectedErr bool
		expectedIns map[string]struct{}
	}{
		{
			name:        "get resource view failed",
			data:        make(chan struct{}),
			expectedErr: true,
		},
		{
			name:        "unmarshal failed",
			data:        "12345",
			expectedErr: true,
		},
		{
			name: "sync with slo-agent",
			data: SLOViewResp{
				InstanceList: InsView{
					VpaInsView: []vpa.FuncInstanceInfo{
						{PodName: "podA"},
						{PodName: "podB"},
					},
				},
			},
			expectedErr: false,
			expectedIns: map[string]struct{}{
				"podA": {},
				"podB": {},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deleted := make(map[string]struct{})
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(SendInstanceInfoToSLOAgent, func(info vpa.FuncInstanceInfo) error {
					if info.Action == vpa.FuncInsDelete {
						deleted[info.PodName] = struct{}{}
					}
					return nil
				}),
				gomonkey.ApplyFunc(GetCurrentResourceView, func(string) ([]byte, snerror.SNError) {
					data, err := json.Marshal(tt.data)
					if err != nil {
						return nil, snerror.New(1111, err.Error())
					}
					return data, nil
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()

			err := SyncWithSLOAgent()
			if tt.expectedErr {
				assert.Error(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
				assert.Equal(t, tt.expectedIns, deleted, tt.name)
			}
		})
	}
}
