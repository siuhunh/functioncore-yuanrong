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

package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
	mockUtils "functioncore/test/utils"
)

func TestSendBuildWebSocketConRequestToWorker(t *testing.T) {
	config.InitDefaultConfig()
	config.FConfig = &config.Config{
		LocalAuth: &localauth.AuthConfig{
			AKey:     "000",
			SKey:     "111",
			Duration: 5,
		},
		HTTPSConfig: &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
			TLSProtocol: "TLSv1.2",
			TLSCiphers:  "TLS_ECDHE_RSA",
		},
	}
	type args struct {
		podIP   string
		podPort string
		traceID string
		reqBody []byte
	}
	tests := []struct {
		name           string
		args           args
		wantErr        error
		expectedErrMsg string
		patchesFunc    mockUtils.PatchesFunc
	}{
		{
			name: "case 1 send websocket building connection request to worker successfully",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
				reqBody: nil,
			},
			wantErr: nil,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusOK)
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "case 2 send websocket building connection request to worker failed",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
				reqBody: nil,
			},
			wantErr: errors.New("mock error"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusInternalServerError)
						return errors.New("mock error")
					}),
				})
				return patches
			},
		}, {
			name: "case 3 send websocket building connection request to worker failed and returned error code",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
				reqBody: nil,
			},
			wantErr: errors.New("worker-manager internal error"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusInternalServerError)
						resp.SetBody([]byte(`{"code":150200,"message":"worker-manager internal error"}`))
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "case 4 send websocket building connection request to worker failed and returned bad body",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
				reqBody: nil,
			},
			wantErr: errors.New("failed to unmarshal the response body (%s) from worker"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusInternalServerError)
						resp.SetBody([]byte(`}`))
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			err := SendBuildWebSocketConRequestToWorker(
				tt.args.podIP,
				tt.args.podPort,
				tt.args.traceID,
				tt.args.reqBody,
				time.Second,
			)
			if err != tt.wantErr && err.Error() != tt.wantErr.Error() {
				t.Errorf("SendBuildWebSocketConRequestToWorker() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestSendCountWebSocketConRequestToWorker(t *testing.T) {
	config.InitDefaultConfig()
	config.FConfig = &config.Config{
		LocalAuth: &localauth.AuthConfig{
			AKey:     "000",
			SKey:     "111",
			Duration: 5,
		},
		HTTPSConfig: &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
			TLSProtocol: "TLSv1.2",
			TLSCiphers:  "TLS_ECDHE_RSA",
		},
	}
	type args struct {
		podIP   string
		podPort string
		traceID string
	}
	type expected struct {
		total uint32
		ws    uint32
		err   error
	}
	tests := []struct {
		name        string
		args        args
		expected    expected
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 send websocket counting connection request to worker successfully",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
			},
			expected: expected{
				total: 3,
				ws:    2,
				err:   nil,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusOK)
						resp.SetBody([]byte(`{"totalConnNum":3,"wssConnNum":2}`))
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "case 2 send websocket counting connection request to worker failed",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
			},
			expected: expected{
				err: errors.New("mock DoTimeout error"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						return errors.New("mock DoTimeout error")
					}),
				})
				return patches
			},
		}, {
			name: "case 3 send websocket counting connection request to worker then status is not StatusOk",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
			},
			expected: expected{
				err: errors.New("worker-manager internal error"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusInternalServerError)
						resp.SetBody([]byte(`{"code":150200,"message":"worker-manager internal error"}`))
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "case 4 send websocket counting connection request to worker then got bad body",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
			},
			expected: expected{
				err: errors.New("failed to unmarshal the response body from worker when counting connections"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusInternalServerError)
						resp.SetBody([]byte("{"))
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "case 5 send websocket counting connection request to worker then can not unmarshal WorkerWssCountResponse",
			args: args{
				podIP:   "127.0.0.1",
				podPort: "8081",
				traceID: "mockTraceID",
			},
			expected: expected{
				err: errors.New("failed to unmarshal the response body from worker when counting connections"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*fasthttp.Client).DoTimeout, func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, d time.Duration) error {
						resp.SetStatusCode(fasthttp.StatusOK)
						resp.SetBody([]byte("{"))
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			total, ws, err := SendCountWebSocketConRequestToWorker(
				tt.args.podIP,
				tt.args.podPort,
				tt.args.traceID,
			)
			if tt.expected.err != err && tt.expected.err.Error() != err.Error() {
				t.Errorf("SendCountWebSocketConRequestToWorker() err actual = %v, expected %v",
					err, tt.expected.err)
			}
			if tt.expected.total != total {
				t.Errorf("SendCountWebSocketConRequestToWorker() total actual = %d, expected %d",
					total, tt.expected.total)
			}
			if tt.expected.ws != ws {
				t.Errorf("SendCountWebSocketConRequestToWorker() ws actual = %d, expected %d",
					ws, tt.expected.ws)
			}
			patches.ResetAll()
		})
	}
}

func TestPrepareBuildWebSocketConRequestBody(t *testing.T) {
	type expected struct {
		body []byte
		err  error
	}
	tests := []struct {
		name        string
		args        *schedulerapi.AcquireInstanceReq
		expected    expected
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 send websocket counting connection request to worker successfully",
			args: &schedulerapi.AcquireInstanceReq{
				FuncKeyWithRes: "7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/0@default@test/latest/400-256",
				WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{
					TargetIP:   "127.0.0.1",
					TargetPort: "8766",
				},
			},
			expected: expected{
				body: []byte(`{"functionKey":"7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/0@default@test/latest","WebSocketConfig":{"instanceIP":"127.0.0.1","instancePort":"8766"}}`),
				err:  nil,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 2 send websocket counting connection request to worker failed",
			args: &schedulerapi.AcquireInstanceReq{},
			expected: expected{
				body: nil,
				err:  errors.New("json marshal error"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("json marshal error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			body, err := PrepareBuildWebSocketConRequestBody(tt.args)
			if tt.expected.err != err && tt.expected.err.Error() != err.Error() {
				t.Errorf("PrepareBuildWebSocketConRequestBody() err actual = %v, expected %v",
					err, tt.expected.err)
			}
			if !bytes.Equal(tt.expected.body, body) {
				t.Errorf("PrepareBuildWebSocketConRequestBody() body actual = %s, expected %s",
					tt.expected.body, body)
			}
			patches.ResetAll()
		})
	}
}
