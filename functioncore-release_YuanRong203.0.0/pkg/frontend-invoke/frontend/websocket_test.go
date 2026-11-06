/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package frontend deal with websocket
package frontend

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/frontend-invoke/constant"
	mockutils "functioncore/test/utils"
)

func Test_GetOrAddWebsocketWatcher(t *testing.T) {
	convey.Convey("Test watcher not exist and add", t, func() {
		watcher := Instance().GetOrAddWebsocketWatcher("123")
		convey.So(watcher.Upstream, convey.ShouldBeNil)
		convey.So(watcher.Downstream, convey.ShouldBeNil)
	})
}

func Test_GetWebsocketWatcher(t *testing.T) {
	convey.Convey("Test watcher not exist", t, func() {
		manager := &WebsocketManager{
			socketManager: make(map[string]*WebSocketWatcher),
			socketLock:    sync.RWMutex{},
		}
		manager.socketManager["111"] = &WebSocketWatcher{}
		watcher := manager.GetWebsocketWatcher("123")
		convey.So(watcher, convey.ShouldBeNil)
		watcher = manager.GetWebsocketWatcher("111")
		convey.So(watcher, convey.ShouldNotBeNil)
	})
}

func Test_ReleaseWebsocketWatcher(t *testing.T) {
	convey.Convey("Test release", t, func() {
		manager := &WebsocketManager{
			socketManager: make(map[string]*WebSocketWatcher),
			socketLock:    sync.RWMutex{},
		}
		manager.socketManager["111"] = &WebSocketWatcher{}
		manager.ReleaseWebsocketWatcher("111")
		convey.So(manager.socketManager["111"], convey.ShouldBeNil)
	})
}

func TestWebSocketWorkerConnect(t *testing.T) {
	constant.InitDefaultConfig()
	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{}
	constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	urnutils.Separator = "@"
	constant.FConfig.AuthenticationEnable = true
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
	}()

	router := gin.New()
	router.GET("/serverless/v1/functions/:podName/:requestId/:projectId/:function-name/:version/websocket", WebSocketWorkerConnect)
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
			name:         "empty podName",
			url:          "/serverless/v1/functions//traceID-123456/projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "empty traceID",
			url:          "/serverless/v1/functions/testPodName//projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "empty projectID",
			url:          "/serverless/v1/functions/testPodName/traceID-123456//0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "empty funcName",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID//latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "empty version",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc//websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "invalid functionName",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@_123/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name:         "invalid serviceID",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@_123@func/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
		},
		{
			name: "failed to authCheck",
			url:  "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc/latest/websocket",
			header: map[string]string{
				constants.HeaderAuthorization: "invalid Authorization",
			},
			method:       http.MethodGet,
			exceptedCode: http.StatusUnauthorized,
		},
		{
			name:         "not found watcher",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusBadRequest,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string,
						timestamp string, duration int) error {
						return nil
					}),
				})
				return patches
			},
		},
		{
			name:         "funcUrn is not equal",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusForbidden,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string,
						timestamp string, duration int) error {
						return nil
					}),
					gomonkey.ApplyFunc((*WebsocketManager).GetWebsocketWatcher, func(
						_ *WebsocketManager, traceID string) *WebSocketWatcher {
						return &WebSocketWatcher{FuncKey: "testFunc1"}
					}),
				})
				return patches
			},
		},
		{
			name:         "failed to connect rdispatcher",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusInternalServerError,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string,
						timestamp string, duration int) error {
						return nil
					}),
					gomonkey.ApplyFunc((*WebsocketManager).GetWebsocketWatcher, func(
						_ *WebsocketManager, traceID string) *WebSocketWatcher {
						return &WebSocketWatcher{FuncKey: "projectID/0@default@testFunc/latest"}
					}),
					gomonkey.ApplyFunc(createUpstreamWssConn, func(targetIP, targetPort, podName, traceID,
						functionKey string) (*commonwebsocket.WssConnection, error) {
						return nil, errors.New("create failed")
					}),
				})
				return patches
			},
		},
		{
			name:         "failed to upgrade request to websocket",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusInternalServerError,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string,
						timestamp string, duration int) error {
						return nil
					}),
					gomonkey.ApplyFunc((*WebsocketManager).GetWebsocketWatcher, func(
						_ *WebsocketManager, traceID string) *WebSocketWatcher {
						return &WebSocketWatcher{FuncKey: "projectID/0@default@testFunc/latest"}
					}),
					gomonkey.ApplyFunc(createUpstreamWssConn, func(targetIP, targetPort, podName, traceID,
						functionKey string) (*commonwebsocket.WssConnection, error) {
						return &commonwebsocket.WssConnection{}, nil
					}),
					gomonkey.ApplyFunc(commonwebsocket.Upgrade, func(w http.ResponseWriter, r *http.Request,
						responseHeader http.Header) (*websocket.Conn, error) {
						return nil, errors.New("upgrade failed")
					}),
				})
				return patches
			},
		},
		{
			name:         "succeed to connect and forward message",
			url:          "/serverless/v1/functions/testPodName/traceID-123456/projectID/0@default@testFunc/latest/websocket",
			method:       http.MethodGet,
			exceptedCode: http.StatusOK,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(patched)
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string,
						timestamp string, duration int) error {
						return nil
					}),
					gomonkey.ApplyFunc((*WebsocketManager).GetWebsocketWatcher, func(
						_ *WebsocketManager, traceID string) *WebSocketWatcher {
						return &WebSocketWatcher{FuncKey: "projectID/0@default@testFunc/latest"}
					}),
					gomonkey.ApplyFunc(createUpstreamWssConn, func(targetIP, targetPort, podName, traceID,
						functionKey string) (*commonwebsocket.WssConnection, error) {
						return &commonwebsocket.WssConnection{}, nil
					}),
					gomonkey.ApplyFunc(commonwebsocket.Upgrade, func(w http.ResponseWriter, r *http.Request,
						responseHeader http.Header) (*websocket.Conn, error) {
						return &websocket.Conn{}, nil
					}),
					gomonkey.ApplyFunc((*commonwebsocket.WssConnection).ToString, func(
						_ *commonwebsocket.WssConnection) string {
						return "123"
					}),
					gomonkey.ApplyFunc((*WebSocketWatcher).release, func(
						_ *WebSocketWatcher) {
					}),
					gomonkey.ApplyFunc(commonwebsocket.Connect,
						func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
							return &commonwebsocket.WssConnection{}, nil
						}),
					gomonkey.ApplyFunc((*WebSocketWatcher).ForwardMessageLoop,
						func(_ *WebSocketWatcher, from *commonwebsocket.WssConnection,
							to *commonwebsocket.WssConnection, msgHandler WssMsgHandlerFunc) {
							return
						}),
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
			req, _ := http.NewRequest(tt.method, tt.url, r)
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

func TestWssConnectionPair_forwardMessage(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case1 read msg error",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, errors.New("error")
					}),
					gomonkey.ApplyFunc((*commonwebsocket.WssConnection).TrySendMessage,
						func(_ *commonwebsocket.WssConnection, message commonwebsocket.WssMessage) error {
							return nil
						}),
				})
				return patches
			},
		}, {
			name: "case2 wrap msg error",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, nil
					}),
					gomonkey.ApplyFunc(commonwebsocket.UnwrapSystemInfo, func(data []byte) (string, []byte, error) {
						return "", nil, errors.New("mock error")
					}),
				})
				return patches
			},
		}, {
			name: "case3 send msg successfully",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, nil
					}),
					gomonkey.ApplyFunc(commonwebsocket.UnwrapSystemInfo, func(data []byte) (string, []byte, error) {
						return "test trace id", []byte("mock body"), nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &WebSocketWatcher{
				TraceID: "ID",
				Downstream: &commonwebsocket.WssConnection{
					Connection:  &websocket.Conn{},
					ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
					CloseChan:   make(chan struct{}),
					IsClosed:    uint32(1),
				},
				Upstream: &commonwebsocket.WssConnection{
					Connection:  &websocket.Conn{},
					ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
					CloseChan:   make(chan struct{}),
					IsClosed:    uint32(1),
				},
			}
			patches := tt.patchesFunc()
			done := make(chan bool)
			go func() {
				go w.forwardReceivedMessages(w.Upstream, w.Downstream, commonwebsocket.UnwrapSystemInfo)
				go w.sendForwardMessage(w.Downstream)
				time.Sleep(time.Second)
				done <- true
			}()
			isDone, _ := <-done
			if !isDone {
				t.Errorf("forwardMessage failed to test")
			}
			patches.ResetAll()
		})
	}
}

func TestShutDownAllWebsocketConnection(t *testing.T) {
	watcher := Instance().GetOrAddWebsocketWatcher("testShotDown")
	watcher.Downstream = &commonwebsocket.WssConnection{
		Connection:  &websocket.Conn{},
		ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		CloseChan:   make(chan struct{}),
		IsClosed:    uint32(1),
	}
	watcher.Upstream = &commonwebsocket.WssConnection{
		Connection:  &websocket.Conn{},
		ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		CloseChan:   make(chan struct{}),
		IsClosed:    uint32(1),
	}
	ShutDownAllWebsocketConnection()
	assert.NotNil(t, watcher)
	assert.True(t, watcher.Downstream.IsConnClosed())
	assert.True(t, watcher.Upstream.IsConnClosed())
}
