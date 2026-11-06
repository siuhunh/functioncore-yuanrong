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

package websocket

import (
	"errors"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
	mockUtils "functioncore/test/utils"
)

func TestBuildStreamOption(t *testing.T) {
	Convey("BuildStreamOption should return correct StreamOption", t, func() {
		traceID := "12345"
		conf := &config.Configuration{
			ServerConfig: config.ServerConfig{
				HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: false},
			},
		}
		req := &schedulerapi.CreateWebSocketConnectionReq{
			WebSocketConfig: schedulerapi.WebSocketInsAcquireConfig{
				TargetIP:   "127.0.0.1",
				TargetPort: "8080",
				Path:       "/userPath",
			},
			FunctionKey: "testFunction",
		}
		expected := &commonwebsocket.StreamOption{
			UpstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://127.0.0.1:8080/v2/fgs/i/worker/PodName/12345/testFunction/websocket",
			},
			DownstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://127.0.0.1:8000/userPath",
			},
		}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyGlobalVar(&podName, "PodName"),
		})
		actual := BuildStreamOption(traceID, req, conf)
		So(actual, ShouldResemble, expected)
		patches.ResetAll()
	})
}

func TestBuildStreamOptionEnableRemoteDebug(t *testing.T) {
	Convey("BuildStreamOption should return correct StreamOption, enable remote debug", t, func() {
		traceID := "12345"
		conf := &config.Configuration{
			ServerConfig: config.ServerConfig{
				HTTPSConfig: tls.InternalHTTPSConfig{HTTPSEnable: false},
			},
		}
		req := &schedulerapi.CreateWebSocketConnectionReq{
			WebSocketConfig: schedulerapi.WebSocketInsAcquireConfig{
				TargetIP:   "127.0.0.1",
				TargetPort: "8080",
			},
			FunctionKey: "testFunction",
		}
		expected := &commonwebsocket.StreamOption{
			UpstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://127.0.0.1:8080/v2/fgs/i/worker/PodName/12345/testFunction/websocket",
			},
			DownstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://127.0.0.1:8801/websocket",
			},
		}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyGlobalVar(&podName, "PodName"),
			gomonkey.ApplyGlobalVar(&enableRemoteDebug, true),
		})
		actual := BuildStreamOption(traceID, req, conf)
		So(actual, ShouldResemble, expected)
		patches.ResetAll()
	})
}

func TestCreateWssConnPair(t *testing.T) {
	Convey("CreateWssConnPair should create WssConnectionPair successfully", t, func() {
		m := WssManager{
			wssCache: make(map[string]*WssConnectionPair),
		}
		traceID := "12345"
		opt := &commonwebsocket.StreamOption{
			UpstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://localhost:8080",
			},
			DownstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://localhost:8081",
			},
		}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(wssConnect,
				func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
					return &commonwebsocket.WssConnection{}, nil
				}),
		})
		pair, err := m.CreateWssConnPair(traceID, opt)
		time.Sleep(time.Millisecond * 100)
		patches.ResetAll()
		So(err, ShouldBeNil)
		So(pair, ShouldNotBeNil)
	})
	Convey("CreateWssConnPair should create WssConnectionPair failed: downstream websocket connect failed", t, func() {
		m := WssManager{
			wssCache: make(map[string]*WssConnectionPair),
		}
		traceID := "12345"
		opt := &commonwebsocket.StreamOption{
			UpstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://localhost:8080",
			},
			DownstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://localhost:8081",
			},
		}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(wssConnect,
				func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
					if opt.URL == "ws://localhost:8081" {
						return nil, errors.New("error")
					}
					return &commonwebsocket.WssConnection{}, nil
				}),
		})
		pair, err := m.CreateWssConnPair(traceID, opt)
		time.Sleep(time.Millisecond * 100)
		patches.ResetAll()
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "cannot connect user's websocket server")
		So(pair, ShouldBeNil)
	})
	Convey("CreateWssConnPair should create WssConnectionPair failed: upstream websocket connect failed", t, func() {
		m := WssManager{
			wssCache: make(map[string]*WssConnectionPair),
		}
		traceID := "12345"
		opt := &commonwebsocket.StreamOption{
			UpstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://localhost:8080",
			},
			DownstreamOption: &commonwebsocket.SingleStreamOption{
				URL: "ws://localhost:8081",
			},
		}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(wssConnect,
				func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
					if opt.URL == "ws://localhost:8080" {
						return nil, errors.New("error")
					}
					return &commonwebsocket.WssConnection{}, nil
				}),
		})
		pair, err := m.CreateWssConnPair(traceID, opt)
		time.Sleep(time.Millisecond * 100)
		patches.ResetAll()
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "cannot connect websocket server of rdispatcher")
		So(pair, ShouldBeNil)
	})
}

func TestGetWssConnPair(t *testing.T) {
	Convey("GetWssConnPair should return correct WssConnectionPair", t, func() {
		m := Instance()
		traceID := "12345"
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID}
		actual := m.GetWssConnPair(traceID)
		So(actual, ShouldNotBeNil)
		So(actual.TraceID, ShouldEqual, traceID)
		actual = m.GetWssConnPair("otherID")
		So(actual, ShouldBeNil)
	})
}

func TestReleaseWssConnPair(t *testing.T) {
	Convey("ReleaseWssConnPair should remove WssConnectionPair from cache", t, func() {
		m := Instance()
		traceID := "12345"
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*runtime.Metrics).DecreaseRequestCount,
				func(_ *runtime.Metrics, futureID string) {
					return
				}),
		})
		defer patches.ResetAll()
		m.ReleaseWssConnPair(traceID)
		actual := m.GetWssConnPair(traceID)
		So(actual, ShouldBeNil)
	})
	Convey("ReleaseWssConnPair, debug agent", t, func() {
		m := Instance()
		traceID := "12345"
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*runtime.Metrics).DecreaseRequestCount,
				func(_ *runtime.Metrics, futureID string) {
					return
				}),
			gomonkey.ApplyGlobalVar(&enableRemoteDebug, true),
		})
		defer patches.ResetAll()
		m.ReleaseWssConnPair(traceID)
		actual := m.GetWssConnPair(traceID)
		So(actual, ShouldBeNil)
		_, ok := <-runtime.ProcessExitSignal
		So(ok, ShouldBeFalse)
	})
}

func TestGracefulShutdown(t *testing.T) {
	Convey("GracefulShutdown should close all WssConnections in cache", t, func(c C) {
		m := Instance()
		done := make(chan bool)
		go func(c C) {
			m.GracefulShutdown()
			done <- true
		}(c)
		isDone, _ := <-done
		So(isDone, ShouldBeTrue)
	})
}

func Test_createWssConnection(t *testing.T) {
	Convey("createWssConnection should create WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &commonwebsocket.SingleStreamOption{
			URL:         target,
			HTTPSEnable: false,
		}
		mockConn := &websocket.Conn{}
		dialer := &websocket.Dialer{}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(dialer), "Dial", func(_ *websocket.Dialer, _ string, _ http.Header) (*websocket.Conn, *http.Response, error) {
				return mockConn, nil, nil
			}),
		})
		defer patches.ResetAll()
		conn, err := createWssConnection(opt)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
	})
	Convey("createWssConnection should create WssConnection successfully with tls", t, func() {
		target := "ws://example.com"
		opt := &commonwebsocket.SingleStreamOption{
			URL:         target,
			HTTPSEnable: true,
		}
		mockConn := &websocket.Conn{}
		dialer := &websocket.Dialer{}
		now := time.Now().String()
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "asdfsdffsdafasdf", now
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(dialer), "Dial", func(_ *websocket.Dialer, _ string, _ http.Header) (*websocket.Conn, *http.Response, error) {
				return mockConn, nil, nil
			}),
		})
		defer patches.ResetAll()
		conn, err := createWssConnection(opt)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
	})
	Convey("createWssConnection should create WssConnection failed", t, func() {
		target := "ws://example.com"
		opt := &commonwebsocket.SingleStreamOption{
			URL: target,
		}
		dialer := &websocket.Dialer{}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(dialer), "Dial", func(_ *websocket.Dialer, _ string, _ http.Header) (*websocket.Conn, *http.Response, error) {
			return nil, nil, errors.New("create failed")
		})
		defer patches.Reset()
		conn, err := createWssConnection(opt)
		So(err, ShouldNotBeNil)
		So(conn, ShouldBeNil)
	})
}

func Test_wssConnect(t *testing.T) {
	Convey("wssConnect should create WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &commonwebsocket.SingleStreamOption{
			URL: target,
		}
		patches := gomonkey.ApplyFunc(createWssConnection, func(_ *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
			return &commonwebsocket.WssConnection{}, nil
		})
		defer patches.Reset()
		conn, err := wssConnect(opt)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
	})

	Convey("wssConnect should create WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &commonwebsocket.SingleStreamOption{
			URL: target,
		}
		patches := gomonkey.ApplyFunc(createWssConnection, func(_ *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
			return &commonwebsocket.WssConnection{}, errors.New("conn error")
		})
		defer patches.Reset()
		conn, err := wssConnect(opt)
		So(err, ShouldNotBeNil)
		So(conn, ShouldBeNil)
	})
}

func TestWssManager_GetWssConnPairsCount(t *testing.T) {
	Convey("get websocket connection pair count", t, func() {
		m := Instance()
		traceID := "12345"
		m.wssCacheLock.Lock()
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID}
		m.wssCacheLock.Unlock()
		count1 := m.GetWssConnPairsCount()
		So(count1, ShouldEqual, 1)
		traceID = "6789"
		m.wssCacheLock.Lock()
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID}
		m.wssCacheLock.Unlock()
		count2 := m.GetWssConnPairsCount()
		So(count2, ShouldEqual, 2)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*runtime.Metrics).DecreaseRequestCount,
				func(_ *runtime.Metrics, futureID string) {
					return
				}),
		})
		defer patches.ResetAll()
		m.ReleaseWssConnPair("6789")
		count3 := m.GetWssConnPairsCount()
		So(count3, ShouldEqual, 1)
	})
}

func TestSendMsgToRDispatcher(t *testing.T) {
	type args struct {
		msg string
	}
	isMocked := false
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		isMocked    bool
	}{
		{
			name: "test wsManager is nil",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return nil
					}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test wsManager is not nil but wsManager.wssCache length is 0",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return &WssManager{}
					}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test wsManager.wssCache length is 1",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return &WssManager{
							wssCache: map[string]*WssConnectionPair{
								"mockTraceID": {},
							},
						}
					}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test upstream is not nil",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return &WssManager{
							wssCache: map[string]*WssConnectionPair{
								"mockTraceID": {
									Upstream: &commonwebsocket.WssConnection{},
								},
							},
						}
					}),
					gomonkey.ApplyFunc((*commonwebsocket.WssConnection).TrySendMessage,
						func(_ *commonwebsocket.WssConnection, message commonwebsocket.WssMessage) error {
							return nil
						}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test upstream is not nil and wrapSystemInfo return error",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return &WssManager{
							wssCache: map[string]*WssConnectionPair{
								"mockTraceID": {
									Upstream: &commonwebsocket.WssConnection{},
								},
							},
						}
					}),
					gomonkey.ApplyFunc((*commonwebsocket.WssConnection).TrySendMessage,
						func(_ *commonwebsocket.WssConnection, message commonwebsocket.WssMessage) error {
							return nil
						}),
					gomonkey.ApplyFunc(wrapSystemInfo, func(data []byte) (string, []byte, error) {
						return "mockTraceID", []byte("mockWrapperMsgBytes"), errors.New("mock wrapSystemInfo error")
					}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test upstream is nil",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return &WssManager{
							wssCache: map[string]*WssConnectionPair{
								"mockTraceID": {
									Upstream: nil,
								},
							},
						}
					}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test upstream is not nil",
			args: args{},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(Instance, func() *WssManager {
						isMocked = true
						return &WssManager{
							wssCache: map[string]*WssConnectionPair{
								"mockTraceID": {
									Upstream: &commonwebsocket.WssConnection{},
								},
							},
						}
					}),
					gomonkey.ApplyFunc((*commonwebsocket.WssConnection).TrySendMessage,
						func(_ *commonwebsocket.WssConnection, message commonwebsocket.WssMessage) error {
							return errors.New("mock try send message error")
						}),
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMocked = false
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			SendMsgToRDispatcher(tt.args.msg)
			if isMocked != tt.isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
		})
	}
}

func TestWssConnectionPair_createStreams(t *testing.T) {
	argStreamOption := &commonwebsocket.StreamOption{
		UpstreamOption:   &commonwebsocket.SingleStreamOption{URL: "up"},
		DownstreamOption: &commonwebsocket.SingleStreamOption{URL: "down"},
	}
	type args struct {
		opt      *commonwebsocket.StreamOption
		finished chan int
	}
	tests := []struct {
		name        string
		args        args
		expected    int
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "failed to build down stream connection",
			args: args{
				opt:      argStreamOption,
				finished: make(chan int, 1),
			},
			expected: downStreamFail,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(wssConnect, func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
						if opt.URL == "down" {
							return nil, errors.New("mock error")
						}
						return nil, nil
					}),
				})
				return patches
			},
		}, {
			name: "failed to build up stream connection",
			args: args{
				opt:      argStreamOption,
				finished: make(chan int, 1),
			},
			expected: upStreamFail,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(wssConnect, func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
						if opt.URL == "up" {
							return nil, errors.New("mock error")
						}
						return nil, nil
					}),
				})
				return patches
			},
		}, {
			name: "succeed to build up and down stream connection",
			args: args{
				opt:      argStreamOption,
				finished: make(chan int, 1),
			},
			expected: success,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(wssConnect, func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
						return nil, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &WssConnectionPair{}
			p.createStreams(tt.args.opt, tt.args.finished)
			finished, ok := <-tt.args.finished
			if !ok || finished != tt.expected {
				t.Errorf("failed to create up or down stream, expected: %d, actual: %d", tt.expected, finished)
			}
			patches.ResetAll()
		})
	}
}

func Test_wrapSystemInfo(t *testing.T) {
	traceID := uuid.New()
	type args struct {
		data []byte
	}
	tests := []struct {
		name            string
		testFunc        func(data []byte) (string, []byte, error)
		args            args
		expectedTraceID string
		expectedMessage []byte
		errIsNil        bool
		patchesFunc     mockUtils.PatchesFunc
	}{
		{
			name:            "wrapSystemInfo normal case",
			testFunc:        wrapSystemInfo,
			args:            args{data: []byte("234")},
			expectedTraceID: traceID.String(),
			expectedMessage: []byte(`{"data":"MjM0","traceID":"` + traceID.String() + `"}`),
			errIsNil:        true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(uuid.New, func() uuid.UUID {
						return traceID
					}),
				})
				return patches
			},
		}, {
			name:            "unwrapSystemInfo normal case",
			testFunc:        unwrapSystemInfo,
			args:            args{data: []byte(`{"data":"NTY3","traceID":"` + traceID.String() + `"}`)},
			expectedTraceID: traceID.String(),
			expectedMessage: []byte(`567`),
			errIsNil:        true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			id, msg, err := tt.testFunc(tt.args.data)
			if (err == nil) != tt.errIsNil {
				t.Errorf("wrapSystemInfo() error = %t, wantErr %t", err == nil, tt.errIsNil)
				return
			}
			if id != tt.expectedTraceID {
				t.Errorf("wrapSystemInfo() got = %s, want %s", id, tt.expectedTraceID)
			}
			if !reflect.DeepEqual(msg, tt.expectedMessage) {
				t.Errorf("wrapSystemInfo() got1 = %s, want %s", msg, tt.expectedMessage)
			}
			patches.ResetAll()
		})
	}
}

func TestWssConnectionPair_forwardMessage(t *testing.T) {
	var pair *WssConnectionPair
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1 read msg error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, errors.New("error")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.Downstream), "TrySendMessage",
						func(_ *commonwebsocket.WssConnection, message commonwebsocket.WssMessage) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.Downstream), "Close", func(_ *commonwebsocket.WssConnection) {
						return
					}),
				})
				return patches
			},
		}, {
			name: "case2 wrap msg error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, nil
					}),
					gomonkey.ApplyFunc(unwrapSystemInfo, func(data []byte) (string, []byte, error) {
						return "", nil, errors.New("mock error")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.Downstream), "Close",
						func(_ *commonwebsocket.WssConnection) {
							return
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.Upstream), "Close",
						func(_ *commonwebsocket.WssConnection) {
							return
						}),
				})
				return patches
			},
		}, {
			name: "case3 send msg successfully",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, nil
					}),
					gomonkey.ApplyFunc(unwrapSystemInfo, func(data []byte) (string, []byte, error) {
						return "test trace id", []byte("mock body"), nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pair = &WssConnectionPair{
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
				pair.forwardMessage(pair.Upstream, pair.Downstream, unwrapSystemInfo)
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
