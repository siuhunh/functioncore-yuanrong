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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/tls"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/network"
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
			wssCache:       make(map[string]*WssConnectionPair),
			functionLogger: functionlog.NewFunctionLogger(zap.NewNop(), "INFO", ""),
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
			gomonkey.ApplyFunc(commonwebsocket.Connect,
				func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
					return &commonwebsocket.WssConnection{}, nil
				}),
		})
		defer patches.ResetAll()
		pair, err := m.CreateWssConnPair(traceID, opt)
		time.Sleep(time.Millisecond * 100)
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
			gomonkey.ApplyFunc(commonwebsocket.Connect,
				func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
					if opt.URL == "ws://localhost:8081" {
						return nil, errors.New("error")
					}
					return &commonwebsocket.WssConnection{}, nil
				}),
		})
		defer patches.ResetAll()
		pair, err := m.CreateWssConnPair(traceID, opt)
		time.Sleep(time.Millisecond * 100)
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
			gomonkey.ApplyFunc(commonwebsocket.Connect,
				func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
					if opt.URL == "ws://localhost:8080" {
						return nil, errors.New("error")
					}
					return &commonwebsocket.WssConnection{}, nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}),
				"Close", func(_ *websocket.Conn) error {
					return nil
				}),
		})
		defer patches.ResetAll()
		pair, err := m.CreateWssConnPair(traceID, opt)
		time.Sleep(time.Millisecond * 100)
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
		m.functionLogger = functionlog.NewFunctionLogger(zap.NewNop(), "INFO", "")
		traceID := "12345"
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID, Summary: &runtime.Summary{}}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*runtime.Metrics).DecreaseRequestCount,
				func(_ *runtime.Metrics, futureID string) {
					return
				}),
			gomonkey.ApplyFunc((*functionlog.FunctionLogger).GetLogRecorder,
				func(_ *functionlog.FunctionLogger, _ invokectx.InvokeCtx) *functionlog.LogRecorder {
					return &functionlog.LogRecorder{}
				}),
			gomonkey.ApplyFunc((*functionlog.LogRecorder).FinishFooter,
				func(_ *functionlog.LogRecorder, _ invokectx.InvokeCtx) {
					return
				}),
			gomonkey.ApplyFunc((*invokectx.Metrics).ReleaseInvokeCtx,
				func(_ *invokectx.Metrics, _ invokectx.InvokeCtx) {
					return
				}),
		})
		defer patches.ResetAll()
		m.ReleaseWssConnPair(traceID, true)
		actual := m.GetWssConnPair(traceID)
		So(actual, ShouldBeNil)
	})
	Convey("ReleaseWssConnPair, debug agent", t, func() {
		m := Instance()
		m.functionLogger = functionlog.NewFunctionLogger(zap.NewNop(), "INFO", "")
		traceID := "12345"
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID, Summary: &runtime.Summary{}}
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*runtime.Metrics).DecreaseRequestCount,
				func(_ *runtime.Metrics, futureID string) {
					return
				}),
			gomonkey.ApplyGlobalVar(&enableRemoteDebug, true),
			gomonkey.ApplyFunc((*functionlog.FunctionLogger).GetLogRecorder,
				func(_ *functionlog.FunctionLogger, _ invokectx.InvokeCtx) *functionlog.LogRecorder {
					return &functionlog.LogRecorder{}
				}),
			gomonkey.ApplyFunc((*functionlog.LogRecorder).FinishFooter,
				func(_ *functionlog.LogRecorder, _ invokectx.InvokeCtx) {
					return
				}),
			gomonkey.ApplyFunc((*invokectx.Metrics).ReleaseInvokeCtx,
				func(_ *invokectx.Metrics, _ invokectx.InvokeCtx) {
					return
				}),
		})
		defer patches.ResetAll()
		m.ReleaseWssConnPair(traceID, true)
		actual := m.GetWssConnPair(traceID)
		So(actual, ShouldBeNil)
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
	Convey("GracefulShutdown close all WssConnections timeout", t, func(c C) {
		m := Instance()
		m.wssCache["aprilyolies"] = &WssConnectionPair{}
		done := make(chan bool)
		timer := time.NewTimer(time.Millisecond)
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return timer
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		go func(c C) {
			m.GracefulShutdown()
			done <- true
		}(c)
		isDone, _ := <-done
		So(isDone, ShouldBeTrue)
		delete(m.wssCache, "aprilyolies")
	})
}

func TestWssManager_GetWssConnPairsCount(t *testing.T) {
	Convey("get websocket connection pair count", t, func() {
		m := Instance()
		traceID := "12345"
		m.wssCacheLock.Lock()
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID, Summary: &runtime.Summary{}}
		m.wssCacheLock.Unlock()
		count1 := m.GetWssConnPairsCount()
		So(count1, ShouldEqual, 1)
		traceID = "6789"
		m.wssCacheLock.Lock()
		m.wssCache[traceID] = &WssConnectionPair{TraceID: traceID, Summary: &runtime.Summary{}}
		m.wssCacheLock.Unlock()
		count2 := m.GetWssConnPairsCount()
		So(count2, ShouldEqual, 2)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*runtime.Metrics).DecreaseRequestCount,
				func(_ *runtime.Metrics, futureID string) {
					return
				}),
			gomonkey.ApplyFunc((*functionlog.FunctionLogger).GetLogRecorder,
				func(_ *functionlog.FunctionLogger, _ invokectx.InvokeCtx) *functionlog.LogRecorder {
					return &functionlog.LogRecorder{}
				}),
			gomonkey.ApplyFunc((*functionlog.LogRecorder).FinishFooter,
				func(_ *functionlog.LogRecorder, _ invokectx.InvokeCtx) {
					return
				}),
			gomonkey.ApplyFunc((*invokectx.Metrics).ReleaseInvokeCtx,
				func(_ *invokectx.Metrics, _ invokectx.InvokeCtx) {
					return
				}),
		})
		defer patches.ResetAll()
		m.ReleaseWssConnPair("6789", false)
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
					gomonkey.ApplyFunc(commonwebsocket.WrapSystemInfo, func(data []byte) (string, []byte, error) {
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
					gomonkey.ApplyFunc(commonwebsocket.Connect, func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
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
					gomonkey.ApplyFunc(commonwebsocket.Connect, func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
						if opt.URL == "up" {
							return nil, errors.New("mock error")
						}
						return &commonwebsocket.WssConnection{
							Connection: &websocket.Conn{},
							CloseChan:  make(chan struct{}),
							IsClosed:   uint32(0),
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "Close", func(_ *websocket.Conn) error {
						return nil
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
					gomonkey.ApplyFunc(commonwebsocket.Connect, func(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
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
			if finished == upStreamFail && !p.Downstream.IsConnClosed() {
				t.Error("when upStreamFail expect Downstream closed")
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
					gomonkey.ApplyFunc(commonwebsocket.UnwrapSystemInfo, func(data []byte) (string, []byte, error) {
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
				pair.forwardMessage(pair.Upstream, pair.Downstream, commonwebsocket.UnwrapSystemInfo)
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

func Test_changeWebSocketIptables_concurrent(t *testing.T) {
	t.Run("test same ip+port deduplicate", func(t *testing.T) {
		testCount := 0
		var wg sync.WaitGroup
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
				testCount++
				return 0, "", nil
			}),
		})
		numRoutines := 10
		for i := 0; i < numRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				connReq := &schedulerapi.CreateWebSocketConnectionReq{
					WebSocketConfig: schedulerapi.WebSocketInsAcquireConfig{TargetIP: "127.127.127.127", TargetPort: "80"}}
				m := Instance()
				err := m.ChangeWebSocketIptables(connReq)
				if err != nil {
					return
				}
			}()
		}
		for i := 0; i < numRoutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				connReq := &schedulerapi.CreateWebSocketConnectionReq{
					WebSocketConfig: schedulerapi.WebSocketInsAcquireConfig{TargetIP: "127.127.127.128", TargetPort: "80"}}
				m := Instance()
				err := m.ChangeWebSocketIptables(connReq)
				if err != nil {
					return
				}
			}()
		}
		wg.Wait()
		// only two different IP addresses and ports, the command should be executed only 2 times
		assert.Equal(t, 2, testCount)
		patches.ResetAll()
	})
}

func Test_changeWebSocketIptables(t *testing.T) {
	tests := []struct {
		name        string
		targetIp    string
		targetPort  string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:       "case 1",
			targetIp:   "127.127.127.127",
			targetPort: "80",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return 0, "", nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:       "case 2",
			targetIp:   "abcsd",
			targetPort: "80",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return 0, "", nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:       "case 3",
			targetIp:   "127.127.127.127",
			targetPort: "9999999",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(network.ExecuteShellCommandLineWithCombinedOutput, func(cmdline string) (int, string, error) {
						return 0, "", nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			connReq := &schedulerapi.CreateWebSocketConnectionReq{
				WebSocketConfig: schedulerapi.WebSocketInsAcquireConfig{TargetIP: tt.targetIp, TargetPort: tt.targetPort}}
			m := Instance()
			err := m.ChangeWebSocketIptables(connReq)
			expectErr := err != nil
			if expectErr != tt.wantErr {
				t.Errorf("failed to test func: addRouteOfDispatcher, actual = %v, want %v", expectErr, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestWSRT(t *testing.T) {
	defer func() {
		globalWSRT = nil
	}()
	assert.Nil(t, GetWebRuntimeException())
	assert.Equal(t, &config.Configuration{}, GetWebSocketRuntimeConfiguration())

	testErr := &runtime.Error{}

	testWSRT := &WSRT{
		Configuration: &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				Language: worker.CustomContainerLanguage,
			},
		},
		Handlers: runtime.Handlers{
			GetLastExceptionHandler: func() *runtime.Error {
				return testErr
			},
		},
	}

	SetGlobalWSRT(testWSRT)
	assert.Equal(t, testErr, GetWebRuntimeException())
	assert.Equal(t, testWSRT.Configuration, GetWebSocketRuntimeConfiguration())
}

func TestCheckWsConnIsCloseNormal(t *testing.T) {
	isCloseNormal := checkWsConnIsCloseNormal(nil)
	assert.True(t, isCloseNormal)

	isCloseNormal = checkWsConnIsCloseNormal(errors.New("use of closed network connection"))
	assert.False(t, isCloseNormal)

	isCloseNormal = checkWsConnIsCloseNormal(&websocket.CloseError{Code: websocket.CloseNormalClosure})
	assert.True(t, isCloseNormal)

	isCloseNormal = checkWsConnIsCloseNormal(&websocket.CloseError{Code: websocket.CloseAbnormalClosure})
	assert.False(t, isCloseNormal)
}
