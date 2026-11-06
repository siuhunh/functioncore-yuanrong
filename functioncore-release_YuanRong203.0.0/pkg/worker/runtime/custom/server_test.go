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
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/stream"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/custom/config"
	mockutils "functioncore/test/utils"
)

var (
	serverPort = "8888"

	urls = config.ServerUrl{
		"/runtime/init/ready",
		"/runtime/invocation/next",
		"/runtime/invocation/response",
		"/runtime/invocation/error",
	}

	serverConfig = &config.ServerConfiguration{
		ServerUrl: urls,
	}
)

func TestNextRequest(t *testing.T) {
	convey.Convey("test next request", t, func() {
		patch := gomonkey.ApplyFunc(config.GetServerConfig,
			func() (*config.ServerConfiguration, error) {
				return &config.ServerConfiguration{ServerUrl: config.ServerUrl{
					NextPath:     "a",
					ResponsePath: "b",
					ErrorPath:    "c",
				}}, nil
			})
		patch.ApplyFunc(config.GetRuntimeConfig, func() (*config.RuntimeConfigSingleton, error) {
			return &config.RuntimeConfigSingleton{
				MaxRequestBodySize: "6",
			}, nil
		})
		defer func() {
			patch.Reset()
		}()
		server, err := getServer(serverPort)
		if err != nil {
			t.Error("failed to get server")
			return
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(server.eventManager), "NextRequest",
				func(*EventManager) *runtime.InvokeRequest {
					return nil
				}),
			gomonkey.ApplyGlobalVar(&fusionCustomRuntime, &fusionCustom{}),
			gomonkey.ApplyGlobalVar(&clientReadyChan, make(chan struct{})),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		writer := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/testurl", nil)
		go func() {
			close(server.eventManager.requestChan)
		}()
		server.NextRequest(writer, request)
		convey.ShouldEqual(writer.Code, http.StatusNoContent)
	})
}

func TestReplyResult(t *testing.T) {
	patch := gomonkey.ApplyFunc(config.GetServerConfig,
		func() (*config.ServerConfiguration, error) {
			return &config.ServerConfiguration{ServerUrl: config.ServerUrl{
				NextPath:     "a",
				ResponsePath: "b",
				ErrorPath:    "c",
			}}, nil
		})
	patch.ApplyFunc(config.GetRuntimeConfig, func() (*config.RuntimeConfigSingleton, error) {
		return &config.RuntimeConfigSingleton{
			MaxRequestBodySize: "6",
		}, nil
	})
	defer func() {
		patch.Reset()
	}()
	server, err := getServer(serverPort)
	if err != nil {
		t.Error("failed to get server")
		return
	}
	writer := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/testurl", bytes.NewReader([]byte("{\"test\":\"data\"}")))
	request.Header.Set("X-Request-Id", "test")
	server.eventManager.waitMap.waiters["test"] = make(chan *runtime.InvokeResponse, 1)
	server.ReplyResult(writer, request)
	convey.ShouldEqual(writer.Code, http.StatusOK)
}

func TestReplyError(t *testing.T) {
	patch := gomonkey.ApplyFunc(config.GetServerConfig,
		func() (*config.ServerConfiguration, error) {
			return nil, nil
		})
	patch.ApplyFunc(config.GetRuntimeConfig, func() (*config.RuntimeConfigSingleton, error) {
		return &config.RuntimeConfigSingleton{
			MaxRequestBodySize: "6",
		}, nil
	})
	defer func() {
		patch.Reset()
	}()
	server, err := getServer(serverPort)
	if err != nil {
		t.Error("failed to get server")
		return
	}
	writer := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/testurl", bytes.NewReader([]byte("{\"test\":\"data\"}")))
	request.Header.Set("X-Request-Id", "test")
	server.eventManager.waitMap.waiters["test"] = make(chan *runtime.InvokeResponse, 1)
	server.ReplyError(writer, request)
	convey.ShouldEqual(writer.Code, http.StatusOK)
}

func Test_server_setResponseHeader(t *testing.T) {
	type fields struct {
		httpServer         *http.Server
		serverConfig       *config.ServerConfiguration
		RWMutex            sync.RWMutex
		eventManager       *EventManager
		closeServerFlag    bool
		healthy            bool
		maxRequestBodySize int
	}
	type args struct {
		request *runtime.InvokeRequest
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[string]string
	}{
		{
			name: "test001",
			fields: fields{
				serverConfig: &config.ServerConfiguration{
					ServerHeaders: config.ServerHeaders{
						HeaderRequestIDKey:      "1",
						HeaderAccessKey:         "3",
						HeaderSecretKey:         "4",
						HeaderAuthToken:         "5",
						HeaderSecurityToken:     "6",
						HeaderSecuritySecretKey: "7",
						HeaderSecurityAccessKey: "8",
						HeaderOriginVersionTag:  "9",
						HeaderWorkflowID:        "10",
						HeaderWorkflowRunID:     "11",
						HeaderWorkflowStateID:   "12",
					},
				},
			},
			args: args{
				request: &runtime.InvokeRequest{
					Extra: map[string]string{
						constants.HeaderAccessKey:         "1",
						constants.HeaderSecretKey:         "2",
						constants.HeaderAuthToken:         "3",
						constants.HeaderSecurityToken:     "4",
						constants.HeaderSecurityAccessKey: "5",
						constants.HeaderSecuritySecretKey: "6",
						constants.HeaderOriginVersionTag:  "7",
						constants.HeaderWorkflowID:        "8",
						constants.HeaderWorkflowRunID:     "9",
						constants.HeaderWorkflowStateID:   "10",
					},
					TraceID: "mock-trace-id",
				},
			},
			want: map[string]string{"1": "mock-trace-id", "3": "1", "4": "2", "5": "3", "6": "4", "7": "6", "8": "5", "9": "7", "10": "8", "11": "9", "12": "10"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{
				httpServer:         tt.fields.httpServer,
				serverConfig:       tt.fields.serverConfig,
				RWMutex:            tt.fields.RWMutex,
				eventManager:       tt.fields.eventManager,
				closeServerFlag:    tt.fields.closeServerFlag,
				healthy:            tt.fields.healthy,
				maxRequestBodySize: tt.fields.maxRequestBodySize,
			}
			if got := s.setResponseHeader(tt.args.request); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setResponseHeader() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getServer(t *testing.T) {
	type args struct {
		address string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		want        *server
		wantErr     bool
	}{
		{
			name: "test001",
			args: args{
				address: "",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetServerConfig,
						func() (*config.ServerConfiguration, error) {
							return nil, errors.New("mock error")
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "test002",
			args: args{
				address: "",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetServerConfig,
						func() (*config.ServerConfiguration, error) {
							return &config.ServerConfiguration{ServerUrl: config.ServerUrl{}}, nil
						}),
					gomonkey.ApplyFunc(config.GetRuntimeConfig, func() (*config.RuntimeConfigSingleton, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := getServer(tt.args.address)
			if (err != nil) != tt.wantErr {
				t.Errorf("getServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getServer() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getServerAddress(t *testing.T) {
	type args struct {
		port int32
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				port: 8080,
			},
			want: "localhost:8080",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getServerAddress(tt.args.port); got != tt.want {
				t.Errorf("getServerAddress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processClientClosed(t *testing.T) {
	convey.Convey("Test processClientClosed", t, func() {
		convey.Convey("Test 1", func() {
			serverChan := make(chan *runtime.InvokeRequest)
			s := &server{
				eventManager: &EventManager{
					requestChan: serverChan,
				},
			}
			w := httptest.NewRecorder()
			r := &http.Request{}
			var request *runtime.InvokeRequest
			s.processClientClosed(w, r, request)
			convey.So(request, convey.ShouldBeNil)
		})
		convey.Convey("Test 2", func() {
			serverChan := make(chan *runtime.InvokeRequest)
			s := &server{
				eventManager: &EventManager{
					requestChan: serverChan,
				},
			}
			w := httptest.NewRecorder()
			r := &http.Request{}
			var request *runtime.InvokeRequest
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(w), "Write", func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
					return 0, errors.New("res read err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processClientClosed(w, r, request)
			convey.So(request, convey.ShouldBeNil)
		})
		convey.Convey("Test chan", func() {
			serverChan := make(chan *runtime.InvokeRequest, 5)
			defer close(serverChan)
			s := &server{
				eventManager: &EventManager{
					requestChan: serverChan,
				},
			}
			w := httptest.NewRecorder()
			r := &http.Request{}
			request := &runtime.InvokeRequest{}
			s.processClientClosed(w, r, request)
			res, ok := <-s.eventManager.requestChan
			if ok == true {
				convey.So(res, convey.ShouldEqual, request)
			}
		})
		convey.Convey("Test chan close", func() {
			s := &server{
				eventManager: &EventManager{
					requestChan: nil,
				},
			}
			w := httptest.NewRecorder()
			r := &http.Request{}
			request := &runtime.InvokeRequest{}
			s.processClientClosed(w, r, request)
			convey.So(w.Code, convey.ShouldEqual, http.StatusOK)
		})
	})
}

func Test_processChannelClosed(t *testing.T) {
	convey.Convey("Test processChannelClosed", t, func() {
		convey.Convey("Test 1", func() {
			s := &server{}
			w := httptest.NewRecorder()
			r := &http.Request{}
			s.processTimeOut(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusRequestTimeout)
		})
		convey.Convey("Test processChannelClosed 2", func() {
			s := &server{}
			w := httptest.NewRecorder()
			r := &http.Request{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(w), "Write", func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
					return 0, errors.New("res read err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processTimeOut(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusRequestTimeout)
		})
	})
}

func Test_processTimeOut(t *testing.T) {
	convey.Convey("Test processTimeOut", t, func() {
		convey.Convey("Test 1", func() {
			s := &server{}
			w := httptest.NewRecorder()
			r := &http.Request{}
			s.processTimeOut(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusRequestTimeout)
		})
		convey.Convey("Test 2", func() {
			s := &server{}
			w := httptest.NewRecorder()
			r := &http.Request{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(w), "Write", func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
					return 0, errors.New("res read err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processTimeOut(w, r)
			convey.So(w.Code, convey.ShouldEqual, http.StatusRequestTimeout)
		})
	})
}

func Test_processPutResponseError(t *testing.T) {
	convey.Convey("Test processPutResponseError", t, func() {
		convey.Convey("Test 1", func() {
			s := &server{}
			w := httptest.NewRecorder()
			r := &http.Request{}
			requestID := "test 1"
			err := errors.New("test err 1")
			s.processPutResponseError(w, r, requestID, err)
			convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		})
		convey.Convey("Test 2", func() {
			s := &server{}
			w := httptest.NewRecorder()
			r := &http.Request{}
			requestID := "test 1"
			err := errors.New("test err 1")
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(w), "Write", func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
					return 0, errors.New("res read err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processPutResponseError(w, r, requestID, err)
			convey.So(w.Code, convey.ShouldEqual, http.StatusBadRequest)
		})
	})
}

func Test_getNextRequest(t *testing.T) {
	convey.Convey("Test getNextRequest", t, func() {
		convey.Convey("Test 1", func() {
			requestChan := make(chan *runtime.InvokeRequest)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
				},
			}
			w := httptest.NewRecorder()
			r := &http.Request{}
			close(s.eventManager.requestChan)
			getNextRequest(s, w, r)
		})
		convey.Convey("Test 2", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
				},
			}
			w := httptest.NewRecorder()
			r := &http.Request{}
			var testRequest *runtime.InvokeRequest
			s.eventManager.requestChan <- testRequest
			getNextRequest(s, w, r)
		})
		convey.Convey("Test 3", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			waiters := make(map[string]chan *runtime.InvokeResponse)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
					waitMap: struct {
						sync.RWMutex
						waiters       map[string]chan *runtime.InvokeResponse
						timers        map[string]*time.Timer
						streamOptions map[string]*stream.ClientOption
					}{
						waiters: waiters,
					},
				},
				serverConfig: &config.ServerConfiguration{
					ServerHeaders: config.ServerHeaders{},
				},
			}
			s.eventManager.waitMap.waiters["Test 1"] = make(chan *runtime.InvokeResponse, 1)
			w := httptest.NewRecorder()
			r := &http.Request{}
			testRequest := &runtime.InvokeRequest{
				TraceID: "Test 1",
			}
			s.eventManager.requestChan <- testRequest
			getNextRequest(s, w, r)
		})
		convey.Convey("Test 4", func() {
			s := &server{
				eventManager: &EventManager{
					requestChan: nil,
				},
			}
			w := httptest.NewRecorder()
			testContext := context.TODO()
			testContext, cancel := context.WithCancel(testContext)
			r := &http.Request{}
			r = r.WithContext(testContext)
			cancel()
			getNextRequest(s, w, r)
		})
	})
}

func Test_processResult(t *testing.T) {
	convey.Convey("Test processResult", t, func() {
		convey.Convey("Test 1", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			waiters := make(map[string]chan *runtime.InvokeResponse)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
					waitMap: struct {
						sync.RWMutex
						waiters       map[string]chan *runtime.InvokeResponse
						timers        map[string]*time.Timer
						streamOptions map[string]*stream.ClientOption
					}{
						waiters: waiters,
					},
				},
				serverConfig: &config.ServerConfiguration{
					ServerHeaders: config.ServerHeaders{},
				},
			}
			s.eventManager.waitMap.waiters["TestUrl"] = make(chan *runtime.InvokeResponse, 1)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/Test/TestUrl", bytes.NewReader([]byte("{\"test\":\"data\"}")))
			isError := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processResult(w, r, isError)
			res, _ := <-s.eventManager.waitMap.waiters["TestUrl"]
			convey.So(res, convey.ShouldNotBeNil)
		})
		convey.Convey("Test 2", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			waiters := make(map[string]chan *runtime.InvokeResponse)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
					waitMap: struct {
						sync.RWMutex
						waiters       map[string]chan *runtime.InvokeResponse
						timers        map[string]*time.Timer
						streamOptions map[string]*stream.ClientOption
					}{
						waiters: waiters,
					},
				},
				serverConfig: &config.ServerConfiguration{
					ServerHeaders: config.ServerHeaders{},
				},
			}
			s.eventManager.waitMap.waiters["Test 1"] = make(chan *runtime.InvokeResponse, 1)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/Test/TestUrl", bytes.NewReader([]byte("{\"test\":\"data\"}")))
			isError := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
					return nil, errors.New("ioutil ReadAll fail")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processResult(w, r, isError)
		})
		convey.Convey("Test 3", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			waiters := make(map[string]chan *runtime.InvokeResponse)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
					waitMap: struct {
						sync.RWMutex
						waiters       map[string]chan *runtime.InvokeResponse
						timers        map[string]*time.Timer
						streamOptions map[string]*stream.ClientOption
					}{
						waiters: waiters,
					},
				},
				serverConfig: &config.ServerConfiguration{
					ServerHeaders: config.ServerHeaders{},
				},
				maxRequestBodySize: 25,
			}
			s.eventManager.waitMap.waiters["Test 1"] = make(chan *runtime.InvokeResponse, 1)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/Test/TestUrl", bytes.NewReader([]byte("{\"test\":\"data\"}")))
			isError := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processResult(w, r, isError)
		})
		convey.Convey("Test 4", func() {
			requestChan := make(chan *runtime.InvokeRequest, 1)
			waiters := make(map[string]chan *runtime.InvokeResponse)
			s := &server{
				eventManager: &EventManager{
					requestChan: requestChan,
					waitMap: struct {
						sync.RWMutex
						waiters       map[string]chan *runtime.InvokeResponse
						timers        map[string]*time.Timer
						streamOptions map[string]*stream.ClientOption
					}{
						waiters: waiters,
					},
				},
				serverConfig: &config.ServerConfiguration{
					ServerHeaders: config.ServerHeaders{},
				},
				maxRequestBodySize: 25,
			}
			s.eventManager.waitMap.waiters["Test 1"] = make(chan *runtime.InvokeResponse, 1)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/Test/TestUrl", bytes.NewReader([]byte("{\"test\":\"data\"}")))
			isError := true
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return nil, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s.processResult(w, r, isError)
		})
	})
}

func TestProcessStreamEvent(t *testing.T) {
	requestChan := make(chan *runtime.InvokeRequest, 1)
	waiters := make(map[string]chan *runtime.InvokeResponse)
	timers := make(map[string]*time.Timer)
	options := make(map[string]*stream.ClientOption)
	s := &server{
		eventManager: &EventManager{
			requestChan: requestChan,
			waitMap: struct {
				sync.RWMutex
				waiters       map[string]chan *runtime.InvokeResponse
				timers        map[string]*time.Timer
				streamOptions map[string]*stream.ClientOption
			}{
				waiters:       waiters,
				timers:        timers,
				streamOptions: options,
			},
		},
		serverConfig: &config.ServerConfiguration{
			ServerHeaders: config.ServerHeaders{},
		},
		maxRequestBodySize: 25,
	}
	convey.Convey("TestProcessStreamEvent", t, func() {
		convey.Convey("invoke timer isn't exist", func() {
			s.processStreamEvent(httptest.NewRecorder(), &http.Request{}, false, "request001")
		})
		convey.Convey("stream client option isn't exist", func() {
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			s.eventManager.waitMap.timers["request001"] = timer
			s.processStreamEvent(httptest.NewRecorder(), &http.Request{}, false, "request001")
		})
		convey.Convey("processStreamEvent", func() {
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			s.eventManager.waitMap.timers["request001"] = timer
			s.eventManager.waitMap.streamOptions["request001"] = &stream.ClientOption{}
			s.processStreamEvent(httptest.NewRecorder(), &http.Request{}, false, "request001")
		})
	})
}
