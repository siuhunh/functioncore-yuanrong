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

// Package websocket operates websocket
package websocket

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/uuid"
	mockUtils "functioncore/test/utils"
)

func TestWssConnection_Close(t *testing.T) {
	Convey("wssConnect has been closed", t, func() {
		conn := &WssConnection{
			Connection: &websocket.Conn{},
			Endpoint:   "ws://example.com",
			IsClosed:   uint32(1),
			CloseChan:  make(chan struct{}),
		}
		conn.Close()
	})
	Convey("wssConnect close with no error", t, func() {
		conn := &WssConnection{
			Connection: &websocket.Conn{},
			Endpoint:   "ws://example.com",
			IsClosed:   uint32(0),
			CloseChan:  make(chan struct{}),
		}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(conn.Connection), "Close", func(_ *websocket.Conn) error {
			return nil
		})
		defer patches.Reset()
		conn.Close()
		_, ok := <-conn.CloseChan
		So(ok, ShouldBeFalse)
	})
	Convey("wssConnect close with error", t, func() {
		conn := &WssConnection{
			Connection: &websocket.Conn{},
			Endpoint:   "ws://example.com",
			IsClosed:   uint32(0),
			CloseChan:  make(chan struct{}),
		}
		patches := gomonkey.ApplyMethod(reflect.TypeOf(conn.Connection), "Close", func(_ *websocket.Conn) error {
			return errors.New("websocket close error")
		})
		defer patches.Reset()
		conn.Close()
		_, ok := <-conn.CloseChan
		So(ok, ShouldBeFalse)
	})
}

func TestWssConnection_TrySendMessage(t *testing.T) {
	Convey("try send message but is closed", t, func() {
		conn := &WssConnection{
			Connection:  &websocket.Conn{},
			ForwardChan: make(chan WssMessage, ForwardChanSize),
			CloseChan:   make(chan struct{}),
			IsClosed:    uint32(1),
		}
		err := conn.TrySendMessage(WssMessage{MessageType: 0})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "connection is closed")
	})
	Convey("try send message but failed", t, func() {
		conn := &WssConnection{
			Connection:  &websocket.Conn{},
			ForwardChan: make(chan WssMessage, ForwardChanSize),
			CloseChan:   make(chan struct{}),
			IsClosed:    uint32(0),
		}
		gomonkey.ApplyFunc((*websocket.Conn).WriteMessage, func(_ *websocket.Conn, messageType int, data []byte) error {
			return errors.New("error")
		})
		err := conn.TrySendMessage(WssMessage{MessageType: 0})
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "error")
	})
	Convey("try send message success", t, func() {
		conn := &WssConnection{
			Connection:  &websocket.Conn{},
			ForwardChan: make(chan WssMessage, ForwardChanSize),
			CloseChan:   make(chan struct{}),
			IsClosed:    uint32(0),
		}
		gomonkey.ApplyFunc((*websocket.Conn).WriteMessage, func(_ *websocket.Conn, messageType int, data []byte) error {
			return nil
		})
		err := conn.TrySendMessage(WssMessage{MessageType: 0})
		So(err, ShouldBeNil)
	})
}

func Test_createWssConnection(t *testing.T) {
	Convey("createWssConnection should create WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &SingleStreamOption{
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
		opt := &SingleStreamOption{
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
		opt := &SingleStreamOption{
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
	Convey("createWssConnection create internal WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &SingleStreamOption{
			URL:         target,
			HTTPSEnable: true,
			Internal:    true,
		}
		mockConn := &websocket.Conn{}
		dialer := &websocket.Dialer{}
		patches := mockUtils.InitPatchSlice()
		now := time.Now().String()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "asdfsdffsdafasdf", now
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(dialer), "Dial", func(_ *websocket.Dialer, _ string,
				headers http.Header) (*websocket.Conn, *http.Response, error) {
				if strings.Contains(headers.Get(constants.HeaderAuthorization), "appid=internal") {
					return mockConn, nil, nil
				}
				return nil, nil, errors.New("need appid")
			}),
		})
		defer patches.ResetAll()
		conn, err := createWssConnection(opt)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
	})
}

func TestConnect(t *testing.T) {
	Convey("wssConnect should create WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &SingleStreamOption{
			URL: target,
		}
		patches := gomonkey.ApplyFunc(createWssConnection, func(_ *SingleStreamOption) (*WssConnection, error) {
			return &WssConnection{}, nil
		})
		defer patches.Reset()
		conn, err := Connect(opt)
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)
	})

	Convey("wssConnect should create WssConnection successfully", t, func() {
		target := "ws://example.com"
		opt := &SingleStreamOption{
			URL: target,
		}
		patches := gomonkey.ApplyFunc(createWssConnection, func(_ *SingleStreamOption) (*WssConnection, error) {
			return &WssConnection{}, errors.New("conn error")
		})
		defer patches.Reset()
		conn, err := Connect(opt)
		So(err, ShouldNotBeNil)
		So(conn, ShouldBeNil)
	})
}

func Test_Upgrade(t *testing.T) {
	Convey("Test upgrade succeed", t, func() {
		req := &http.Request{}
		defer gomonkey.ApplyMethod(reflect.TypeOf(&upgrade), "Upgrade", func(u *websocket.Upgrader, w http.ResponseWriter,
			r *http.Request, responseHeader http.Header) (*websocket.Conn, error) {
			return &websocket.Conn{}, nil
		}).Reset()
		conn, err := Upgrade(nil, req, nil)
		So(conn, ShouldNotBeNil)
		So(err, ShouldBeNil)
	})
	Convey("Test upgrade failed", t, func() {
		req := &http.Request{}
		defer gomonkey.ApplyMethod(reflect.TypeOf(&upgrade), "Upgrade", func(u *websocket.Upgrader, w http.ResponseWriter,
			r *http.Request, responseHeader http.Header) (*websocket.Conn, error) {
			return &websocket.Conn{}, fmt.Errorf("123")
		}).Reset()
		conn, err := Upgrade(nil, req, nil)
		So(conn, ShouldBeNil)
		So(err.Error(), ShouldEqual, "123")
	})
}

func Test_ReceiveMessage(t *testing.T) {
	Convey("Test read succeed", t, func() {
		wssConn := NewWssConnection("", &websocket.Conn{})
		defer gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Conn{}), "ReadMessage", func(w *websocket.Conn) (int, []byte, error) {
			return 1, []byte("123"), nil
		}).Reset()
		msg, err := wssConn.ReceiveMessage()
		So(err, ShouldBeNil)
		So(msg.MessageType, ShouldEqual, 1)
	})
	Convey("Test connection is nil", t, func() {
		wssConn := NewWssConnection("", nil)
		msg, err := wssConn.ReceiveMessage()
		So(err.Error(), ShouldEqual, "connection not established")
		So(msg.MessageType, ShouldEqual, 0)
	})
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
			testFunc:        WrapSystemInfo,
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
			testFunc:        UnwrapSystemInfo,
			args:            args{data: []byte(`{"data":"NTY3","traceID":"` + traceID.String() + `"}`)},
			expectedTraceID: traceID.String(),
			expectedMessage: []byte(`567`),
			errIsNil:        true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name:     "unwrapSystemInfo not normal case",
			testFunc: UnwrapSystemInfo,
			args:     args{data: []byte(`"data":"NTY3","traceID":"` + traceID.String() + `"}`)},
			errIsNil: false,
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
			if tt.expectedTraceID != "" && id != tt.expectedTraceID {
				t.Errorf("wrapSystemInfo() got = %s, want %s", id, tt.expectedTraceID)
			}
			if tt.expectedMessage != nil && !reflect.DeepEqual(msg, tt.expectedMessage) {
				t.Errorf("wrapSystemInfo() got1 = %s, want %s", msg, tt.expectedMessage)
			}
			patches.ResetAll()
		})
	}
}

func Test_WaitUtilInterrupt(t *testing.T) {
	Convey("test WaitUtilInterrupt", t, func() {
		Convey("case1: stopCh is nil", func() {
			isCalled := false
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "Shutdown", func(_ *http.Server, _ context.Context) error {
					isCalled = true
					return nil
				}),
			})
			defer patches.ResetAll()

			server := &http.Server{}
			go WaitUtilInterrupt(nil, server)
			time.Sleep(100 * time.Millisecond)
			So(isCalled, ShouldBeFalse)
		})
		Convey("case2: http server shut down", func() {
			isCalled := false
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "Shutdown", func(_ *http.Server, _ context.Context) error {
					isCalled = true
					return nil
				}),
			})
			defer patches.ResetAll()

			server := &http.Server{}
			stopCh := make(chan struct{})
			go WaitUtilInterrupt(stopCh, server)
			time.Sleep(100 * time.Millisecond)
			close(stopCh)
			time.Sleep(100 * time.Millisecond)
			So(isCalled, ShouldBeTrue)
		})
	})
}
