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

// Package webshell define function to exec webshell
package webshell

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"

	commonwebsocket "functioncore/pkg/common/websocket"
	mockUtils "functioncore/test/utils"
)

func TestSessionManager(t *testing.T) {
	Convey("Test SessionManager", t, func() {
		patches := gomonkey.ApplyFunc((*SafeWsConn).setUp, func(_ *SafeWsConn) {})
		Convey("Test Add", func() {
			Convey("Should add a new session to the manager", func() {
				conn := &websocket.Conn{}
				GetSessionMgrInstance().Add("1", "cluster1_a_podA", conn, true)
				So(GetSessionMgrInstance().Get("1").downStream.targetID, ShouldEqual, "cluster1_a_podA")
			})
		})

		Convey("Test Get", func() {
			Convey("Should return the session for the given cluster ID", func() {
				conn := &websocket.Conn{}
				GetSessionMgrInstance().Add("2", "cluster2_a_podA", conn, false)
				fetchedSessionPair := GetSessionMgrInstance().Get("2")
				So(fetchedSessionPair, ShouldNotBeNil)
			})

			Convey("Should return nil if the session does not exist", func() {
				fetchedSession := GetSessionMgrInstance().Get("3")
				So(fetchedSession, ShouldBeNil)
			})
		})

		Convey("Test Remove", func() {
			Convey("Should remove the session from the manager", func() {
				conn := &websocket.Conn{}
				GetSessionMgrInstance().Add("3", "cluster4_a_podA", conn, false)
				patch := gomonkey.ApplyFunc((*websocket.Conn).Close, func(_ *websocket.Conn) error {
					return nil
				})
				GetSessionMgrInstance().Remove("3")
				fetchedSession := GetSessionMgrInstance().Get("3")
				So(fetchedSession, ShouldBeNil)
				patch.Reset()
			})
		})

		Convey("Test Session Close", func() {
			Convey("Should close the WebSocket connection and stop the pings", func() {
				conn := &websocket.Conn{}
				GetSessionMgrInstance().Add("4", "cluster5_a_podA", conn, true)

				patch := gomonkey.ApplyFunc((*Session).stopPings, func(_ *Session) {})
				patch.ApplyFunc((*websocket.Conn).Close, func(_ *websocket.Conn) error {
					return nil
				})
				fetchedSession := GetSessionMgrInstance().Get("4")
				fetchedSession.downStream.Close()
				patch.Reset()
			})
		})
		patches.Reset()
	})
}

func Test_forwardMessage(t *testing.T) {
	var pair *SessionPair
	patch := gomonkey.ApplyFunc((*SafeWsConn).setUp, func(_ *SafeWsConn) {})
	defer patch.Reset()
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1 read msg error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).SetReadDeadline, func(_ *websocket.Conn, t time.Time) error {
						return nil
					}),
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, errors.New("error")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.downStream), "TrySendMessage",
						func(_ *Session, message commonwebsocket.WssMessage) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.downStream), "Close", func(_ *Session) {
						return
					}),
				})
				return patches
			},
		}, {
			name: "case2 codec error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).SetReadDeadline, func(_ *websocket.Conn, t time.Time) error {
						return nil
					}),
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, nil
					}),
					gomonkey.ApplyFunc(codecData, func(data []byte, isDecode bool) ([]byte, error) {
						return nil, errors.New("mock error")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.downStream), "Close",
						func(_ *Session) {
							return
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.upStream), "Close",
						func(_ *Session) {
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
					gomonkey.ApplyFunc((*websocket.Conn).SetReadDeadline, func(_ *websocket.Conn, t time.Time) error {
						return nil
					}),
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, nil
					}),
					gomonkey.ApplyFunc(codecData, func(data []byte, isDecode bool) ([]byte, error) {
						return []byte("mock body"), nil
					}),
				})
				return patches
			},
		},
		{
			name: "case4 SetReadDeadline error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*websocket.Conn).SetReadDeadline, func(_ *websocket.Conn, t time.Time) error {
						return errors.New("error")
					}),
					gomonkey.ApplyFunc((*websocket.Conn).ReadMessage, func(_ *websocket.Conn) (int, []byte, error) {
						return 0, []byte{1, 2, 3}, errors.New("error")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.downStream), "TrySendMessage",
						func(_ *Session, message commonwebsocket.WssMessage) error {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(pair.downStream), "Close", func(_ *Session) {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pair = &SessionPair{
				traceID: "ID",
				downStream: &Session{
					SafeWsConn:  newSafeWsConn(&websocket.Conn{}),
					forwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
					closeChan:   make(chan struct{}),
					isClosed:    uint32(1),
				},
				upStream: &Session{
					SafeWsConn:  newSafeWsConn(&websocket.Conn{}),
					forwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
					closeChan:   make(chan struct{}),
					isClosed:    uint32(1),
				},
			}
			patches := tt.patchesFunc()
			done := make(chan bool)
			go func() {
				pair.forwardMessage(pair.upStream, pair.downStream, codecData)
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

func TestSessionManager_CreateSessionPair(t *testing.T) {
	Convey("test CreateSessionPair", t, func() {
		traceID := "trace123"
		targetID := "target456"
		upConn := &websocket.Conn{}
		downConn := &websocket.Conn{}
		m := GetSessionMgrInstance()
		patch := gomonkey.ApplyFunc((*SafeWsConn).setUp, func(_ *SafeWsConn) {})
		sessionPair := m.CreateSessionPair(traceID, targetID, upConn, downConn)
		defer patch.Reset()
		So(sessionPair, ShouldNotBeNil)
		So(sessionPair.traceID, ShouldEqual, traceID)
		So(sessionPair.upStream.targetID, ShouldEqual, targetID)
		So(sessionPair.downStream.targetID, ShouldEqual, targetID)
	})
}

func Test_codecData(t *testing.T) {
	type args struct {
		data     []byte
		isDecode bool
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "test decode",
			args:    args{data: []byte("1W3NuQHBvb2wyMi0zMDAtMTI4LWZ1c2lvbi04Nzc0ZDZmN2ItZjdtcXMgZnVuY3Rpb25dJCA="), isDecode: true},
			want:    []byte("[sn@pool22-300-128-fusion-8774d6f7b-f7mqs function]$ "),
			wantErr: false,
		},
		{
			name:    "test encode",
			args:    args{data: []byte("pwd\n"), isDecode: false},
			want:    []byte("0cHdkCg=="),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := codecData(tt.args.data, tt.args.isDecode)
			if (err != nil) != tt.wantErr {
				t.Errorf("codecData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("codecData() got = %v, want %v", got, tt.want)
			}
		})
	}
}
