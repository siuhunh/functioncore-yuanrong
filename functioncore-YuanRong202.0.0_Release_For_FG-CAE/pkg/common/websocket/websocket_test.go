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
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gorilla/websocket"
	. "github.com/smartystreets/goconvey/convey"
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
