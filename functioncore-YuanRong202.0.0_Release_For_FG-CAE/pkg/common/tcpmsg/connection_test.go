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

package tcpmsg

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/snerror"
)

type fakeConn struct {
}

func (f fakeConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (f fakeConn) Write(b []byte) (n int, err error)  { return 0, nil }
func (f fakeConn) Close() error                       { return nil }
func (f fakeConn) LocalAddr() net.Addr                { return &fakeAddr{} }
func (f fakeConn) RemoteAddr() net.Addr               { return &fakeAddr{} }
func (f fakeConn) SetDeadline(t time.Time) error      { return nil }
func (f fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (f fakeConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeConnErr struct {
}

func (f fakeConnErr) Read(b []byte) (n int, err error) {
	err = errors.New("test")
	return 0, err
}
func (f fakeConnErr) Write(b []byte) (n int, err error) {
	err = errors.New("test")
	return 0, err
}
func (f fakeConnErr) Close() error                       { return nil }
func (f fakeConnErr) LocalAddr() net.Addr                { return &fakeAddr{} }
func (f fakeConnErr) RemoteAddr() net.Addr               { return &fakeAddr{} }
func (f fakeConnErr) SetDeadline(t time.Time) error      { return nil }
func (f fakeConnErr) SetReadDeadline(t time.Time) error  { return nil }
func (f fakeConnErr) SetWriteDeadline(t time.Time) error { return nil }

type fakeAddr struct{}

func (a fakeAddr) Network() string {
	return "net"
}

func (a fakeAddr) String() string {
	return "127.0.0.1:8080"
}

func Test_buildResp(t *testing.T) {
	convey.Convey("buildResp", t, func() {
		msg := &Message{
			Header: Header{
				MsgType: StopProcess,
			},
		}
		resMsg, err := buildResp(msg, errors.New("test"), 1, RuntimeRSP{})
		convey.So(err, convey.ShouldBeNil)
		convey.So(resMsg.Header.MsgType, convey.ShouldEqual, StopProcess)
	})
}

func Test_read(t *testing.T) {
	convey.Convey("read", t, func() {
		convey.Convey("failed case", func() {
			var buf bytes.Buffer
			buf.Write([]byte("test"))
			con := &SocketConn{}
			_, err := con.read(&buf)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("successful case", func() {
			msg := &Message{
				Header: Header{
					MagicNumber: 0x08,
					Version:     0x01,
					MsgType:     StopProcess,
					BodyLen:     10,
					PacketID:    newPacketID(),
				},
				Body: []byte("0123456789"),
			}
			data, err := Encode(msg)
			convey.So(data, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
			var buf bytes.Buffer
			buf.Write(data)
			con := &SocketConn{}
			res, err := con.read(&buf)
			convey.So(err, convey.ShouldBeNil)
			convey.So(res.Header.MsgType, convey.ShouldEqual, StopProcess)
		})
	})
}

func Test_SendMessageWithNoResp(t *testing.T) {
	convey.Convey("SendMessageWithNoResp", t, func() {
		convey.Convey("failed case 1", func() {
			con := &SocketConn{}
			msg := &Message{
				Header: Header{
					MsgType: StopProcess,
				},
			}
			fakeConnect := fakeConn{}
			con.conn = fakeConnect
			err := con.SendMessageWithNoResp(msg)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("successful case", func() {
			con := &SocketConn{}
			msg := &Message{
				Header: Header{
					MagicNumber: 0x08,
					Version:     0x01,
					MsgType:     StopProcess,
					BodyLen:     10,
					PacketID:    newPacketID(),
				},
				Body: []byte("0123456789"),
			}
			fakeConnect := fakeConn{}
			con.conn = fakeConnect
			err := con.SendMessageWithNoResp(msg)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_processRequest(t *testing.T) {
	convey.Convey("SendMessageWithNoResp", t, func() {
		convey.Convey("failed case 1", func() {
			con := &SocketConn{}
			con.handlerFunc = func(*Message) (int, RuntimeRSP, error) {
				return 10, RuntimeRSP{}, nil
			}
			msg := &Message{
				Header: Header{
					MsgType: StopProcess,
				},
			}
			fakeConnect := fakeConn{}
			con.conn = fakeConnect
			err := con.processRequest(msg)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("failed case 2", func() {
			con := &SocketConn{}
			con.handlerFunc = func(*Message) (int, RuntimeRSP, error) {
				return 10, RuntimeRSP{}, nil
			}
			msg := &Message{
				Header: Header{
					MagicNumber: 0x08,
					Version:     0x01,
					MsgType:     StopProcess,
					BodyLen:     10,
					PacketID:    newPacketID(),
				},
				Body: []byte("0123456789"),
			}
			fakeConnect := fakeConnErr{}
			con.conn = fakeConnect
			err := con.processRequest(msg)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func Test_processResponse(t *testing.T) {
	convey.Convey("SendMessageWithNoResp", t, func() {
		convey.Convey("failed case", func() {
			msg := &Message{
				Header: Header{
					MsgType: StopProcess,
				},
			}
			con := &SocketConn{}
			err := con.processResponse(msg)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("successful case", func() {
			msg := &Message{
				Header: Header{
					MsgType:  StopProcess,
					PacketID: "test",
				},
			}
			con := &SocketConn{}
			tmpData := make(chan *Message, 10)
			con.packetMap.Store("test", tmpData)
			err := con.processResponse(msg)
			convey.So(err, convey.ShouldBeNil)
			len := 0
			con.packetMap.Range(func(k, v interface{}) bool {
				len++
				return true
			})
			convey.So(len, convey.ShouldEqual, 0)
		})
	})
}

func Test_SendMessage(t *testing.T) {
	convey.Convey("SendMessage", t, func() {
		convey.Convey("failed case 1", func() {
			msg := &Message{
				Header: Header{
					MsgType: StopProcess,
				},
			}
			con := &SocketConn{}
			con.isConnected = false
			data := con.SendMessage(msg)
			convey.So(data.Code, convey.ShouldEqual, snerror.RuntimeManagerProcessExited)
		})

		convey.Convey("failed case 2", func() {
			msg := &Message{
				Header: Header{
					MagicNumber: 0x08,
					Version:     0x01,
					MsgType:     StopProcess,
					BodyLen:     10,
					PacketID:    newPacketID(),
				},
				Body: []byte("0123456789"),
			}
			con := &SocketConn{}
			errCon := fakeConnErr{}
			con.isConnected = true
			con.conn = errCon
			data := con.SendMessage(msg)
			convey.So(data.Err, convey.ShouldEqual, "test")
		})

		convey.Convey("failed case 3", func() {
			msg := &Message{
				Header: Header{
					MagicNumber: 0x08,
					Version:     0x01,
					MsgType:     StopProcess,
					BodyLen:     10,
					PacketID:    newPacketID(),
				},
				Body: []byte("0123456789"),
			}
			con := &SocketConn{}
			normalCon := fakeConn{}
			con.isConnected = true
			con.conn = normalCon
			patch := ApplyFunc(context.WithTimeout, func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
				return context.WithDeadline(parent, time.Now().Add(100*time.Millisecond))
			})
			defer patch.Reset()
			data := con.SendMessage(msg)
			convey.So(data.Err, convey.ShouldEqual, "read response timeout")
		})
	})
}

func TestSocketConn_Reconnect(t *testing.T) {
	convey.Convey("TestSocketConn_Reconnect", t, func() {
		convey.Convey("TestSocketConn_Reconnect is connected", func() {
			conn := &SocketConn{
				conn:        fakeConn{},
				isConnected: true,
			}
			ok := conn.Reconnect()
			convey.So(ok, convey.ShouldBeTrue)
		})
		convey.Convey("TestSocketConn_Reconnect addr is empty", func() {
			conn := &SocketConn{
				conn:        fakeConn{},
				isConnected: false,
			}
			ok := conn.Reconnect()
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("TestSocketConn_Reconnect connect failed", func() {
			conn := &SocketConn{
				conn:        fakeConn{},
				isConnected: false,
				addr:        RuntimeUDS,
			}
			ok := conn.Reconnect()
			convey.So(ok, convey.ShouldBeFalse)
		})
		convey.Convey("TestSocketConn_Reconnect connect success", func() {
			conn := &SocketConn{
				conn:        fakeConn{},
				isConnected: false,
				addr:        RuntimeUDS,
			}
			convey.So(conn.IsConnected(), convey.ShouldBeFalse)
			patches := []*Patches{
				ApplyFunc(net.Dial, func(network, address string) (net.Conn, error) {
					return nil, nil
				}),
				ApplyMethod(reflect.TypeOf(conn), "ReceiveMessage", func(c *SocketConn) {}),
			}
			defer func() {
				time.Sleep(time.Millisecond)
				for _, p := range patches {
					p.Reset()
				}
			}()
			ok := conn.Reconnect()
			convey.So(ok, convey.ShouldBeTrue)
		})
	})
}

func TestSocketConn_ReceiveMessage(t *testing.T) {
	convey.Convey("TestSocketConn_ReceiveMessage", t, func() {
		convey.Convey("receive request", func() {
			first, done := true, false
			defer gomonkey.ApplyFunc(Decode, func(io.Reader) (*Message, error) {
				if !first {
					return nil, io.EOF
				}
				first = false
				return &Message{Header: Header{PacketID: "PacketID"}}, nil
			}).Reset()
			con := &SocketConn{}
			con.conn = fakeConn{}
			con.ConnectionErr = make(chan struct{})
			con.handlerFunc = func(message *Message) (int, RuntimeRSP, error) {
				done = true
				return MessageWithNoResponse, RuntimeRSP{}, nil
			}
			con.ReceiveMessage()
			convey.So(done, convey.ShouldEqual, true)
		})
		convey.Convey("receive response", func() {
			first := true
			defer gomonkey.ApplyFunc(Decode, func(io.Reader) (*Message, error) {
				if !first {
					return nil, io.EOF
				}
				first = false
				return &Message{Header: Header{PacketID: "PacketID"}}, nil
			}).Reset()
			con := &SocketConn{}
			con.packetMap.Store("PacketID", true)
			con.conn = fakeConn{}
			con.ConnectionErr = make(chan struct{})
			con.ReceiveMessage()
			_, exist := con.packetMap.Load("PacketID")
			convey.So(exist, convey.ShouldEqual, false)
		})
	})
}

func TestListenSocketServer(t *testing.T) {
	convey.Convey("TestListenSocketServer", t, func() {
		handlerFunc := func(message *Message) (int, RuntimeRSP, error) {
			return MessageWithNoResponse, RuntimeRSP{}, nil
		}
		err := ListenSocketServer("127.0.0.1:6789", handlerFunc, func(conn *SocketConn) {})
		convey.So(err, convey.ShouldBeNil)
		con, err := CreateSocketClient("127.0.0.1:6789", handlerFunc)
		if con != nil {
			defer con.conn.Close()
		}
		convey.So(err, convey.ShouldBeNil)
	})
}

func Test_getSocketType(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		excepted string
	}{
		{
			name:     "tcp addr",
			addr:     "127.0.0.1:8080",
			excepted: socketTypeTCP,
		},
		{
			name:     "unix addr 1",
			addr:     "/tmp/a.sock",
			excepted: socketTypeUnix,
		},
		{
			name:     "unix addr 2",
			addr:     RuntimeUDS,
			excepted: socketTypeUnix,
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.excepted, getSocketType(tt.addr), tt.name)
	}
}
