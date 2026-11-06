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

package tcpbus

import (
	"bytes"
	"errors"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/monitor"
	bufPool "functioncore/pkg/common/pool"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	errcode "functioncore/pkg/functionbus/proxyerrors"
	testutils "functioncore/test/utils"
)

type mockConn struct {
	buf []byte
}

type mockAddr struct{}

func (a *mockAddr) Network() string {
	return ""
}

func (a *mockAddr) String() string {
	return ""
}

func (conn *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (conn *mockConn) Write(b []byte) (n int, err error) {
	conn.buf = make([]byte, len(b))
	copy(conn.buf, b)
	return len(conn.buf), nil
}

func (conn *mockConn) Close() error {
	return nil
}

func (conn *mockConn) LocalAddr() net.Addr {
	return &mockAddr{}
}

func (conn *mockConn) RemoteAddr() net.Addr {
	return &mockAddr{}
}

func (conn *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (conn *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (conn *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}

type mockConnPool struct {
	conn net.Conn
	err  error
}

func (p *mockConnPool) Get() (net.Conn, error) {
	return p.conn, p.err
}

func (p *mockConnPool) Put(conn net.Conn) error {
	return nil
}

func (p *mockConnPool) Delete(conn net.Conn) {
}

func (p *mockConnPool) Close() {

}

func (p *mockConnPool) DisableInformer() {

}

func mockDecode(msg *protocol.Message, r io.Reader) error {
	*msg = protocol.Message{
		Header: protocol.MsgHeader{
			MsgType: protocol.CallRspMsg,
		},
	}
	return nil
}

func TestHandleConn(t *testing.T) {
	convey.Convey("TestHandleConn", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(protocol.Decode, mockDecode),
			gomonkey.ApplyFunc(monitor.ReachMemoryThreshold, func() bool { return false }),
			gomonkey.ApplyFunc(messageDecode, func(msg *protocol.Message, conn net.Conn) bool {
				*msg = protocol.Message{
					Header: protocol.MsgHeader{
						MsgType: protocol.CallRspMsg,
					},
				}
				return true
			}),
			gomonkey.ApplyFunc(defaultMsgTypeProcess, func(msg *protocol.Message, opt pool.Options, conn net.Conn) bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		conn := &mockConn{}
		connWG := &sync.WaitGroup{}
		msgChan := make(chan interface{}, defaultChanCap)
		isMsgChanOpen := int32(1)
		go handleConn(conn, pool.Options{
			ConnWG:        connWG,
			MsgChan:       msgChan,
			IsMsgChanOpen: &isMsgChanOpen,
		})
		_, ok := <-msgChan
		convey.So(ok, convey.ShouldBeTrue)
	})
}

func Test_messageDecode(t *testing.T) {
	convey.Convey("Test_messageDecode 1", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(protocol.Decode, func(msg *protocol.Message, r io.Reader) error {
				return io.EOF
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		conn := &mockConn{}
		msg := &protocol.Message{}
		if messageDecode(msg, conn) {
			t.Error("this should be false")
		}
	})
	convey.Convey("Test_messageDecode 2", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(protocol.Decode, func(msg *protocol.Message, r io.Reader) error {
				return errors.New("error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		conn := &mockConn{}
		msg := &protocol.Message{}
		if messageDecode(msg, conn) {
			t.Error("this should be false")
		}
	})

	convey.Convey("Test_messageDecode 3", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(protocol.Decode, func(msg *protocol.Message, r io.Reader) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		conn := &mockConn{}
		msg := &protocol.Message{}
		if !messageDecode(msg, conn) {
			t.Error("this should be false")
		}
	})
}

func Test_defaultMsgTypeProcess(t *testing.T) {
	convey.Convey("Test_defaultMsgTypeProcess 1", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(atomic.LoadInt32, func(addr *int32) (val int32) {
				return 1
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		conn := &mockConn{}
		msg := &protocol.Message{}
		connWG := &sync.WaitGroup{}
		msgChan := make(chan interface{}, defaultChanCap)
		isMsgChanOpen := int32(1)
		if !defaultMsgTypeProcess(msg, pool.Options{
			ConnWG:        connWG,
			MsgChan:       msgChan,
			IsMsgChanOpen: &isMsgChanOpen,
		}, conn) {
			t.Error("this should be false")
		}
	})
	convey.Convey("Test_defaultMsgTypeProcess 2", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(atomic.LoadInt32, func(addr *int32) (val int32) {
				return 0
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		conn := &mockConn{}
		msg := &protocol.Message{}
		connWG := &sync.WaitGroup{}
		msgChan := make(chan interface{}, defaultChanCap)
		isMsgChanOpen := int32(0)
		if defaultMsgTypeProcess(msg, pool.Options{
			ConnWG:        connWG,
			MsgChan:       msgChan,
			IsMsgChanOpen: &isMsgChanOpen,
		}, conn) {
			t.Error("this should be false")
		}
	})
}

func TestClearPendingReq(t *testing.T) {
	conn := Connection{pending: clientsMap}
	req := &pendingReq{
		done: make(chan *protocol.Message),
	}
	var err error

	go func() {
		resp := <-req.done
		if _, ok := conn.pending.Load(1); ok &&
			resp.Header.ErrCode != errcode.ClientExitErrCode {
			err = errors.New("cannot clear pending request")
		}
	}()
	conn.pending.Store(1, req)
	conn.clearPendingReq()
	if err != nil {
		t.Error(err.Error())
	}
}

func TestProcessCallReportMsg(t *testing.T) {
	patch := gomonkey.ApplyFunc(workerevaluator.SyncAbandoned, func(_ *pb.CallResponse) {
		return
	})
	defer patch.Reset()

	conn := Connection{pending: clientsMap}
	pb := pb.Message{
		CallResponse: &pb.CallResponse{
			InstanceID: "123",
		},
	}
	bytes, _ := proto.Marshal(&pb)
	msg := &protocol.Message{
		Payload: bytes,
	}
	conn.processCallReportMsg(msg)
}

func TestWatchServerDisable(t *testing.T) {
	convey.Convey("TestHandleConn", t, func() {
		c := Connection{
			netPool: &mockConnPool{
				conn: nil,
				err:  errors.New("error"),
			},
			msgChan: make(chan interface{}, 1),
			pending: &sync.Map{},
		}

		connWG := &sync.WaitGroup{}
		isMsgChanOpen := int32(1)
		c.watchServerDisable(connWG, &isMsgChanOpen)
		_, ok := <-c.msgChan
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestProcessCallReqMsg(t *testing.T) {
	convey.Convey("TestProcessCallReqMsg", t, func() {
		conn := &mockConn{}
		c := Connection{
			netPool: &mockConnPool{
				conn: conn,
				err:  nil,
			},
			msgChan: make(chan interface{}, 1),
			pending: &sync.Map{},
			wg:      &sync.WaitGroup{},
		}

		msg := &protocol.Message{
			Header: protocol.MsgHeader{
				ErrCode: errcode.HeavyLoadCode,
			},
		}
		c.wg.Add(1)
		c.processCallReqMsg(msg)
		convey.So(len(conn.buf), convey.ShouldBeGreaterThan, 0)
		convey.So(strings.Contains(string(conn.buf), errcode.HeavyLoadErr), convey.ShouldBeTrue)

		msg = &protocol.Message{
			Header: protocol.MsgHeader{},
		}
		c.wg.Add(1)
		c.processCallReqMsg(msg)
		convey.So(len(conn.buf), convey.ShouldBeGreaterThan, 0)
		convey.So(strings.Contains(string(conn.buf), errcode.UnRegisterCallBack), convey.ShouldBeTrue)

		msg = &protocol.Message{
			Header: protocol.MsgHeader{
				ErrCode: errcode.ClientExitErrCode,
			},
		}
		c.wg.Add(1)
		c.processCallReqMsg(msg)
		convey.So(len(conn.buf), convey.ShouldBeGreaterThan, 0)
	})
}

func TestGetPeerIP(t *testing.T) {
	convey.Convey("Test get right peer ip", t, func() {
		convey.So(getPeerIP("127.0.0.1:8080"), convey.ShouldEqual, "127.0.0.1")
	})
	convey.Convey("Test get wrong peer ip", t, func() {
		convey.So(getPeerIP("127.0.0.1"), convey.ShouldEqual, "")
	})
}

func TestConnection_SendWithoutRetry(t *testing.T) {
	mockFunc := func() testutils.PatchSlice {
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{gomonkey.ApplyMethod(reflect.TypeOf(&bufPool.BufferPool{}), "Get",
			func(_ *bufPool.BufferPool) *bytes.Buffer { return &bytes.Buffer{} }),
			gomonkey.ApplyMethod(reflect.TypeOf(&pool.ConnPool{}), "Get",
				func(_ *pool.ConnPool) (net.Conn, error) { return &mockConn{}, nil })})
		return patches
	}
	patches := mockFunc()
	c := &Connection{netPool: &pool.ConnPool{}}
	if err := c.SendWithoutRetry(&protocol.Message{}); (err != nil) != false {
		t.Errorf("SendWithoutRetry() error = %v, wantErr %v", err, false)
	}
	patches.ResetAll()
}

func Test_sendCallResult(t *testing.T) {
	c := &Connection{wg: &sync.WaitGroup{}}
	patches := testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&msgPool{}), "Get", func(_ *msgPool) *protocol.Message {
			return &protocol.Message{Header: protocol.MsgHeader{}}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&msgPool{}), "Put", func(_ *msgPool, _ *protocol.Message) {}),
		gomonkey.ApplyFunc((*Connection).send, func(_ *Connection, msg *protocol.Message) error {
			return errors.New("error")
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&sync.WaitGroup{}), "Done", func(_ *sync.WaitGroup) {}),
	})
	defer patches.ResetAll()
	packetID := "testPacketID"
	res := types.Result{ErrorCode: 500}
	msg := make(map[string]string)
	c.sendCallResult(packetID, res, msg)
}

func TestConnection_setConnNormalExitFlag(t *testing.T) {
	c := &Connection{wg: &sync.WaitGroup{}}
	convey.Convey("Test set normal exit flag", t, func() {
		c.setConnNormalExitFlag()
		convey.So(c.normalExit, convey.ShouldBeTrue)
	})
}

func TestConnection_isConnNormalExit(t *testing.T) {
	c := &Connection{wg: &sync.WaitGroup{}, msgChan: make(chan interface{}, 1), pending: &sync.Map{}}
	convey.Convey("Test check connection normal exit, NormalExit is false", t, func() {
		var isMsgChanOpen int32 = 0
		connWG := &sync.WaitGroup{}
		exitFlag := c.isConnNormalExit(connWG, &isMsgChanOpen)
		convey.So(exitFlag, convey.ShouldBeFalse)
	})
	convey.Convey("Test check connection normal exit, NormalExit is true", t, func() {
		var isMsgChanOpen int32 = 0
		c.setConnNormalExitFlag()
		exitFlag := c.isConnNormalExit(c.wg, &isMsgChanOpen)
		convey.So(exitFlag, convey.ShouldBeTrue)
	})
}
