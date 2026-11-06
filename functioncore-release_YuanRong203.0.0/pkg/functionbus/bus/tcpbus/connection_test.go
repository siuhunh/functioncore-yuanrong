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
	"context"
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

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/monitor"
	bufPool "functioncore/pkg/common/pool"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	errcode "functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
	"github.com/stretchr/testify/assert"
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
		syncCond := sync.NewCond(&sync.Mutex{})
		isConnectListened := int32(0)
		msgChan := make(chan interface{}, defaultChanCap)
		isMsgChanOpen := int32(1)
		go handleConn(conn, pool.Options{
			SyncCond:          syncCond,
			MsgChan:           msgChan,
			IsMsgChanOpen:     &isMsgChanOpen,
			IsConnectListened: &isConnectListened,
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
		syncCond := sync.NewCond(&sync.Mutex{})
		isConnectListened := int32(0)
		msgChan := make(chan interface{}, defaultChanCap)
		isMsgChanOpen := int32(1)
		if !defaultMsgTypeProcess(msg, pool.Options{
			SyncCond:          syncCond,
			MsgChan:           msgChan,
			IsMsgChanOpen:     &isMsgChanOpen,
			IsConnectListened: &isConnectListened,
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
		syncCond := sync.NewCond(&sync.Mutex{})
		isConnectListened := int32(0)
		msgChan := make(chan interface{}, defaultChanCap)
		isMsgChanOpen := int32(0)
		if defaultMsgTypeProcess(msg, pool.Options{
			SyncCond:          syncCond,
			MsgChan:           msgChan,
			IsMsgChanOpen:     &isMsgChanOpen,
			IsConnectListened: &isConnectListened,
		}, conn) {
			t.Error("this should be false")
		}
	})
}

func TestClearPendingReq(t *testing.T) {
	conn := Connection{pending: clientsMap, directAsyncPending: &sync.Map{}}
	conn.directAsyncPending.Store("request001", "traceID001")
	conn.RegisterDirectAsyncRequestHandler(mockDirectAsyncHandler)
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

func TestProcessDirectAsyncRequest(t *testing.T) {
	conn := Connection{}
	rsp := &protocol.Message{}
	rsp.Header.PacketID = "packageID001"
	rsp.Header.ErrCode = errcode.ClientExitErrCode
	rsp.Payload = []byte(errcode.ClientExitErr)
	conn.processDirectAsyncRequest(rsp)
	conn.directAsyncPending = &sync.Map{}
	conn.directAsyncPending.Store("packageID001", "traceID001")
	conn.RegisterDirectAsyncRequestHandler(mockDirectAsyncHandler)
	conn.processDirectAsyncRequest(rsp)
	traceID, ok := conn.directAsyncPending.Load("packageID001")
	assert.Equal(t, false, ok)
	assert.Equal(t, nil, traceID)
}

func mockDirectAsyncHandler(msg *protocol.Message, requestID string, exited bool) {
	if exited {
		return
	}
	if msg.Header.ErrCode != errcode.ClientExitErrCode {
		panic("cannot clear pending request")
	}
}

func TestProcessCallReportMsg(t *testing.T) {
	convey.Convey("TestProcessCallReportMsg", t, func() {
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
	})
	convey.Convey("TestProcessCallReportMsg with error unmarshal", t, func() {
		patch := gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
			return errors.New("test")
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
	})
	convey.Convey("TestProcessCallReportMsg with posix", t, func() {
		patch := gomonkey.ApplyFunc(workerevaluator.SyncAbandonedByPosixCallResult, func(_ *pb.PosixCallResult) {
			return
		})
		defer patch.Reset()

		conn := Connection{pending: clientsMap}
		callResult := &pb.PosixCallResult{
			Logs:       "log",
			Summary:    "summary",
			InstanceID: "123",
		}

		message := &pb.Message{
			Type:            pb.Message_POSIX_CALL_RESPONSE,
			PosixCallResult: callResult,
		}

		bytes, _ := proto.Marshal(message)
		msg := &protocol.Message{
			Payload: bytes,
		}
		conn.processCallReportMsg(msg)
	})
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

		var isConnectListened int32 = 0
		syncCond := sync.NewCond(&sync.Mutex{})
		isMsgChanOpen := int32(1)
		c.watchServerDisable(syncCond, &isMsgChanOpen, &isConnectListened)
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
	msg := &protocol.Message{Payload: make([]byte, 100)}
	if err := c.SendWithoutRetry(msg); (err != nil) != false {
		t.Errorf("SendWithoutRetry() error = %v, wantErr %v", err, false)
	}
	if len(msg.Payload) != 100 {
		t.Errorf("payload is invalid, length is %d", len(msg.Payload))
	}
	patches.ResetAll()
}

func Test_sendCallResult(t *testing.T) {
	c := &Connection{wg: &sync.WaitGroup{}, workerID: "aaa"}
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

func Test_sendCallResult_sendWithRetry(t *testing.T) {
	convey.Convey("Test SendWithRetryCallBack", t, func(con convey.C) {
		c := &Connection{wg: &sync.WaitGroup{}}
		count := 0
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&msgPool{}), "Get", func(_ *msgPool) *protocol.Message {
				return &protocol.Message{Header: protocol.MsgHeader{}}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&msgPool{}), "Put", func(_ *msgPool, _ *protocol.Message) {}),
			gomonkey.ApplyFunc((*Connection).sendWithRetry, func(_ *Connection, msg *protocol.Message) {
				count = 1
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&sync.WaitGroup{}), "Done", func(_ *sync.WaitGroup) {}),
		})
		defer patches.ResetAll()
		packetID := "testPacketID"
		res := types.Result{ErrorCode: 200}
		msg := make(map[string]string)
		c.sendCallResult(packetID, res, msg)
		convey.So(count, convey.ShouldEqual, 1)
	})
}

func Test_sendWithRetry(t *testing.T) {
	callCount := 0
	tests := []struct {
		name        string
		patchesFunc testutils.PatchesFunc
		connection  *Connection
		count       int
	}{
		{
			name: "case1 isTimeout",
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*Connection).send, func(_ *Connection, msg *protocol.Message) error {
						callCount++
						return errors.New("error")
					}),
				})

				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(isTimeout, func(startTime time.Time) bool {
						return true
					}),
				})
				return patches
			},
			connection: &Connection{},
			count:      0,
		},
		{
			name: "case2 send retry success",
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*Connection).send, func(_ *Connection, msg *protocol.Message) error {
						if len(msg.Payload) == 0 {
							return errors.New("msg payload is not valid")
						}
						callCount++
						if callCount >= 3 {
							return nil
						}
						return errors.New("test error")
					}),
				})
				return patches
			},
			connection: &Connection{},
			count:      3,
		},
		{
			name: "case3 send retry failed",
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*Connection).send, func(_ *Connection, msg *protocol.Message) error {
						callCount++
						return errors.New("retry error")
					}),
				})
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(isTimeout, func(startTime time.Time) bool {
						return callCount >= 10
					}),
				})
				return patches
			},
			connection: &Connection{},
			count:      10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			callCount = 0
			msg := &protocol.Message{Payload: make([]byte, 10)}
			tt.connection.sendWithRetry(msg)
			if tt.count != callCount {
				t.Errorf("SendWithRetryCallBack test failed, actual %d, expected: %d", callCount, tt.count)
			}
			if len(msg.Payload) != 0 {
				t.Errorf("msg reset failed")
			}
			patches.ResetAll()
		})
	}
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
		var isConnectListened int32 = 0
		syncCond := sync.NewCond(&sync.Mutex{})
		exitFlag := c.isConnNormalExit(syncCond, &isMsgChanOpen, &isConnectListened)
		convey.So(exitFlag, convey.ShouldBeFalse)
	})
	convey.Convey("Test check connection normal exit, NormalExit is true", t, func() {
		var isMsgChanOpen int32 = 0
		var isConnectListened int32 = 0
		syncCond := sync.NewCond(&sync.Mutex{})
		c.setConnNormalExitFlag()
		exitFlag := c.isConnNormalExit(syncCond, &isMsgChanOpen, &isConnectListened)
		convey.So(exitFlag, convey.ShouldBeTrue)
	})
}

func TestSendRequest_asyncResponse(t *testing.T) {
	c := &Connection{wg: &sync.WaitGroup{},
		msgChan: make(chan interface{}, 1), pending: &sync.Map{}, directAsyncPending: &sync.Map{}}
	patches := testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc((*Connection).send, func(c *Connection, msg *protocol.Message) error {
			return nil
		}),
		gomonkey.ApplyFunc(constants.GetSyncRequestTimeout, func() int64 {
			return 1
		}),
	})
	defer patches.ResetAll()

	ctx, cancelFunc := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFunc()
	ctx = utils.ContextWithValueString(ctx, constants.HeaderTraceID, "asyncTrace001")
	ctx = utils.ContextWithValueString(ctx, constants.HeaderAsync, "true")

	msg := &protocol.Message{}
	msg.Header.PacketID = "packageID001"
	result := c.sendRequest(ctx, msg, false, 1)
	assert.Equal(t, uint32(snerror.RequestAcceptedCode), result.ErrorCode)
}
