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

package connect

import (
	"bytes"
	"errors"
	"fmt"
	"functioncore/pkg/worker/utils"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime/rpc/connect/message"
)

func runServer() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}

	msgPool := &sync.Pool{
		New: func() interface{} {
			return &message.Message{}
		},
	}
	bufPool := &sync.Pool{
		New: func() interface{} {
			return &bytes.Buffer{}
		},
	}
	process := func(conn net.Conn) {
		for {
			msg := msgPool.Get().(*message.Message)
			err := message.Decode(msg, conn)
			if err != nil {
				panic(err)
			}
			go func() {
				msg.Header.PacketType = message.NormalResp
				buf := bufPool.Get().(*bytes.Buffer)
				defer bufPool.Put(buf)
				err := message.Encode(msg, buf)
				if err != nil {
					panic(err)
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					panic(err)
				}
				msgPool.Put(msg)
			}()
		}
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			go process(conn)
		}
	}()

	return listener.Addr().String(), nil
}

func TestNewMessage(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}
	Convey("test new message", t, func() {
		Convey("test new message success", func() {
			c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
			if err != nil {
				t.Fatal(err)
			}
			metaData := []byte("meta-data")
			payload := []byte("payload")
			msg := c.newMessage("", message.Req, metaData, payload)
			So(string(msg.Payload), ShouldEqual, string(payload))
			So(string(msg.MetaData), ShouldEqual, string(metaData))
			So(msg.Header.PacketType, ShouldEqual, message.Req)
		})
	})
}

func TestTinyComm(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}

	c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
	if err != nil {
		t.Fatal(err)
	}

	expectMeta := []byte("meta-data")
	expectPayload := []byte("payload")
	meta, payload, err := c.Request(5*time.Second, expectMeta, expectPayload)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectMeta, meta)
	assert.Equal(t, expectPayload, payload)
}

func TestTinyCommParallel(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}

	c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
	if err != nil {
		t.Fatal(err)
	}

	n := 10
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("%d", i)
		t.Run(s, func(t *testing.T) {
			t.Parallel()
			expectMeta := []byte("meta-data" + s)
			expectPayload := []byte("payload" + s)
			meta, payload, err := c.Request(5*time.Second, expectMeta, expectPayload)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, expectMeta, meta)
			assert.Equal(t, expectPayload, payload)
		})
	}
}

type testReceiver struct {
	t *testing.T
}

func (t *testReceiver) HandleRequest(metaData []byte, payload []byte) ([]byte, []byte, error) {
	assert.Equal(t.t, []byte("req meta"), metaData)
	assert.Equal(t.t, []byte("req payload"), payload)
	return []byte("resp meta"), []byte("resp payload"), nil
}

func (t *testReceiver) HandlePost(metaData []byte, payload []byte) error {
	assert.Equal(t.t, []byte("post meta"), metaData)
	assert.Equal(t.t, []byte("post payload"), payload)
	return nil
}

func (t *testReceiver) RecvRequest(ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
}

func TestTinyCommReceiver(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(err)
			}

			req := &message.Message{}
			err = message.Decode(req, conn)
			if err != nil {
				panic(err)
			}

			{
				// send post
				msg := &message.Message{}
				msg.Header.PacketType = message.Post
				msg.MetaData = []byte("post meta")
				msg.Payload = []byte("post payload")
				msg.Header.PacketID = fmt.Sprintf("%036d", 1)
				buf := &bytes.Buffer{}
				err = message.Encode(msg, buf)
				if err != nil {
					panic(err)
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					panic(err)
				}
			}

			{
				// send req
				msg := &message.Message{}
				msg.Header.PacketType = message.Req
				msg.MetaData = []byte("req meta")
				msg.Payload = []byte("req payload")
				msg.Header.PacketID = fmt.Sprintf("%036d", 2)
				buf := &bytes.Buffer{}
				err = message.Encode(msg, buf)
				if err != nil {
					panic(err)
				}
				_, err = conn.Write(buf.Bytes())
				if err != nil {
					panic(err)
				}

				// receive req's resp
				resp := &message.Message{}
				err = message.Decode(resp, conn)
				if err != nil {
					panic(err)
				}
				assert.Equal(t, msg.Header.PacketID, resp.Header.PacketID)
				assert.Equal(t, message.NormalResp, resp.Header.PacketType)
				assert.Equal(t, []byte("resp meta"), resp.MetaData)
				assert.Equal(t, []byte("resp payload"), resp.Payload)
			}

			buf := &bytes.Buffer{}
			req.Header.PacketType = message.NormalResp
			err = message.Encode(req, buf)
			if err != nil {
				panic(err)
			}
			_, err = conn.Write(buf.Bytes())
			if err != nil {
				panic(err)
			}

			wg.Done()
		}
	}()

	receiver := &testReceiver{t: t}
	c, err := Build(receiver, BuildOptions{ServerAddr: listener.Addr().String(), ReconnectTimes: 1})
	if err != nil {
		panic(err)
	}

	expectMeta := []byte("meta-data")
	expectPayload := []byte("payload")
	meta, payload, err := c.Request(5*time.Second, expectMeta, expectPayload)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expectMeta, meta)
	assert.Equal(t, expectPayload, payload)

	wg.Wait()
}

func TestTinyCommSendRequest(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}

	receiver := &testReceiver{}
	c, err := Build(receiver, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
	if err != nil {
		t.Fatal(err)
	}

	metrics := invokectx.DefaultMetrics

	expectedMeta := []byte("meta-data")
	expectedPayload := []byte("payload")

	n := int64(1)
	waitCh := make(chan struct{})
	patches := gomonkey.ApplyMethod(reflect.TypeOf(receiver), "RecvRequest", func(_ *testReceiver, ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
		assert.Nil(t, err)
		assert.Equal(t, expectedMeta, metaData)
		assert.Equal(t, expectedPayload, payload)
		metrics.ReleaseInvokeCtx(ic)
		if atomic.AddInt64(&n, -1) == 0 {
			close(waitCh)
		}
	})
	defer patches.Reset()

	ic := metrics.AcquireInvokeCtx()
	c.SendRequest(ic, 5*time.Second, expectedMeta, expectedPayload)
	<-waitCh
}

func TestTinyCommSendRequestTimeout(t *testing.T) {
	receiver := &testReceiver{}
	metrics := invokectx.DefaultMetrics

	expectedMeta := []byte("meta-data")
	expectedPayload := []byte("payload")

	n := int64(1)
	waitCh := make(chan struct{})
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(receiver), "RecvRequest", func(_ *testReceiver, ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
		assert.NotNil(t, err) // timeout error
		metrics.ReleaseInvokeCtx(ic)
		if atomic.AddInt64(&n, -1) == 0 {
			close(waitCh)
		}
	})
	conn := &net.TCPConn{}
	patches.ApplyFunc(net.DialTimeout, func(network, address string, timeout time.Duration) (net.Conn, error) { return conn, nil })
	patches.ApplyMethod(reflect.TypeOf(conn), "Write", func(_ *net.TCPConn, b []byte) (n int, err error) {
		return len(b), nil
	})
	patches.ApplyMethod(reflect.TypeOf(conn), "Read", func(_ *net.TCPConn, b []byte) (n int, err error) {
		select {} // blocking
	})
	patches.ApplyMethod(reflect.TypeOf(conn), "SetKeepAlive", func(_ *net.TCPConn, keepalive bool) error { return nil })
	patches.ApplyMethod(reflect.TypeOf(conn), "SetNoDelay", func(_ *net.TCPConn, keepalive bool) error { return nil })
	defer patches.Reset()

	c, err := Build(receiver, BuildOptions{ServerAddr: "", ReconnectTimes: 1})
	if err != nil {
		t.Fatal(err)
	}
	ic := metrics.AcquireInvokeCtx()
	c.SendRequest(ic, 1*time.Second, expectedMeta, expectedPayload)
	<-waitCh
}

func TestTinyCommSendRequestEOF(t *testing.T) {
	receiver := &testReceiver{}
	metrics := invokectx.DefaultMetrics

	expectedMeta := []byte("meta-data")
	expectedPayload := []byte("payload")

	n := int64(1)
	waitCh := make(chan struct{})
	patches := gomonkey.NewPatches()
	patches.ApplyMethod(reflect.TypeOf(receiver), "RecvRequest", func(_ *testReceiver, ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
		assert.NotNil(t, err)
		metrics.ReleaseInvokeCtx(ic)
		if atomic.AddInt64(&n, -1) == 0 {
			close(waitCh)
		}
	})
	conn := &net.TCPConn{}
	patches.ApplyFuncSeq(net.DialTimeout, []gomonkey.OutputCell{
		{Values: gomonkey.Params{conn, nil}},
		{Values: gomonkey.Params{nil, errors.New("mock error")}},
	})
	patches.ApplyMethod(reflect.TypeOf(conn), "Write", func(_ *net.TCPConn, b []byte) (n int, err error) {
		return len(b), nil
	})
	patches.ApplyMethod(reflect.TypeOf(conn), "Read", func(_ *net.TCPConn, b []byte) (n int, err error) {
		return 0, io.EOF
	})
	patches.ApplyMethod(reflect.TypeOf(conn), "SetKeepAlive", func(_ *net.TCPConn, keepalive bool) error { return nil })
	patches.ApplyMethod(reflect.TypeOf(conn), "SetNoDelay", func(_ *net.TCPConn, keepalive bool) error { return nil })
	patches.ApplyMethod(reflect.TypeOf(conn), "RemoteAddr", func(_ *net.TCPConn) net.Addr { return &net.IPAddr{} })
	defer patches.Reset()

	c, err := Build(receiver, BuildOptions{ServerAddr: "", ReconnectTimes: 1})
	if err != nil {
		t.Fatal(err)
	}
	ic := metrics.AcquireInvokeCtx()
	c.SendRequest(ic, 1*time.Second, expectedMeta, expectedPayload)
	<-waitCh
}

type sendRequestCtx struct {
	record *record
}

func (*sendRequestCtx) Reset() {}

var getter = invokectx.DefaultMetrics.Register(func() invokectx.Poolable { return &sendRequestCtx{} })

func BenchmarkTinyCommSendRequest(b *testing.B) {
	addr, err := runServer()
	if err != nil {
		b.Fatal(err)
	}

	receiver := &testReceiver{}
	c, err := Build(receiver, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
	if err != nil {
		b.Fatal(err)
	}

	metrics := invokectx.DefaultMetrics

	meta := []byte("meta-data")
	payload := []byte("payload")

	r := newRecorder(true, int64(b.N))

	n := int64(b.N)
	waitCh := make(chan struct{})
	patches := gomonkey.ApplyMethod(reflect.TypeOf(receiver), "RecvRequest", func(_ *testReceiver, ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
		ctx := getter(ic).(*sendRequestCtx)
		if err != nil {
			b.Error(err)
		}
		r.requestFinish(ctx.record, err)
		metrics.ReleaseInvokeCtx(ic)
		if atomic.AddInt64(&n, -1) == 0 {
			close(waitCh)
		}
	})
	defer patches.Reset()

	b.SetParallelism(200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ic := metrics.AcquireInvokeCtx()
			ctx := getter(ic).(*sendRequestCtx)
			rr := r.requestStart()
			ctx.record = rr

			c.SendRequest(ic, 5*time.Second, meta, payload)
		}
	})
	<-waitCh
	r.reportSummary()
}

func BenchmarkTinyCommRequest(b *testing.B) {
	addr, err := runServer()
	if err != nil {
		b.Fatal(err)
	}

	c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
	if err != nil {
		b.Fatal(err)
	}

	meta := []byte("meta-data")
	payload := []byte("payload")

	r := newRecorder(true, int64(b.N))

	b.SetParallelism(200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rr := r.requestStart()
			_, _, err := c.Request(5*time.Second, meta, payload)
			if err != nil {
				b.Error(err)
			}
			r.requestFinish(rr, err)
		}
	})

	r.reportSummary()
}

type recorder struct {
	enable        bool
	times         int64
	errCount      uint64
	maxDuration   int64
	totalDuration int64
}

type record struct {
	t time.Time
}

func newRecorder(enable bool, times int64) *recorder {
	return &recorder{
		enable: enable,
		times:  times,
	}
}

func (r *recorder) requestStart() *record {
	if !r.enable {
		return nil
	}

	start := time.Now()
	return &record{t: start}
}

func (r *recorder) requestFinish(rr *record, err error) {
	if !r.enable {
		return
	}

	if err != nil {
		atomic.AddUint64(&r.errCount, 1)
	}
	duration := time.Since(rr.t).Nanoseconds()
	for {
		max := atomic.LoadInt64(&r.maxDuration)
		if duration < max {
			break
		}

		if atomic.CompareAndSwapInt64(&r.maxDuration, max, duration) {
			break
		}
	}
	atomic.AddInt64(&r.totalDuration, duration)
}

func (r *recorder) reportSummary() {
	if !r.enable {
		return
	}

	fmt.Printf("total times: %d\n", r.times)
	fmt.Printf("error times: %d\n", atomic.LoadUint64(&r.errCount))
	fmt.Printf("max duration: %d\n", atomic.LoadInt64(&r.maxDuration)/1e6)
	fmt.Printf("average duration: %d\n", atomic.LoadInt64(&r.totalDuration)/r.times/1e6)
}

type fakeConn struct {
	readBuf *bytes.Buffer
}

func (f fakeConn) Read(b []byte) (n int, err error) { return f.readBuf.Read(b) }

func (fakeConn) Write(b []byte) (n int, err error) { return 0, nil }

func (fakeConn) Close() error { return nil }

func (fakeConn) LocalAddr() net.Addr { return &net.IPAddr{} }

func (fakeConn) RemoteAddr() net.Addr { return &net.IPAddr{} }

func (fakeConn) SetDeadline(t time.Time) error { return nil }

func (fakeConn) SetReadDeadline(t time.Time) error { return nil }

func (fakeConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeReceiver struct{}

func (f fakeReceiver) HandleRequest(metaData []byte, payload []byte) ([]byte, []byte, error) {
	return nil, nil, nil
}

func (f fakeReceiver) HandlePost(metaData []byte, payload []byte) error {
	return nil
}

func (f fakeReceiver) RecvRequest(ic invokectx.InvokeCtx, metaData, payload []byte, err error) {}

func TestTinyCommClearReq(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}
	Convey("test tinyComm clear request", t, func() {
		Convey("test tinyComm clear request success", func() {
			c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan *message.Message, 1)
			c.pending = map[string]*pendingReq{
				"req": {done: done},
			}
			c.clearReq()
			msg := <-done
			assert.Equal(t, message.DisconnectResp, msg.Header.PacketType)
		})
		Convey("test tinyComm send request", func() {
			var wg sync.WaitGroup
			conn := fakeConn{}
			gomonkey.ApplyMethod(reflect.TypeOf(conn), "Read", func(fakeConn, []byte) (int, error) { select {} })

			receiver := fakeReceiver{}
			gomonkey.ApplyMethod(reflect.TypeOf(receiver), "RecvRequest", func(_ fakeReceiver, ic invokectx.InvokeCtx, metaData, payload []byte, err error) {
				wg.Done()
			})

			c, err := Build(receiver, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
			if err != nil {
				t.Fatal(err)
			}
			c.conn = conn

			wg.Add(1)
			c.SendRequest(invokectx.DefaultMetrics.AcquireInvokeCtx(), 3*time.Second, nil, nil)

			c2, err := Build(receiver, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
			if err != nil {
				t.Fatal(err)
			}
			c2.conn = conn
			// requests made by other TinyComm instances should not be affected.
			c2.clearReq()

			c.clearReq()
		})
	})
}

func TestTinyCommGetConnInfo(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}
	Convey("test tinyComm get connect info", t, func() {
		Convey("test tinyComm get connect info success", func() {
			c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
			if err != nil {
				t.Fatal(err)
			}
			connInfo := c.GetConnInfo()
			So(connInfo, ShouldNotEqual, "")
		})
	})
}

func TestTinyCommRecvMsg(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}
	Convey("test tinyComm receive message", t, func() {
		c, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 1})
		if err != nil {
			t.Fatal(err)
		}
		Convey("test tinyComm receive normal resp", func() {
			metaData := []byte("meta-data")
			payload := []byte("payload")
			msg := c.newMessage(genPacketID(1), message.Post, metaData, payload)
			err := c.send(msg)
			So(err, ShouldEqual, nil)
		})
	})
}

func TestSetTCPOption(t *testing.T) {

	Convey("test setTCPOption", t, func() {
		Convey("test setTCPOption error scene1", func() {
			conn := net.IPConn{}
			err := setTCPOption(&conn)
			So(err, ShouldNotEqual, nil)
		})

		Convey("test setTCPOption error scene2", func() {
			conn := net.TCPConn{}
			err := setTCPOption(&conn)
			So(err, ShouldNotEqual, nil)
		})

		Convey("test setTCPOption error success", func() {
			conn := net.TCPConn{}
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&conn), "SetKeepAlive", func(_ *net.TCPConn, keepalive bool) error {
				return nil
			})
			defer patch.Reset()
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&conn), "SetNoDelay", func(_ *net.TCPConn, noDelay bool) error {
				return nil
			})
			defer patch1.Reset()

			err := setTCPOption(&conn)
			fmt.Println(err)
			So(err, ShouldEqual, nil)
		})
	})
}

func TestIsBroken(t *testing.T) {
	tc := &TinyComm{}

	atomic.StoreUint32(&tc.brokenConn, 0)
	if tc.IsBroken() {
		t.Error("Expected IsBroken to return false when brokenConn is 0")
	}

	atomic.StoreUint32(&tc.brokenConn, 1)
	if !tc.IsBroken() {
		t.Error("Expected IsBroken to return true when brokenConn is not 0")
	}
}

func TestBuildWithSpecialErr(t *testing.T) {
	addr, err := runServer()
	if err != nil {
		t.Fatal(err)
	}
	Convey("test build with special error", t, func() {
		Convey("test build with special error", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyFunc(net.DialTimeout, func(network, address string, timeout time.Duration) (net.Conn, error) {
				return nil, errors.New("dial unix /home/snuser/socket/java8.sock: connect: no such file or directory")
			})
			defer patches.Reset()
			_ = os.Setenv("PodName", "pool6-4000-16000-java1.8-67f66bbdbc-l7m2h")
			_ = os.Unsetenv("PodName")
			utils.MemMoreThan512M = true
			start := time.Now()
			_, err := Build(nil, BuildOptions{ServerAddr: addr, ReconnectTimes: 10})
			diff := time.Since(start).Milliseconds()
			So(diff, ShouldBeGreaterThan, 200)
			So(err, ShouldNotEqual, nil)
			_ = os.Unsetenv("PodName")
			utils.MemMoreThan512M = false
		})
	})
}
