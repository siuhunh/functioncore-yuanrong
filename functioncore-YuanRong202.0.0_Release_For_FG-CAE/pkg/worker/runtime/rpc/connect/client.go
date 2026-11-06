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

// Package connect realize socket connection
package connect

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/connection"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime/rpc/connect/message"
	"functioncore/pkg/worker/utils"
)

const (
	defaultTimeOut       = 1 * time.Second
	retryTimes           = 3
	defaultRetryInterval = 10 * time.Millisecond
	reconnectTimes       = 3000
	// RestartReconnectTimes when the worker re-establishes a link
	RestartReconnectTimes = 3000
	// retryPeriodicTime may affect the connection time between the runtime and worker,resulting in pod startup failure.
	retryPeriodicTime = 10 * time.Millisecond
	// specificErrRetryPeriodicTime retry interval for no such file or directory errors
	specificErrRetryPeriodicTime = 30 * time.Millisecond
)

type timeOutCtx struct {
	tinyComm *TinyComm
	duration time.Duration
	stop     func() bool
}

// Reset implements invokectx.Poolable
func (tc *timeOutCtx) Reset() {
	tc.tinyComm = nil
}

var recRuntimeExitWhenRestore = false
var tcRange = invokectx.DefaultMetrics.RegisterRange(func() invokectx.Poolable { return &timeOutCtx{} })

func getTimeOutCtx(ic invokectx.InvokeCtx, f func(invokectx.InvokeCtx) invokectx.Poolable) *timeOutCtx {
	timeOutCtxIf := f(ic)
	timeOutCtx, ok := timeOutCtxIf.(*timeOutCtx)
	if !ok {
		log.GetLogger().Errorf("failed to assert timeOutCtx")
		return nil
	}
	return timeOutCtx
}

// RecRuntimeExitMsg Restore is dead and set the flag true
func RecRuntimeExitMsg() {
	recRuntimeExitWhenRestore = true
}

func rangeTimeOutCtx(f func(invokectx.InvokeCtx, *timeOutCtx) bool) {
	tcRange.Range(func(ic invokectx.InvokeCtx, timeOutCtxIf invokectx.Poolable) bool {
		timeOutCtx, ok := timeOutCtxIf.(*timeOutCtx)
		if !ok {
			log.GetLogger().Errorf("failed to assert timeOutCtx")
			return false
		}
		return f(ic, timeOutCtx)
	})
}

type pendingReq struct {
	done chan *message.Message
}

// BuildOptions configuration options of tiny communication
type BuildOptions struct {
	// write socket timeout option
	WriteTimeOut time.Duration
	// read socket timeout option
	ReadTimeout time.Duration
	// connect socket timeout option
	ConnectTimeout time.Duration
	// server
	ServerAddr string
	// tcp, udp or unix socket
	Network string
	// reconnect times
	ReconnectTimes int
	// IsDirectFunc
	IsDirectFunc bool
}

// AbstractReceiver the user implements this interface
type AbstractReceiver interface {
	HandleRequest(metaData []byte, payload []byte) ([]byte, []byte, error)

	HandlePost(metaData []byte, payload []byte) error

	RecvRequest(ic invokectx.InvokeCtx, metaData, payload []byte, err error)
}

// TinyComm tiny communication
type TinyComm struct {
	msgPool     *sync.Pool
	bufPool     *sync.Pool
	reqPool     *sync.Pool
	pending     map[string]*pendingReq // save sent request and matched response.
	mux         sync.Mutex
	conn        net.Conn
	absReceiver AbstractReceiver
	brokenConn  uint32 // atomic bool
}

// Set tcp socket option
func setTCPOption(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		log.GetLogger().Errorf("conn.(*net.TCPConn) is not OK")
		return fmt.Errorf("conn.(*net.TCPConn) is not OK")
	}
	err := tcpConn.SetKeepAlive(true)
	if err != nil {
		return err
	}
	err = tcpConn.SetNoDelay(true)
	if err != nil {
		return err
	}
	return nil
}

// Build build TinyComm to user
func Build(receiver AbstractReceiver, opts BuildOptions) (*TinyComm, error) {
	if opts.Network == "" {
		opts.Network = worker.DefaultSocketType
	}
	if opts.ConnectTimeout.String() == "" {
		opts.ConnectTimeout = defaultTimeOut
	}
	retryTimes := reconnectTimes
	if opts.ReconnectTimes != 0 {
		retryTimes = opts.ReconnectTimes
	}
	var conn net.Conn
	var err error
	for i := 0; i < retryTimes; i++ {
		if conn, err = net.DialTimeout(opts.Network, opts.ServerAddr, opts.ConnectTimeout); err == nil {
			break
		} else if strings.Contains(err.Error(), "no such file or directory") && utils.MemMoreThan512M {
			time.Sleep(specificErrRetryPeriodicTime)
		} else {
			time.Sleep(retryPeriodicTime)
		}
		if recRuntimeExitWhenRestore && opts.Network == "tcp" {
			break
		}
	}
	if err != nil {
		log.GetLogger().Warnf("rpcClientConnectFail | %s", err.Error())
		return nil, err
	}

	if opts.Network == "tcp" {
		if err = setTCPOption(conn); err != nil {
			return nil, err
		}
	}

	tinyComm := &TinyComm{
		msgPool: &sync.Pool{New: func() interface{} {
			return &message.Message{}
		}},
		bufPool: &sync.Pool{New: func() interface{} {
			return &bytes.Buffer{}
		}},
		reqPool: &sync.Pool{New: func() interface{} {
			return &pendingReq{
				done: make(chan *message.Message, 1),
			}
		}},
		pending:     make(map[string]*pendingReq),
		conn:        connection.NewBufferedConn(conn),
		absReceiver: receiver,
	}
	// go func for receiving msg of socket server
	go tinyComm.recvMsg()
	return tinyComm, nil
}

// ConnInfo -
type ConnInfo interface {
	GetConnInfo() string
}

// GetConnInfo -
func (t *TinyComm) GetConnInfo() string {
	return t.conn.LocalAddr().String() + " --> " + t.conn.RemoteAddr().String()
}

func (t *TinyComm) newMessage(packetID string, typ message.PacketType, metaData, payload []byte) *message.Message {
	msgIf := t.msgPool.Get()
	msg, ok := msgIf.(*message.Message)
	if !ok {
		log.GetLogger().Errorf("failed to assert")
		return nil
	}
	msg.Header.PacketType = typ
	msg.MetaData = metaData
	msg.Payload = payload
	msg.Header.PacketID = packetID
	return msg
}

// Request TinyComm client send a request to server, and return a response
//
// Deprecated: In favor of SendRequest
func (t *TinyComm) Request(timeout time.Duration, metaData, payload []byte) ([]byte, []byte, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	msg := t.newMessage(commUtils.UniqueID(), message.Req, metaData, payload)
	pendingReq := &pendingReq{done: make(chan *message.Message, 1)}

	// save this request in pending
	packetID := msg.Header.PacketID
	t.mux.Lock()
	t.pending[packetID] = pendingReq
	t.mux.Unlock()
	if err := t.send(msg); err != nil {
		log.GetLogger().Errorf("failed to send msg, id %s, %s", packetID, err.Error())
		return nil, nil, snerror.New(snerror.FuncInsExceptionCode, snerror.ErrText(snerror.FuncInsExceptionCode))
	}
	select {
	case <-timer.C:
		return nil, nil, snerror.New(snerror.RuntimeInvokeTimeoutCode,
			fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), timeout.String()))
	case rsp, ok := <-pendingReq.done:
		if !ok {
			return nil, nil, snerror.New(snerror.InternalError, snerror.InternalErrorMsg)
		}
		defer t.msgPool.Put(rsp)
		if rsp.Header.PacketType == message.DisconnectResp {
			return nil, nil, snerror.New(snerror.FuncInsExceptionCode, snerror.ErrText(snerror.FuncInsExceptionCode))
		}
		return rsp.MetaData, rsp.Payload, nil
	}
}

// SendRequest sends a request to server
func (t *TinyComm) SendRequest(ic invokectx.InvokeCtx, timeout time.Duration, metaData, payload []byte) {
	packetID := genPacketID(ic.ID)
	msg := t.newMessage(packetID, message.Req, metaData, payload)

	timer := time.AfterFunc(timeout, func() {
		t.processRsp(packetID, nil, true)
	})

	timeOutCtx := getTimeOutCtx(ic, tcRange.Mark)
	timeOutCtx.tinyComm = t
	timeOutCtx.duration = timeout
	timeOutCtx.stop = timer.Stop

	invokectx.GetTimeTraceContext(ic).Start(worker.TimeTraceRuntime, time.Now())
	if err := t.send(msg); err != nil {
		log.GetLogger().Errorf("failed to send msg, id %d, %s", ic.ID, err.Error())
		timer.Stop()
		t.absReceiver.RecvRequest(ic, nil, nil,
			snerror.New(snerror.FuncInsExceptionCode, snerror.ErrText(snerror.FuncInsExceptionCode)))
	}
}

// 处理socket收到的消息
func (t *TinyComm) recvMsg() {
	for {
		msgIf := t.msgPool.Get()
		msg, ok := msgIf.(*message.Message)
		if !ok {
			log.GetLogger().Errorf("failed to assert")
			continue
		}
		err := message.Decode(msg, t.conn)
		if err != nil {
			log.GetLogger().Warnf("socketDisconnectErr | %s", err.Error())
			atomic.StoreUint32(&t.brokenConn, 1)
			t.clearReq()

			err = t.conn.Close()
			if err != nil {
				log.GetLogger().Errorf("rpc conn close error: %s", err.Error())
			}
			return
		}
		switch msg.Header.PacketType {
		case message.Req: // received request message
			go t.processReq(msg)
		case message.Post: // received post message
			t.processPost(msg)
		case message.NormalResp, message.ErrorResp:
			t.processRsp(msg.Header.PacketID, msg, false)
		default:
			log.GetLogger().Warnf("skip packet type")
		}
	}
}

func (t *TinyComm) clearReq() {
	rangeTimeOutCtx(func(ic invokectx.InvokeCtx, c *timeOutCtx) bool {
		if c.tinyComm != t {
			// Make sure we don't accidentally handle requests made by other TinyComm instances.
			return true
		}
		go func() {
			timeOutCtx := getTimeOutCtx(ic, tcRange.Unmark)
			if !timeOutCtx.stop() {
				// already stopped
				return
			}
			t.absReceiver.RecvRequest(ic, nil, nil,
				snerror.New(snerror.FuncInsExceptionCode, snerror.ErrText(snerror.FuncInsExceptionCode)))
		}()
		return true
	})
	t.mux.Lock()
	for key, value := range t.pending {
		rsp := &message.Message{}
		rsp.Header.PacketType = message.DisconnectResp
		value.done <- rsp
		delete(t.pending, key)
	}
	t.mux.Unlock()
}

func (t *TinyComm) processReq(msg *message.Message) {
	// When a request is received, the request is sent to the user interface function for processing
	metaData, payload, err := t.absReceiver.HandleRequest(msg.MetaData, msg.Payload)
	if err != nil {
		payload = []byte(err.Error())
	}
	msg.Header.PacketType = message.NormalResp
	msg.MetaData = metaData
	msg.Payload = payload
	err = t.send(msg)
	if err != nil {
		log.GetLogger().Errorf("failed to send request, packetID: %s, error: %s",
			msg.Header.PacketID, err.Error())
	}
}

func (t *TinyComm) processRsp(packetID string, msg *message.Message, isTimedOut bool) {
	id := parsePacketID(packetID)
	if id == 0 {
		// Cannot parse the packetID, is it in the pending map?
		t.mux.Lock()
		if pending, ok := t.pending[packetID]; ok {
			delete(t.pending, packetID)
			select {
			case pending.done <- msg:
			default:
				// no wait
			}
		} else {
			log.GetLogger().Errorf("unknown packetID: %s", packetID)
		}
		t.mux.Unlock()
		return
	}
	if msg != nil {
		defer t.msgPool.Put(msg)
	}

	ic, ok := invokectx.DefaultMetrics.GetInvokeCtx(id)
	if !ok {
		log.GetLogger().Warnf("failed to get invoke ctx, id: %d", id)
		return
	}
	invokectx.GetTimeTraceContext(ic).End(worker.TimeTraceRuntime, time.Now())

	timeOutCtx := getTimeOutCtx(ic, tcRange.Unmark)
	if isTimedOut {
		t.absReceiver.RecvRequest(ic, nil, nil, snerror.New(snerror.RuntimeInvokeTimeoutCode,
			fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), timeOutCtx.duration.String())))
		return
	}
	if !timeOutCtx.stop() {
		// already stopped
		return
	}

	t.absReceiver.RecvRequest(ic, msg.MetaData, msg.Payload, nil)
}

func (t *TinyComm) processPost(msg *message.Message) {
	err := t.absReceiver.HandlePost(msg.MetaData, msg.Payload)
	if err != nil {
		log.GetLogger().Warnf("Process post error %s", err.Error())
	}
	t.msgPool.Put(msg)
}

func (t *TinyComm) send(msg *message.Message) error {
	defer t.msgPool.Put(msg)

	bufIf := t.bufPool.Get()
	buf, ok := bufIf.(*bytes.Buffer)
	if !ok {
		log.GetLogger().Errorf("failed to assert")
		return errors.New("failed to assert")
	}
	defer t.bufPool.Put(buf)
	err := message.Encode(msg, buf)
	if err != nil {
		return err
	}
	if atomic.LoadUint32(&t.brokenConn) != 0 {
		return errors.New("connection was broken")
	}
	for i := 0; i < retryTimes; i++ {
		_, err = t.conn.Write(buf.Bytes())
		if err == nil {
			return nil
		}
		time.Sleep(defaultRetryInterval)
	}
	if atomic.LoadUint32(&t.brokenConn) != 0 {
		log.GetLogger().Infof("connection is broken during send, %s", err.Error())
		// 'clearReq' has already returned the results. We don't need to populate any errors here.
		return nil
	}
	return err
}

// IsBroken -
func (t *TinyComm) IsBroken() bool {
	if atomic.LoadUint32(&t.brokenConn) != 0 {
		return true
	}
	return false
}
