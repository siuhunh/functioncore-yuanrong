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

// Package tcpbus contains interface of connection and basic codes
package tcpbus

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	bufPool "functioncore/pkg/common/pool"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/proxyview"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	errcode "functioncore/pkg/functionbus/proxyerrors"
)

const (
	heartbeatInterval        = 5
	authCheckTimeout         = 5 * time.Second
	connWriteTimeout         = 30 * time.Second
	defaultConns             = 1
	defaultChanCap           = 5000
	reconnectionTimes        = 3
	internalMaxRetryTimes    = 6
	caeMaxRetryTimes         = 180
	sendRetryTimes           = 10
	addrLen                  = 2
	delay                    = time.Duration(1) * time.Second
	heartbeatRedundancyTimes = 2
	// serverConnection is connection created by server
	serverConnection = "server"
	// clientConnection is connection created by client
	clientConnection = "client"
	// retryTimesAfterTimeout is retry times after reporting an asynchronous request timeout(30s)
	// retry 15min*2 = 30min
	retryTimesAfterTimeout = 2
)

// ServerReceiver is implemented by the upper layer, process received request message and registry services
type ServerReceiver interface {
	HandleRequest(payload []byte, clientIP string, client *Connection) types.Result
	RegisterService(client *Connection) error
}

// ServerOption defines server properties
type ServerOption struct {
	// Receiver interface process client received request message
	Receiver ServerReceiver
	// ListenAddr server listen address
	ListenAddr string
	// IsServer is server
	IsServer bool
	// DisableHeartbeat whether to disable auto heartbeat in connection
	DisableHeartbeat bool
}

type pendingReq struct {
	done chan *protocol.Message
}

type socketConn struct {
	netPool     *pool.Pool             // connection by socket
	receiveChan chan *protocol.Message // connection by channel
}

// Connection struct
type Connection struct {
	pending        *sync.Map // save sent request and matched response
	wg             *sync.WaitGroup
	msgChan        chan interface{} // save received message
	reqHandler     func(payload []byte, peerIP string, w func(types.Result))
	mux            sync.RWMutex
	netPool        pool.Pool
	isConnect      bool
	connectionType string
	workerID       string
	// isTrustedClient is used to check the validity of the client connection in server
	isTrustedClient       bool
	disableWhenServerExit bool

	// PeerIP remote connect peer ip
	PeerIP string
	// DisabledSign peer is disabled
	DisabledSignal chan struct{}
	// RuntimeDisconn sends notify when runtime is disconnected since bus is the server, there is no reconnect
	RuntimeDisconn chan struct{}
	// worker call this callback method to return response to bus
	callback SendWithRetryCallBack

	runtimeDisconnOnce sync.Once

	normalExit bool
}

// SendWithRetryCallBack -
type SendWithRetryCallBack func(msg *protocol.Message)

// RegisterSendCallBack register send callback function
func (c *Connection) RegisterSendCallBack(f SendWithRetryCallBack) {
	c.callback = f
}

func handleConn(conn net.Conn, opt pool.Options) {
	if opt.MsgChan == nil {
		log.GetLogger().Error("failed to handle connection: received channel is null")
		return
	}
	heartbeatCh := make(chan struct{}, defaultChanCap)
	if !opt.DisableHeartbeat {
		go heartbeatCheck(conn, heartbeatCh, opt.IsServer)
	}

	opt.ConnWG.Add(1)
	for {
		msg := mp.Get()
		if monitor.ReachMemoryThreshold() {
			msg.Header.ErrCode = errcode.HeavyLoadCode
			msg.Payload = []byte(errcode.HeavyLoadErr)
			mp.Put(msg)
			continue
		}
		if !messageDecode(msg, conn) {
			mp.Put(msg)
			break
		}
		if !opt.DisableHeartbeat {
			heartbeatCh <- struct{}{}
		}
		switch msg.Header.MsgType {
		case protocol.HeartBeat:
			if sendACK(conn) != nil {
				log.GetLogger().Warnf("failed to send heartbeat ack %s", getConnInfo(conn))
			}
			mp.Put(msg)
			continue
		case protocol.ACK:
			mp.Put(msg)
		default:
			if !defaultMsgTypeProcess(msg, opt, conn) {
				mp.Put(msg)
				break
			}
			opt.MsgChan <- msg
		}
	}
	opt.ConnWG.Done()
	close(heartbeatCh)
}

func messageDecode(msg *protocol.Message, conn net.Conn) bool {
	if err := protocol.Decode(msg, conn); err != nil {
		if err == io.EOF {
			log.GetLogger().Warnf("socketDisconnect | %s | %s", getConnInfo(conn), err.Error())
		} else {
			log.GetLogger().Warnf("failed to decode response: %s", err)
		}
		return false
	}
	return true
}

func defaultMsgTypeProcess(msg *protocol.Message, opt pool.Options, conn net.Conn) bool {
	log.GetLogger().Debugf("receive msgType: %d packetID: %s from %s",
		msg.Header.MsgType, msg.Header.PacketID, conn.RemoteAddr().String())
	openFlag := atomic.LoadInt32(opt.IsMsgChanOpen)
	if openFlag > 0 {
		return true
	}
	log.GetLogger().Errorf("msgCh is closed, socket: %s msgType: %d packetID: %s",
		getConnInfo(conn), msg.Header.MsgType, msg.Header.PacketID)
	return false
}

// Heartbeats are blocked when the worker is restarted.
// Therefore, the maximum number of heartbeat detections is set based on
// the init timeout period. The init timeout period is divided by the detection period
// (5s + 2 redundancy times) and the default value 6 is used as the maximum value.
func getMaxRetryTimes(isServer bool) int {
	log.GetLogger().Debugf("current connection isServer %v, initTimeout %v", isServer, InitTimeout)
	if BusinessType == "CAE" {
		return caeMaxRetryTimes
	}
	if !isServer {
		return internalMaxRetryTimes
	}
	if InitTimeout/heartbeatInterval > internalMaxRetryTimes {
		return int(InitTimeout/heartbeatInterval) + heartbeatRedundancyTimes
	}
	return internalMaxRetryTimes
}

func heartbeatCheck(conn net.Conn, heartbeatCh chan struct{}, isServer bool) {
	if heartbeatCh == nil {
		log.GetLogger().Warnf("heartbeat channel is nil")
		return
	}
	consecutiveTimes := 1
	d := heartbeatInterval * time.Second
	maxRetryTimes := getMaxRetryTimes(isServer)
	t := time.NewTimer(d)
	for {
		select {
		case _, ok := <-heartbeatCh:
			if !ok {
				log.GetLogger().Debugf("received channel of heartbeat is closed %s", getConnInfo(conn))
				return
			}
			consecutiveTimes = 1
			t.Reset(d)
		case <-t.C:
			if consecutiveTimes > maxRetryTimes {
				log.GetLogger().Warnf("heartbeat detection failed %s, consecutiveTimes: %d",
					getConnInfo(conn), consecutiveTimes)
				if conn.Close() != nil {
					log.GetLogger().Warnf("failed to close conn")
				}
				return
			}
			consecutiveTimes++
			if sendHeartbeat(conn) != nil {
				log.GetLogger().Warnf("failed to send heartbeat %s", getConnInfo(conn))
			}
			t.Reset(d)
		}
	}
}

func (c *Connection) clearPendingReq() {
	c.pending.Range(func(key, value interface{}) bool {
		if req, ok := value.(*pendingReq); ok {
			rsp := &protocol.Message{}
			rsp.Header.ErrCode = errcode.ClientExitErrCode
			rsp.Payload = []byte(errcode.ClientExitErr)
			c.pending.Delete(key)
			req.done <- rsp
			log.GetLogger().Debugf("failed to send req %s to the exited peer %s ",
				rsp.Header.PacketID, c.PeerIP)
			return true
		}
		log.GetLogger().Errorf("%s value is not pendingReq", key)
		return false
	})
}

func (c *Connection) setConnNormalExitFlag() {
	c.mux.Lock()
	c.normalExit = true
	c.mux.Unlock()
}

func (c *Connection) isConnNormalExit(connWG *sync.WaitGroup, isMsgChanOpen *int32) bool {
	c.mux.Lock()
	normalExit := c.normalExit
	if normalExit {
		c.isConnect = false
		c.clearPendingReq()
		clientsMap.Delete(c.PeerIP)
		c.mux.Unlock()
		atomic.StoreInt32(isMsgChanOpen, 0)
		connWG.Wait()
		log.GetLogger().Debugf("all connections exited %s", c.PeerIP)
		setSignal(c.DisabledSignal)
		close(c.msgChan)
	} else {
		c.mux.Unlock()
	}
	return normalExit
}

func (c *Connection) sendCallResult(packetID string, result types.Result, metadata map[string]string) {
	res := mp.Get()
	res.Header.PacketID = packetID
	res.Header.MsgType = protocol.CallRspMsg
	res.Header.MetaData = metadata
	if result.ErrorCode != errcode.StateOKCode {
		res.Header.ErrCode = result.ErrorCode
		res.Payload = []byte(result.ErrorMsg)
	} else {
		res.Payload = result.RspData
	}

	callback := c.callback
	if callback != nil {
		callback(res)
	} else {
		if err := c.send(res); err != nil {
			log.GetLogger().Errorf("failed to send response %s to %s: %s", res.Header.PacketID, c.PeerIP, err)
		}
	}
	c.wg.Done()
	mp.Put(res)
}

func (c *Connection) processCallReqMsg(msg *protocol.Message) {
	packetID := msg.Header.PacketID
	log.GetLogger().Debugf("get req type: %d, packetID %s", msg.Header.MsgType, packetID)

	if c.reqHandler != nil && msg.Header.ErrCode != errcode.HeavyLoadCode {
		c.reqHandler(msg.Payload, c.PeerIP, func(r types.Result) { c.sendCallResult(packetID, r, msg.Header.MetaData) })
		mp.Put(msg)
		return
	}

	var result types.Result
	if msg.Header.ErrCode == errcode.HeavyLoadCode {
		result.ErrorCode = errcode.HeavyLoadCode
		result.ErrorMsg = errcode.HeavyLoadErr
	} else if c.reqHandler == nil {
		result.ErrorMsg = errcode.UnRegisterCallBack
		result.ErrorCode = errcode.UnRegisterCallBackCode
	}
	c.sendCallResult(packetID, result, msg.Header.MetaData)
	mp.Put(msg)
}

func (c *Connection) processCallRspMsg(msg *protocol.Message) {
	reqIf, ok := c.pending.Load(msg.Header.PacketID)
	if !ok {
		log.GetLogger().Warnf("failed to find request of this response %s "+
			"which is from  %s", msg.Header.PacketID, c.PeerIP)
		return
	}
	c.pending.Delete(msg.Header.PacketID)
	req, ok := reqIf.(*pendingReq)
	if !ok {
		log.GetLogger().Errorf("failed to assert message type, packetID %s", msg.Header.PacketID)
		return
	}
	// return this response through channel
	req.done <- msg
}

func (c *Connection) processCallReportMsg(msg *protocol.Message) {
	message := &pb.Message{}
	if err := proto.Unmarshal(msg.Payload, message); err != nil {
		log.GetLogger().Warnf("bus call response unmarshal error: %s", err.Error())
		mp.Put(msg)
		return
	}
	if message.CallResponse == nil {
		log.GetLogger().Warnf("ignore invalid message")
		mp.Put(msg)
		return
	}
	log.GetLogger().Debugf("start to sync abandoned response, workerID: %s", message.CallResponse.InstanceID)
	workerevaluator.SyncAbandoned(message.CallResponse)
	mp.Put(msg)
}

func (c *Connection) send(msg *protocol.Message) error {
	buf := bufPool.BuffPool.Get()
	defer bufPool.BuffPool.Put(buf)
	protocol.NewEncode(buf, msg)
	var conn net.Conn
	var err error
	for i := 0; i < sendRetryTimes; i++ {
		conn, err = c.netPool.Get()
		if err != nil {
			if i < sendRetryTimes-1 {
				time.Sleep(delay)
				continue
			}
			return err
		}
		err = newWriteWithTimeout(conn, buf)
		if err == nil {
			return nil
		} else if err == io.EOF {
			c.runtimeDisconnOnce.Do(func() {
				close(c.RuntimeDisconn)
			})
		}
	}
	return err
}

// SendWithoutRetry send response to bus without retry
func (c *Connection) SendWithoutRetry(msg *protocol.Message) error {
	buf := bufPool.BuffPool.Get()
	defer bufPool.BuffPool.Put(buf)
	protocol.NewEncode(buf, msg)
	var conn net.Conn
	var err error

	conn, err = c.netPool.Get()
	if err != nil {
		return err
	}
	err = newWriteWithTimeout(conn, buf)
	return err
}

func (c *Connection) sendRequest(ctx context.Context, msg *protocol.Message, isSendingRemoteProxy bool) types.Result {
	pendingReq := new(pendingReq)
	pendingReq.done = make(chan *protocol.Message, 1)
	c.pending.Store(msg.Header.PacketID, pendingReq)
	err := c.send(msg)
	if err != nil {
		if isSendingRemoteProxy {
			proxyview.UpdateProxyAvailableStatus(c.PeerIP, false)
		}
		log.GetLogger().Errorf("send request failed: %s, packetID %s", err, msg.Header.PacketID)
		return types.Result{RspData: nil, ErrorCode: errcode.SendReqErrCode, ErrorMsg: errcode.SendReqErr}
	}
	log.GetLogger().Debugf("request interface send request, packetID %s", msg.Header.PacketID)
	select {
	case _, ok := <-ctx.Done():
		if !ok {
			log.GetLogger().Warnf("waiting response timeout, packetID %s", msg.Header.PacketID)
			return types.Result{RspData: nil, ErrorCode: errcode.TimeoutErrCode, ErrorMsg: errcode.TimeoutErr}
		}
	case rsp, ok := <-pendingReq.done:
		defer mp.Put(rsp)
		if !ok {
			log.GetLogger().Warnf("response channel is closed, packetID %s", msg.Header.PacketID)
			return types.Result{RspData: nil, ErrorCode: errcode.InternalErrCode, ErrorMsg: errcode.InternalErr}
		}
		if rsp.Header.ErrCode != errcode.StateOKCode {
			return types.Result{RspData: nil, ErrorCode: rsp.Header.ErrCode, ErrorMsg: string(rsp.Payload)}
		}
		return types.Result{RspData: rsp.Payload, ErrorCode: errcode.StateOKCode, ErrorMsg: ""}
	}
	return types.Result{RspData: nil, ErrorCode: errcode.InternalErrCode, ErrorMsg: errcode.InternalErr}
}

// Stop all goroutine done
func (c *Connection) Stop() {
	c.wg.Wait()
	c.isConnect = false
	log.GetLogger().Infof("stopping the client to %s", c.PeerIP)
}

func (c *Connection) forceStop() {
	c.netPool.Close()
	c.isConnect = false
}

// request function lets the user call the request.
func (c *Connection) request(ctx context.Context, payload []byte, isSendingRemoteProxy bool,
	isStateful bool) types.Result {
	if !c.isConnect {
		if isStateful {
			log.GetLogger().Warnf("received a state request, but connection to %s was disabled", c.PeerIP)
			return types.Result{RspData: nil, ErrorCode: snerror.StatefulFunctionReloading,
				ErrorMsg: snerror.ErrText(snerror.StatefulFunctionReloading)}
		}
		if isSendingRemoteProxy {
			log.GetLogger().Warnf("received a remote proxy request, but connection to %s was disabled", c.PeerIP)
			return types.Result{RspData: nil, ErrorCode: errcode.IsUpgradingCode, ErrorMsg: errcode.IsUpgradingErr}
		}
		log.GetLogger().Warnf("received a request, but connection to %s was disabled", c.PeerIP)
		return types.Result{RspData: nil, ErrorCode: errcode.SendReqErrCode, ErrorMsg: errcode.SendReqErr}
	}
	packetID := uuid.New().String()
	msgHeader := protocol.MsgHeader{MsgType: protocol.CallReqMsg, PacketID: packetID}
	if isStateful {
		metadata := make(map[string]string)
		metadata[protocol.StatefulMsgKey] = protocol.StatefulMsgKey
		msgHeader.MetaData = metadata
	}
	msg := &protocol.Message{Header: msgHeader, Payload: payload}
	return c.sendRequest(ctx, msg, isSendingRemoteProxy)
}

// ctrlRtmRequest send ctrlRuntimeMsg to worker
func (c *Connection) ctrlRtmRequest(ctx context.Context, payload []byte) types.Result {
	if !c.isConnect {
		log.GetLogger().Warnf("received a control runtime request, but connection to %s was disabled", c.PeerIP)
		return types.Result{RspData: nil, ErrorCode: errcode.SendReqErrCode, ErrorMsg: errcode.SendReqErr}
	}
	packetID := uuid.New().String()
	msgHeader := protocol.MsgHeader{MsgType: protocol.CtrlRuntimeMsg, PacketID: packetID}
	msg := &protocol.Message{Header: msgHeader, Payload: payload}
	return c.sendRequest(ctx, msg, false)
}

func setSignal(signalChan chan struct{}) {
	if signalChan == nil {
		log.GetLogger().Warnf("failed to set signal: signalChan is null")
		return
	}
	select {
	case signalChan <- struct{}{}:
	default:
	}
}

func sendHeartbeat(conn net.Conn) error {
	data := protocol.NewHeartBeat()
	err := writeWithTimeout(conn, data)
	if err != nil {
		log.GetLogger().Warnf("failed to send heartbeat message: %s", err)
		return err
	}
	return nil
}

func sendACK(conn net.Conn) error {
	data := protocol.NewACK()
	err := writeWithTimeout(conn, data)
	if err != nil {
		log.GetLogger().Warnf("failed to send ack message: %s", err)
		return err
	}
	return nil
}

func getPeerIP(addr string) string {
	items := strings.Split(addr, ":")
	if len(items) != addrLen {
		log.GetLogger().Errorf("failed to handle connection %s", addr)
		return ""
	}
	return items[0]
}

func getConnInfo(conn net.Conn) string {
	return conn.LocalAddr().String() + "->" + conn.RemoteAddr().String()
}

func newWriteWithTimeout(conn net.Conn, buf *bytes.Buffer) error {
	err := conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
	if err != nil {
		return err
	}
	_, err = buf.WriteTo(conn)
	return err
}

func writeWithTimeout(conn net.Conn, data []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(connWriteTimeout))
	if err != nil {
		return err
	}

	// the first parameter returned by the Write is number of bytes of data written, which is not used here
	_, err = conn.Write(data)
	return err
}

// SendRecoverRequest send recover request to worker
func (c *Connection) SendRecoverRequest(ctx context.Context, req *commontypes.TCPRequest, traceID string, self string) (
	*commontypes.TCPResponse, error) {
	callMsg := &pb.Message_RecoverMessage{
		TraceID:  traceID,
		WorkerID: req.WorkerID,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:           pb.Message_RECOVER_REQUEST,
		RecoverMessage: callMsg,
	})
	if err != nil {
		log.GetLogger().Errorf("bus recover, self: %s, marshal failed: %s, traceID: %s", self, err.Error(), traceID)
		return nil, fmt.Errorf("report error: %s", err.Error())
	}
	var isStatefulFunc bool
	if common.WorkerMap.CheckVal(c.workerID) {
		isStatefulFunc = true
	}
	result := c.request(ctx, data, false, isStatefulFunc)
	log.GetLogger().Debugf("recovering finished, traceID: %s", traceID)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("recover failed, self: %s, errCode: %d, traceID: %s", self, result.ErrorCode, traceID)
		return &commontypes.TCPResponse{
			RequestID:    req.RequestID,
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}
	message := &pb.Message{}
	err = proto.Unmarshal(result.RspData, message)
	if err != nil {
		log.GetLogger().Errorf("bus recover response unmarshal error: %s, self: %s, traceID: %s",
			err.Error(), self, traceID)
		return nil, fmt.Errorf("bus report response unmarshal error: %s", err.Error())
	}
	if message.RecoverResponse == nil {
		log.GetLogger().Errorf("recover response is nil! traceID: %s", traceID)
		return &commontypes.TCPResponse{
			ErrorCode:    errcode.InternalErrCode,
			ErrorMessage: errcode.InternalErr,
		}, nil
	}
	return &commontypes.TCPResponse{
		RawData:      message.RecoverResponse.RawData,
		ErrorCode:    message.RecoverResponse.ErrorCode,
		ErrorMessage: message.RecoverResponse.ErrorMessage,
	}, nil
}
