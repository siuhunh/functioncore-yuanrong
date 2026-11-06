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

// Package channelbus contains implementation of channel connection with functiontask and bus proxy
package channelbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/calls"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

const defaultChanCap = 5000

// for connection by channel, one process only has a server and a client
var (
	sendChan    = make(chan *common.ChannelBusMessage, defaultChanCap)
	receiveChan = make(chan *common.ChannelBusMessage, defaultChanCap)
)

type pendingReq struct {
	done chan *common.ChannelBusMessage
}

type chanConn struct {
	sendChan    chan *common.ChannelBusMessage // connect by channel
	receiveChan chan *common.ChannelBusMessage // connect by channel
}

// ChannelBus struct
type ChannelBus struct {
	pending       *sync.Map // save sent request and matched response
	collector     *calls.CallCollector
	chanConn      chanConn
	Property      common.FunctionProperty
	proxyReceiver common.BusReceiver
	taskReceiver  common.BusReceiver
	callback      common.Call
	isProxy       bool // true: proxy, false: task
}

// NewChannelBus new channelBus
func NewChannelBus(busOption common.BusOption) (*ChannelBus, error) {
	if busOption.NewBusOpt == nil {
		return nil, fmt.Errorf("NewBusOpt is nil")
	}

	channelBusOpt, ok := busOption.NewBusOpt.(*common.ChannelBusOption)
	if !ok {
		return nil, fmt.Errorf("failed to assert type")
	}

	if channelBusOpt.IsProxy {
		return newChannelBusForProxy(channelBusOpt)
	}

	return newChannelBusForTask(), nil
}

// newChannelBusForTask new channelBus for functiontask
func newChannelBusForTask() *ChannelBus {
	defaultProperty := common.FunctionProperty{
		FunctionName: common.DefaultFunctionName,
	}
	conn := chanConn{receiveChan: receiveChan, sendChan: sendChan}

	channelBus := &ChannelBus{
		collector: calls.InitCollector(common.DefaultConcurrency),
		chanConn:  conn,
		pending:   &sync.Map{},
		Property:  defaultProperty,
		isProxy:   false,
	}

	log.GetLogger().Infof("new channelBus for functiontask successfully")

	return channelBus
}

// newChannelBusForProxy new channelBus for bus proxy
func newChannelBusForProxy(opt *common.ChannelBusOption) (*ChannelBus, error) {
	if opt.Receiver == nil {
		return nil, fmt.Errorf("receiver is nil")
	}

	defaultProperty := common.FunctionProperty{
		FunctionName: common.DefaultFunctionName,
	}
	conn := chanConn{receiveChan: receiveChan, sendChan: sendChan}

	channelBus := &ChannelBus{
		collector:     calls.InitCollector(common.DefaultConcurrency),
		chanConn:      conn,
		pending:       &sync.Map{},
		proxyReceiver: opt.Receiver,
		isProxy:       true,
		Property:      defaultProperty,
	}

	log.GetLogger().Infof("new channelBus for proxy successfully")
	return channelBus, nil
}

// recvChanMsg receive message by channel
func (cb *ChannelBus) recvChanMsg() {
	var listenChan chan *common.ChannelBusMessage

	if cb.isProxy {
		listenChan = cb.chanConn.receiveChan
	} else {
		listenChan = cb.chanConn.sendChan
	}

	for {
		if listenChan == nil {
			log.GetLogger().Errorf("the received channel is nil")
			return
		}
		select {
		case msg, ok := <-listenChan:
			if !ok {
				log.GetLogger().Errorf("the received channel is closed")
				return
			}
			switch msg.MsgType {
			case common.ReqMsg:
				go cb.processChanReq(msg)
			case common.RegMsg:
				go cb.processChanReq(msg)
			case common.RspMsg:
				go cb.processChanRsp(msg)
			default:
				log.GetLogger().Warnf("unknown message type, skip")
			}
		}
	}
}

// WaitBusExit -
func (cb *ChannelBus) WaitBusExit() {
}

// processChanReq process request message
func (cb *ChannelBus) processChanReq(msg *common.ChannelBusMessage) {
	var result common.ReqResult

	if cb.isProxy && cb.proxyReceiver != nil {
		// proxy Receiver process message
		result = cb.proxyReceiver.HandleRequest(msg)
	} else if cb.taskReceiver != nil {
		// task Receiver process message
		result = cb.taskReceiver.HandleRequest(msg)
	}

	msg.MsgType = common.RspMsg

	if result.ErrorCode != proxyerrors.StateOKCode {
		msg.ErrorCode = result.ErrorCode
		msg.ErrorMsg = result.ErrorMsg
	} else {
		msg.Response = result.Response
	}
	cb.chanSend(msg)
	return
}

// processChanRsp process response message
func (cb *ChannelBus) processChanRsp(msg *common.ChannelBusMessage) {
	reqIf, ok := cb.pending.Load(msg.PacketID)
	if !ok {
		log.GetLogger().Warnf("there is no request of this response, packetID %s", msg.PacketID)
		return
	}
	req, ok := reqIf.(*pendingReq)
	if !ok {
		log.GetLogger().Errorf("failed to assert message type, packetID %s", msg.PacketID)
		return
	}
	// return this response through channel
	req.done <- msg
	cb.pending.Delete(msg.PacketID)
	close(req.done)
}

// chanSend send response by chan
func (cb *ChannelBus) chanSend(msg *common.ChannelBusMessage) {
	if cb.isProxy {
		cb.chanConn.sendChan <- msg
	} else {
		cb.chanConn.receiveChan <- msg
	}
	return
}

// RegisterBus channel bus of functiontask register to bus proxy.
// if the register is successful, then the channel can communicate
func (cb *ChannelBus) RegisterBus(property common.FunctionProperty) error {
	cb.Property = property

	ctx, cancel := context.WithTimeout(context.Background(), common.DefaultRegisterTimeout)
	defer cancel()

	msg := &common.ChannelBusMessage{
		MsgType:  common.RegMsg,
		Property: &property,
	}

	var result common.ReqResult
	for j := 0; j < common.DefaultRetryTimes; j++ {
		result = cb.request(ctx, msg)
		if result.ErrorCode == 0 {
			break
		}
		time.Sleep(common.DefaultRetryInterval)
	}
	if result.ErrorCode != 0 {
		log.GetLogger().Errorf("failed to register to bus proxy, error: %s, self: %s",
			result.ErrorMsg, cb.Property.FunctionName)
		return fmt.Errorf("failed to register to bus proxy, error: %s", result.ErrorMsg)
	}

	log.GetLogger().Infof("channel bus registers successfully, self: %s", cb.Property.FunctionName)

	return nil
}

// request function called by function RegisterBus and Call for request
func (cb *ChannelBus) request(ctx context.Context, msg *common.ChannelBusMessage) common.ReqResult {
	packetID := uuid.New().String()
	msg.PacketID = packetID
	pendingReq := new(pendingReq)
	pendingReq.done = make(chan *common.ChannelBusMessage, 1)
	cb.pending.Store(packetID, pendingReq)

	log.GetLogger().Debugf("channelBus request, cb.isProxy:%t, msg.MsgType:%d", cb.isProxy, msg.MsgType)
	if cb.isProxy {
		// proxy send msg to task by put msg in sendChan channel
		cb.chanConn.sendChan <- msg
	} else {
		// task send msg to proxy by put msg in receiveChan channel
		cb.chanConn.receiveChan <- msg
	}
	select {
	case _, ok := <-ctx.Done():
		if !ok {
			log.GetLogger().Warnf("waiting the request timeout, packetID %s", msg.PacketID)
			return common.ReqResult{Response: nil, ErrorCode: proxyerrors.TimeoutErrCode,
				ErrorMsg: proxyerrors.TimeoutErr}
		}
	case rsp, ok := <-pendingReq.done:
		if !ok {
			log.GetLogger().Warnf("response channel is closed, packetID is %s", msg.PacketID)
			return common.ReqResult{Response: nil, ErrorCode: proxyerrors.InternalErrCode,
				ErrorMsg: proxyerrors.InternalErr}
		}
		if rsp.ErrorCode != proxyerrors.StateOKCode {
			return common.ReqResult{Response: nil, ErrorCode: rsp.ErrorCode, ErrorMsg: rsp.ErrorMsg}
		}
		return common.ReqResult{Response: rsp.Response, ErrorCode: rsp.ErrorCode, ErrorMsg: ""}
	}
	return common.ReqResult{Response: nil, ErrorCode: proxyerrors.InternalErrCode,
		ErrorMsg: proxyerrors.InternalErr}
}

// RegisterCallback registers Called method for bus user
func (cb *ChannelBus) RegisterCallback(callback common.Call) {
	cb.taskReceiver = cb
	cb.callback = callback
}

// RegisterCancel implement CommunicationBus, ChannelBus actually doesn't need cancel
func (cb *ChannelBus) RegisterCancel(cancel common.Cancel) {
	return
}

// RegisterRecover implement CommunicationBus, ChannelBus actually doesn't need recover
func (cb *ChannelBus) RegisterRecover(recover common.Recover) {
	return
}

// Init is a concrete Init implementation for channel bus, this init won't be used
func (cb *ChannelBus) Init(ctx context.Context) (*types.TCPResponse, error) {
	log.GetLogger().Warnf("channel bus doesn't support init")
	return nil, errors.New("not implement")
}

// Call is a concrete Call implementation for channel bus, Call may triggered by functiontask
func (cb *ChannelBus) Call(ctx context.Context, dst *common.FunctionProperty,
	req *types.TCPRequest, isSendingRemoteProxy bool) (*types.TCPResponse, error) {
	traceID := utils.ContextValueString(ctx, constants.HeaderTraceID)
	self := cb.Property.FunctionName
	// request ID may change after next request
	reqID := req.RequestID
	log.GetLogger().Debugf("call request start from: %s to: %s, traceID: %s, requestID: %d", self,
		dst.FunctionName, traceID, reqID)

	// request ID changed after
	msg := &common.ChannelBusMessage{
		TraceID:  traceID,
		MsgType:  common.ReqMsg,
		Property: dst,
		Request:  req,
	}

	result := cb.request(ctx, msg)
	log.GetLogger().Debugf("finished calling request from: %s to: %s, traceID: %s, requestID: %d", self,
		dst.FunctionName, traceID, reqID)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("call failed, self: %s, errCode: %d, traceID: %s", self, result.ErrorCode, traceID)
		return &types.TCPResponse{
			RequestID:    reqID,
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}

	if result.Response == nil {
		log.GetLogger().Errorf("call response is nil, self: %s, traceID: %s", self, traceID)
		return nil, fmt.Errorf("call response is nil")
	}

	result.Response.RequestID = reqID
	return result.Response, nil
}

// HandleRequest task process message: request
func (cb *ChannelBus) HandleRequest(msg *common.ChannelBusMessage) common.ReqResult {
	selfFuncName := cb.Property.FunctionName

	if msg.MsgType != common.ReqMsg {
		log.GetLogger().Errorf("failed to handle request, self: %s, unmarshal error", selfFuncName)
		return common.ReqResult{Response: nil, ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg: proxyerrors.InternalErr}
	}

	log.GetLogger().Debugf("called request start, self: %s, traceID: %s", selfFuncName, msg.TraceID)
	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, msg.TraceID)

	var resp *types.TCPResponse
	var err error

	if resp, err = cb.asyncRequest(ctx, msg.Property, msg.Request); err != nil {
		log.GetLogger().Errorf("bus call error: %s, self: %s, traceID: %s",
			err.Error(), selfFuncName, msg.TraceID)
		return common.ReqResult{Response: nil, ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg: proxyerrors.InternalErr}
	}
	log.GetLogger().Debugf("called request finished, self: %s, traceID: %s", selfFuncName, msg.TraceID)

	return common.ReqResult{Response: resp, ErrorCode: proxyerrors.StateOKCode, ErrorMsg: ""}
}

func (cb *ChannelBus) asyncRequest(ctx context.Context, dst *common.FunctionProperty,
	req *types.TCPRequest) (*types.TCPResponse, error) {
	seq, ch, err := cb.collector.AddRequest()
	req.RequestID = seq

	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, fmt.Errorf("add request to pendingCalls error")
	}

	err = cb.callback(ctx, dst, req)
	if err != nil {
		log.GetLogger().Warnf("failed to process callback function, error: %s", err.Error())
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("read response from channel failed, channel may closed")
		}
		return resp, nil
	}
}

// SetResult sets Called result
func (cb *ChannelBus) SetResult(resp *types.TCPResponse) error {
	cb.collector.AddResponse(resp)
	return nil
}

// CheckHealth check health, in order to adapt interface method
func (cb *ChannelBus) CheckHealth() bool {
	return true
}

// CheckRuntimeConnect -
func (cb *ChannelBus) CheckRuntimeConnect() bool {
	return true
}

// WaitRequestsFinished -
func (cb *ChannelBus) WaitRequestsFinished(directExit <-chan struct{}) error {
	return nil
}

// Stop -
func (cb *ChannelBus) Stop() {}

// CheckAuth check auth, in order to adapt interface method
func (cb *ChannelBus) CheckAuth(key string, source string) bool {
	return true
}

// StartRecvMessage channel starts to receive message
func (cb *ChannelBus) StartRecvMessage() {
	go cb.recvChanMsg()
}

// Cancel implement CommunicationBus.Cancel
func (cb *ChannelBus) Cancel(ctx context.Context, dst *common.CancelProperty,
	req *types.TCPRequest) (*types.TCPResponse, error) {
	log.GetLogger().Warnf("channel bus doesn't support cancel message")
	return nil, errors.New("not implement")
}

// CheckRuntime implement CommunicationBus.CheckRuntime
func (cb *ChannelBus) CheckRuntime(ctx context.Context, traceID string, shouldWait bool) (*types.TCPResponse, error) {
	log.GetLogger().Warnf("channel bus doesn't support check runtime")
	return nil, errors.New("not implement")
}

// Recover implement CommunicationBus.Recover
func (cb *ChannelBus) Recover(ctx context.Context, req *types.TCPRequest) (*types.TCPResponse, error) {
	log.GetLogger().Warnf("channel bus doesn't support recover message")
	return nil, errors.New("not implement")
}

// SetCancelResult implement CommunicationBus.SetCancelResult
func (cb *ChannelBus) SetCancelResult(resp *types.TCPResponse) error {
	log.GetLogger().Warnf("channel bus doesn't support cancel message")
	return errors.New("not implement")
}

// SetRecoverResult implement CommunicationBus.SetRecoverResult
func (cb *ChannelBus) SetRecoverResult(resp *types.TCPResponse) error {
	log.GetLogger().Warnf("channel bus doesn't support recover message")
	return errors.New("not implement")
}

// ReportRuntimeMetrics implement CommunicationBus.reportRuntimeMetrics
func (cb *ChannelBus) ReportRuntimeMetrics(ctx context.Context,
	metricsProperty *common.RuntimeMetricsProperty) (*types.TCPResponse, error) {
	return nil, errors.New("not implement")
}

// RegisterReportRuntimeMetrics implement CommunicationBus.RegisterReportRuntimeMetrics
func (cb *ChannelBus) RegisterReportRuntimeMetrics(rr common.ReportRuntimeMetrics) {}

// RegisterStoreResourceUsage implement CommunicationBus.RegisterStoreResourceUsage
func (cb *ChannelBus) RegisterStoreResourceUsage(rr common.StoreResourceUsage) {}

// RegisterStoreCaeMetrics register store cae metrics
func (cb *ChannelBus) RegisterStoreCaeMetrics(storeCaeMetrics common.StoreCaeMetrics) {}
