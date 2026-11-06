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

// Package tcpbus contains implementation of Bus with TCP and custom protocol
package tcpbus

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/functionbus/bus/calls"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/tcpbustool"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/caemetrics"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

const (
	maxReportErrMsgNum = 1000
)

var (
	errResult = types.Result{
		RspData:   nil,
		ErrorCode: proxyerrors.InternalErrCode,
		ErrorMsg:  proxyerrors.InternalErr,
	}
	errRecoverRuntimeResult = types.Result{
		RspData:   nil,
		ErrorCode: proxyerrors.RecoverRuntimeErrCode,
		ErrorMsg:  proxyerrors.RecoverRuntimeErr,
	}
	curPodName = os.Getenv("PodName")
)

// TCPBus is a concrete CommunicationBus implementation for interacting with TCP and custom protocol
type TCPBus struct {
	client               *Connection
	protoCachePool       *sync.Pool // *cache
	collector            *calls.CallCollector
	Property             common.FunctionProperty
	callback             common.Call
	asyncCallback        common.AsyncCall
	cancel               common.Cancel
	recover              common.Recover
	exit                 bool
	reportRuntimeMetrics common.ReportRuntimeMetrics
	storeResourceUsage   common.StoreResourceUsage
	storeCaeMetrics      common.StoreCaeMetrics
	singleMode           bool
	pauseRuntime         common.PauseRuntime
	recoverRuntime       common.RecoverRuntime
	checkRuntime         common.CheckRuntime
	reportCache          *sync.Map
}

// GetClient -
func (t *TCPBus) GetClient() *Connection {
	return t.client
}

// Option option for newing TCPBus, using existed client or created client
type Option struct {
	// if using existed client, set client
	Client *Connection
	// using existed client or create client
	CreateClient bool
	// if creating client, set serverAddr
	ServerAddr string
	// true: use socket, false: use channel.
	Remote bool
	// true: single client, false: multi clients
	SingleMode  bool
	ReportCache *sync.Map
}

type cache struct {
	buf []byte
}

func (c *cache) reset() {
	// NOTE: c.buf[:0] never panics even if c.buf is nil
	c.buf = c.buf[:0]
}

// NewTCPBus new tcp bus
func NewTCPBus(busOption common.BusOption) (*TCPBus, error) {
	if busOption.NewBusOpt == nil {
		return nil, fmt.Errorf("NewBusOpt is nil")
	}
	tcpBusOpt, ok := busOption.NewBusOpt.(*Option)
	if !ok {
		return nil, fmt.Errorf("assert type failed")
	}

	defaultProperty := common.FunctionProperty{
		// As server: bus-proxy
		FunctionName: common.DefaultFunctionName,
	}

	var collector *calls.CallCollector
	if tcpBusOpt.SingleMode {
		collector = calls.InitCollectorOnce(common.DefaultConcurrency)
	} else {
		collector = calls.InitCollector(common.DefaultConcurrency)
	}

	protoCachePool := &sync.Pool{
		New: func() interface{} { return &cache{} },
	}

	if !tcpBusOpt.CreateClient {
		return &TCPBus{
			collector:      collector,
			protoCachePool: protoCachePool,
			client:         tcpBusOpt.Client,
			Property:       defaultProperty,
		}, nil
	}

	// create client to connect
	clientOpt := ClientOption{
		ServerAddr:            tcpBusOpt.ServerAddr,
		NeedVerify:            true,
		DisableWhenServerExit: true,
	}
	if common.FConfig.HTTPSConfig != nil {
		clientOpt.TLSEnable = common.FConfig.HTTPSConfig.HTTPSEnable
		clientOpt.Config = tls.GetClientTLSConfig()
	}
	client, err := BuildClientConnection(clientOpt)
	if err != nil {
		return nil, err
	}

	return &TCPBus{
		collector:      collector,
		protoCachePool: protoCachePool,
		client:         client,
		Property:       defaultProperty,
		singleMode:     tcpBusOpt.SingleMode,
		reportCache:    tcpBusOpt.ReportCache,
	}, nil
}

// NewTCPBusAsServer creates TCPBus as server with given client connection
func NewTCPBusAsServer(conn *Connection, property common.FunctionProperty) *TCPBus {
	collector := calls.InitCollectorOnce(common.DefaultConcurrency)
	protoCachePool := &sync.Pool{
		New: func() interface{} { return &cache{} },
	}
	return &TCPBus{
		collector:      collector,
		protoCachePool: protoCachePool,
		client:         conn,
		Property:       property,
	}
}

func (t *TCPBus) acquireProtoCache() *cache {
	cache, ok := t.protoCachePool.Get().(*cache)
	if !ok {
		log.GetLogger().Errorf("failed to assert protoCachePool")
		return nil
	}
	cache.reset()
	return cache
}

func (t *TCPBus) releaseProtoCache(cache *cache) {
	t.protoCachePool.Put(cache)
}

// RegisterBus -
func (t *TCPBus) RegisterBus(property common.FunctionProperty) error {
	return nil
}

// WaitBusExit block until bus exited
func (t *TCPBus) WaitBusExit() {
	<-t.client.DisabledSignal
}

func (t *TCPBus) doCancel(ctx context.Context, dst *common.CancelProperty,
	req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
	seq, ch, err := t.collector.AddRequest()
	req.RequestID = seq
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, fmt.Errorf("add cancel request to pendingCalls error")
	}
	err = t.cancel(ctx, dst, req)
	if err != nil {
		log.GetLogger().Warnf("failed to process cancel, error: %s", err.Error())
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("failed to read cancel response")
		}
		return resp, nil
	}
}

func (t *TCPBus) doRecover(req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
	seq, ch, err := t.collector.AddRequest()
	req.RequestID = seq
	if err != nil {
		log.GetLogger().Errorf("add request to pendingCalls error, error: %s", err.Error())
		return nil, err
	}
	if ch == nil {
		return nil, fmt.Errorf("add recover request to pendingCalls error")
	}
	err = t.recover(req, t)
	if err != nil {
		log.GetLogger().Warnf("failed to recover, error: %s", err.Error())
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("failed to read recover response")
		}
		return resp, nil
	}
}

func (t *TCPBus) doRequest(ctx context.Context, dst *common.FunctionProperty,
	req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
	seq, ch, err := t.collector.AddRequest()
	req.RequestID = seq
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, fmt.Errorf("add request to pendingCalls error")
	}
	err = t.callback(ctx, dst, req)
	if err != nil {
		log.GetLogger().Warnf("failed to process callback function, error: %s", err.Error())
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("failed to read response from channel")
		}
		return resp, nil
	}
}

func (t *TCPBus) asyncRequest(
	ctx context.Context,
	dst *common.FunctionProperty,
	req *commontypes.TCPRequest,
	w func(*commontypes.TCPResponse, error)) {

	seq, err := t.collector.AddAsyncRequest(func(resp *commontypes.TCPResponse) {
		w(resp, nil)
	})
	req.RequestID = seq
	if err != nil {
		w(nil, err)
		return
	}

	t.asyncCallback(ctx, dst, req)
}

func (t *TCPBus) handleCallRequest(message *pb.Message, w func(types.Result)) {
	callMsg := message.CallMessage
	traceID := callMsg.TraceID

	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID)

	writeResult := func(resp *commontypes.TCPResponse, err error) {
		if err != nil {
			log.GetLogger().Errorf("bus call error: %s, self: %s, traceID: %s",
				err.Error(), t.Property.FunctionName, traceID)
			w(errResult)
			return
		}

		cache := t.acquireProtoCache()
		defer t.releaseProtoCache(cache)

		callRspPb := utils.ToCallResponsePb(resp)
		cache.buf, err = proto.MarshalOptions{}.MarshalAppend(cache.buf, &pb.Message{
			Type:         pb.Message_CALL_RESPONSE,
			CallResponse: callRspPb,
		})
		if err != nil {
			log.GetLogger().Errorf("bus call error: %s, self: %s, traceID: %s",
				err.Error(), t.Property.FunctionName, traceID)
			w(errResult)
			return
		}
		w(types.Result{
			RspData: cache.buf,
		})
	}

	if t.asyncCallback != nil {
		t.asyncRequest(ctx, utils.FromFunctionPropertyPb(callMsg.Dst),
			utils.FromCallRequestPb(callMsg.Req), writeResult)
		return
	}

	resp, err := t.doRequest(ctx, utils.FromFunctionPropertyPb(callMsg.Dst), utils.FromCallRequestPb(callMsg.Req))
	if err != nil {
		log.GetLogger().Errorf("failed to do request: %s", err.Error())
		writeResult(nil, err)
		return
	}
	writeResult(resp, nil)

}

func (t *TCPBus) handleCancelRequest(message *pb.Message) types.Result {
	var err error
	cancelMsg := message.CancelMessage
	log.GetLogger().Debugf("started to cancel request, self: %s, traceID: %s",
		t.Property.FunctionName, cancelMsg.TraceID)

	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, cancelMsg.TraceID)
	var data []byte

	dst := &common.CancelProperty{
		FutureID:  cancelMsg.FutureID,
		Force:     cancelMsg.Force,
		Recursive: cancelMsg.Recursive,
	}
	req := &commontypes.TCPRequest{
		RequestID: cancelMsg.RequestID,
	}
	resp, err := t.doCancel(ctx, dst, req)
	if err != nil {
		log.GetLogger().Errorf("bus cancel error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, cancelMsg.TraceID)
		return errResult
	}

	log.GetLogger().Debugf("finished canceling , self: %s, traceID: %s",
		t.Property.FunctionName, cancelMsg.TraceID)

	if data, err = proto.Marshal(&pb.Message{
		Type: pb.Message_CANCEL_RESPONSE,
		CancelResponse: &pb.Message_CancelResponse{
			ErrorCode:    resp.ErrorCode,
			ErrorMessage: resp.ErrorMessage,
		},
	}); err != nil {
		log.GetLogger().Errorf("bus cancel error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, cancelMsg.TraceID)
		return errResult
	}
	return types.Result{
		RspData: data,
	}
}

func (t *TCPBus) handlePauseRequest() types.Result {
	var data []byte
	var err error
	err = t.pauseRuntime()
	if err != nil {
		log.GetLogger().Warnf("failed to pause runtime %s", err.Error())
	}
	if data, err = proto.Marshal(&pb.Message{
		Type: pb.Message_PAUSE_RESPONSE,
	}); err != nil {
		log.GetLogger().Errorf("faild to marshal pause response err: %s", err.Error())
		return errResult
	}
	return types.Result{
		RspData: data,
	}
}

func (t *TCPBus) handleCheckRuntime(message *pb.Message) types.Result {
	data, err := t.checkRuntime(message.CheckRuntime.TraceID, message.CheckRuntime.ShouldWait)
	if err != nil {
		log.GetLogger().Warnf("failed to check runtime %s", err.Error())
		return errResult
	}
	return types.Result{RspData: data}
}

func (t *TCPBus) handleRecoverRuntime(message *pb.Message) types.Result {
	err := t.recoverRuntime(message.RecoverRuntime.RestartRuntime)
	if err != nil {
		log.GetLogger().Warnf("failed to recover runtime %s", err.Error())
		return errRecoverRuntimeResult
	}
	return types.Result{}
}

func (t *TCPBus) handleWorkerReport(message *pb.Message) types.Result {
	if t.reportCache != nil {
		if message.WorkerReport.TraceID == "" {
			log.GetLogger().Warnf("empty traceID of worker report %s", t.GetWorkerID())
			return types.Result{}
		}
		notifyIf, _ := t.reportCache.LoadOrStore(message.WorkerReport.TraceID, common.WorkerReportNotify{
			ReportTime: time.Now(),
			Ch:         make(chan common.WorkerReport, 1),
		})
		if notify, ok := notifyIf.(common.WorkerReportNotify); ok {
			select {
			case notify.Ch <- common.WorkerReport{
				Logs: message.WorkerReport.RuntimeLog,
			}:
			default:
				log.GetLogger().Warnf("worker report channel is full %s %s", t.GetWorkerID(),
					message.WorkerReport.TraceID)
			}
		}
	}
	return types.Result{}
}

func (t *TCPBus) handleRecoverRequest(message *pb.Message) types.Result {
	var err error
	var data []byte
	recoverMsg := message.RecoverMessage
	log.GetLogger().Debugf("started to recover request, self: %s, traceID: %s",
		t.Property.FunctionName, recoverMsg.TraceID)
	var flag bool
	if common.WorkerMap.CheckVal(recoverMsg.WorkerID) {
		flag = true
	}
	req := &commontypes.TCPRequest{
		RequestID:  recoverMsg.RequestID,
		WorkerID:   recoverMsg.WorkerID,
		IsStateFul: flag,
	}
	resp, err := t.doRecover(req)
	if err != nil {
		log.GetLogger().Errorf("bus recover error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, recoverMsg.TraceID)
		return errResult
	}

	log.GetLogger().Debugf("finished recovering , self: %s, traceID: %s",
		t.Property.FunctionName, recoverMsg.TraceID)

	if data, err = proto.Marshal(&pb.Message{
		Type: pb.Message_RECOVER_RESPONSE,
		RecoverResponse: &pb.Message_RecoverResponse{
			ErrorCode:    resp.ErrorCode,
			ErrorMessage: resp.ErrorMessage,
			RawData:      resp.RawData,
		},
	}); err != nil {
		log.GetLogger().Errorf("bus recover error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, recoverMsg.TraceID)
		return errResult
	}
	return types.Result{
		RspData: data,
	}
}

// HandleRequest is callback of client
func (t *TCPBus) HandleRequest(payload []byte, peerIP string, w func(types.Result)) {
	message := &pb.Message{}
	err := proto.Unmarshal(payload, message)
	if err != nil {
		log.GetLogger().Errorf("failed to handle request, self: %s, unmarshal error", t.Property.FunctionName)
		w(types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  proxyerrors.InternalErr,
		})
		return
	}
	// make sure terminating long-run worker can be recovered
	if t.exit && message.Type != pb.Message_RECOVER_REQUEST {
		log.GetLogger().Errorf("failed to handle request, self: %s, status: Exit", t.Property.FunctionName)
		w(types.Result{
			ErrorCode: proxyerrors.WorkerExitErrCode,
			ErrorMsg:  proxyerrors.WorkerExitErr,
		})
		return
	}
	t.handleRequest(message, w)
}

func (t *TCPBus) handleRequest(message *pb.Message, w func(types.Result)) {
	switch message.Type {
	case pb.Message_CALL_REQUEST:
		t.handleCallRequest(message, w)
	case pb.Message_CANCEL_REQUEST:
		w(t.handleCancelRequest(message))
	case pb.Message_RECOVER_REQUEST:
		w(t.handleRecoverRequest(message))
	case pb.Message_REPORT_RUNTIME_METRICS_REQUEST:
		w(t.handleRuntimeMetricsRequest(message))
	case pb.Message_REPORT_RESOURCE_USAGE_REQUEST:
		w(t.handleResourceUsageRequest(message))
	case pb.Message_REPORT_WORKER_EXIT_REQUEST:
		w(t.handleWorkerExitRequest())
	case pb.Message_PAUSE_REQUEST:
		w(t.handlePauseRequest())
	case pb.Message_WORKER_REPORT:
		w(t.handleWorkerReport(message))
	case pb.Message_RECOVER_RUNTIME:
		w(t.handleRecoverRuntime(message))
	case pb.Message_REPORT_CAE_METRICS_REQUEST:
		w(t.handleCaeMetricsRequest(message))
	case pb.Message_CHECK_RUNTIME:
		w(t.handleCheckRuntime(message))
	case pb.Message_REPORT_ERROR_MESSAGE_REQUEST:
		w(t.handleReportErrorMessageRequest(message))
	default:
		log.GetLogger().Errorf("failed to handle request, self: %s, invalid message type %d",
			t.Property.FunctionName, message.Type)
		w(types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  proxyerrors.InternalErr,
		})
	}
}

// RegisterCallback registers Called method for bus user
func (t *TCPBus) RegisterCallback(callback common.Call) {
	t.client.registerCallBack(t.HandleRequest)
	t.callback = callback
}

// RegisterAsyncCallback registers Called method for bus user. User should either call RegisterCallback or
// RegisterAsyncCallback.
func (t *TCPBus) RegisterAsyncCallback(asyncCallback common.AsyncCall) {
	t.client.registerCallBack(t.HandleRequest)
	t.asyncCallback = asyncCallback
}

// RegisterCancel registers cancel method for bus user
func (t *TCPBus) RegisterCancel(cancel common.Cancel) {
	t.cancel = cancel
}

// RegisterRecover registers recover method for bus user
func (t *TCPBus) RegisterRecover(recover common.Recover) {
	t.recover = recover
}

// RegisterRecoverRuntime register recover runtime method
func (t *TCPBus) RegisterRecoverRuntime(recoverRuntime common.RecoverRuntime) {
	t.recoverRuntime = recoverRuntime
}

// RegisterPauseRuntime register pause runtime method
func (t *TCPBus) RegisterPauseRuntime(pauseRuntime common.PauseRuntime) {
	t.pauseRuntime = pauseRuntime
}

// RegisterCheckRuntime registers check method for bus user
func (t *TCPBus) RegisterCheckRuntime(checkRuntime common.CheckRuntime) {
	t.checkRuntime = checkRuntime
}

// RegisterReportRuntimeMetrics registers ReportRuntimeMetrics method for bus user
func (t *TCPBus) RegisterReportRuntimeMetrics(reportRuntimeMetrics common.ReportRuntimeMetrics) {
	t.reportRuntimeMetrics = reportRuntimeMetrics
}

// RegisterStoreResourceUsage registers reportResourceUsage method for bus user
func (t *TCPBus) RegisterStoreResourceUsage(storeResourceUsage common.StoreResourceUsage) {
	t.storeResourceUsage = storeResourceUsage
}

// RegisterStoreCaeMetrics registers reportCaeMetrics method for bus user
func (t *TCPBus) RegisterStoreCaeMetrics(storeCaeMetrics common.StoreCaeMetrics) {
	t.storeCaeMetrics = storeCaeMetrics
}

// Init is a concrete Init implementation for TCPBus
func (t *TCPBus) Init(ctx context.Context) (*commontypes.TCPResponse, error) {
	traceID, self := utils.ContextValueString(ctx, constants.HeaderTraceID), t.GetWorkerID()
	callMsg := &pb.Message_InitRequest{
		TraceID: traceID,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:        pb.Message_INIT_REQUEST,
		InitRequest: callMsg,
	})
	if err != nil {
		log.GetLogger().Errorf("bus init, self: %s, marshal failed: %s, traceID: %s", self, err.Error(), traceID)
		return nil, fmt.Errorf("init error: %s", err.Error())
	}
	result := t.client.request(ctx, data, false, false)
	log.GetLogger().Debugf("request finished, from: %s, traceID: %s", self, traceID)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("init failed, self: %s, errCode: %d, traceID: %s", self, result.ErrorCode, traceID)
		return &commontypes.TCPResponse{
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}

	message := &pb.Message{}
	if err = proto.Unmarshal(result.RspData, message); err != nil {
		log.GetLogger().Errorf("bus init response unmarshal error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("bus init response unmarshal error: %s", err.Error())
	}

	if message.InitResponse == nil {
		log.GetLogger().Errorf("bus init response is nil, self: %s, traceID: %s", t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("bus init response is nil")
	}

	return &commontypes.TCPResponse{
		ErrorCode:    message.InitResponse.ErrorCode,
		ErrorMessage: message.InitResponse.ErrorMessage,
		Summary:      message.InitResponse.RuntimeSummary,
	}, nil
}

// Call is a concrete Call implementation for TCPBus
func (t *TCPBus) Call(ctx context.Context, dst *common.FunctionProperty,
	req *commontypes.TCPRequest, isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
	traceID, self := utils.ContextValueString(ctx, constants.HeaderTraceID), t.Property.FunctionName
	log.GetLogger().Debugf("call request start, from: %s, to: %s, traceID: %s", self, dst.FunctionName, traceID)
	callMsg := &pb.Message_CallMessage{
		TraceID: traceID,
		Dst:     utils.ToFunctionPropertyPb(dst),
		Req:     utils.ToCallRequestPb(req),
	}
	data, err := proto.Marshal(&pb.Message{
		Type:        pb.Message_CALL_REQUEST,
		CallMessage: callMsg,
	})
	if err != nil {
		log.GetLogger().Errorf("bus call, self: %s, marshal failed: %s, traceID: %s", self, err.Error(), traceID)
		return nil, fmt.Errorf("call error: %s", err.Error())
	}
	req.RawData = nil
	callMsg.Req.RawData = nil
	result := t.client.request(ctx, data, isSendingRemoteProxy, req.IsStateFul)
	log.GetLogger().Debugf("request finished, from: %s, to: %s, traceID: %s", self, dst.FunctionName, traceID)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("call failed, self: %s, errCode: %d, traceID: %s", self, result.ErrorCode, traceID)
		return &commontypes.TCPResponse{
			RequestID:    req.RequestID,
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}

	message := &pb.Message{}
	if err = proto.Unmarshal(result.RspData, message); err != nil {
		log.GetLogger().Errorf("bus call response unmarshal error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("bus call response unmarshal error: %s", err.Error())
	}

	if message.CallResponse == nil {
		log.GetLogger().Errorf("bus call response is nil, self: %s, traceID: %s", t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("bus call response is nil")
	}
	message.CallResponse.RequestID = req.RequestID
	return utils.FromCallResponsePb(message.CallResponse), nil
}

// Cancel is a concrete Cancel implementation for TCPBus
func (t *TCPBus) Cancel(ctx context.Context, dst *common.CancelProperty,
	req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {

	traceID, self := utils.ContextValueString(ctx, constants.HeaderTraceID), t.Property.FunctionName
	log.GetLogger().Debugf("cancel request start, futureID: %s, traceID: %s", dst.FutureID, traceID)
	callMsg := &pb.Message_CancelMessage{
		TraceID:   traceID,
		FutureID:  dst.FutureID,
		Force:     dst.Force,
		Recursive: dst.Recursive,
		WorkerID:  req.WorkerID,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:          pb.Message_CANCEL_REQUEST,
		CancelMessage: callMsg,
	})
	if err != nil {
		log.GetLogger().Errorf("bus cancel, self: %s, marshal failed: %s, traceID: %s", self, err.Error(), traceID)
		return nil, fmt.Errorf("cancel error: %s", err.Error())
	}
	var isStatefulFunc bool
	if req.IsStateFul || common.WorkerMap.CheckVal(req.WorkerID) {
		isStatefulFunc = true
	}
	result := t.client.request(ctx, data, false, isStatefulFunc)
	log.GetLogger().Debugf("cancel finished, from: %s, to: %s, traceID: %s", self, dst.FutureID, traceID)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("cancel failed, self: %s, errCode: %d, traceID: %s", self, result.ErrorCode, traceID)
		return &commontypes.TCPResponse{
			RequestID:    req.RequestID,
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}
	message := &pb.Message{}
	err = proto.Unmarshal(result.RspData, message)
	if err != nil {
		log.GetLogger().Errorf("bus cancel response unmarshal error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("bus cancel response unmarshal error: %s", err.Error())
	}
	if message.CancelResponse == nil {
		log.GetLogger().Errorf("cancel response is nil!")
		return &commontypes.TCPResponse{
			ErrorCode:    0,
			ErrorMessage: "success",
		}, nil
	}
	return &commontypes.TCPResponse{
		ErrorCode:    message.CancelResponse.ErrorCode,
		ErrorMessage: message.CancelResponse.ErrorMessage,
	}, nil
}

// CheckRuntime -
func (t *TCPBus) CheckRuntime(ctx context.Context, traceID string, shouldWait bool) (*commontypes.TCPResponse, error) {
	checkMsg := &pb.Message_CheckRuntime{
		TraceID:    traceID,
		ShouldWait: shouldWait,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:         pb.Message_CHECK_RUNTIME,
		CheckRuntime: checkMsg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal check runtime request %s", err.Error())
		return nil, err
	}
	result := t.client.ctrlRtmRequest(ctx, data)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("check runtime failed, pod %s, errCode: %d", t.GetWorkerID(), result.ErrorCode)
		return &commontypes.TCPResponse{
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}
	message := &pb.Message{}
	if err = proto.Unmarshal(result.RspData, message); err != nil {
		log.GetLogger().Errorf("check runtime response unmarshal error: %s, self: %s, traceID: %s",
			err.Error(), t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("check runtime response unmarshal error: %s", err.Error())
	}
	if message.WorkerReport == nil {
		log.GetLogger().Errorf("check runtime response is nil, self: %s, traceID: %s", t.Property.FunctionName, traceID)
		return nil, fmt.Errorf("check runtime response is nil")
	}
	return &commontypes.TCPResponse{
		ErrorCode:    message.WorkerReport.ErrorCode,
		ErrorMessage: message.WorkerReport.ErrorMessage,
		Logs:         message.WorkerReport.RuntimeLog,
		Summary:      message.WorkerReport.RuntimeSummary,
	}, nil
}

// Recover -
func (t *TCPBus) Recover(ctx context.Context, req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
	traceID, self := utils.ContextValueString(ctx, constants.HeaderTraceID), t.Property.FunctionName
	log.GetLogger().Debugf("recovering request start, traceID: %s", traceID)
	return t.client.SendRecoverRequest(ctx, req, traceID, self)
}

// RecoverRuntime send recover runtime to worker to recover runtime process
func (t *TCPBus) RecoverRuntime(ctx context.Context, restartRuntime bool) (*commontypes.TCPResponse, error) {
	recoverMsg := &pb.Message_RecoverRuntime{
		RestartRuntime: restartRuntime,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:           pb.Message_RECOVER_RUNTIME,
		RecoverRuntime: recoverMsg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal recover runtime request %s", err.Error())
		return nil, err
	}
	result := t.client.ctrlRtmRequest(ctx, data)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to recover pod %s, errCode: %d", t.GetWorkerID(), result.ErrorCode)
		return &commontypes.TCPResponse{
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}
	return &commontypes.TCPResponse{}, nil
}

// Pause send pause to worker
func (t *TCPBus) Pause(ctx context.Context) (*commontypes.TCPResponse, error) {
	data, err := proto.Marshal(&pb.Message{Type: pb.Message_PAUSE_REQUEST})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal pause request %s", err.Error())
		return nil, err
	}
	result := t.client.request(ctx, data, false, false)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to pause pod %s, errCode: %d", t.GetWorkerID(), result.ErrorCode)
		return nil, errors.New(result.ErrorMsg)
	}
	message := &pb.Message{}
	err = proto.Unmarshal(result.RspData, message)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal pause rsp, pod %s, error: %s", t.GetWorkerID(), err.Error())
		return nil, err
	}
	return &commontypes.TCPResponse{}, nil
}

// SetResult sets Called result
func (t *TCPBus) SetResult(resp *commontypes.TCPResponse) error {
	ok := t.collector.AddResponse(resp)

	// When connection is reset (we have a new TCPBus) or "SetResult" of a request is called for the second time,
	// send a "ReportRspMsg" to peer.
	if !ok {
		response := &pb.CallResponse{
			FunctionKey:  resp.FunctionKey,
			InstanceID:   resp.InstanceID,
			Labels:       resp.Labels,
			RawData:      resp.RawData,
			Logs:         resp.Logs,
			Summary:      resp.Summary,
			ErrorCode:    resp.ErrorCode,
			ErrorMessage: resp.ErrorMessage,
			DcCallStack:  resp.DcCallStack,
		}
		data, err := proto.Marshal(&pb.Message{CallResponse: response})
		if err != nil {
			log.GetLogger().Errorf("failed to marshal call response: %s", err.Error())
			return err
		}
		msg := &protocol.Message{
			Header: protocol.MsgHeader{
				MsgType:  protocol.ReportRspMsg,
				PacketID: uuid.New().String(),
			},
			Payload: data,
		}
		for i := 0; i < retryTimesAfterTimeout; i++ {
			err = t.client.send(msg)
			if err != nil {
				continue
			}
			break
		}
		if err == pool.ErrConnectionPoolEmpty {
			return err
		}
	}
	return nil
}

// SetCancelResult -
func (t *TCPBus) SetCancelResult(resp *commontypes.TCPResponse) error {
	t.collector.AddResponse(resp)
	return nil
}

// SetRecoverResult -
func (t *TCPBus) SetRecoverResult(resp *commontypes.TCPResponse) error {
	t.collector.AddResponse(resp)
	return nil
}

// CheckHealth checks health
func (t *TCPBus) CheckHealth() bool {
	return t.client.isConnect
}

// CheckRuntimeConnect checks health of runtime connect
func (t *TCPBus) CheckRuntimeConnect() bool {
	select {
	case <-t.client.RuntimeDisconn:
		return false
	default:
		return true
	}
}

// CheckAuth checks health
func (t *TCPBus) CheckAuth(key string, source string) bool {
	return t.client.checkAuth(key, source)
}

// WaitRequestsFinished -
func (t *TCPBus) WaitRequestsFinished(directExit <-chan struct{}) error {
	t.exit = true
	// wait for requests finished
	requestExit := make(chan struct{})
	go func() {
		t.collector.Clear()
		close(requestExit)
	}()

	select {
	case <-directExit:
		log.GetLogger().Infof("exit when no request for long time")
	case <-requestExit:
	}
	log.GetLogger().Warnf("it will start to report worker exit request to bus")
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	err := t.ReportWorkerExitRequest(ctx)
	cancel()
	if err != nil {
		log.GetLogger().Warnf("failed to report worker exit request to bus with error %s", err.Error())
	}
	return nil
}

// Stop Stop() should be called after WaitRequestsFinished
func (t *TCPBus) Stop() {
	t.client.Stop()
}

// StartRecvMessage channel starts to receive message
func (t *TCPBus) StartRecvMessage() {

}

// ReportRuntimeInfo -
func (t *TCPBus) ReportRuntimeInfo(ctx context.Context, info *connecttypes.WorkerReportInfo) (*commontypes.TCPResponse,
	error) {
	log.GetLogger().Debugf("Started to report runtime log, function: %s, info: %v", t.Property.FunctionName, info)
	msg := &pb.Message_WorkerReport{
		TraceID:        info.TraceID,
		RuntimeLog:     info.RuntimeLog,
		RuntimeSummary: info.RuntimeSummary,
		ErrorCode:      (uint32)(info.ErrorCode),
		ErrorMessage:   info.ErrorMessage,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:         pb.Message_WORKER_REPORT,
		WorkerReport: msg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal, %s", err.Error())
		return nil, fmt.Errorf("report runtime metrics error: %s", err.Error())
	}

	result := t.client.request(ctx, data, false, false)
	log.GetLogger().Debugf("finished to report runtime info, function: %s", t.Property.FunctionName)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report runtime info, function: %s, errCode: %d",
			t.Property.FunctionName, result.ErrorCode)
		return &commontypes.TCPResponse{
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}
	return &commontypes.TCPResponse{}, nil
}

// ReportErrMsgToBus -
func (t *TCPBus) ReportErrMsgToBus(ctx context.Context, URNWithVersion, errMsg string, properties map[string]string) (
	*commontypes.TCPResponse, error) {
	log.GetLogger().Debugf("Started to report errMsg, function: %s, errMsg: %s, properties %v",
		t.Property.FunctionName, errMsg, properties)
	msg := &pb.Message_ReportErrorMessage{
		PodName:            curPodName,
		FuncUrnWithVersion: URNWithVersion,
		ErrorMessage:       errMsg,
		UploadTime:         time.Now().Unix(),
		Properties:         properties,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:               pb.Message_REPORT_ERROR_MESSAGE_REQUEST,
		ReportErrorMessage: msg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal, %s", err.Error())
		return nil, fmt.Errorf("report runtime metrics error: %s", err.Error())
	}

	result := t.client.request(ctx, data, false, false)
	log.GetLogger().Debugf("finished to report errMsg, function: %s", t.Property.FunctionName)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report errMsg, function: %s, errCode: %d",
			t.Property.FunctionName, result.ErrorCode)
		return &commontypes.TCPResponse{
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}
	return &commontypes.TCPResponse{}, nil
}

// ReportRuntimeMetrics -
func (t *TCPBus) ReportRuntimeMetrics(ctx context.Context,
	metricsProperty *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
	log.GetLogger().Debugf("Started to report runtime metrics, function: %s, pod: %s "+
		"metricsName: %s, metricsValue: %s ", t.Property.FunctionName, metricsProperty.PodName,
		metricsProperty.MetricsName, metricsProperty.MetricsValue)
	msg := &pb.Message_ReportRuntimeMetricsMessage{
		FunctionURN:      metricsProperty.FunctionURN,
		PodName:          metricsProperty.PodName,
		MetricsName:      metricsProperty.MetricsName,
		MetricsValue:     metricsProperty.MetricsValue,
		GPUMem:           metricsProperty.GPUMem,
		EphemeralStorage: metricsProperty.EphemeralStorage,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:                        pb.Message_REPORT_RUNTIME_METRICS_REQUEST,
		ReportRuntimeMetricsMessage: msg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal reportRuntimeMetricsMessage, %s", err.Error())
		return nil, fmt.Errorf("report runtime metrics error: %s", err.Error())
	}

	result := t.client.request(ctx, data, false, false)
	log.GetLogger().Debugf("finished to report runtime metrics, function: %s, pod: %s",
		t.Property.FunctionName, metricsProperty.PodName)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report runtime metrics, function: %s, pod: %s, errCode: %d",
			t.Property.FunctionName, metricsProperty.PodName, result.ErrorCode)
		return &commontypes.TCPResponse{
			ErrorCode:    result.ErrorCode,
			ErrorMessage: result.ErrorMsg,
		}, nil
	}

	message := &pb.Message{}
	if err = proto.Unmarshal(result.RspData, message); err != nil {
		log.GetLogger().Errorf("failed to unmarshal report runtime metrics response, %s", err.Error())
		return nil, fmt.Errorf("bus report runtime metrics response unmarshal error: %s", err.Error())
	}
	if message.ReportRuntimeMetricsResponse == nil {
		return &commontypes.TCPResponse{
			ErrorCode:    0,
			ErrorMessage: "success",
		}, nil
	}

	return &commontypes.TCPResponse{
		ErrorCode:    message.ReportRuntimeMetricsResponse.ErrorCode,
		ErrorMessage: message.ReportRuntimeMetricsResponse.ErrorMessage,
	}, nil
}

// ReportWorkerExitRequest report worker exit request to bus
func (t *TCPBus) ReportWorkerExitRequest(ctx context.Context) error {
	data, err := proto.Marshal(&pb.Message{
		Type: pb.Message_REPORT_WORKER_EXIT_REQUEST,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal Message_REPORT_WORKER_EXIT_REQUEST, %s", err.Error())
		return fmt.Errorf("report worker exit request error: %s", err.Error())
	}
	if t.client == nil {
		// no need to report
		return nil
	}
	result := t.client.request(ctx, data, false, false)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report worker exit request, errCode: %d, errMsg: %s",
			result.ErrorCode, result.ErrorMsg)
		return fmt.Errorf("failed to report worker exit request, errCode: %d, errMsg: %s",
			result.ErrorCode, result.ErrorMsg)
	}
	log.GetLogger().Infof("succeed to report worker exit request to bus")
	return nil
}

// ReportResourceUsage worker report resource usage by this function
func (t *TCPBus) ReportResourceUsage(ctx context.Context,
	resourceUsage *common.ResourceUsage) error {
	log.GetLogger().Debugf("start to report resource usage, functionKey: %s, pod: %s cpuUsage: %f, "+
		"cpuLimit: %d, memoryUsage: %s ,memoryLimit: %d", resourceUsage.FunctionKey, resourceUsage.PodName,
		resourceUsage.CPUUsage, resourceUsage.CPULimit, resourceUsage.MemoryUsage, resourceUsage.MemoryLimit)
	msg := &pb.Message_ReportResourceUsageMessage{
		FunctionKey: resourceUsage.FunctionKey,
		PodName:     resourceUsage.PodName,
		CpuUsage:    resourceUsage.CPUUsage,
		MemoryUsage: resourceUsage.MemoryUsage,
		CpuLimit:    resourceUsage.CPULimit,
		MemoryLimit: resourceUsage.MemoryLimit,
		ReportTime:  resourceUsage.ReportTime,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:                       pb.Message_REPORT_RESOURCE_USAGE_REQUEST,
		ReportResourceUsageMessage: msg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal reportResourceUsageMessage, %s", err.Error())
		return fmt.Errorf("report resource usage error: %s", err.Error())
	}

	result := t.client.request(ctx, data, false, false)
	log.GetLogger().Debugf("finished to report resource usage, function: %s, pod: %s",
		t.Property.FunctionName, resourceUsage.PodName)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report resource usage, function: %s, pod: %s, errCode: %d",
			t.Property.FunctionName, resourceUsage.PodName, result.ErrorCode)
		return fmt.Errorf("failed to report resource usage,errCode: %d, errMsg: %s",
			result.ErrorCode, result.ErrorMsg)
	}
	return nil
}

// ReportCaeMetrics worker report cae metrics by this function
func (t *TCPBus) ReportCaeMetrics(ctx context.Context,
	caeMetrics *common.CaeMetrics) error {

	log.GetLogger().Debugf("start to report cae metrics, functionKey: %s, pod: %s cpuUsageRate: %f, "+
		"memoryUsageRate: %s", caeMetrics.FunctionKey, caeMetrics.PodName,
		caeMetrics.CPUUsageRate, caeMetrics.MemoryUsageRate)
	msg := &pb.Message_ReportCaeMetricsMessage{
		FunctionKey:                   caeMetrics.FunctionKey,
		PodName:                       caeMetrics.PodName,
		CpuUsageRate:                  caeMetrics.CPUUsageRate,
		MemoryUsageRate:               caeMetrics.MemoryUsageRate,
		ReceiveBytesPerSecond:         caeMetrics.ReceiveBytesPerSecond,
		ReceivePackagePerSecond:       caeMetrics.ReceivePackagePerSecond,
		TransmitBytesPerSecond:        caeMetrics.TransmitBytesPerSecond,
		TransmitPackagePerSecond:      caeMetrics.TransmitPackagePerSecond,
		FileSystemReadBytesPerSecond:  caeMetrics.FileSystemReadBytesPerSecond,
		FileSystemWriteBytesPerSecond: caeMetrics.FileSystemWriteBytesPerSecond,
		CustomReportMetrics:           caeMetrics.CustomReportMetrics.ToPb(),
		ReportTime:                    caeMetrics.ReportTime,
		Properties:                    caeMetrics.Properties,
		FileSystemUsageRate:           caeMetrics.FileSystemUsageRate,
		FileSystemEmptySpace:          caeMetrics.FileSystemEmptySpace,
		FileSystemCapacity:            caeMetrics.FileSystemCapacity,
		MemoryUsage:                   caeMetrics.MemoryUsage,
		MemoryCapacity:                caeMetrics.MemoryCapacity,
		CpuUsage:                      caeMetrics.CPUUsage,
		CpuCapacity:                   caeMetrics.CPUCapacity,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:                    pb.Message_REPORT_CAE_METRICS_REQUEST,
		ReportCaeMetricsMessage: msg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal reportCaeMetricsMessage, %s", err.Error())
		return fmt.Errorf("report cae metrics error: %s", err.Error())
	}

	result := t.client.request(ctx, data, false, false)
	log.GetLogger().Debugf("finished to report cae metrics, function: %s, pod: %s",
		t.Property.FunctionName, caeMetrics.PodName)
	if result.ErrorCode != 0 {
		log.GetLogger().Warnf("failed to report cae metrics, function: %s, pod: %s, errCode: %d",
			t.Property.FunctionName, caeMetrics.PodName, result.ErrorCode)
		return fmt.Errorf("failed to report cae metrics usage,errCode: %d, errMsg: %s",
			result.ErrorCode, result.ErrorMsg)
	}
	return nil
}

func (t *TCPBus) handleRuntimeMetricsRequest(message *pb.Message) types.Result {
	metricsMessage := message.ReportRuntimeMetricsMessage
	log.GetLogger().Debugf("started to handle runtime metrics request, self: %s, pod: %s,"+
		"metricsName: %s, metricsValue: %s ", t.Property.FunctionName, metricsMessage.PodName,
		metricsMessage.MetricsName, metricsMessage.MetricsValue)

	metricsProperty := common.RuntimeMetricsProperty{
		FunctionURN:      metricsMessage.FunctionURN,
		PodName:          metricsMessage.PodName,
		MetricsName:      metricsMessage.MetricsName,
		MetricsValue:     metricsMessage.MetricsValue,
		GPUMem:           metricsMessage.GPUMem,
		EphemeralStorage: metricsMessage.EphemeralStorage,
	}

	if err := t.reportRuntimeMetrics(metricsProperty); err != nil {
		log.GetLogger().Errorf("failed to report runtime metrics, %s", err.Error())
		return errResult
	}

	return types.Result{}
}

func (t *TCPBus) handleReportErrorMessageRequest(message *pb.Message) types.Result {
	errorMessage := message.ReportErrorMessage
	log.GetLogger().Debugf("started to handle report error message request, self: %s, podName: %s, urn: %s, "+
		"ErrorMessage: %s", t.Property.FunctionName, errorMessage.PodName, errorMessage.FuncUrnWithVersion,
		errorMessage.ErrorMessage)

	//  The situation where an error is obtained through the bus directly without insertion and deletion operations
	podErrorInfo := generatePodErrorInfo(errorMessage)
	caemetrics.GetCaeMetricsHandler().PushPodErrMetrics(errorMessage.FuncUrnWithVersion, podErrorInfo)
	poderrormessage.ReportPodErrMsgToRedis(redisclient.GetRedisCmd(), errorMessage.FuncUrnWithVersion,
		maxReportErrMsgNum, podErrorInfo)
	return types.Result{}
}

func generatePodErrorInfo(errorMessage *pb.Message_ReportErrorMessage) poderrormessage.PodErrorInfo {
	return poderrormessage.PodErrorInfo{
		PodName:         errorMessage.PodName,
		ErrorMessage:    errorMessage.ErrorMessage,
		EnvName:         errorMessage.Properties[constants.CAEPropertiesEnvName],
		ComponentName:   errorMessage.Properties[constants.CAEPropertiesComponentName],
		ApplicationName: errorMessage.Properties[constants.CAEPropertiesAppName],
		EnvID:           errorMessage.Properties[constants.CAEPropertiesEnvID],
		ComponentID:     errorMessage.Properties[constants.CAEPropertiesComponentID],
		ApplicationID:   errorMessage.Properties[constants.CAEPropertiesAppID],
		UploadTime:      errorMessage.UploadTime,
	}
}

func (t *TCPBus) handleResourceUsageRequest(message *pb.Message) types.Result {
	resourceUsageMessage := message.ReportResourceUsageMessage
	log.GetLogger().Debugf("started to handle report resource usage request, self: %s, funcKey: %s, pod: %s,"+
		"cpu usage: %f, cpu limit: %d, memory usage: %f, memory limit : %d, report time: %d", t.Property.FunctionName,
		resourceUsageMessage.FunctionKey, resourceUsageMessage.PodName, resourceUsageMessage.CpuUsage,
		resourceUsageMessage.CpuLimit, resourceUsageMessage.MemoryUsage, resourceUsageMessage.MemoryLimit,
		resourceUsageMessage.ReportTime)

	resourceUsage := common.ResourceUsage{
		FunctionKey: resourceUsageMessage.FunctionKey,
		PodName:     resourceUsageMessage.PodName,
		CPUUsage:    resourceUsageMessage.CpuUsage,
		MemoryUsage: resourceUsageMessage.MemoryUsage,
		CPULimit:    resourceUsageMessage.CpuLimit,
		MemoryLimit: resourceUsageMessage.MemoryLimit,
		ReportTime:  resourceUsageMessage.ReportTime,
	}

	t.storeResourceUsage(resourceUsage)
	return types.Result{}
}

func (t *TCPBus) handleCaeMetricsRequest(message *pb.Message) types.Result {
	caeMetricsMessage := message.ReportCaeMetricsMessage
	log.GetLogger().Debugf("started to handle report cae metrics request, self: %s, funcKey: %s, pod: %s,"+
		"cpu usage rate: %f, memory usage rate: %f, rbps: %f, rpps: %f, tbps: %f, tpps: %f, io_read_bps: %f, "+
		"io_write_bps: %s, report time: %d, properties: %v", t.Property.FunctionName, caeMetricsMessage.FunctionKey,
		caeMetricsMessage.PodName, caeMetricsMessage.CpuUsageRate,
		caeMetricsMessage.MemoryUsageRate, caeMetricsMessage.ReceiveBytesPerSecond, caeMetricsMessage.ReceivePackagePerSecond,
		caeMetricsMessage.TransmitBytesPerSecond, caeMetricsMessage.TransmitPackagePerSecond,
		caeMetricsMessage.FileSystemReadBytesPerSecond, caeMetricsMessage.FileSystemWriteBytesPerSecond,
		caeMetricsMessage.ReportTime, caeMetricsMessage.Properties)

	customReportMetrics := tcpbustool.CustomReportMetrics{}
	customReportMetrics.ParseFromPb(caeMetricsMessage.CustomReportMetrics)
	caeMetrics := common.CaeMetrics{
		FunctionKey:                   caeMetricsMessage.FunctionKey,
		PodName:                       caeMetricsMessage.PodName,
		CPUUsageRate:                  caeMetricsMessage.CpuUsageRate,
		MemoryUsageRate:               caeMetricsMessage.MemoryUsageRate,
		ReceiveBytesPerSecond:         caeMetricsMessage.ReceiveBytesPerSecond,
		ReceivePackagePerSecond:       caeMetricsMessage.ReceivePackagePerSecond,
		TransmitBytesPerSecond:        caeMetricsMessage.TransmitBytesPerSecond,
		TransmitPackagePerSecond:      caeMetricsMessage.TransmitPackagePerSecond,
		FileSystemReadBytesPerSecond:  caeMetricsMessage.FileSystemReadBytesPerSecond,
		FileSystemWriteBytesPerSecond: caeMetricsMessage.FileSystemWriteBytesPerSecond,
		CustomReportMetrics:           customReportMetrics,
		ReportTime:                    caeMetricsMessage.ReportTime,
		Properties:                    caeMetricsMessage.Properties,
		FileSystemUsageRate:           caeMetricsMessage.FileSystemUsageRate,
		FileSystemEmptySpace:          caeMetricsMessage.FileSystemEmptySpace,
		FileSystemCapacity:            caeMetricsMessage.FileSystemCapacity,
		MemoryUsage:                   caeMetricsMessage.MemoryUsage,
		MemoryCapacity:                caeMetricsMessage.MemoryCapacity,
		CPUUsage:                      caeMetricsMessage.CpuUsage,
		CPUCapacity:                   caeMetricsMessage.CpuCapacity,
	}

	t.storeCaeMetrics(caeMetrics)
	return types.Result{}
}

func (t *TCPBus) handleWorkerExitRequest() types.Result {
	log.GetLogger().Infof("start to handle worker exit request")
	t.client.setConnNormalExitFlag()
	t.client.clearPendingReq()
	log.GetLogger().Infof("finished to handle worker exit request")
	return types.Result{}
}

// SetWorkerID set worker id
func (t *TCPBus) SetWorkerID(name string) {
	if t != nil && t.client != nil {
		t.client.workerID = name
	}
}

// CheckWorkerID check worker id match worker pod name
func (t *TCPBus) CheckWorkerID(data []byte) bool {
	if t == nil || t.client == nil || t.client.workerID == "" {
		return true
	}
	return t.client.checkWorkerID(data)
}

// GetWorkerID return workerID
func (t *TCPBus) GetWorkerID() string {
	if t == nil || t.client == nil {
		return ""
	}
	return t.client.workerID
}
