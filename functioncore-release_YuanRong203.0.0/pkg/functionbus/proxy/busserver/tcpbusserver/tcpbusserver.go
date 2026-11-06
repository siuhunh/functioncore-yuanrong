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

// Package tcpbusserver is used for routing messages between nodes in the cluster or between bus proxy with worker
package tcpbusserver

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	commonTypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/busserver"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/proxyview"
	"functioncore/pkg/functionbus/proxy/vpa"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxyerrors"
)

const mapInitSize = 16

// TCPBusServer tcp bus server struct
type TCPBusServer struct {
	baseBusServer  *busserver.BaseBusServer
	requestHandler internal.HandleReq
	listenAddr     string
}

// CreateTCPBusServer create tcp bus server
func CreateTCPBusServer(opt *busserver.Option) (*TCPBusServer, error) {
	if opt == nil || opt.RequestHandler == nil {
		return nil, fmt.Errorf("request handle of tcp bus is nil")
	}
	busClients := &busserver.ConnectionClient{
		ProxyClients: make(map[string]bus.CommunicationBus, mapInitSize),
		Mux:          sync.RWMutex{},
	}
	bus := &TCPBusServer{
		baseBusServer: &busserver.BaseBusServer{
			BusClients: busClients,
		},
		requestHandler: opt.RequestHandler,
		listenAddr:     opt.ListenAddr,
	}
	return bus, nil
}

// StartServer start tcp bus server
func (s *TCPBusServer) StartServer() error {
	// build connection server by tcp
	err := tcpbus.StartServer(tcpbus.ServerOption{
		Receiver:   s,
		ListenAddr: s.listenAddr,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to create socket server: %s", err.Error())
		return err
	}

	workerbroker.GetInstance().RegisterRequestHandler(s.requestHandler)
	workerbroker.GetInstance().RegisterDeleteFuncInsHandler(vpa.DeleteFuncInsInSLOAgentCallBack)
	return nil
}

// NotifyRemote notify other proxy
func (s *TCPBusServer) NotifyRemote(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error {
	log.GetLogger().Infof("notify remote, ip: %s, instance: %s, worker state %t",
		remoteIP, dst.InstanceID, event.Enable)
	client, err := s.getBusClient(remoteIP)
	if err != nil {
		log.GetLogger().Errorf("failed to get proxy client %s#%s: %s", remoteIP, dst.InstanceID, err)
		return err
	}
	return busserver.NotifyRemoteProxy(client, dst, event)
}

// SendRequest send requests to tasks or workers by tcp bus
func (s *TCPBusServer) SendRequest(nodeID, traceID string, callReq *commonTypes.TCPRequest,
	dst *common.FunctionProperty) *commonTypes.TCPResponse {
	client, err := s.getBusClient(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get proxy tcpClient %s#%s: %s, traceID %s",
			nodeID, dst.InstanceID, err, traceID)
		proxyview.UpdateProxyAvailableStatus(nodeID, false)
		return &commonTypes.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
			RequestID:    callReq.RequestID,
		}
	}
	return s.baseBusServer.SendRequest(client, nodeID, traceID, callReq, dst)
}

// SendCancelRequest forward cancel request to other proxy
func (s *TCPBusServer) SendCancelRequest(nodeID, traceID string, req *commonTypes.TCPRequest,
	dst *common.CancelProperty) *commonTypes.TCPResponse {
	client, err := s.getBusClient(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get proxy client %s: %s, traceID %s",
			nodeID, err, traceID)
		return &commonTypes.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
			RequestID:    req.RequestID,
		}
	}
	return s.baseBusServer.SendCancelRequest(client, nodeID, traceID, req, dst)
}

// SendRecoverRequest -
func (s *TCPBusServer) SendRecoverRequest(nodeID, traceID string, req *commonTypes.TCPRequest,
) *commonTypes.TCPResponse {
	client, err := s.getBusClient(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get proxy client %s: %s, traceID %s",
			nodeID, err.Error(), traceID)
		return &commonTypes.TCPResponse{
			RequestID:    req.RequestID,
			ErrorMessage: proxyerrors.SendReqErr,
			ErrorCode:    proxyerrors.SendReqErrCode,
		}
	}
	return s.baseBusServer.SendRecoverRequest(client, nodeID, traceID, req)
}

// HandleRequest handle request from clientIP
func (s *TCPBusServer) HandleRequest(payload []byte, clientIP string, client *tcpbus.Connection) types.Result {
	log.GetLogger().Debugf("receive request message from %s", clientIP)
	msg := &pb.Message{}
	if err := proto.Unmarshal(payload, msg); err != nil {
		log.GetLogger().Errorf("failed to unmarshal message: %s", err.Error())
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  proxyerrors.InternalErr,
		}
	}
	switch msg.Type {
	case pb.Message_CALL_REQUEST:
		resp := s.handleReqMsg(msg.CallMessage)
		return resp
	case pb.Message_POSIX_CALL_REQUEST:
		resp := s.handlePosixReqMsg(msg.PosixCallRequest)
		return resp
	case pb.Message_CANCEL_REQUEST:
		return s.handleCancelMsg(msg.CancelMessage)
	case pb.Message_RECOVER_REQUEST:
		return s.handleRecoverMsg(msg.RecoverMessage)
	default:
		log.GetLogger().Errorf("unknown message type: %d", msg.Type)
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  "unknown message type",
		}
	}
}

func (s *TCPBusServer) handleRecoverMsg(msg *pb.Message_RecoverMessage) types.Result {
	log.GetLogger().Infof("handle recover msg for worker: %s, traceID: %s", msg.WorkerID, msg.TraceID)
	flag := common.WorkerMap.CheckVal(msg.WorkerID)
	return internal.BuildRecoverResult(workerbroker.GetInstance().Recover(msg.WorkerID, 0, msg.TraceID, flag))
}

func (s *TCPBusServer) handleCancelMsg(msg *pb.Message_CancelMessage) types.Result {
	log.GetLogger().Infof("handle cancel msg for worker: %s, futureID: %s, traceID: %s", msg.WorkerID,
		msg.FutureID, msg.TraceID)
	dst := &common.CancelProperty{
		FutureID:  msg.FutureID,
		Recursive: msg.Recursive,
		Force:     msg.Force,
	}
	return internal.BuildCancelResult(workerbroker.GetInstance().Cancel(msg.WorkerID, dst, 0, msg.TraceID))
}

func (s *TCPBusServer) handleReqMsg(msg *pb.Message_CallMessage) types.Result {
	log.GetLogger().Debugf("start to handle request, tenantID %s funcName %s insID %s, stateID %s,"+
		" traceID %s", urnutils.Anonymize(msg.Dst.TenantID), msg.Dst.FunctionName, msg.Dst.InstanceID,
		msg.Dst.StateID, msg.TraceID)
	busCallReq, dst, err := internal.BuildCallRequest(msg)
	if err != nil {
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  proxyerrors.InternalErr,
		}
	}
	resp := s.requestHandler(msg.TraceID, busCallReq, dst)
	log.GetLogger().Debugf("request got errCode %d traceID %s", resp.ErrorCode, msg.TraceID)
	log.GetLogger().Infof("%s | %s | %s | workerCost %.3f | invokeEndBus",
		urnutils.ExtractFuncName(dst.FunctionName), urnutils.Anonymize(dst.TenantID), msg.TraceID,
		utils.ConvertSecondToMillisecond(resp.ActualCallCost.Seconds()))
	return internal.BuildReqResult(resp)
}

func (s *TCPBusServer) handlePosixReqMsg(msg *pb.PosixCallRequest) types.Result {
	log.GetLogger().Debugf("start to handle request, tenantID %s funcName %s insID %s, stateID %s,"+
		" traceID %s", urnutils.Anonymize(msg.TenantID), msg.Function, msg.InstanceID,
		msg.StateID, msg.TraceID)
	busCallReq, dst, err := internal.BuildPosixCallRequest(msg)
	if err != nil {
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  proxyerrors.InternalErr,
		}
	}
	resp := s.requestHandler(msg.TraceID, busCallReq, dst)
	log.GetLogger().Debugf("request got errCode %d traceID %s", resp.ErrorCode, msg.TraceID)
	log.GetLogger().Infof("%s | %s | %s | workerCost %.3f | invokeEndBus",
		urnutils.ExtractFuncName(dst.FunctionName), urnutils.Anonymize(dst.TenantID), msg.TraceID,
		utils.ConvertSecondToMillisecond(resp.ActualCallCost.Seconds()))
	return internal.BuildPosixReqResult(resp)
}

// RegisterService register client to server
func (s *TCPBusServer) RegisterService(client *tcpbus.Connection) error {
	return nil
}

func (s *TCPBusServer) getBusClient(nodeIP string) (bus.CommunicationBus, error) {
	s.baseBusServer.BusClients.Mux.Lock()
	defer s.baseBusServer.BusClients.Mux.Unlock()
	client, exit := s.baseBusServer.BusClients.ProxyClients[nodeIP]
	if exit && client != nil {
		return client, nil
	}
	opt := tcpbus.ClientOption{
		ServerAddr: nodeIP + ":" + constants.TCPort,
	}
	busClient, err := s.baseBusServer.CreateProxyClient(nodeIP, opt)
	if err != nil {
		return nil, err
	}
	return busClient, nil
}
