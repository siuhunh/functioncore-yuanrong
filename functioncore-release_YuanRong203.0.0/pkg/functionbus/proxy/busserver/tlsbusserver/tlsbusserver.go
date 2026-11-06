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

// Package tlsbusserver contains communication with busproxy by tls bus server
package tlsbusserver

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	commonTLS "functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
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

const (
	mapInitSize = 16
)

// TLSBusServer tls bus server struct
type TLSBusServer struct {
	config         *tls.Config
	baseBusServer  *busserver.BaseBusServer
	requestHandler internal.HandleReq
	listenAddr     string
}

// CreateTLSBusServer create tls bus server
func CreateTLSBusServer(opt *busserver.Option) (*TLSBusServer, error) {
	if opt == nil || opt.RequestHandler == nil {
		return nil, fmt.Errorf("request handle of tls bus is nil")
	}
	tlsConfig := commonTLS.GetClientTLSConfig()
	busClients := &busserver.ConnectionClient{
		ProxyClients: make(map[string]bus.CommunicationBus, mapInitSize),
		Mux:          sync.RWMutex{},
	}
	bus := &TLSBusServer{
		baseBusServer: &busserver.BaseBusServer{
			BusClients: busClients,
		},
		requestHandler: opt.RequestHandler,
		listenAddr:     opt.ListenAddr,
		config:         tlsConfig,
	}
	return bus, nil
}

// StartServer build a tls socket client connection
func (s *TLSBusServer) StartServer() error {
	serverNet, err := tls.Listen("tcp", s.listenAddr, s.config)
	if err != nil {
		log.GetLogger().Errorf("failed to create tls bus server: %s", err.Error())
		return err
	}
	go func(netListen net.Listener) {
		defer netListen.Close()
		for {
			conn, err := netListen.Accept()
			if err == nil {
				log.GetLogger().Debugf("accept a new connection %s --> %s ",
					conn.LocalAddr().String(), conn.RemoteAddr().String())
				opt := tcpbus.ServerOption{
					Receiver:   s,
					ListenAddr: s.listenAddr,
				}
				go tcpbus.ProcessNewConn(opt, conn)
				continue
			}
			log.GetLogger().Errorf("failed to accept connection: %s", err.Error())
			break
		}
	}(serverNet)
	log.GetLogger().Infof("server start to listen tls server addr %s", s.listenAddr)
	workerbroker.GetInstance().RegisterRequestHandler(s.requestHandler)
	workerbroker.GetInstance().RegisterDeleteFuncInsHandler(vpa.DeleteFuncInsInSLOAgentCallBack)
	return nil
}

// NotifyRemote notify other proxy
func (s *TLSBusServer) NotifyRemote(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error {
	log.GetLogger().Debugf("notify remote, ip: %s, instance: %s, worker state %t",
		remoteIP, dst.InstanceID, event.Enable)
	client, err := s.getBusClient(remoteIP)
	if err != nil {
		log.GetLogger().Errorf("failed to get proxy client %s#%s: %s", remoteIP, dst.InstanceID, err.Error())
		return err
	}
	return busserver.NotifyRemoteProxy(client, dst, event)
}

// SendRecoverRequest -
func (s *TLSBusServer) SendRecoverRequest(nodeID, traceID string, req *commontypes.TCPRequest,
) *commontypes.TCPResponse {
	client, err := s.getBusClient(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get tls proxy client %s: %s, traceID %s",
			nodeID, err.Error(), traceID)
		return &commontypes.TCPResponse{
			RequestID:    req.RequestID,
			ErrorMessage: proxyerrors.SendReqErr,
			ErrorCode:    proxyerrors.SendReqErrCode,
		}
	}
	return s.baseBusServer.SendRecoverRequest(client, nodeID, traceID, req)
}

// SendCancelRequest forward cancel request to other proxy
func (s *TLSBusServer) SendCancelRequest(nodeID, traceID string, req *commontypes.TCPRequest,
	dst *common.CancelProperty) *commontypes.TCPResponse {
	client, err := s.getBusClient(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get tls proxy client %s: %s, traceID %s",
			nodeID, err.Error(), traceID)
		return &commontypes.TCPResponse{
			RequestID:    req.RequestID,
			ErrorMessage: proxyerrors.SendReqErr,
			ErrorCode:    proxyerrors.SendReqErrCode,
		}
	}
	return s.baseBusServer.SendCancelRequest(client, nodeID, traceID, req, dst)
}

// SendRequest send requests to tasks or workers by tcp bus
func (s *TLSBusServer) SendRequest(nodeID, traceID string, callReq *commontypes.TCPRequest,
	dst *common.FunctionProperty) *commontypes.TCPResponse {
	client, err := s.getBusClient(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get tls proxy client %s#%s: %s, traceID %s",
			nodeID, dst.InstanceID, err, traceID)
		proxyview.UpdateProxyAvailableStatus(nodeID, false)
		return &commontypes.TCPResponse{
			RequestID:    callReq.RequestID,
			ErrorMessage: proxyerrors.SendReqErr,
			ErrorCode:    proxyerrors.SendReqErrCode,
		}
	}
	return s.baseBusServer.SendRequest(client, nodeID, traceID, callReq, dst)
}

// HandleRequest handle request from clientIP
func (s *TLSBusServer) HandleRequest(payload []byte, clientIP string, client *tcpbus.Connection) types.Result {
	log.GetLogger().Debugf("receive tls request message from %s", clientIP)
	msg := &pb.Message{}
	err := proto.Unmarshal(payload, msg)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal message: %s", err.Error())
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  proxyerrors.InternalErr,
		}
	}
	switch msg.Type {
	case pb.Message_CANCEL_REQUEST:
		return s.handleCancelMsg(msg.CancelMessage)
	case pb.Message_CALL_REQUEST:
		return s.handleReqMsg(msg.CallMessage)
	case pb.Message_POSIX_CALL_REQUEST:
		return s.handlePosixReqMsg(msg.PosixCallRequest)
	case pb.Message_RECOVER_REQUEST:
		return s.handleRecoverMsg(msg.RecoverMessage)
	default:
		log.GetLogger().Errorf("unknown message type: %d", msg.Type)
		return types.Result{
			ErrorMsg:  "unknown message type",
			ErrorCode: proxyerrors.InternalErrCode,
		}
	}
}

func (s *TLSBusServer) handleReqMsg(msg *pb.Message_CallMessage) types.Result {
	log.GetLogger().Debugf("start to handle request, tenantID %s funcName %s insID %s, stateID %s,"+
		" traceID %s", urnutils.Anonymize(msg.Dst.TenantID), msg.Dst.FunctionName, msg.Dst.InstanceID,
		msg.Dst.StateID, msg.TraceID)
	busCallReq, dst, err := internal.BuildCallRequest(msg)
	if err != nil {
		return types.Result{
			ErrorMsg:  proxyerrors.InternalErr,
			ErrorCode: proxyerrors.InternalErrCode,
		}
	}
	resp := s.requestHandler(msg.TraceID, busCallReq, dst)
	log.GetLogger().Debugf("request got errCode %d traceID %s", resp.ErrorCode, msg.TraceID)
	return internal.BuildReqResult(resp)
}

func (s *TLSBusServer) handlePosixReqMsg(msg *pb.PosixCallRequest) types.Result {
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

func (s *TLSBusServer) handleCancelMsg(msg *pb.Message_CancelMessage) types.Result {
	log.GetLogger().Infof("start to handle cancel msg for worker: %s, futureID: %s, traceID: %s", msg.WorkerID,
		msg.FutureID, msg.TraceID)
	dst := &common.CancelProperty{
		FutureID:  msg.FutureID,
		Recursive: msg.Recursive,
		Force:     msg.Force,
	}
	return internal.BuildCancelResult(workerbroker.GetInstance().Cancel(msg.WorkerID, dst, 0, msg.TraceID))
}

func (s *TLSBusServer) handleRecoverMsg(msg *pb.Message_RecoverMessage) types.Result {
	log.GetLogger().Infof("start to handle recover msg for worker: %s, traceID: %s", msg.WorkerID, msg.TraceID)
	flag := common.WorkerMap.CheckVal(msg.WorkerID)
	return internal.BuildRecoverResult(workerbroker.GetInstance().Recover(msg.WorkerID, 0, msg.TraceID, flag))
}

// RegisterService register client to server
func (s *TLSBusServer) RegisterService(client *tcpbus.Connection) error {
	return nil
}

func (s *TLSBusServer) getBusClient(nodeIP string) (bus.CommunicationBus, error) {
	s.baseBusServer.BusClients.Mux.Lock()
	defer s.baseBusServer.BusClients.Mux.Unlock()
	client, exit := s.baseBusServer.BusClients.ProxyClients[nodeIP]
	if exit && client != nil {
		return client, nil
	}
	opt := tcpbus.ClientOption{
		ServerAddr: nodeIP + ":" + constants.TCPort,
		TLSEnable:  true,
		Config:     commonTLS.GetClientTLSConfig(),
	}
	busClient, err := s.baseBusServer.CreateProxyClient(nodeIP, opt)
	if err != nil {
		return nil, err
	}
	return busClient, nil
}
