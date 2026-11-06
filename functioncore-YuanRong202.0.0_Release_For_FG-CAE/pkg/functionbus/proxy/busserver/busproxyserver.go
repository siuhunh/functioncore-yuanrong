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

// Package busserver is used for routing messages between nodes in the cluster
package busserver

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

// BusProxyServer interface provides call method
type BusProxyServer interface {
	StartServer() error
	NotifyRemote(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error
	SendRecoverRequest(nodeID, traceID string, req *commontypes.TCPRequest) *commontypes.TCPResponse
	SendCancelRequest(nodeID, traceID string, req *commontypes.TCPRequest,
		dst *common.CancelProperty) *commontypes.TCPResponse
	SendRequest(nodeID, traceID string, callReq *commontypes.TCPRequest,
		dst *common.FunctionProperty) *commontypes.TCPResponse
}

// BaseBusServer base bus server struct
type BaseBusServer struct {
	BusClients *ConnectionClient
}

// ConnectionClient recodes proxy clients
type ConnectionClient struct {
	ProxyClients map[string]bus.CommunicationBus // remote proxy connection clients
	Mux          sync.RWMutex
}

// Option option of creating tcp bus server
type Option struct {
	ListenAddr     string
	TLSEnable      bool
	RequestHandler internal.HandleReq
	ReportHandler  internal.HandleReport
}

// SendRequest send call request to busproxy
func (s *BaseBusServer) SendRequest(client bus.CommunicationBus, nodeID, traceID string,
	callReq *commontypes.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID)
	resp, err := client.Call(ctx, dst, callReq, true)
	if err != nil {
		log.GetLogger().Errorf("failed to call %s#%s: %s, traceID %s",
			nodeID, dst.InstanceID, err, traceID)
		return &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	return resp
}

// SendCancelRequest send cancel request to busproxy
func (s *BaseBusServer) SendCancelRequest(client bus.CommunicationBus, nodeID, traceID string,
	req *commontypes.TCPRequest, dst *common.CancelProperty) *commontypes.TCPResponse {
	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID)
	resp, err := client.Cancel(ctx, dst, req)
	if err != nil {
		log.GetLogger().Errorf("failed to cancel %s: %s, traceID %s, futureID: %s",
			nodeID, err, traceID, dst.FutureID)
		return &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	return resp
}

// SendRecoverRequest -
func (s *BaseBusServer) SendRecoverRequest(client bus.CommunicationBus, nodeID, traceID string,
	req *commontypes.TCPRequest) *commontypes.TCPResponse {
	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID)
	resp, err := client.Recover(ctx, req)
	if err != nil {
		log.GetLogger().Errorf("failed to recover %s: %s, traceID %s", nodeID, err, traceID)
		return &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	return resp
}

// NotifyRemoteProxy notify remote busproxy
func NotifyRemoteProxy(client bus.CommunicationBus, dst *common.FunctionProperty, event *common.NotifyEvent) error {
	data, err := getNotifyData(event)
	if err != nil {
		log.GetLogger().Errorf("failed to generate notify data: %s", err)
		return err
	}
	var flag bool
	if dst.StateID != "" || dst.InstanceID != "" {
		flag = true
	}
	req := &commontypes.TCPRequest{
		UserData:   data,
		IsStateFul: flag,
	}
	ctx, cancel := utils.ContextWithTimeout(context.Background(),
		common.DefaultNotifyRemoteTimeout, constants.HeaderTraceID, "")
	defer cancel()
	resp, err := client.Call(ctx, dst, req, true)
	if err != nil {
		log.GetLogger().Errorf("failed to notify remote, err: %s", err.Error())
		return err
	}
	if resp.ErrorCode != 0 {
		log.GetLogger().Errorf("failed to notify remote, code: %d", resp.ErrorCode)
		return fmt.Errorf("failed to notify remote, error code: %d, error message: %s",
			resp.ErrorCode, resp.ErrorMessage)
	}
	return nil
}

func getNotifyData(event *common.NotifyEvent) (map[string]string, error) {
	switch event.NotifyType {
	case common.ChangeState:
		workerInfoStr := utils.GenerateWorkerInfoStr(event.FuncKeyWithResource, event.WorkerAddress,
			event.WorkerOwnerIP, event.IsDirectFunc, event.HasInitializer)
		if event.Enable {
			return map[string]string{
				common.RemoteNotify: strconv.Itoa(event.NotifyType),
				common.RemoteEnable: workerInfoStr,
			}, nil
		}
		return map[string]string{
			common.RemoteNotify:  strconv.Itoa(event.NotifyType),
			common.RemoteDisable: workerInfoStr,
		}, nil
	default:
		return nil, fmt.Errorf("unsupport notify event %d", event.NotifyType)
	}
}

// CreateProxyClient create client that connect with other bus proxy
func (s *BaseBusServer) CreateProxyClient(nodeIP string, opt tcpbus.ClientOption) (bus.CommunicationBus, error) {
	if nodeIP == "" {
		return nil, fmt.Errorf("nodeIP is empty")
	}
	client, err := tcpbus.BuildClientConnection(opt)
	if err != nil {
		return nil, err
	}
	tcpBusOpt := &tcpbus.Option{
		Client:       client,
		CreateClient: false,
		Remote:       true,
	}
	tcpBus, err := tcpbus.NewTCPBus(common.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		return nil, err
	}
	s.BusClients.ProxyClients[nodeIP] = tcpBus
	go func(ip string) {
		select {
		case <-client.DisabledSignal:
			log.GetLogger().Infof("the remote proxy %s is disconnected", ip)
			s.BusClients.Mux.Lock()
			delete(s.BusClients.ProxyClients, ip)
			s.BusClients.Mux.Unlock()
			return
		}
	}(nodeIP)
	return tcpBus, nil
}
