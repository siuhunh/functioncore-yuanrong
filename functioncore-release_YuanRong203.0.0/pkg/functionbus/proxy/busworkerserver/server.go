/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package busworkerserver is used for routing messages between nodes in the cluster
package busworkerserver

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

// BusServerConfig bus server config struct
type BusServerConfig struct {
	workerConf  *common.BusServerConfig
	runtimeConf *common.BusServerConfig
}

// BusConnectionServer bus worker server struct
type BusConnectionServer struct {
	conf           BusServerConfig
	requestHandler internal.HandleReq
}

// InitServer initializes a new server connected to busProxy
func InitServer(requestHandler internal.HandleReq) (*BusConnectionServer, error) {
	server := &BusConnectionServer{
		conf: BusServerConfig{
			workerConf: common.FConfig.BusWorkerServerConfig,
		},
	}
	serverOpt := tcpbus.ServerOption{
		Receiver:   server,
		ListenAddr: internal.SelfPodIP + ":" + constants.BusWorkerServerTCPort,
	}
	server.requestHandler = requestHandler

	if err := startBusServer(server.conf.workerConf, serverOpt); err != nil {
		log.GetLogger().Errorf("failed to start bus server, error: %s", err.Error())
		return nil, err
	}
	return server, nil
}

// InitServerWithRuntime init server for runtime direct connect
func InitServerWithRuntime(requestHandler internal.HandleReq) (*BusConnectionServer, error) {
	server := &BusConnectionServer{
		conf: BusServerConfig{
			runtimeConf: common.FConfig.BusRuntimeServerConfig,
		},
	}
	serverOpt := tcpbus.ServerOption{
		Receiver:         server,
		ListenAddr:       internal.SelfPodIP + ":" + constants.BusRuntimeServerPort,
		DisableHeartbeat: true,
	}
	server.requestHandler = requestHandler
	serverConf := server.conf.runtimeConf
	if err := startBusServer(serverConf, serverOpt); err != nil {
		log.GetLogger().Errorf("failed to start bus server, error: %s", err.Error())
		return nil, err
	}
	return server, nil
}

func startBusServer(conf *common.BusServerConfig, opt tcpbus.ServerOption) error {
	if conf == nil {
		log.GetLogger().Errorf("failed to get bus server config")
		return errors.New("bus server config empty")
	}
	if conf.Protocol == "" {
		conf.Protocol = "tcp"
	}
	if conf.Protocol != "tcp" {
		return fmt.Errorf("unsupported protocol: to be implemented")
	}

	if conf.EnableTLS {
		if err := tcpbus.StartServerWithTLS(opt); err != nil {
			log.GetLogger().Errorf("failed to start tls socket server: %s", err.Error())
			return err
		}
	} else {
		if err := tcpbus.StartServer(opt); err != nil {
			log.GetLogger().Errorf("failed to start socket server: %s", err.Error())
			return err
		}
	}
	return nil
}

// HandleRequest bus handle request
func (b *BusConnectionServer) HandleRequest(payload []byte, clientIP string, client *tcpbus.Connection) types.Result {
	log.GetLogger().Debugf("receive bus worker message from %s", clientIP)
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
		return b.handleRequest(msg.CallMessage)
	case pb.Message_POSIX_CALL_REQUEST:
		return b.handlePosixRequest(msg.PosixCallRequest)
	case pb.Message_RUNTIME_REPORT:
		return b.handleRuntimeReport(msg.RuntimeReport, client)
	default:
		log.GetLogger().Errorf("unknown message type: %d", msg.Type)
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  "unknown message type",
		}
	}
}

func (b *BusConnectionServer) handleRuntimeReport(msg *pb.Message_RuntimeReport,
	client *tcpbus.Connection) types.Result {
	log.GetLogger().Infof("handle runtime report msg for worker: %s, stage: %s", msg.PodName, msg.Stage)
	if client == nil {
		return types.Result{}
	}
	tcpbus.DefaultPodConnCache().SetOrUpdateConnReady(msg.PodName, tcpbus.RuntimeReport{
		RequestMap: msg.AsyncRequests,
		Stage:      msg.Stage,
		Conn:       client,
	})
	return types.Result{}
}

func (b *BusConnectionServer) handleRequest(msg *pb.Message_CallMessage) types.Result {
	traceID := msg.TraceID
	dst := utils.FromFunctionPropertyPb(msg.Dst)
	req := utils.FromCallRequestPb(msg.Req)
	req.StateID = msg.Dst.StateID
	resp := b.requestHandler(traceID, req, dst)
	resp.RequestID = req.RequestID
	log.GetLogger().Debugf("request got errCode %d traceID %s requestID: %d", resp.ErrorCode,
		traceID, resp.RequestID)
	return internal.BuildReqResult(resp)
}

func (b *BusConnectionServer) handlePosixRequest(msg *pb.PosixCallRequest) types.Result {
	traceID := msg.TraceID
	dst := utils.GetFuncPropertyFromPosixPb(msg)
	req := utils.FromPosixCallRequestPb(msg)
	req.StateID = msg.StateID
	resp := b.requestHandler(traceID, req, dst)
	resp.RequestID = req.RequestID
	log.GetLogger().Debugf("request got errCode %d traceID %s requestID: %d", resp.ErrorCode,
		traceID, resp.RequestID)
	return internal.BuildPosixReqResult(resp)
}

// RegisterService bus register service
func (b *BusConnectionServer) RegisterService(client *tcpbus.Connection) error {
	return nil
}
