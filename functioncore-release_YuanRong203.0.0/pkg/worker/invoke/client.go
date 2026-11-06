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

package invoke

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/functionbus/bus/tcpbus"
	buscommon "functioncore/pkg/functionbus/common"
	busutils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/adaptor"
	"functioncore/pkg/functiontask/task/api/invoke"
	taskcommon "functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
)

const (
	functionTask = "function-task"
)

var (
	podName = os.Getenv(constants.PodName)
)

// CancelByBus send cancel request to bus
func (s *Server) CancelByBus(ctx context.Context, request *runtime.CancelRequest) *runtime.CancelResponse {
	log.GetLogger().Debugf("start to cancel by bus, id: %s, force: %t, rec: %t, traceID: %s",
		request.FutureID, request.Force, request.Recursive, request.TraceID)
	response := &runtime.CancelResponse{}
	target := &buscommon.CancelProperty{
		FutureID:  request.FutureID,
		Force:     request.Force,
		Recursive: request.Recursive,
	}
	ctx = busutils.ContextWithValueString(ctx, constants.BusWorkerServerTCPort, request.TraceID)
	busRsp, err := s.bus.Cancel(ctx, target, &types.TCPRequest{
		RequestID: utils.InvokeID2BusID(request.InvokeID),
	})
	if err != nil {
		log.GetLogger().Errorf("cancel by bus error: %s, traceID: %s", err.Error(), request.TraceID)
		response.Error = &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	} else if busRsp.ErrorCode != 0 {
		response.Error = &runtime.Error{
			Code:    int32(busRsp.ErrorCode),
			Message: busRsp.ErrorMessage,
		}
	}
	log.GetLogger().Debugf("cancel by bus over, traceID %s", request.TraceID)
	return response
}

func (s *Server) callByBus(traceCtx context.Context, request *runtime.CallRequest, response *runtime.CallResponse) {
	s.waitForInitBusClient()
	if s.bus == nil && status.Get() != status.Initializing {
		processBusNilError(response)
		return
	}
	target := getTargetFunctionProperty(request)

	ctx := busutils.ContextWithValueString(context.Background(), constants.HeaderTraceID, request.TraceID)
	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.Timeout)*time.Second)
	defer cancel()

	userData := map[string]string{
		constants.SNServiceIDFlag: request.ServiceID,
		constants.SrcTenantFlag:   utils.LocalFuncURN.TenantID,
		constants.SrcFuncUrnFlag:  utils.LocalFuncURN.String(),
		constants.SrcPodNameFlag:  podName,
	}
	log.GetLogger().Debugf("called the function by bus, callID %s, traceID %s",
		request.CallID, request.TraceID)
	req := getReq(traceCtx, request, userData)
	busRsp := &types.TCPResponse{}
	var err error
	if s.bus == nil && status.Get() == status.Initializing {
		busRsp, err = s.processInitCall(ctx, target, req, false)
		go s.clearConn()
	} else if s.bus == nil {
		processBusNilError(response)
		return
	} else {
		busRsp, err = s.bus.Call(ctx, target, req, false)
	}
	if err != nil {
		log.GetLogger().Errorf("called the function error: %s", err.Error())
		response.Error = &runtime.Error{
			Code:    snerror.CallFunctionErrorCode,
			Message: err.Error(),
		}
		return
	}
	if busRsp.ErrorMessage != "" {
		log.GetLogger().Errorf("called the function error: %d %s", busRsp.ErrorCode, busRsp.ErrorMessage)
		response.Error = &runtime.Error{
			Code:    int32(busRsp.ErrorCode),
			Message: busRsp.ErrorMessage,
		}
		return
	}

	response.Result = busRsp.RawData
	response.Logs = busRsp.Logs
	log.GetLogger().Debugf("called the function by bus over, callID %s, traceID %s",
		request.CallID, request.TraceID)
}

func processBusNilError(response *runtime.CallResponse) {
	log.GetLogger().Errorf("abstract bus is nil")
	response.Error = &runtime.Error{
		Code:    snerror.CallFunctionErrorCode,
		Message: snerror.ErrText(snerror.CallFunctionErrorCode),
	}
}

func (s *Server) waitForInitBusClient() {
	if s.bus == nil && status.Get() != status.Initializing {
		retryTimes := 3
		// for init function async call another function at first time
		s.initBusClientOnce.Do(func() {
			log.GetLogger().Warnf("abstract bus is nil and it will wait for %d seconds", retryTimes)
			for i := 0; i < retryTimes; i++ {
				time.Sleep(time.Second)
				if s.bus != nil {
					log.GetLogger().Infof("abstract bus has finished to be init")
					break
				}
			}
		})
	}
}

// process initialize stage call for TCPBus
func (s *Server) processInitCall(ctx context.Context, dst *buscommon.FunctionProperty,
	req *types.TCPRequest, isSendingRemoteProxy bool) (*types.TCPResponse, error) {
	traceID := busutils.ContextValueString(ctx, constants.HeaderTraceID)
	log.GetLogger().Infof("called the function by bus in initialize stage, traceID: %s", traceID)
	cache.GetMessageQueue().SetNoBlockType(true)
	if s.initBus != nil {
		return s.initBus.Call(ctx, dst, req, isSendingRemoteProxy)
	}
	configuration, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return nil, err
	}
	serverAddr := fmt.Sprintf("%s:%s", os.Getenv("HOST_IP"), constants.BusWorkerServerTCPort)
	opt := tcpbus.ClientOption{
		ServerAddr: serverAddr,
	}
	if configuration.HTTPSConfig.HTTPSEnable {
		opt.TLSEnable = true
		opt.Config = tls.GetClientTLSConfig()
	}
	client, err := tcpbus.BuildClientConnection(opt)
	if err != nil {
		log.GetLogger().Errorf("build client connection error: %s", err.Error())
		return nil, err
	}
	tcpBusOpt := &tcpbus.Option{CreateClient: false, Client: client}
	tcpBus, err := tcpbus.NewTCPBus(buscommon.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		log.GetLogger().Errorf("new tcp bus error: %s", err.Error())
		return nil, err
	}
	s.initBus = tcpBus
	return s.initBus.Call(ctx, dst, req, isSendingRemoteProxy)
}

func (s *Server) checkCanceled(futureID string, callRequest *adaptor.CallRequestPb,
	response *runtime.CallResponse) {
	canceled, err := runtime.RunMetrics.IsCanceled(futureID)
	if err != nil {
		log.GetLogger().Errorf("failed to check isCanceled: %s", err.Error())
		response.Error = &runtime.Error{
			Code:    snerror.CallFunctionErrorCode,
			Message: err.Error(),
		}
		return
	}
	if canceled {
		errMsg := fmt.Sprintf("request futureID: %s has been canceled", futureID)
		log.GetLogger().Errorf(errMsg)
		response.Error = &runtime.Error{
			Code:    snerror.CallFunctionErrorCode,
			Message: errMsg,
		}
		return
	}
}

func (s *Server) clearConn() {
	if s.initBus == nil {
		log.GetLogger().Infof("no init TCP connection to clear")
		return
	}
	for {
		select {
		case <-s.readyChan:
			// init bus is no need to report
			if err := s.initBus.WaitRequestsFinished(false, nil); err != nil {
				log.GetLogger().Errorf("failed to wait requests finished, %s", err.Error())
				return
			}
			s.initBus.Stop()
			cmdLine := fmt.Sprintf("iptables -D OUTPUT -p tcp -m tcp --dport " + constants.BusWorkerServerTCPort +
				" -j ACCEPT")
			cmd := exec.Command("/bin/bash", "-c", cmdLine)
			err := cmd.Run()
			if err != nil {
				log.GetLogger().Errorf("failed to drop port %s，err:%s", constants.BusWorkerServerTCPort, err.Error())
				return
			}
			log.GetLogger().Infof("succeed to clear init TCP connection")
			return
		}
	}
}

// ReportSummaryLogsToBus send summary and log to bus
func (s *Server) ReportSummaryLogsToBus(traceCtx context.Context, info *connecttypes.WorkerReportInfo) error {
	_, err := s.bus.ReportRuntimeInfo(traceCtx, info)
	if err != nil {
		log.GetLogger().Warnf("failed to report runtime logs %s, with err:%s",
			info.RuntimeLog, err.Error())
		return err
	}
	return nil
}

// ReportErrMsgToBus send error message to bus
func (s *Server) ReportErrMsgToBus(traceCtx context.Context, URNWithVersion, errMsg, level string,
	properties map[string]string) error {
	if s.bus != nil {
		_, err := s.bus.ReportErrMsgToBus(traceCtx, URNWithVersion, errMsg, level, properties)
		if err != nil {
			log.GetLogger().Warnf("failed to report runtime errMsg %s, with err:%s", errMsg, err.Error())
			return err
		}
	}
	return nil
}

// SetExit set bus exit state
func (s *Server) SetExit() {
	if s.bus != nil {
		s.bus.SetExit(1)
	}
}

// CallByBus call other function by bus
func (s *Server) CallByBus(traceCtx context.Context, request *runtime.CallRequest) *runtime.CallResponse {
	response := &runtime.CallResponse{}
	if request.FutureID == "" {
		log.GetLogger().Debugf("failed to get request's futureID")
		s.callByBus(traceCtx, request, response)
		return response
	}

	callRequest := &adaptor.CallRequestPb{}
	if err := proto.Unmarshal(request.Event, callRequest); err != nil {
		log.GetLogger().Errorf("failed to unmarshal call request: %s", err.Error())
		response.Error = &runtime.Error{
			Code:    snerror.CallFunctionErrorCode,
			Message: err.Error(),
		}
		return response
	}

	s.checkCanceled(request.FutureID, callRequest, response)
	if response.Error != nil {
		return response
	}

	s.callByBus(traceCtx, request, response)
	if response.Error != nil {
		return response
	}

	switch callRequest.Method {
	case taskcommon.MethodInvoke:
		var invokeResponse invoke.ResponsePb
		if err := proto.Unmarshal(response.Result, &invokeResponse); err != nil {
			log.GetLogger().Errorf("failed to unmarshal invoke response: %s", err.Error())
			response.Error = &runtime.Error{
				Code:    snerror.CallFunctionErrorCode,
				Message: err.Error(),
			}
			return response
		}

		log.GetLogger().Infof("recordFuture, from: %s, to: %s", request.FutureID, invokeResponse.FutureID)
		runtime.RunMetrics.AddTraceRecord(request.FutureID, invokeResponse.FutureID)
	case taskcommon.MethodGetFuture:
		getReq := callRequest.GetGetReq()
		runtime.RunMetrics.RemoveTraceRecord(request.FutureID, getReq.FutureID)
	default:
	}

	return response
}

func getReq(traceCtx context.Context, request *runtime.CallRequest, userData map[string]string) *types.TCPRequest {
	var flag bool
	if request.FuncName == functionTask {
		flag = true
	}
	return &types.TCPRequest{
		LogType:    request.LogType,
		RawData:    request.Event,
		RequestID:  utils.InvokeID2BusID(request.InvokeID),
		UserData:   userData,
		TraceCtx:   traceCtx,
		IsStateFul: flag,
	}
}

func getTargetFunctionProperty(request *runtime.CallRequest) *buscommon.FunctionProperty {
	return &buscommon.FunctionProperty{
		TenantID:        request.TenantID,
		FunctionName:    request.FuncName,
		FunctionVersion: request.Version,
	}
}
