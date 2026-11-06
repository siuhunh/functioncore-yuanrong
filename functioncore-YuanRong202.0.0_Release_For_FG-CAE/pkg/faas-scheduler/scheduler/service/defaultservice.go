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

// Package service contains scheduler domain service
package service

import (
	"errors"
	"fmt"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/scheduler/funcinsmanager"
	"functioncore/pkg/faas-scheduler/scheduler/funcmetamanager"
	"functioncore/pkg/faas-scheduler/scheduler/insscheduler"
	"functioncore/pkg/faas-scheduler/scheduler/schedulermanager"
	"functioncore/pkg/faas-scheduler/types"
	"functioncore/pkg/faas-scheduler/utils"
)

const (
	// timeout for waiting time of user request
	defaultScalingOutReqTimeout = 15 * time.Minute
)

func createDefaultService(fetcher common.FuncMetaFetcher) (SchedulerService, error) {
	funcMetaManager := funcmetamanager.CreateManager(fetcher)
	funcInsManager := funcinsmanager.CreateManager()
	schedulerManager := schedulermanager.CreateManager()
	service := &defaultService{
		funcMetaManager:  funcMetaManager,
		funcInsManager:   funcInsManager,
		schedulerManager: schedulerManager,
	}
	return service, nil
}

type defaultService struct {
	funcMetaManager  *funcmetamanager.Manager
	funcInsManager   *funcinsmanager.Manager
	schedulerManager *schedulermanager.Manager
}

// AcquireInstance -
func (service *defaultService) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	log.GetLogger().Infof("funcKeyWithRes: %s | requestType: %d | start to acquire instance",
		req.FuncKeyWithRes, req.ReqType)
	scheduler, err := service.getOrCreateScheduler(req.FuncKeyWithRes)
	if err != nil {
		log.GetLogger().Errorf("failed to AcquireInstance funcKeyWithRes %s traceID %s err %s",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, err.Error())
		return &schedulerapi.AcquireInstanceRsp{Error: snerror.NoSchedulerError}, nil
	}
	rsp, err := scheduler.AcquireInstance(req)
	if err != nil {
		log.GetLogger().Errorf("failed to AcquireInstance funcKeyWithRes %s traceID %s err %s",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, err.Error())
		return &schedulerapi.AcquireInstanceRsp{Error: snerror.SchedulerInternalError}, nil
	}
	if req.ReqType > schedulerapi.NormalRequest {
		if rsp != nil && rsp.Error.ErrorCode == snerror.RemoteDebugInsNotReadyErrorCode {
			log.GetLogger().Warnf("%s | %s | remote debug instance is not ready, please try again later",
				req.FuncKeyWithRes, req.TraceID)
			return rsp, nil
		}
		scalingOutTimeout, err := time.ParseDuration(req.Timeout)
		if err != nil {
			log.GetLogger().Errorf("%s | failed to parse timeout string: %s, use default timeout setting",
				req.TraceID, err.Error())
			scalingOutTimeout = defaultScalingOutReqTimeout
		}
		service.acquireWebSocketInstance(req, rsp, scalingOutTimeout)
	}
	log.GetLogger().Infof("%s | finished to process acquiring function instance request", req.TraceID)
	return rsp, nil
}

func (service *defaultService) getOrCreateScheduler(funcKeyWithRes string) (*insscheduler.Scheduler, error) {
	scheduler := service.schedulerManager.GetScheduler(funcKeyWithRes)
	if scheduler != nil {
		return scheduler, nil
	}

	functionKey := urnutils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithRes)
	funcMeta, err := service.funcMetaManager.GetFuncMeta(functionKey)
	if err != nil {
		log.GetLogger().Warnf("failed to find funcMeta of funcKeyWithRes: %s, error: %s",
			urnutils.AnonymizeTenantKey(funcKeyWithRes), err.Error())
		return nil, errors.New("failed to find funcMeta")
	}
	addObserver := service.funcInsManager.AddObserver
	return service.schedulerManager.CreateScheduler(funcKeyWithRes, funcMeta, addObserver, true)
}

// ReleaseInstance -
func (service *defaultService) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	processorKey := urnutils.GetProcessorKey(req.InstanceKey, req.ProcessorID, req.LeaseVersion)
	scheduler, err := service.getOrCreateScheduler(req.FuncKeyWithRes)
	if scheduler == nil {
		return fmt.Errorf("failed to ReleaseInstance funcKeyWithRes %s"+
			" traceID %s processorKey %s no scheduler",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, processorKey)
	}
	if err != nil {
		return fmt.Errorf("failed to ReleaseInstance funcKeyWithRes %s traceID %s err %s",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, err.Error())
	}
	err = scheduler.ReleaseInstance(req)
	if err != nil {
		return fmt.Errorf("failed to ReleaseInstance funcKeyWithRes %s traceID %s processorKey %s err %s",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, processorKey, err.Error())
	}

	log.GetLogger().Infof("releaseIns | %s | %s | %s | processorID %d | leaseVer %d",
		urnutils.FuncKeyWithResToShort(req.FuncKeyWithRes), req.TraceID, urnutils.ExtractPodName(req.InstanceKey),
		req.ProcessorID, req.LeaseVersion)
	return nil
}

// RetainInstance -
func (service *defaultService) RetainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	processorKey := urnutils.GetProcessorKey(req.InstanceKey, req.ProcessorID, req.LeaseVersion)

	scheduler := service.schedulerManager.GetScheduler(req.FuncKeyWithRes)
	if scheduler == nil {
		log.GetLogger().Errorf("failed to RetainInstance funcKeyWithRes %s"+
			" traceID %s processorKey %s no scheduler",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, processorKey)
		return &schedulerapi.RetainInstanceRsp{Error: snerror.NoSchedulerError}, nil
	}

	rsp, err := scheduler.RetainInstance(req)
	if err != nil {
		log.GetLogger().Errorf("failed to AcquireInstance funcKeyWithRes %s traceID %s processorKey %s err %s",
			urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), req.TraceID, processorKey, err.Error())
		return &schedulerapi.RetainInstanceRsp{Error: snerror.SchedulerInternalError}, nil
	}

	log.GetLogger().Debugf("RetainInstance end funcKeyWithRes %s traceID %s "+
		"processorKey %s errorCode %d errorMsg %s", urnutils.AnonymizeTenantKey(req.FuncKeyWithRes),
		req.TraceID, processorKey, rsp.Error.ErrorCode, rsp.Error.ErrorMessage)
	return rsp, nil
}

// UpdateFuncMeta -
func (service *defaultService) UpdateFuncMeta(funcKey string, info *types.FunctionMetaInfo) {
	if !service.funcMetaManager.UpdateFuncMeta(funcKey, info) {
		return
	}
	service.schedulerManager.UpdateFuncMeta(funcKey, info, service.funcInsManager.AddObserver)
}

// DeleteFuncMeta -
func (service *defaultService) DeleteFuncMeta(funcKey string, source etcd3.EtcdType) {
	if !service.funcMetaManager.DeleteFuncMeta(funcKey, source) {
		return
	}
	service.schedulerManager.DeleteScheduler(funcKey, service.funcInsManager.DeleteObserver)
}

// AddInstance -
func (service *defaultService) AddInstance(funcKeyWithRes string, info *types.WorkerInstance) {
	service.funcInsManager.AddInstance(funcKeyWithRes, info)
}

// DeleteInstance -
func (service *defaultService) DeleteInstance(funcKeyWithRes string, info *types.WorkerInstance) {
	service.funcInsManager.DeleteInstance(funcKeyWithRes, info)
	service.schedulerManager.GcScheduler(funcKeyWithRes, service.funcInsManager.DeleteObserver)
}

func (service *defaultService) acquireWebSocketInstance(
	req *schedulerapi.AcquireInstanceReq, rsp *schedulerapi.AcquireInstanceRsp, scalingOutTimeout time.Duration) {
	// used to stop the process of suiciding countdown for placeholder instance
	var stopChan chan struct{}
	if req.ReqType > schedulerapi.NormalRequest && rsp.Error.ErrorCode == snerror.NoIdleInstanceErrorCode {
		stopChan = make(chan struct{}, 1)
		log.GetLogger().Infof("%s | no idle instance left and scale out new instances", req.TraceID)
		funcScheduler, err := service.getOrCreateScheduler(req.FuncKeyWithRes)
		if err != nil {
			log.GetLogger().Errorf("%s | failed to get function scheduler, err: %s", req.TraceID, err.Error())
			rsp.Error = snerror.NoSchedulerError
			return
		}
		req.WebSocketInsAcquireConfig.Concurrency = uint32(funcScheduler.GetConcurrency())
		instancePlaceHolderKey := funcScheduler.AddPlaceHolderInstance(
			&common.Instance{InstanceKey: common.InstancePlaceHolderPrefix}, rsp, stopChan)
		// send scaling out request to worker manager
		err = utils.ScaleOutInstance(req)
		if err != nil {
			service.handleInternalWorkerManagerError(err, rsp, req.FuncKeyWithRes, instancePlaceHolderKey)
			return
		}
	}
	// request will wait for worker manager scaling out or time out
	if rsp.Error.ErrorCode == snerror.GetPlaceHolderInstanceError {
		log.GetLogger().Infof("%s | get placeholder instance and wait for new instance added event", req.TraceID)
		select {
		case websocketIns := <-rsp.PlaceHolderChan:
			service.processScalingOutInternal(req, rsp, websocketIns, stopChan)
		case <-time.After(scalingOutTimeout):
			service.processScalingOutTimeoutInternal(req, rsp)
			return
		}
	}
	if req.ReqType == schedulerapi.WebSocketRequest {
		service.sendWebSocketBuildingRequestToWorker(req, rsp, scalingOutTimeout)
	}
	log.GetLogger().Infof("traceID: %s | nodeIP: %s | nodePort: %s | instanceKey: %s | instanceIP: %s | "+
		"processorID: %d | finished to acquire websocket instance", req.TraceID, rsp.NodeIP, rsp.NodePort,
		rsp.InstanceKey, rsp.InstanceIP, rsp.ProcessorID)
}

func (service *defaultService) handleInternalWorkerManagerError(
	err error, rsp *schedulerapi.AcquireInstanceRsp, funcKeyWithRes, insKey string) {
	if snErr, ok := err.(snerror.SNError); ok {
		rsp.Error = schedulerapi.Error{}
		errCode := snErr.Code()
		if snErr.Code() == snerror.InnerRuntimeInitTimeoutCode {
			errCode = snerror.RuntimeInitTimeoutCode
		}
		rsp.Error.ErrorCode = uint32(errCode)
		rsp.Error.ErrorMessage = snErr.Error()
	} else {
		rsp.Error = snerror.ScalingOutFunctionInstanceError
	}
	funcScheduler, err := service.getOrCreateScheduler(funcKeyWithRes)
	if err != nil {
		log.GetLogger().Errorf("failed to get function scheduler, err: %s", err.Error())
		rsp.Error = snerror.NoSchedulerError
		return
	}
	funcScheduler.ReleasePlaceHolderInstance(insKey)
}

func (service *defaultService) processScalingOutInternal(req *schedulerapi.AcquireInstanceReq,
	rsp *schedulerapi.AcquireInstanceRsp, websocketIns *schedulerapi.WebsocketInsInfo, stopChan chan struct{}) {
	if websocketIns.Error.ErrorCode != 0 {
		log.GetLogger().Errorf("traceID: %s | errorCode: %d | errorMsg: %s | failed to acquire websocket instance",
			req.TraceID, rsp.Error.ErrorCode, rsp.Error.ErrorMessage)
		req.WebSocketInsAcquireConfig.TargetIP = ""
		rsp.Error = websocketIns.Error
		return
	}
	// scaling out successfully then cancel suiciding goroutine
	if stopChan != nil {
		stopChan <- struct{}{}
	}
	rsp.InstanceIP = websocketIns.PodIP
	rsp.InstancePort = common.WorkerDefaultWebSocketPort
	rsp.InstanceKey = websocketIns.PodName
	rsp.ProcessorID = websocketIns.ProcessorID
	rsp.NodeIP = websocketIns.NodeIP
	rsp.NodePort = websocketIns.NodePort
	rsp.Error = schedulerapi.Error{}
}

func (service *defaultService) processScalingOutTimeoutInternal(
	req *schedulerapi.AcquireInstanceReq, rsp *schedulerapi.AcquireInstanceRsp) {
	funcScheduler, err := service.getOrCreateScheduler(req.FuncKeyWithRes)
	if err != nil {
		log.GetLogger().Errorf("%s | failed to get function scheduler, err: %s", req.TraceID, err.Error())
		rsp.Error = snerror.NoSchedulerError
		return
	}
	funcScheduler.ReleasePlaceHolderInstanceProcessor(rsp.InstanceKey, req.FuncKeyWithRes, rsp.ProcessorID)
	log.GetLogger().Errorf("traceID: %s | time out for waiting for scaling out function instance", req.TraceID)
	rsp.Error = snerror.ScalingOutFunctionInstanceError
}

func (service *defaultService) sendWebSocketBuildingRequestToWorker(
	req *schedulerapi.AcquireInstanceReq, rsp *schedulerapi.AcquireInstanceRsp, scalingOutTimeout time.Duration) {
	log.GetLogger().Infof("traceID: %s | instanceKey: %s | instanceIP: %s | processorID: %d | sent building "+
		"websocket connection request to worker", req.TraceID, rsp.InstanceKey, rsp.InstanceIP, rsp.ProcessorID)
	requestBody, err := utils.PrepareBuildWebSocketConRequestBody(req)
	if err != nil {
		log.GetLogger().Errorf("traceID: %s | instanceKey: %s | instanceIP: %s | failed to generate request "+
			"body", req.TraceID, rsp.InstanceKey, rsp.InstanceIP)
		service.handleSendWebSocketBuildingRequestToWorkerError(req, rsp, err)
		return
	}
	err = utils.SendBuildWebSocketConRequestToWorker(
		rsp.InstanceIP,
		common.WorkerServerPort,
		req.TraceID,
		requestBody,
		scalingOutTimeout,
	)
	if err != nil {
		service.handleSendWebSocketBuildingRequestToWorkerError(req, rsp, err)
		return
	}
	log.GetLogger().Infof("traceID: %s | instanceKey: %s | instanceIP: %s | succeed to build websocket "+
		"connection between worker and rdispatcher", req.TraceID, rsp.InstanceKey, rsp.InstanceIP)
}

func (service *defaultService) handleSendWebSocketBuildingRequestToWorkerError(
	req *schedulerapi.AcquireInstanceReq, rsp *schedulerapi.AcquireInstanceRsp, err error) {
	log.GetLogger().Errorf("traceID: %s | instanceKey: %s | instanceIP: %s | failed to build connection "+
		"between worker and rdispatcher", req.TraceID, rsp.InstanceKey, rsp.InstanceIP)
	schedulerErr := schedulerapi.Error{}
	if snErr, ok := err.(snerror.SNError); ok {
		schedulerErr.ErrorCode = uint32(snErr.Code())
		schedulerErr.ErrorMessage = snErr.Error()
	} else {
		schedulerErr = snerror.WebsocketConnectionBuildError
	}
	rsp.Error = schedulerErr
	scheduler := service.schedulerManager.GetScheduler(req.FuncKeyWithRes)
	if scheduler == nil {
		log.GetLogger().Errorf("traceID: %s | release processor error after building ws connection "+
			"failed when try to get scheduler", req.TraceID)
		return
	}
	releaseInstanceErr := scheduler.ReleaseInstance(&schedulerapi.ReleaseInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    rsp.InstanceKey,
		ProcessorID:    rsp.ProcessorID,
		IsWebSocketReq: true,
	})
	if releaseInstanceErr != nil {
		log.GetLogger().Errorf("traceID: %s | release processor error after building ws connection "+
			"failed with worker", req.TraceID)
		return
	}
}
