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

// Package workerbroker contains communication with workers
package workerbroker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/panjf2000/ants/v2"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

// Manager -
type Manager struct {
	brokerTaskPool *ants.Pool
	workerBrokers  sync.Map
	eventSignal    sync.Map
}

const (
	brokerTaskPoolSize = 20000

	waitWorkerReadyWhenDuplicatedTimeout = 100 * time.Millisecond

	waitEventSignalTimeout = 3 * time.Second

	websocketFunctionInstanceRetryNumber      = 5
	websocketFunctionInstanceRetryMillisecond = 600
	defaultDeleteWorkerBrokerTimeout          = 5 * time.Second
	defaultUserDataMaxSize                    = 512
	expansionUserDataMaxSize                  = 10240
	defaultWorkerIDSplit                      = "-#-"
	defaultWorkerIDSplitLength                = 2
	defaultPodNameIndex                       = 1

	// retry to get worker broker cache for vpa requests
	brokerRetryCounter = 10
	brokerRetryUnit    = 100
)

var (
	once                  sync.Once
	instance              *Manager
	requestHandler        internal.HandleReq
	deleteFuncInsCallBack vpa.DeleteFuncInsHandler
)

// GetInstance get singleton of workerBrokerManager
func GetInstance() *Manager {
	once.Do(func() {
		instance = &Manager{
			brokerTaskPool: newBrokerTaskPool(),
		}
	})

	return instance
}

// BrokerConfig used to create worker broker
type BrokerConfig struct {
	IsMinInstance       bool
	CreateEventChan     bool
	StateID             string
	FuncKeyWithResource string
	WorkerID            string
	WorkerAddr          string
	RemoteIP            string
	IsDirectFunc        bool
	HasInitializer      bool
	BusinessType        string
	PodUID              string
	ContainerIDs        vpa.ContainerInfo
	Resource            vpa.PodResourceInfo
	ViewResource        vpa.ViewResourceInfo
}

// SendInstanceInfoToSLOFunc func type to send instance info to slo-agent
type SendInstanceInfoToSLOFunc func(info vpa.FuncInstanceInfo) error

var sendInstanceToSLOAgent SendInstanceInfoToSLOFunc

// SetSendInstanceInfoToSLOFunc set callback func to send instance info to slo-agent
func SetSendInstanceInfoToSLOFunc(f SendInstanceInfoToSLOFunc) {
	sendInstanceToSLOAgent = f
}

// RegisterRequestHandler register request handler for request from worker
func (m *Manager) RegisterRequestHandler(call internal.HandleReq) {
	requestHandler = call
}

// RegisterDeleteFuncInsHandler delete local worker call back to delete cache in SLO agent
func (m *Manager) RegisterDeleteFuncInsHandler(handler vpa.DeleteFuncInsHandler) {
	deleteFuncInsCallBack = handler
}

const workerIDPrefix = "defaultaz-#-"

func getPodName(workerID string) string {
	if strings.HasPrefix(workerID, workerIDPrefix) {
		return workerID[len(workerIDPrefix):]
	}
	return workerID
}

func (m *Manager) waitRuntimeConnection(workerID string, broker *workerBroker) error {
	err := broker.recoverRuntime(false)
	if err != nil {
		log.GetLogger().Warnf("failed to recover runtime %s, err: %s", workerID, err.Error())
		return err
	}
	podName := getPodName(workerID)
	notifier := tcpbus.DefaultPodConnCache().GetOrCreateConnWaiter(podName)
	if notifier == nil {
		log.GetLogger().Warnf("get conn waiter is nil of %s", workerID)
		return errors.New("failed to get conn waiter")
	}
	log.GetLogger().Infof("start to wait runtime connection, workerID %s %s", workerID, podName)
	ctx, cancel := context.WithTimeout(context.Background(), common.DefaultDialTimeout)
	defer cancel()

	var report tcpbus.RuntimeReport
	var ok bool

	select {
	case <-ctx.Done():
		log.GetLogger().Errorf("failed to wait ready conn of %s", podName)
		return errors.New("failed to wait ready conn")
	case report, ok = <-notifier:
		if !ok {
			log.GetLogger().Warnf("notifyier channel is closed of %s", podName)
		}
	}

	if report.Conn == nil {
		log.GetLogger().Errorf("wait ready conn is nil for %s", podName)
		return errors.New("connection is not exit")
	}
	log.GetLogger().Infof("succeed to wait runtime connection %s %s", workerID, podName)
	if !broker.isMinInstance {
		gpuMem, enableTmpExpansion, urn, err := GetSummaryFunctionData(broker.funcKeyWithResource)
		if err != nil {
			log.GetLogger().Errorf("failed to get concurrentNum, functionKey: %s, err: %s",
				broker.funcKeyWithResource, err.Error())
			return errors.New("failed to get function data")
		}
		runtimeMetrics := generateRuntimeMetrics(broker, gpuMem, urn, enableTmpExpansion)
		broker.summaryRecorder = newSummaryRecorder(broker, runtimeMetrics, broker.stopCh)
		report.Conn.RegisterBillingCallBack(broker.summaryRecorder.StartBilling, broker.summaryRecorder.StopBilling)
	}
	report.Conn.RegisterDirectAsyncRequestHandler(broker.ProcessAsyncResult)
	tcpBus := tcpbus.NewTCPBusAsServer(report.Conn, *broker.dstProperty)
	tcpBus.SetWorkerID(workerID)
	broker.runtimeConn.Store(tcpBus)
	broker.runtimeConnUpdate = notifier
	broker.isInitialized = report.Stage == common.RuntimeStageInitialized
	broker.RecoverAsyncRequest(report)
	return nil
}

// Process process requests to specific worker
func (m *Manager) Process(workerID string, request *types.DispatcherRequest) {
	if request == nil {
		log.GetLogger().Errorf("args error, request is nil, workerID:%s", workerID)
		return
	}
	brokerIf, ok := m.workerBrokers.Load(workerID)
	if request.Request != nil && request.Request.BusinessType == constants.BusinessTypeWebSocket {
		retryCounter := 0
		for !ok && retryCounter < websocketFunctionInstanceRetryNumber {
			time.Sleep(time.Millisecond * websocketFunctionInstanceRetryMillisecond)
			brokerIf, ok = m.workerBrokers.Load(workerID)
			retryCounter++
			if ok {
				log.GetLogger().Warnf("worker broker has been created successfully when request waiting in "+
					"bus, workerID: %s", workerID)
			}
		}
	}
	if !ok {
		log.GetLogger().Errorf("no broker in workerBrokers, workerID: %s", workerID)
		funcKeyWithResource := utils.GetFuncKeyWithResource(request.FunctionKey,
			types.InstanceResource{CPU: request.Request.CPU, Memory: request.Request.Memory})
		workerevaluator.GetInstance().DeleteWorkerMetrics(workerID, funcKeyWithResource)
		request.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
		return
	}
	broker, ok := brokerIf.(broker)
	if !ok {
		log.GetLogger().Errorf("failed to assert broker type, workerID: %s", workerID)
		m.workerBrokers.Delete(workerID)
		return
	}
	broker.process(request)
}

// GetWorkerBrokerCPUOverloaded -
func (m *Manager) GetWorkerBrokerCPUOverloaded(workerID string) bool {
	brokerIf, ok := m.workerBrokers.Load(workerID)
	if !ok {
		log.GetLogger().Errorf("not found workerID: %s, circuitBreakerEnabled is false", workerID)
		return false
	}
	broker, ok := brokerIf.(*workerBroker)
	if !ok || broker == nil {
		log.GetLogger().Errorf("failed to assert broker type recover error, workerID: %s", workerID)
		return false
	}
	return broker.getCPUOverloaded()
}

// ProcessRemote process requests to remote worker
func (m *Manager) ProcessRemote(node string, instanceID string, request *types.DispatcherRequest) {
	if request == nil || request.Request == nil {
		log.GetLogger().Warnf("invalid request, ignore")
		return
	}
	tenantID, funcName, funcVer := utils.ParseFuncKey(request.FunctionKey)
	dstProperty := &common.FunctionProperty{
		TenantID:        tenantID,
		FunctionName:    funcName,
		FunctionVersion: funcVer,
		InstanceID:      common.ProxyReqPrefix + instanceID,
	}
	go func() {
		if common.SendCallReq != nil {
			request.Response <- common.SendCallReq(node, request.TraceID, request.Request, dstProperty)
		}
	}()
}

// Cancel send CancelRequest to specific worker
func (m *Manager) Cancel(workerID string, dst *common.CancelProperty,
	requestID uint64, traceID string) *types.TCPResponse {
	brokerIf, ok := m.workerBrokers.Load(workerID)
	if !ok {
		log.GetLogger().Errorf("no broker in workerBrokers, workerID: %s", workerID)
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	broker, ok := brokerIf.(broker)
	if !ok {
		log.GetLogger().Errorf("failed to assert broker type, workerID: %s", workerID)
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}
	return broker.cancel(dst, requestID, traceID)
}

// Recover send RecoverRequest to specific worker
func (m *Manager) Recover(workerID string, requestID uint64, traceID string, flag bool) *types.TCPResponse {
	brokerIf, ok := m.workerBrokers.Load(workerID)
	if !ok {
		log.GetLogger().Errorf("no broker in workerBrokers recover error, workerID: %s", workerID)
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	broker, ok := brokerIf.(broker)
	if !ok {
		log.GetLogger().Errorf("failed to assert broker type recover error, workerID: %s", workerID)
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}
	return broker.recover(requestID, traceID, flag)
}

// CreateRemoteBroker create a remote broker which send request to remote worker
func (m *Manager) CreateRemoteBroker(funcKeyWithResource, workerID, nodeIP, stateID string) error {
	if funcKeyWithResource == "" || workerID == "" || nodeIP == "" {
		log.GetLogger().Errorf("check args failed")
		return errors.New("check args failed")
	}
	defer func() {
		m.addRemoteWorker(workerID, funcKeyWithResource, nodeIP, stateID)
	}()
	log.GetLogger().Infof("begin create remoteBroker podID is: %s", workerID)
	tenantID, functionName, functionVersion := utils.ParseFuncKey(funcKeyWithResource)

	dstProperty := &common.FunctionProperty{
		StateID:         stateID,
		TenantID:        tenantID,
		FunctionName:    functionName,
		FunctionVersion: functionVersion,
		InstanceID:      workerID,
	}
	broker := &remoteBroker{
		dstProperty:         dstProperty,
		funcKeyWithResource: funcKeyWithResource,
		workerID:            workerID,
		nodeIP:              nodeIP,
	}

	concurrentNum := GetConcurrentNum(broker.funcKeyWithResource)
	broker.isInitialConcurrent = concurrentNum > 1
	_, loaded := m.workerBrokers.LoadOrStore(workerID, broker)
	if loaded {
		log.GetLogger().Warnf("create duplicated remote broker for worker %s function %s", workerID,
			urnutils.AnonymizeTenantKey(funcKeyWithResource))
		return fmt.Errorf("create duplicated worker broker for worker" + workerID)
	}
	log.GetLogger().Infof("create remote broker success, worker:%s function:%s", workerID,
		urnutils.AnonymizeTenantKey(funcKeyWithResource))
	return nil
}

// DeleteRemoteBroker delete remote broker when remote worker quit
func (m *Manager) DeleteRemoteBroker(funcKeyWithResource, workerID string) {
	m.workerBrokers.Delete(workerID)
	workerevaluator.GetInstance().DeleteWorkerMetrics(workerID, funcKeyWithResource)
}

// CreateWorkerBroker create broker for registered worker and clear when connection crashed
// remoteIP is empty or self nodeIP if it is a local worker; else is remote worker
func (m *Manager) CreateWorkerBroker(config *BrokerConfig) error {
	log.GetLogger().Debugf("begin to create workerBroker: %s, ownerIP: %s, workerAddr: %s",
		config.WorkerID, config.RemoteIP, config.WorkerAddr)
	if config.CreateEventChan && config.WorkerID != "" {
		ch := make(chan struct{}, 1)
		if _, loaded := m.eventSignal.LoadOrStore(config.WorkerID, ch); !loaded {
			defer func() {
				ch <- struct{}{}
				m.eventSignal.Delete(config.WorkerID)
			}()
		}
	}
	newBroker := newWorkerBroker(config)
	broker, loaded := m.loadOrStoreWorkerBroker(newBroker)
	if loaded {
		if m.processDuplicatedBroker(config, broker) {
			return nil
		}
		log.GetLogger().Warnf("worker address changed from %s to %s, recreate worker broker: %s",
			broker.workerAddr, config.WorkerAddr, config.WorkerID)
		// delete old worker broker and create new one
		m.DeleteWorkerBroker(config.FuncKeyWithResource, config.WorkerID, broker.workerAddr, false)
		broker = newBroker
		m.workerBrokers.Store(config.WorkerID, broker)
	}
	broker.reportCache = &sync.Map{}
	broker.asyncDirectRequestMap = &sync.Map{}
	commBus, err := linkWithWorker(config.WorkerAddr, config.WorkerID, config.IsDirectFunc, broker.reportCache)
	if err != nil {
		log.GetLogger().Errorf("failed to create bus for worker %s function %s err: %s", config.WorkerID,
			urnutils.AnonymizeTenantKey(config.FuncKeyWithResource), err)
		m.workerBrokers.Delete(config.WorkerID)
		return err
	}
	commBus.RegisterCallback(broker.handleRequest)
	commBus.RegisterCancel(broker.handleCancel)
	commBus.RegisterReportRuntimeMetrics(broker.reportRuntimeMetrics)
	commBus.RegisterStoreResourceUsage(broker.storeResourceUsage)
	commBus.RegisterStoreCaeMetrics(broker.storeCaeMetrics)
	commBus.RegisterReportBridgeFuncMetrics(broker.storeBridgeMetrics)
	broker.bus = commBus
	var flag bool
	if config.StateID != "" {
		flag = true
	}
	if config.IsDirectFunc {
		concurrentNum := GetConcurrentNum(broker.funcKeyWithResource)
		broker.isConcurrent = concurrentNum > 1
	}
	n := m.recoverAndGetCurrentRequestNum(config.WorkerID, flag)
	if n == -1 {
		m.workerBrokers.Delete(config.WorkerID)
		return errors.New("failed to recover because failed to get current requests num")
	}
	if config.IsDirectFunc {
		if err := m.waitRuntimeConnection(config.WorkerID, broker); err != nil {
			log.GetLogger().Errorf("failed to wait runtime connection for %s, err: %s", err.Error(), config.WorkerID)
			m.workerBrokers.Delete(config.WorkerID)
			tcpBus, ok := broker.bus.(*tcpbus.TCPBus)
			if ok {
				tcpBus.SetResetFlag(false)
			}
			return err
		}
		broker.isDirectFunction = true
		broker.hasInitializer = config.HasInitializer
		if config.HasInitializer && !broker.isInitialized {
			go broker.doInitialize(uuid.New().String())
		}
	}
	tcpBus, ok := broker.bus.(*tcpbus.TCPBus)
	if ok {
		tcpBus.SetResetFlag(false)
	}

	close(broker.readyChan)

	if config.RemoteIP == "" || config.RemoteIP == internal.SelfNodeIP {
		m.addLocalWorker(config, n)
	}
	log.GetLogger().Infof("create worker broker success, worker:%s function:%s, and ownerIP is:%s",
		config.WorkerID, urnutils.AnonymizeTenantKey(config.FuncKeyWithResource), config.RemoteIP)
	go m.watchDisable(commBus, broker)
	go broker.updateRuntimeConn()
	go broker.backgroundPause()
	go broker.watchSyncRequestTimeout()
	return nil
}

func createVPAFuncInstanceInfo(broker *workerBroker, vpaInfo *verticalAutoscalingInfo) vpa.FuncInstanceInfo {
	return vpa.FuncInstanceInfo{
		FuncName:     utils.GetFuncKeyFromFuncKeyWithResource(broker.funcKeyWithResource),
		PodName:      broker.workerID,
		BusinessType: vpaInfo.businessType,
		PodUID:       vpaInfo.podUID,
		ContainerIDs: &vpa.ContainerInfo{
			Worker:  vpaInfo.workerContainerID,
			Runtime: vpaInfo.runtimeContainerID,
		},
		Resource: &vpa.ResourceConfig{
			CPULimit:      vpaInfo.runtimeCPULimit,
			CPURequest:    vpaInfo.runtimeCPURequest,
			MemoryLimit:   vpaInfo.runtimeMemoryLimit,
			MemoryRequest: vpaInfo.runtimeMemoryRequest,
		},
		RuntimeInitResource: &vpa.ResourceConfig{
			CPULimit:      vpaInfo.runtimeCPULimit,
			CPURequest:    vpaInfo.runtimeCPURequest,
			MemoryLimit:   vpaInfo.runtimeMemoryLimit,
			MemoryRequest: vpaInfo.runtimeMemoryRequest,
		},
		WorkerInitResource: &vpa.ResourceConfig{
			CPULimit:      vpaInfo.workerCPULimit,
			CPURequest:    vpaInfo.workerCPURequest,
			MemoryLimit:   vpaInfo.workerMemoryLimit,
			MemoryRequest: vpaInfo.workerMemoryRequest,
		},
		RuntimePoolResource: &vpa.ResourceConfig{
			CPULimit:      vpaInfo.runtimePoolCPULimit,
			CPURequest:    vpaInfo.runtimePoolCPURequest,
			MemoryLimit:   vpaInfo.runtimePoolMemoryLimit,
			MemoryRequest: vpaInfo.runtimePoolMemoryRequest,
		},
		WorkerPoolResource: &vpa.ResourceConfig{
			CPULimit:      vpaInfo.workerPoolCPULimit,
			CPURequest:    vpaInfo.workerPoolCPURequest,
			MemoryLimit:   vpaInfo.workerPoolMemoryLimit,
			MemoryRequest: vpaInfo.workerPoolMemoryRequest,
		},
	}
}

func registerReadyInsToSLO(broker *workerBroker) {
	if sendInstanceToSLOAgent == nil || common.FConfig == nil || common.FConfig.VPAConfig == nil ||
		common.FConfig.VPAConfig.Enable == 0 {
		return
	}
	vpaInfo := broker.getVerticalAutoscalingInfo()
	if vpaInfo == nil {
		return
	}
	info := createVPAFuncInstanceInfo(broker, vpaInfo)
	info.Action = vpa.FuncInsReady
	err := sendInstanceToSLOAgent(info)
	if err != nil {
		log.GetLogger().Warnf("failed to register ready instance %s, err %s", broker.workerID, err.Error())
		return
	}
	log.GetLogger().Infof("succeed to register ready instance %s", broker.workerID)
}

// SetVPAInfo cache VPA info for local function instance
func (m *Manager) SetVPAInfo(workerID string, req *vpa.FuncInstanceInfo) (int, string) {
	log.GetLogger().Debugf("set vpa info in worker broker cache, workerID: %s", workerID)
	var wb *workerBroker
	for i := 1; i <= brokerRetryCounter; i++ {
		if wb = m.getWorkerBroker(workerID); wb != nil {
			log.GetLogger().Debugf("succeed to set vpa info in worker broker cache, "+
				"workerID: %s, retry counter: %d", workerID, i)
			break
		}
		time.Sleep(time.Duration(i) * time.Millisecond * brokerRetryUnit)
	}
	if wb == nil {
		return proxyerrors.VPARegisterFunctionInstanceErrCode, "no function instance in bus local cache"
	}
	if req == nil || req.ContainerIDs == nil || req.RuntimeInitResource == nil || req.WorkerInitResource == nil ||
		req.WorkerPoolResource == nil || req.RuntimePoolResource == nil {
		return proxyerrors.VPARegisterFunctionInstanceErrCode, "invalid container ID or resource config in request body"
	}
	wb.verticalAutoscalingInfo.Store(&verticalAutoscalingInfo{
		businessType:             req.BusinessType,
		workerContainerID:        req.ContainerIDs.Worker,
		runtimeContainerID:       req.ContainerIDs.Runtime,
		podUID:                   req.PodUID,
		runtimeCPULimit:          req.RuntimeInitResource.CPULimit,
		runtimeCPURequest:        req.RuntimeInitResource.CPURequest,
		runtimeMemoryLimit:       req.RuntimeInitResource.MemoryLimit,
		runtimeMemoryRequest:     req.RuntimeInitResource.MemoryRequest,
		workerCPURequest:         req.WorkerInitResource.CPURequest,
		workerCPULimit:           req.WorkerInitResource.CPULimit,
		workerMemoryRequest:      req.WorkerInitResource.MemoryRequest,
		workerMemoryLimit:        req.WorkerInitResource.MemoryLimit,
		runtimePoolCPULimit:      req.RuntimePoolResource.CPULimit,
		runtimePoolCPURequest:    req.RuntimePoolResource.CPURequest,
		runtimePoolMemoryLimit:   req.RuntimePoolResource.MemoryLimit,
		runtimePoolMemoryRequest: req.RuntimePoolResource.MemoryRequest,
		workerPoolCPURequest:     req.WorkerPoolResource.CPURequest,
		workerPoolCPULimit:       req.WorkerPoolResource.CPULimit,
		workerPoolMemoryRequest:  req.WorkerPoolResource.MemoryRequest,
		workerPoolMemoryLimit:    req.WorkerPoolResource.MemoryLimit,
	})
	log.GetLogger().Debugf("set vpa info in worker broker cache, req.FuncName: %s, wb.funcKeyWithResource: %s",
		req.FuncName, wb.funcKeyWithResource)
	req.FuncName = wb.funcKeyWithResource
	return 0, ""
}

// GetAllInstanceVPAInfo traverse all local function instance VPA info for recovering SLO agent cache
func (m *Manager) GetAllInstanceVPAInfo(traceID string) *vpa.FuncInstanceInfoWrapper {
	funcInstanceInfos := make([]*vpa.FuncInstanceInfo, 0)
	now := time.Now()
	m.workerBrokers.Range(func(key, value interface{}) bool {
		_, ok := key.(string)
		if !ok {
			log.GetLogger().Warnf("workerBroker key type error, traceID: %s", traceID)
			return true
		}
		loadedBroker, ok := value.(*workerBroker)
		if !ok {
			log.GetLogger().Warnf("workerBroker type error, workerID: %s, traceID: %s", key, traceID)
			return true
		}
		vpaInfo := loadedBroker.getVerticalAutoscalingInfo()
		if vpaInfo == nil {
			log.GetLogger().Warnf("no vpa config found, workerID: %s, traceID: %s", loadedBroker.workerID, traceID)
			return true
		}
		instanceInfo := createVPAFuncInstanceInfo(loadedBroker, vpaInfo)
		funcInstanceInfos = append(funcInstanceInfos, &instanceInfo)
		return true
	})
	log.GetLogger().Infof("finished to collect vpa info, cost: %f, traceID: %s", time.Since(now).Seconds(), traceID)
	return &vpa.FuncInstanceInfoWrapper{FuncInstanceInfo: funcInstanceInfos}
}

func (m *Manager) processDuplicatedBroker(config *BrokerConfig, broker *workerBroker) bool {
	if broker.workerAddr == config.WorkerAddr {
		log.GetLogger().Warnf("duplicateCreateWorkerBroker | %s", urnutils.ExtractPodName(config.WorkerID))
		select {
		case <-broker.readyChan:
		case <-time.After(waitWorkerReadyWhenDuplicatedTimeout):
			log.GetLogger().Warnf("wait worker ready timeout when duplicated, workerID: %s", config.WorkerID)
		}
		return true
	}
	return false
}

func (m *Manager) loadOrStoreWorkerBroker(broker *workerBroker) (*workerBroker, bool) {
	brokerIf, loaded := m.workerBrokers.LoadOrStore(broker.workerID, broker)
	loadedBroker, ok := brokerIf.(*workerBroker)
	if !ok {
		log.GetLogger().Errorf("workerBroker type error, workerID: %s", broker.workerID)
		m.workerBrokers.Store(broker.workerID, broker)
		return broker, false
	}
	return loadedBroker, loaded
}

// WaitEventSignal wait for add event complete in order
func (m *Manager) WaitEventSignal(workerID string) {
	eventChan, ok := m.eventSignal.Load(workerID)
	if !ok {
		return
	}
	if ch, ok := eventChan.(chan struct{}); ok && ch != nil {
		select {
		case <-ch:
			log.GetLogger().Infof("wait for add event complete in order, workerID: %s", workerID)
		case <-time.After(waitEventSignalTimeout):
			log.GetLogger().Infof("wait for event signal timeout, workerID: %s", workerID)
		}
	}
}

// DeleteWorkerBroker delete broker when connection crashed or worker information updated in etcd
func (m *Manager) DeleteWorkerBroker(funcKeyWithResource, workerID, workerAddr string, isBrokenConnection bool) {
	tcpbus.DefaultPodConnCache().Delete(getPodName(workerID))
	brokerITF, ok := m.workerBrokers.LoadAndDelete(workerID)
	if !ok {
		log.GetLogger().Warnf("duplicated deletion of worker broker, workerID: %s", workerID)
		return
	}

	needClear := false
	if _, remoteOK := brokerITF.(*remoteBroker); remoteOK {
		needClear = true
	}

	localBroker, localOK := brokerITF.(*workerBroker)
	// force delete worker if workerAddr is empty or worker is as specified
	if localOK && (workerAddr == "" || localBroker.workerAddr == workerAddr) {
		needClear = true
		close(localBroker.stopCh)
	}

	if needClear {
		log.GetLogger().Debugf("delete worker broker: %s, isRemote: %t", workerID, !localOK)
		internal.UpdateOwnedInstances(workerID, isBrokenConnection)
		workerevaluator.GetInstance().DeleteWorkerMetrics(workerID, funcKeyWithResource)
	} else {
		log.GetLogger().Warnf("duplicated deletion of worker broker, workerID: %s", workerID)
	}

	if common.FConfig == nil || common.FConfig.VPAConfig == nil || common.FConfig.VPAConfig.Enable == 0 {
		return
	}
	// if enable VPA, notify slo agent
	m.deleteWorkerMonitorInSLOAgent(localBroker, workerID)
}

func (m *Manager) deleteWorkerMonitorInSLOAgent(wb *workerBroker, workerID string) {
	if wb == nil || wb.getVerticalAutoscalingInfo() == nil {
		log.GetLogger().Errorf("failed to get vpa info from worker broker cache, workerID: %s", workerID)
		return
	}
	vpaInfo := wb.getVerticalAutoscalingInfo()
	deleteReq := &vpa.FuncInstanceInfo{
		Action:       vpa.FuncInsDelete,
		FuncName:     wb.funcKeyWithResource,
		PodName:      wb.workerID,
		PodUID:       vpaInfo.podUID,
		BusinessType: vpaInfo.businessType,
		ContainerIDs: &vpa.ContainerInfo{
			Worker:  vpaInfo.workerContainerID,
			Runtime: vpaInfo.runtimeContainerID,
		},
	}
	payload, err := json.Marshal(deleteReq)
	if err != nil {
		log.GetLogger().Errorf("failed to serialize vpa request payload, workerID: %s, err: %s",
			wb.workerID, err.Error())
		return
	}
	if deleteFuncInsCallBack != nil {
		deleteFuncInsCallBack(payload, wb.workerID)
	}
}

func (m *Manager) getWorkerBroker(workerID string) *workerBroker {
	brokerIf, ok := m.workerBrokers.Load(workerID)
	if !ok {
		return nil
	}
	broker, ok := brokerIf.(*workerBroker)
	if !ok {
		log.GetLogger().Errorf("workerBroker type error, workerID: %s", workerID)
		m.workerBrokers.Delete(workerID)
		return nil
	}
	return broker
}

func (m *Manager) watchDisable(commBus bus.CommunicationBus, broker *workerBroker) {
	registerReadyInsToSLO(broker)
	commBus.WaitBusExit()
	log.GetLogger().Infof("worker broker exit, worker: %s function: %s", broker.workerID,
		urnutils.AnonymizeTenantKey(broker.funcKeyWithResource))
	if broker.ownerIP != "" && broker.ownerIP != internal.SelfNodeIP {
		common.NotifyRemote(broker.ownerIP, &common.FunctionProperty{
			TenantID:   common.BusProxyTenantID,
			InstanceID: broker.workerID,
		}, &common.NotifyEvent{
			NotifyType:          common.ChangeState,
			Enable:              false,
			FuncKeyWithResource: broker.funcKeyWithResource,
		})
	}
	m.DeleteWorkerBroker(broker.funcKeyWithResource, broker.workerID, broker.workerAddr, true)
	m.workerBrokers.Delete(broker.workerID)
	common.WorkerMap.DelVal(broker.workerID)
}

func (m *Manager) addLocalWorker(config *BrokerConfig, n int) {
	workerevaluator.GetInstance().AddWorker(config.WorkerID,
		workerevaluator.WithFunctionKeyWithResource(config.FuncKeyWithResource),
		workerevaluator.WithNodeIP(internal.SelfNodeIP),
		workerevaluator.WithLocalWorker(config.IsMinInstance),
		workerevaluator.WithRecoveredRequestNum(n),
		workerevaluator.WithStateID(config.StateID),
	)
}

func (m *Manager) addNonOwnedLocalWorker(workerID string) {
	_ = m.recoverAndGetCurrentRequestNum(workerID, false)
}

func (m *Manager) addRemoteWorker(workerID, funcKeyWithResource, nodeIP, stateID string) {
	if workerID == "" || funcKeyWithResource == "" {
		return
	}

	workerevaluator.GetInstance().AddWorker(workerID,
		workerevaluator.WithFunctionKeyWithResource(funcKeyWithResource),
		workerevaluator.WithNodeIP(nodeIP),
		workerevaluator.WithRemoteWorker(),
		workerevaluator.WithStateID(stateID),
	)
}

func (m *Manager) recoverAndGetCurrentRequestNum(workerID string, flag bool) int {
	result := m.Recover(workerID, 0, "", flag)
	if result.ErrorCode != 0 {
		return -1
	}
	if len(result.RawData) == 0 {
		log.GetLogger().Warnf("failed to get recover property, workerID: %s", workerID)
		return 0
	}
	var recoverProperty common.RecoverProperty
	if err := json.Unmarshal(result.RawData, &recoverProperty); err != nil {
		log.GetLogger().Warnf("failed to unmarshal recover property: workerID: %s, %s", workerID, err.Error())
		return 0
	}

	futureIDs := make([]string, 0, len(recoverProperty.Requests))
	for futureID := range recoverProperty.Requests {
		futureIDs = append(futureIDs, futureID)
	}
	cancel.GetInstance().RecoverRequests(workerID, futureIDs...)

	n := len(recoverProperty.Requests)
	log.GetLogger().Infof("workerID %s currentRequests num is %d, futureIDs: %v", workerID, n, futureIDs)
	return n
}

func newBrokerTaskPool() *ants.Pool {
	brokerTaskPool, err := ants.NewPool(brokerTaskPoolSize, ants.WithPreAlloc(true))
	if err != nil {
		log.GetLogger().Errorf("create fetcher task pool error: %s", err.Error())
		return nil
	}
	return brokerTaskPool
}

func getPodNameByWorkerID(workerID string) (podName string) {
	parts := strings.Split(workerID, defaultWorkerIDSplit)
	if len(parts) == defaultWorkerIDSplitLength {
		podName = parts[defaultPodNameIndex]
	}
	return
}

// DeleteAllWorkerBrokerWithGracefulExit -
func (m *Manager) DeleteAllWorkerBrokerWithGracefulExit() {
	deleteDone := make(chan struct{})
	go func() {
		defer m.workerBrokers.Range(func(key, value interface{}) bool {
			loadedBroker, ok := value.(*workerBroker)
			if !ok {
				log.GetLogger().Errorf("workerBroker type error, workerID: %s", loadedBroker.workerID)
				return true
			}
			m.DeleteWorkerBroker(loadedBroker.funcKeyWithResource, loadedBroker.workerID, loadedBroker.workerAddr, true)
			return true
		})
		close(deleteDone)
	}()
	select {
	case <-time.After(defaultDeleteWorkerBrokerTimeout):
	case <-deleteDone:
	}
	// wait worker broker report billing data
	time.Sleep(defaultDeleteWorkerBrokerTimeout)
}

func generateRuntimeMetrics(broker *workerBroker, gpuMem int64, urn string, enableTmpExpansion bool) (
	metrics common.RuntimeMetricsProperty) {
	metrics.GPUMem = gpuMem
	metrics.EphemeralStorage = defaultUserDataMaxSize
	if enableTmpExpansion {
		metrics.EphemeralStorage = expansionUserDataMaxSize
	}
	metrics.MetricsName = constants.MetricsUsedResourceTimeWindow
	metrics.PodName = getPodNameByWorkerID(broker.workerID)
	metrics.FunctionURN = urn
	return
}
