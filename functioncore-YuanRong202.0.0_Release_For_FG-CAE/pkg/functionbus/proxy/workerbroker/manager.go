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

	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
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
)

var (
	once           sync.Once
	instance       *Manager
	requestHandler internal.HandleReq
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
}

// RegisterRequestHandler register request handler for request from worker
func (m *Manager) RegisterRequestHandler(call internal.HandleReq) {
	requestHandler = call
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

	tcpBus := tcpbus.NewTCPBusAsServer(report.Conn, *broker.dstProperty)
	tcpBus.SetWorkerID(workerID)
	broker.runtimeConn.Store(tcpBus)
	broker.runtimeConnUpdate = notifier
	broker.isInitialized = report.Stage == common.RuntimeStageInitialized
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
	commBus, err := linkWithWorker(config.WorkerAddr, config.WorkerID, broker.reportCache)
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
	broker.bus = commBus
	var flag bool
	if config.StateID != "" {
		flag = true
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
			return err
		}
		broker.isDirectFunction = true
		broker.hasInitializer = config.HasInitializer
		if config.HasInitializer && !broker.isInitialized {
			go broker.doInitialize(uuid.New().String())
		}
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
	return nil
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
