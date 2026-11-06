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

// Package proxy is used for routing messages between nodes in the cluster
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/snerror"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commutils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/uuid"
	coretypes "functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/invokemetric"
	"functioncore/pkg/functionbus/proxy/bridgemetrics"
	"functioncore/pkg/functionbus/proxy/busserver"
	"functioncore/pkg/functionbus/proxy/busserver/tcpbusserver"
	"functioncore/pkg/functionbus/proxy/busserver/tlsbusserver"
	"functioncore/pkg/functionbus/proxy/busworkerserver"
	"functioncore/pkg/functionbus/proxy/caemetrics"
	"functioncore/pkg/functionbus/proxy/cancel"
	"functioncore/pkg/functionbus/proxy/channelbusconn"
	"functioncore/pkg/functionbus/proxy/fasthttpserver"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/ratelimit"
	"functioncore/pkg/functionbus/proxy/requestdispatcher"
	"functioncore/pkg/functionbus/proxy/resourceusage"
	"functioncore/pkg/functionbus/proxy/statedispatcher/statedispatcher"
	"functioncore/pkg/functionbus/proxy/taskdispatcher"
	"functioncore/pkg/functionbus/proxy/vpa"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxy/workersview"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/remotescheduler"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/config"
)

const (
	reportDiskMetricsInterval        = "REPORT_DISK_METRICS_INTERVAL"
	defaultReportDiskMetricsInterval = 30 * time.Second
	minReportDiskMetricsInterval     = 10
	maxReportDiskMetricsInterval     = 60
	reportMetricsTimeout             = 5 * time.Second
	defaultBase                      = 10
)

var busProxy *Proxy

var (
	timeWindowsThreshold = 5
	timeWindowsLength    = time.Minute
	nodeIP               = os.Getenv("NODE_IP")
)

// BusProxyConfig BusProxy Config
type BusProxyConfig struct {
	// callback etcd handler
	CallbackETCDHandler common.ETCDConnectHandler
}

// Verification uses to verify function information
type Verification struct {
	IsFunctionExist                  func(tenantID, service, functionName, version string) bool
	IsStateExist                     func(stateID string) bool
	IsStatefulFunction               func(tenantID, service, functionName, version string) bool
	StateRecovery                    func(stateRecoveryConfig coretypes.StateRecoveryConfig)
	GetStatefulFunctionConcurrentNum func(tenantID, functionName, serviceID string) (int, error)
	GetNodeIDByStateID               func(stateID string) string
}

func updateMemoryConfig() {
	protocol.SetRequestMemoryEvaluator(common.FConfig.MemoryEvaluatorConfig.ResponseMemoryEvaluator)
	monitor.SetMemoryControlConfig(common.FConfig.MemoryControlConfig)
}

func registerFunctionCheck(functionCheck *Verification, registry *servicediscovery.Registry) {
	functionCheck.IsFunctionExist = registry.IsFunctionExist
	functionCheck.IsStateExist = registry.IsStateInsExist
	functionCheck.GetNodeIDByStateID = registry.GetNodeIDByStateID
	functionCheck.IsStatefulFunction = registry.IsStatefulFunction
	functionCheck.StateRecovery = registry.StateRecovery
	functionCheck.GetStatefulFunctionConcurrentNum = registry.GetStatefulFunctionConcurrentNum
}

// InitBusProxy init the http server and tcp server
func InitBusProxy(config BusProxyConfig, stopCh <-chan struct{}, registerInfo channelbusconn.RegisterInfo,
	functionCheck *Verification) error {
	utils.SetGlobalStopCh(stopCh)
	if err := internal.GetEnvs(); err != nil {
		return err
	}
	if err := common.InitConfig(common.ConfigPath); err != nil {
		return err
	}
	updateConfig()
	if err := wmhealth.StartWorkerManagerHealthCheck(stopCh); err != nil {
		return err
	}
	registry := servicediscovery.GetRegistry()
	registerFunctionCheck(functionCheck, registry)
	registry.RegisterCallBack(registryCallBack)
	workersView := workersview.GetWorkersView()
	go workersView.Start(stopCh)
	if err := monitor.InitMemMonitor(); err != nil {
		return err
	}
	go waitForShutdown(stopCh, registerInfo)
	if err := startProxy(stopCh, config.CallbackETCDHandler); err != nil {
		return err
	}
	if busProxy == nil {
		return errors.New("failed to init bus proxy")
	}
	registry.RegisterAddWorker(InitDispatcher, workersView.AddWorker)
	registry.RegisterDeleteWorker(workersView.DeleteWorker)
	registry.RegisterEnableNode(workersView.EnableNode)
	registry.RegisterDisableNode(workersView.DisableNode)
	registry.RegisterRecoverLongRun(workersView.RecoverLongRun)
	registry.RegisterCallbackFinishedRecovering(vpa.SetSyncFinishedStatus, vpa.SyncWithSLOAgentWhenSyncFinished)
	workerevaluator.CreateInstance(stopCh)
	go periodCollectPendingRequestMetrics(stopCh)
	go periodReportMirroredDiskSpace(stopCh)
	alarm.GetClient().RegisterAlarmType(alarm.RedisConnection)
	alarm.GetClient().RegisterAlarmType(alarm.BusTenantLimit)
	if err := workerevaluator.CreateMetricsClient(common.FConfig.MetricsServerConfig, stopCh); err != nil {
		log.GetLogger().Errorf("failed to create metrics client, %s", err.Error())
		return err
	}
	workerevaluator.Client.RegisterKeepAliveObserver(busProxy.workerDispatcher)
	remotescheduler.CreateInstanceManager(stopCh)
	addObservers()
	servicediscovery.StartServiceDiscovery(stopCh)
	log.GetLogger().Infof("succeed to start a proxy server")
	// WatchConfig failed do not return, just config hot load not enable
	if err := common.WatchConfig(common.ConfigPath, stopCh, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig %s failed, err %s", common.ConfigPath, err.Error())
	}
	reportResources(stopCh)

	return nil
}

func reportResources(stopCh <-chan struct{}) {
	// start report resource usage to redis
	resourceusage.GetNodeResourceUsageInstance().
		WithStopCh(stopCh).
		WithDataLen(common.FConfig.ReportResourceUsageDataLen).
		WithTickerTime(common.FConfig.ReportResourceUsageTick).
		WithMetricsClient(workerevaluator.Client.GetRedisClient()).
		RegisterReportCallBack(workerevaluator.ReportResourceUsageToRedis).
		StartReportResourceUsageToRedis()

	caemetrics.GetCaeMetricsHandler().WithStopCh(stopCh).
		WithMetricsClient(workerevaluator.Client.GetRedisClient()).
		RegisterReportCallBack(workerevaluator.ReportCaeMetricsToRedis).
		StartReportCaeMetricsToRedis()

	bridgemetrics.GetBridgeMetricsHandler().
		WithStopCh(stopCh).
		WithDataLen(common.FConfig.ReportResourceUsageDataLen).
		WithMetricsClient(workerevaluator.Client.GetRedisClient()).
		RegisterReportCallBack(workerevaluator.ReportBridgeMetricsToRedis).
		StartReportBridgeMetricsToRedis()
}

// InitDispatcher -
func InitDispatcher(funcKeyWithResource, instanceID string, provider *servicediscovery.Provider, isSynced bool) {
	if provider == nil || busProxy == nil || busProxy.workerDispatcher == nil || busProxy.stateDispatcher == nil {
		log.GetLogger().Warnf("InitDispatcher params check nok for %s %s", funcKeyWithResource, instanceID)
		return
	}
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	funcMetaData, err := servicediscovery.GetRegistry().GetFunctionFuncMetaData(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata, funcKeyWithResource %s, err %s", funcKeyWithResource,
			err.Error())
		busProxy.workerDispatcher.InitFunctionDispatcher(funcKeyWithResource, instanceID, provider, isSynced)
		return
	}
	if funcMetaData.IsStatefulFunction {
		busProxy.stateDispatcher.InitFunctionDispatcher(funcKeyWithResource)
	} else {
		busProxy.workerDispatcher.InitFunctionDispatcher(funcKeyWithResource, instanceID, provider, isSynced)
	}
}

func updateInternalConfigs() {
	if common.FConfig.IdleWorkerTimeout > 0 {
		internal.IdleTimeForWorker = time.Duration(common.FConfig.IdleWorkerTimeout) * time.Second
	}
	if common.FConfig.RequestQueueTimeout > 0 {
		internal.RequestQueueTimeout = time.Duration(common.FConfig.RequestQueueTimeout) * time.Second
	}
	log.GetLogger().Infof("IdleTimeForWorker %v, RequestQueueTimeout %v ",
		internal.IdleTimeForWorker, internal.RequestQueueTimeout)
}

func waitForShutdown(stopCh <-chan struct{}, info channelbusconn.RegisterInfo) {
	defer func() {
		log.GetLogger().Sync()
		os.Exit(0)
	}()
	if stopCh == nil {
		return
	}
	select {
	case _, ok := <-stopCh:
		log.GetLogger().Warnf("receive stop signals")
		if ok {
			return
		}
		gracefulShutdown(info)
	}
}

func gracefulShutdown(info channelbusconn.RegisterInfo) {
	if busProxy == nil {
		return
	}
	busProxy.isUpgrading = true
	register := channelbusconn.NewRegistry(info, nil, nil)
	if err := register.Unregister(); err != nil {
		log.GetLogger().Warnf("failed to delete register info from etcd")
	}
	busProxy.workerDispatcher.StopAllDispatchers()

	const requestCheckPeriod = 500 * time.Millisecond
	requestCheckTicker := time.NewTicker(requestCheckPeriod)
loop:
	for {
		select {
		case <-requestCheckTicker.C:
			if atomic.LoadInt32(&busProxy.requestCount) == 0 {
				log.GetLogger().Warnf("shutdown when all request finished")
				break loop
			}
		}
	}
	workerbroker.GetInstance().DeleteAllWorkerBrokerWithGracefulExit()
	log.GetLogger().Infof("succeed to exit gracefully")
}

func hasSynced(etcdType etcd3.EtcdType, watchPath string) bool {
	for _, observer := range servicediscovery.EtcdObserverList {
		etcdObserver, ok := observer.(*servicediscovery.EtcdObserver)
		if ok && etcdObserver.EtcdSelection == etcdType && etcdObserver.WatchPath == watchPath {
			return etcdObserver.HasSynced
		}
	}
	return true
}

func addObservers() {
	registry := servicediscovery.GetRegistry()
	if registry == nil {
		return
	}
	workerObserver := &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.Router,
		WatchPath:     common.WatchPathPrefixWithWorkers,
		Filter:        registry.WorkerPathFilter,
		UpdateHandler: registry.UpdateProviderOnObserve,
		DeleteHandler: registry.DeleteProviderOnObserve,
		SyncHandler:   registry.SyncedProviderOnObserve,
	}
	registry.AddProviderSyncCount()

	functionObserver := &servicediscovery.EtcdObserver{
		EtcdSelection:   etcd3.Meta,
		WatchPath:       common.WatchPathPrefixWithFunction,
		Filter:          registry.FuncPathFilter,
		UpdateHandler:   registry.UpdateFuncOnObserver,
		DeleteHandler:   registry.DeleteFuncOnObserver,
		EnableEtcdCache: config.Conf.EnableMetaEtcdCache,
	}

	functionClusterObserver := buildFunctionClusterObserver(registry)

	frontendObserver := buildFrontendObserver(registry)

	stateObserver := &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.State,
		WatchPath:     common.WatchPathPrefixWithStateWorkers,
		Filter:        registry.StatePathFilter,
		UpdateHandler: registry.UpdateStateOnObserve,
		DeleteHandler: registry.DeleteStateOnObserve,
	}

	aliasObserver := &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.WatchPathPrefixWithAliase,
		Filter:        aliasroute.IsAlias,
		UpdateHandler: servicediscovery.CreateAliasOnObserve,
		DeleteHandler: servicediscovery.DeleteAliasOnObserve,
	}

	cancel.GetInstance().RegisterCancelHandler(workerbroker.GetInstance().Cancel)
	cancelObserver := &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.WatchPathPrefixWithCancel,
		Filter:        cancel.GetInstance().IsCancel,
		UpdateHandler: cancel.GetInstance().UpdateOnObserve,
		DeleteHandler: cancel.GetInstance().DeleteOnObserve,
	}

	tenantQosObserver := &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.QOSEtcdPrefix,
		Filter:        ratelimit.TenantQOSFilter,
		UpdateHandler: ratelimit.Update,
		DeleteHandler: ratelimit.Delete,
	}

	servicediscovery.EtcdObserverList = append(servicediscovery.EtcdObserverList, workerObserver,
		functionObserver, functionClusterObserver, frontendObserver, stateObserver, aliasObserver,
		cancelObserver, tenantQosObserver)

	addCAEEtcdObserver(registry)
	addRemoteSchedulerObserver(registry)
}

func buildFrontendObserver(registry *servicediscovery.Registry) *servicediscovery.EtcdObserver {
	return &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.WatchPathPrefixWithFrontendInstance,
		Filter:        registry.ObserverDefaultFilter,
		UpdateHandler: registry.UpdateFrontendInstances,
		DeleteHandler: registry.DeleteFrontendInstances,
	}
}

func buildFunctionClusterObserver(registry *servicediscovery.Registry) *servicediscovery.EtcdObserver {
	return &servicediscovery.EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.WatchPathPrefixWithFunctionCluster,
		Filter:        registry.ObserverDefaultFilter,
		UpdateHandler: registry.UpdateFuncAvailableCluster,
		DeleteHandler: registry.DeleteFuncAvailableCluster,
	}
}

func addCAEEtcdObserver(registry *servicediscovery.Registry) {
	if common.FConfig.CAEMetaEtcdConfig != nil && len(common.FConfig.CAEMetaEtcdConfig.ServerList) > 0 {
		servicediscovery.EtcdObserverList = append(servicediscovery.EtcdObserverList, &servicediscovery.EtcdObserver{
			EtcdSelection: etcd3.CAEMeta,
			WatchPath:     common.WatchPathPrefixWithFunction,
			Filter:        registry.FuncPathFilter,
			UpdateHandler: registry.UpdateFuncOnObserver,
			DeleteHandler: registry.DeleteFuncOnObserver,
		})
	}

	if common.FConfig.CAERouterEtcdConfig != nil && len(common.FConfig.CAERouterEtcdConfig.ServerList) > 0 {
		servicediscovery.EtcdObserverList = append(servicediscovery.EtcdObserverList, &servicediscovery.EtcdObserver{
			EtcdSelection: etcd3.CAERouter,
			WatchPath:     common.WatchPathPrefixWithWorkers,
			Filter:        registry.WorkerPathFilter,
			UpdateHandler: registry.UpdateProviderOnObserve,
			DeleteHandler: registry.DeleteProviderOnObserve,
			SyncHandler:   registry.SyncedProviderOnObserve,
		})
		registry.AddProviderSyncCount()
	}
}

func addRemoteSchedulerObserver(registry *servicediscovery.Registry) {
	servicediscovery.EtcdObserverList = append(servicediscovery.EtcdObserverList,
		&servicediscovery.EtcdObserver{
			EtcdSelection: etcd3.Router,
			WatchPath:     common.WatchPathSchedulerPrefix,
			Filter:        registry.FaaSSchedulerPathFilter,
			UpdateHandler: registry.UpdateSchedulerOnObserve,
			DeleteHandler: registry.DeleteSchedulerOnObserve,
			SyncHandler:   registry.UpdateSchedulerOnObserve,
		})
}

// Proxy struct
type Proxy struct {
	workerDispatcher *requestdispatcher.WorkerDispatcher
	taskDispatcher   *taskdispatcher.TaskDispatcher
	stateDispatcher  *statedispatcher.StateDispatcher
	channelBusConn   *channelbusconn.ChannelBusConn
	busServer        busserver.BusProxyServer
	busWorkerServer  tcpbus.ServerReceiver
	busRuntimeServer tcpbus.ServerReceiver
	requestCount     int32
	mergeConfig      mergeReqConfig
	isUpgrading      bool
	alarmedTenant    sync.Map
}

type mergeReqConfig struct {
	enableCustomMerge bool
	mergeRequestCfg   map[string]bool
}

func startProxy(stopCh <-chan struct{}, handler func(bool)) error {
	registry := servicediscovery.GetRegistry()
	busProxy = &Proxy{
		workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
		taskDispatcher:   taskdispatcher.CreateTaskDispatcher(registry),
		stateDispatcher:  statedispatcher.CreateStateDispatcher(registry),
	}
	if common.FConfig.HTTPSConfig.HTTPSEnable {
		err := commonTLS.InitTLSConfig(common.FConfig.HTTPSConfig.TLSProtocol, common.FConfig.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the tls config: %s", err.Error())
			return err
		}
	}
	if err := alarm.NewClient(common.FConfig.AlarmServerAddress, constants.FunctionTask, stopCh,
		alarm.WithTLSEnable(commonTLS.GetClientTLSConfig()), alarm.WithAuth(common.FConfig.LocalAuth.AKey,
			common.FConfig.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return err
	}
	if err := utils.InitEtcdClient(stopCh); err != nil {
		fmt.Printf("init etcd client error, msg: %s\n", err.Error())
		return err
	}
	// busServer create and start
	err := createAndStartBusServer(busProxy)
	if err != nil {
		return err
	}
	if err = initWorkerRuntimeServer(busProxy); err != nil {
		fmt.Printf("failed to init worker runtime server, err: %s\n", err.Error())
		return err
	}
	if err = createChannelBus(busProxy, handler); err != nil {
		return err
	}
	go func() {
		busProxy.channelBusConn.AwaitReady()
		listenAddr := internal.SelfPodIP + ":" + constants.HTTPort
		if err := fasthttpserver.CreateFastServer(listenAddr, busProxy.HandReq).Start(); err != nil {
			log.GetLogger().Errorf("failed to start the http server: %s", err)
			return
		}
	}()
	go startUploadResourceViewFromSLOAgent(stopCh)
	return nil
}

func startUploadResourceViewFromSLOAgent(stopCh <-chan struct{}) {
	if common.FConfig != nil && common.FConfig.VPAConfig != nil && common.FConfig.VPAConfig.Enable > 0 &&
		common.FConfig.VPAConfig.ResourceViewAlignInterval > 0 {
		tick := time.NewTicker(time.Duration(common.FConfig.VPAConfig.ResourceViewAlignInterval) * time.Second)
		for {
			select {
			case <-tick.C:
				traceID := uuid.New().String()
				log.GetLogger().Debugf("start to upload resource view to worker manager, traceID: %s", traceID)
				payload, err := vpa.GetCurrentResourceView(traceID)
				if err != nil {
					log.GetLogger().Errorf("failed to get resource view from SLO agent, errorCode: %d, "+
						"errorMessage: %s, traceID: %s", err.Code(), err.Error(), traceID)
					continue
				}
				err = vpa.SyncResourceView(payload, traceID)
				if err != nil {
					log.GetLogger().Errorf("failed to sync resource view to worker manager, errorCode: %d, "+
						"errorMessage: %s, traceID: %s", err.Code(), err.Error(), traceID)
					continue
				}
				log.GetLogger().Infof("succeed to sync resource view from SLO agent to worker manager")
			case _, ok := <-stopCh:
				if !ok {
					log.GetLogger().Warnf("stop channel is closed when uploading resource view")
				}
				tick.Stop()
				log.GetLogger().Infof("stop uploading resource view")
				return
			}
		}
	}
	log.GetLogger().Info("disable VPA feature and will not sync resource view to worker manager")
}

func initWorkerRuntimeServer(busProxy *Proxy) error {
	if busProxy == nil {
		return errors.New("failed to init worker runtime server, un-init busproxy")
	}
	busWorkerServer, err := busworkerserver.InitServer(busProxy.HandReq)
	if err != nil {
		log.GetLogger().Errorf("failed to init bus worker server, error: %s", err.Error())
		return err
	}
	busProxy.busWorkerServer = busWorkerServer

	busRuntimeServer, err := busworkerserver.InitServerWithRuntime(busProxy.HandReq)
	if err != nil {
		log.GetLogger().Errorf("failed to init bus runtime server, error: %s", err.Error())
		return err
	}
	busProxy.busRuntimeServer = busRuntimeServer
	return nil
}

func createChannelBus(proxy *Proxy, handler func(bool)) error {
	// channelBus create and start
	channelBusOpt := &channelbusconn.ConnOption{
		RequestHandler: proxy.HandReq,
		EtcdHandler:    handler,
	}
	channelBusConn, err := channelbusconn.CreateChannelBusConn(channelBusOpt)
	if err != nil {
		log.GetLogger().Errorf("failed to create channel bus conn, error: %s", err.Error())
		return err
	}
	if err = channelBusConn.StartChannelBusConn(); err != nil {
		log.GetLogger().Errorf("failed to start channel bus conn, error: %s", err.Error())
		return err
	}
	proxy.channelBusConn = channelBusConn
	return nil
}

func createAndStartBusServer(busProxy *Proxy) error {
	if busProxy == nil {
		return fmt.Errorf("busProxy is nil")
	}
	// busServer create and start
	serverOpt := &busserver.Option{
		ListenAddr:     internal.SelfPodIP + ":" + constants.TCPort,
		RequestHandler: busProxy.HandReq,
	}
	var busServer busserver.BusProxyServer
	var err error
	if common.FConfig.HTTPSConfig.HTTPSEnable {
		busServer, err = tlsbusserver.CreateTLSBusServer(serverOpt)
	} else {
		busServer, err = tcpbusserver.CreateTCPBusServer(serverOpt)
	}
	if err != nil {
		return err
	}
	if busServer == nil {
		return fmt.Errorf("failed to create busServer")
	}

	common.SendRecoverReq = busServer.SendRecoverRequest
	common.SendCancelReq = busServer.SendCancelRequest
	common.SendCallReq = busServer.SendRequest
	common.NotifyRemote = busServer.NotifyRemote
	err = busServer.StartServer()
	if err != nil {
		return err
	}
	busProxy.busServer = busServer
	return nil
}

// HandReq handle request from http server or tcp bus
func (p *Proxy) HandReq(traceID string,
	busCallReq *types.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	// if busProxy is updating, refuse to process request
	if p.isUpgrading {
		log.GetLogger().Warnf("receive new request while busProxy is updating, traceID %s", traceID)
		response := &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.IsUpgradingCode,
			ErrorMessage: proxyerrors.IsUpgradingErr,
		}
		return response
	}

	if dst.TenantID == common.BusProxyTenantID {
		return p.handleRemoteNotify(busCallReq.UserData, dst)
	}

	resp := p.doFunctionRequest(traceID, busCallReq, dst)
	if resp == nil {
		log.GetLogger().Errorf("the request got an empty response, traceID %s", traceID)
		resp = &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}

	cancel.GetInstance().DeleteRequest(dst.FutureID)
	return resp
}

func getCPUAndMemoryStrFromCache(functionKey string) (string, string) {
	funcMeta, err := servicediscovery.GetRegistry().GetFunctionMetadata(functionKey)
	if err != nil {
		// if data cannot be obtained, an error should not be returned, affecting scheduling.
		log.GetLogger().Warnf("function metadata not found from local cache,"+
			" functionKey: %s, err:%s", urnutils.AnonymizeTenantKey(functionKey), err.Error())
		return "", ""
	}
	if commutils.IsCAEFunc(funcMeta.FuncMetaData.BusinessType) {
		return "0", "0"
	}
	return strconv.Itoa(funcMeta.ResourceMetaData.CPU), strconv.Itoa(funcMeta.ResourceMetaData.Memory)
}

func refreshDynamicResourceFromStateCache(traceID string, busCallReq *types.TCPRequest, dst *common.FunctionProperty) {
	if dst.StateID == "" || dst.FunctionName == "function-task" {
		return
	}
	dynamicCPU := servicediscovery.GetRegistry().GetDynamicCPUByStateID(dst.StateID)
	dynamicMemory := servicediscovery.GetRegistry().GetDynamicMemoryByStateID(dst.StateID)
	if dynamicCPU != "" || dynamicMemory != "" {
		busCallReq.CPU = dynamicCPU
		busCallReq.Memory = dynamicMemory
	}
	log.GetLogger().Infof("refresh stateful dynamic cpu :%s and memory: %s, traceID: %s, stateID: %s",
		dynamicCPU, dynamicMemory, traceID, dst.StateID)
}

func genDispatcherRequest(traceID string, busCallReq *types.TCPRequest,
	dst *common.FunctionProperty) *types.DispatcherRequest {
	functionKey := utils.GetFuncKey(dst.TenantID, dst.FunctionName, dst.FunctionVersion)
	if dst.TenantID != constants.PlatformTenantID && busCallReq.Memory == "" {
		// request with specific cpu/mem same as that in function meta shares the same request dispatcher
		busCallReq.CPU, busCallReq.Memory = getCPUAndMemoryStrFromCache(functionKey)
	}
	refreshDynamicResourceFromStateCache(traceID, busCallReq, dst)

	req := &types.DispatcherRequest{
		Request:     busCallReq,
		Response:    make(chan *commontypes.TCPResponse, 1),
		FunctionKey: functionKey,
		TraceID:     traceID,
		FutureID:    dst.FutureID,
		Canceled:    false,
	}
	// if the request is generated through annotations, the CPU may be "0" and the memory may be "128","256"...,
	// and so on, and the CPU needs to be mapped from the memory
	if busCallReq.CPU != "0" {
		return req
	}
	if busCallReq.Memory == "0" {
		return req
	}
	memory, err := strconv.Atoi(busCallReq.Memory)
	if err != nil {
		log.GetLogger().Warnf("failed to parse memory(string to int), memory: %s, traceID: %s",
			busCallReq.Memory, traceID)
		return req
	}
	busCallReq.CPU = strconv.Itoa(commutils.CalculateCPUByMemory(memory))
	return req
}

func (p *Proxy) handleRemoteNotify(data map[string]string, dst *common.FunctionProperty) *types.TCPResponse {
	if _, ok := data[common.RemoteNotify]; !ok {
		return nil
	}
	response := &types.TCPResponse{}
	if workerInfoStr, ok := data[common.RemoteEnable]; ok {
		workerInfo := utils.ParseFromWorkerInfoStr(workerInfoStr)
		log.GetLogger().Debugf("receive remote enable, funcKeyWithResource: %s, ownerIP: %s, instanceID: %s, "+
			"stateID: %s", workerInfo.FuncKeyWithResource, workerInfo.OwnerIP, dst.InstanceID, dst.StateID)
		config := &workerbroker.BrokerConfig{
			FuncKeyWithResource: workerInfo.FuncKeyWithResource,
			WorkerID:            dst.InstanceID,
			WorkerAddr:          workerInfo.WorkerAddress,
			RemoteIP:            workerInfo.OwnerIP,
			IsDirectFunc:        workerInfo.IsDirectFunc,
			HasInitializer:      workerInfo.HasInitializer,
			StateID:             dst.StateID,
			BusinessType:        workerInfo.BusinessType,
			PodUID:              workerInfo.PodUID,
			ContainerIDs:        workerInfo.ContainerIDs,
			Resource:            workerInfo.Resource,
			ViewResource:        workerInfo.ViewResource,
		}
		if err := workerbroker.GetInstance().CreateWorkerBroker(config); err != nil {
			log.GetLogger().Errorf("failed to create workerBroker, instance:%s, ownerIP:%s, error: %s",
				dst.InstanceID, workerInfo.OwnerIP, err.Error())
			response = &types.TCPResponse{
				ErrorCode:    proxyerrors.WorkerBrokerCreateErrCode,
				ErrorMessage: proxyerrors.WorkerBrokerCreateErrMsg,
			}
		}
		return response
	}
	if workerInfoStr, ok := data[common.RemoteDisable]; ok {
		workerInfo := utils.ParseFromWorkerInfoStr(workerInfoStr)
		log.GetLogger().Infof("receive remote disable, funcKeyWithResource: %s, remoteIP: %s, instance: %s",
			workerInfo.FuncKeyWithResource, workerInfo.OwnerIP, dst.InstanceID)
		workerbroker.GetInstance().DeleteRemoteBroker(workerInfo.FuncKeyWithResource, dst.InstanceID)
		return response
	}
	return nil
}

func (p *Proxy) doFunctionRequest(traceID string,
	busCallReq *types.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	log.GetLogger().ForDebugf("do request for function %s, priority %d, cpu: %s, memory: %s, traceID %s, "+
		"dst=%s-%s-%s-%s-%s-%s, isInvokeAll: %t", dst.FunctionName, busCallReq.Priority, busCallReq.CPU, busCallReq.Memory,
		traceID, urnutils.AnonymizeTenantKey(dst.TenantID), dst.FunctionName, dst.FunctionVersion, dst.InstanceID,
		dst.StateID, dst.FutureID, busCallReq.IsInvokeAll)
	if atomic.LoadInt32(&p.requestCount) == internal.RequestsLimit {
		log.GetLogger().Errorf("current requests reach limit %d, traceId %s", internal.RequestsLimit, traceID)
		go invokemetric.SaveRejectCount(urnutils.GetFuncKey(dst.TenantID, dst.FunctionName, dst.FunctionVersion),
			busCallReq.DirectRequest)
		return &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.OverloadRequestsCode,
			ErrorMessage: proxyerrors.OverloadRequestsErr,
		}
	}
	atomic.AddInt32(&p.requestCount, 1)
	defer atomic.AddInt32(&p.requestCount, -1)
	if dst.InstanceID == "from-coresdk" && dst.FutureID != "" {
		if err := ratelimit.GetTenantLimiterContainer().TakeOneToken(dst.TenantID); err != nil {
			go p.alarm(true, dst.TenantID)
			go invokemetric.SaveRejectCount(urnutils.GetFuncKey(dst.TenantID, dst.FunctionName, dst.FunctionVersion),
				busCallReq.DirectRequest)
			return &commontypes.TCPResponse{
				ErrorCode:    proxyerrors.TrafficLimitEffective,
				ErrorMessage: err.Error(),
			}
		}
		callResp := p.doRequestWithRetry(traceID, busCallReq, dst)
		if common.FConfig.ReportInnerInvokeIndicatorEnable {
			log.GetLogger().Debugf("ReportInnerInvokeIndicatorEnable is true, report inner invoke failure indicator.")
			go recordIndicators(traceID, busCallReq, dst, callResp)
		}
		go p.alarm(false, dst.TenantID)
		return callResp
	}

	return p.doRequest(context.Background(), traceID, busCallReq, dst)
}

func (p *Proxy) alarm(needAlarm bool, tenantID string) {
	if _, exist := p.alarmedTenant.Load(tenantID); !exist && needAlarm {
		p.alarmedTenant.Store(tenantID, struct{}{})
		alarm.GetClient().ReportAlarm(alarm.BusTenantLimit,
			fmt.Sprintf("bus may be in traffic limit, tenantID:%s", tenantID))
	}

	if _, exist := p.alarmedTenant.Load(tenantID); exist && !needAlarm {
		p.alarmedTenant.Delete(tenantID)
		alarm.GetClient().ClearAlarm(alarm.BusTenantLimit)
	}
}

func recordIndicators(traceID string, busCallReq *commontypes.TCPRequest, dst *common.FunctionProperty,
	callResp *commontypes.TCPResponse) {
	if callResp == nil {
		resp := &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
		invokemetric.RecordIndicators(traceID, busCallReq, dst, resp)
	} else if callResp.ErrorCode != 0 {
		invokemetric.RecordIndicators(traceID, busCallReq, dst, callResp)
	}
}

func (p *Proxy) doRequestWithRetry(traceID string,
	busCallReq *types.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	var (
		retryTime = int(atomic.LoadInt32(&common.FConfig.MaxRetryTime))
		rawData   = busCallReq.RawData
	)

	dst.InstanceID = ""
	funcKey := utils.GetFuncKey(dst.TenantID, dst.FunctionName, dst.FunctionVersion)
	timeout := p.getRetryTimeout(funcKey)

	log.GetLogger().Infof("set retry time %d timeout %s to call %s, traceID %s", retryTime, timeout,
		dst.FunctionName, traceID)
	busCallReq.UsedInstance = make(map[string]struct{}, 1)

	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()

	var resp *types.TCPResponse

loop:
	for i := 0; i < retryTime; i++ {
		select {
		case <-ctx.Done():
			break loop
		default:
		}
		if i > 0 {
			busCallReq.RawData = rawData
		}
		tmp := p.doRequest(ctx, traceID, busCallReq, dst)
		if tmp == nil {
			log.GetLogger().Warnf("request response is nil, retry %d traceID %s", i, traceID)
			time.Sleep(defaultRetrySleepTime)
			continue
		}
		if tmp.MetricsErrSaved {
			busCallReq.MetricsErrSaved = true
		}
		// if scheduler specific instance failed(maybe caused by delay of etcd event), do retry
		if tmp.ErrorCode == snerror.NoSuchInstanceName && len(busCallReq.InstanceID) > 0 {
			log.GetLogger().Warnf("schedule specific instance %s failed, retry %d, traceID %s",
				busCallReq.InstanceID, i, traceID)
			resp = tmp
			if !hasSynced(etcd3.CAERouter, common.WatchPathPrefixWithWorkers) {
				time.Sleep(specificInstanceRetrySleepTime)
			} else {
				time.Sleep(defaultRetrySleepTime)
			}
			continue
		}
		if tmp.ErrorCode == 0 || tmp.ErrorCode == snerror.RequestAcceptedCode || snerror.IsUserErr(tmp.ErrorCode) {
			return tmp
		}
		log.GetLogger().Warnf("do function request, retry %d traceID %s code %d FunctionKey: %s, instanceId: %s",
			i, traceID, tmp.ErrorCode, dst.FunctionName, dst.InstanceID)
		if tmp.ErrorCode == proxyerrors.IsUpgradingCode {
			log.GetLogger().Warnf("functiontask is upgrading, retry %d traceID %s", i, traceID)
			time.Sleep(upgradingRetrySleepTime)
			continue
		}
		if tmp.ErrorMessage != proxyerrors.RetryContextTimeoutErr {
			resp = tmp
		}
		time.Sleep(defaultRetrySleepTime)
	}

	return resp
}

func (p *Proxy) doRequest(ctx context.Context, traceID string,
	busCallReq *types.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	req := genDispatcherRequest(traceID, busCallReq, dst)

	cancel.GetInstance().AddRequest(req.FutureID, req)
	if errResp := p.distributeRequest(req, dst); errResp != nil {
		return errResp
	}

	var resp *commontypes.TCPResponse

	if req.Request != nil && req.Request.RemoteScheduleFlag && req.Request.ScheduleInstanceKey != "" {
		funcKeyWithResource := utils.GetFuncKeyWithResource(req.FunctionKey,
			types.InstanceResource{CPU: req.Request.CPU, Memory: req.Request.Memory})
		defer func(resp *commontypes.TCPResponse,
			funcKeyWithResource string, ScheduleInstanceKey string, traceID string) {
			var errFlag bool
			if resp != nil {
				errFlag = resp.ErrorCode != 0
			}
			req.Request.ScheduleInstanceKey = ""
			remotescheduler.ReleaseInstance(funcKeyWithResource, ScheduleInstanceKey, errFlag, traceID)
		}(resp, funcKeyWithResource, req.Request.ScheduleInstanceKey, req.TraceID)
	}

	select {
	case <-ctx.Done():
		log.GetLogger().Warnf("context timedout for %s %s", req.FunctionKey, req.TraceID)
		resp = &commontypes.TCPResponse{
			ErrorCode:    proxyerrors.TimeoutErrCode,
			ErrorMessage: proxyerrors.RetryContextTimeoutErr,
		}
		return resp
	case resp, ok := <-req.Response:
		// zy: add function timeout for waiting response
		if !ok {
			log.GetLogger().Errorf("response chan has been closed, traceID %s", req.TraceID)
			resp = &commontypes.TCPResponse{
				ErrorCode: proxyerrors.InternalErrCode, ErrorMessage: proxyerrors.InternalErr,
			}
			return resp
		}

		// for requests not from frontend, forward itself
		if (!req.Request.DirectRequest || req.Request.IsStateFul) &&
			(resp != nil && (resp.ErrorCode == proxyerrors.ForwardCode ||
				resp.ErrorCode == constants.BackpressureCode)) {
			if resp != nil && resp.ErrorCode == constants.BackpressureCode {
				req.Position.InstanceID = common.ProxyBackPressurePrefix + internal.SelfNodeIP
			}
			if resp != nil && resp.ErrorCode == constants.BackpressureCode && resp.NodeID != "" {
				req.Position.NodeID = resp.NodeID
			}
			log.GetLogger().Debugf("request %s should be forwarded to instance %s, node %s, traceID %s",
				urnutils.AnonymizeTenantKey(req.FunctionKey), req.Position.InstanceID, req.Position.NodeID,
				req.TraceID)
			return p.forwardRequest(req, traceID, dst)
		}
		return resp
	}
}

const (
	e2eFaaSReservedTimeout         = 50
	e2eCAEReservedTimeout          = 10
	defaultRetrySleepTime          = 100 * time.Millisecond
	upgradingRetrySleepTime        = 2 * time.Second
	specificInstanceRetrySleepTime = 1 * time.Second
	// from deployment values.yaml, default reserved 6 minute for sync request timeout
	maxReservedSyncTimeout = 6 * time.Minute
)

var (
	defaultRetryTimeout = time.Duration(constants.GetSyncRequestTimeout())*time.Second + maxReservedSyncTimeout
)

func (p *Proxy) getRetryTimeout(funcKey string) time.Duration {
	if funcKey == "0/function-task/latest" {
		return defaultRetryTimeout
	}
	funcMeta, err := servicediscovery.GetRegistry().GetFunctionMetadata(funcKey)
	if err != nil {
		log.GetLogger().Infof("use default retry timeout for %s", funcKey)
		return defaultRetryTimeout
	}
	if commutils.IsCAEFunc(funcMeta.FuncMetaData.BusinessType) {
		return time.Duration(funcMeta.FuncMetaData.Timeout+funcMeta.ExtendedMetaData.Initializer.InitializerTimeout+
			e2eCAEReservedTimeout) * time.Second
	}
	if funcMeta.FuncMetaData.IsCustomContainer {
		return defaultRetryTimeout
	}
	funcTimeout := funcMeta.FuncMetaData.Timeout
	if funcTimeout > constants.GetSyncRequestTimeout() {
		funcTimeout = constants.GetSyncRequestTimeout()
	}
	return time.Duration(funcMeta.ExtendedMetaData.Initializer.InitializerTimeout+funcTimeout+e2eFaaSReservedTimeout) *
		time.Second
}

// forwardRequest forward request
func (p *Proxy) forwardRequest(req *types.DispatcherRequest, traceID string,
	dst *common.FunctionProperty) *commontypes.TCPResponse {
	if req.Canceled {
		log.GetLogger().Debugf("request is canceled")
		cancel.GetInstance().DeleteRequest(dst.FutureID)
		return &commontypes.TCPResponse{
			RequestID:    req.Request.RequestID,
			ErrorCode:    proxyerrors.RequestCanceledCode,
			ErrorMessage: proxyerrors.RequestCanceledError,
		}
	}

	dst.InstanceID = req.Position.InstanceID
	// forward to specific proxy
	if req.Position.InstanceID == "" {
		dst.InstanceID = common.ProxyReqPrefix + req.Position.NodeID
	}

	var remoteResp *commontypes.TCPResponse

	if req.Position.NodeID == internal.SelfNodeIP {
		// from bus proxy to task
		log.GetLogger().Infof("request %s should be forwarded to task, instance %s, node %s, traceID %s, "+
			"stateID: %s, targetInstance:%s", urnutils.AnonymizeTenantKey(req.FunctionKey), req.Position.InstanceID,
			req.Position.NodeID, req.TraceID, dst.StateID, req.Request.InstanceID)
		remoteResp = p.channelBusConn.SendRequest(traceID, req.Request, dst)
	} else {
		// to other bus proxy
		log.GetLogger().Debugf("request %s should be forwarded to other bus proxy, instance %s, node %s, "+
			"traceID %s, stateID: %s, targetInstance:%s", urnutils.AnonymizeTenantKey(req.FunctionKey),
			req.Position.InstanceID, req.Position.NodeID, req.TraceID, dst.StateID, req.Request.InstanceID)
		remoteResp = p.busServer.SendRequest(req.Position.NodeID, traceID, req.Request, dst)
	}
	return remoteResp
}
func (p *Proxy) distributeRequest(req *commontypes.DispatcherRequest,
	dst *common.FunctionProperty) *commontypes.TCPResponse {
	var err error
	// worker requests should processed by workerDispatcher,
	// for trigger invoke request, should processed by taskDispatcher
	// stateful function requests should processed by stateDispatcher
	if dst.TenantID != common.TaskTenantID && dst.InstanceID != internal.HostName {
		req.Position.InstanceID = dst.InstanceID
		req.NodeLabel = dst.NodeLabel
		if dst.StateID != "" {
			err = p.stateDispatcher.Request(req)
		} else {
			err = p.workerDispatcher.Request(req)
		}
	} else {
		// task requests should processed by taskDispatcher
		err = p.taskDispatcher.Request(req, dst)
	}

	if err != nil {
		if userErr, ok := err.(snerror.SNError); ok {
			log.GetLogger().Errorf("failed to do request, traceID:%s, errorCode %d, err:%s",
				req.TraceID, userErr.Code(), userErr.Error())
			return &commontypes.TCPResponse{
				ErrorCode: uint32(userErr.Code()), ErrorMessage: userErr.Error(),
			}
		}
		if err == requestdispatcher.ErrBusTerminating {
			log.GetLogger().Warnf("process is terminating, traceID: %s", req.TraceID)
			return &commontypes.TCPResponse{
				ErrorCode: proxyerrors.IsUpgradingCode, ErrorMessage: proxyerrors.IsUpgradingErr,
			}
		}
		if err == requestdispatcher.ErrInvokeStatefulWithoutStateID {
			return &commontypes.TCPResponse{
				ErrorCode: snerror.InvokeStatefulFunctionWithoutStateID, ErrorMessage: err.Error()}
		}
		log.GetLogger().Errorf("failed to do request, traceID:%s, err:%s", req.TraceID, err.Error())
		return &commontypes.TCPResponse{
			ErrorCode: proxyerrors.SendReqErrCode, ErrorMessage: proxyerrors.SendReqErr,
		}
	}

	return nil
}

func registryCallBack(eventType string, eventInfo interface{}, isDelCAEFunc bool) {
	if ok := checkType(eventType); !ok {
		log.GetLogger().Debugf("unknown event type:%s", eventType)
		return
	}
	key, ok := eventInfo.(string)
	if !ok && eventType != common.DelStateIDEventStateQueue {
		log.GetLogger().Warnf("failed to assert functionKey")
		return
	}
	if busProxy == nil {
		log.GetLogger().Warnf("bus proxy is nil")
		return
	}

	switch eventType {
	case common.DelFunctionEvent:
		if busProxy.stateDispatcher != nil {
			// key is functionKey
			busProxy.stateDispatcher.DeleteStateReqDispatcher(key)
		}
		if busProxy.workerDispatcher != nil && !isDelCAEFunc {
			busProxy.workerDispatcher.DeleteFunctionDispatcher(key)
		}
		resourceusage.GetNodeResourceUsageInstance().RemoveByFuncKey(key)
		caemetrics.GetCaeMetricsHandler().RemoveByFuncKey(key)
		bridgemetrics.GetBridgeMetricsHandler().RemoveByFuncKey(key)
	case common.DelStateIDEvent:
		if busProxy.stateDispatcher != nil {
			// key is stateID
			busProxy.stateDispatcher.DeleteInstanceByStateID(key)
			busProxy.stateDispatcher.PutStateQueueMetadataByStateID(key)
		}
	// if stateDispatcher has been deleted, we should delete state queue here to avoid getting wrong state
	case common.DelStateIDEventStateQueue:
		stateRouter, valid := eventInfo.(map[string]string)
		if !valid {
			log.GetLogger().Warnf("failed to assert stateRouter, eventInfo is %+v", eventInfo)
			return
		}
		statedispatcher.DeleteStateQueueByStateID(stateRouter)
	case common.UpdFunctionEvent:
		if busProxy.workerDispatcher != nil {
			// key is functionKey
			busProxy.workerDispatcher.UpdateFunctionDispatcher(key)
		}
		if busProxy.stateDispatcher != nil {
			// key is functionKey
			busProxy.stateDispatcher.UpdateFunctionDispatcher(key)
		}
	default:
		// nothing to do
	}
}

func checkType(eventType string) bool {
	return common.DelFunctionEvent == eventType ||
		common.UpdFunctionEvent == eventType ||
		common.DelStateIDEvent == eventType ||
		common.DelStateIDEventStateQueue == eventType
}

func updateConfig() {
	updateInternalConfigs()
	updateMemoryConfig()
	updateLeaseInfo()
	updateEtcdCacheConfig()
	log.GetLogger().Infof("proxy updateConfig success")
}

func updateEtcdCacheConfig() {
	cacheConfig := common.FConfig.EtcdCacheConfig
	if cacheConfig == nil {
		return
	}
	servicediscovery.EtcdWatchChannelsMutex.RLock()
	defer servicediscovery.EtcdWatchChannelsMutex.RUnlock()
	for _, etcdWatchChan := range servicediscovery.EtcdWatchChannels {
		if etcdWatchChan.EtcdCacheConfig.EnableEtcdCache {
			etcdWatchChan.CacheCfgUpdateChan <- cacheConfig
		}
	}
}

func updateLeaseInfo() {
	if common.FConfig.EtcdLeaseConfig == nil {
		return
	}
	leaseTTL := common.FConfig.EtcdLeaseConfig.LeaseTTL
	renewTTL := common.FConfig.EtcdLeaseConfig.RenewTTL
	channelbusconn.UpdateLeaseInfo(leaseTTL, renewTTL)
}

func periodCollectPendingRequestMetrics(stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	// modify default configs
	if metricsTimeWindowConfigs := common.FConfig.WorkerMetricsTimeWindow; metricsTimeWindowConfigs != nil {
		if metricsTimeWindowConfigs.NumberThreshold != 0 {
			log.GetLogger().Infof("set %d as reporting metrics time window threshold",
				metricsTimeWindowConfigs.NumberThreshold)
			timeWindowsThreshold = metricsTimeWindowConfigs.NumberThreshold
		}
		if metricsTimeWindowConfigs.TimeWindowLength != 0 {
			log.GetLogger().Infof("set %d as metrics time window length",
				metricsTimeWindowConfigs.TimeWindowLength)
			timeWindowsLength = time.Duration(metricsTimeWindowConfigs.TimeWindowLength*timeWindowsThreshold) *
				time.Second
		}
	}

	collectTimeWindowTicker := time.NewTicker(timeWindowsLength)
	log.GetLogger().Infof("start to collect and report pending request metrics periodically")
	for {
		select {
		case <-collectTimeWindowTicker.C:
			collectPendingRequestMetrics()
		case <-stopCh:
			return
		}
	}
}

func collectPendingRequestMetrics() {
	if busProxy == nil || busProxy.workerDispatcher == nil {
		return
	}
	view := &workermanager.View{
		Type: workermanager.PendingRequestMetric,
	}
	pendingRequestMetrics := busProxy.workerDispatcher.GetPendingRequestsMetrics()
	if len(pendingRequestMetrics) != 0 {
		view.Object = pendingRequestMetrics
		go reportPendingRequestMetrics(view)
	}
}

func reportPendingRequestMetrics(pendingRequestMetric *workermanager.View) {
	data, err := json.Marshal(pendingRequestMetric)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal pending request views: %s", err.Error())
		return
	}
	requestURI := wmhealth.GetWorkerManagerBaseURL() + "/worker-manager/v1/functions/worker/metrics"
	ctx, cancel := context.WithTimeout(context.Background(), reportMetricsTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Warnf("failed to new request for pending requests view, err %s", err.Error())
		return
	}

	utils.FillInWorkerManagerRequestHeaders(request)

	resp, err := utils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Warnf("failed to report pending requests view, err %s", err.Error())
		return
	}
	defer resp.Body.Close()

	log.GetLogger().Infof("succeed to report pending requests view, view type is %s", pendingRequestMetric.Type)
}

func periodReportMirroredDiskSpace(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Warnf("stop channel is nil")
		return
	}
	var interval time.Duration
	interval = defaultReportDiskMetricsInterval
	reportInterval := os.Getenv(reportDiskMetricsInterval)
	if reportInterval != "" {
		num, err := strconv.ParseInt(reportInterval, defaultBase, 64)
		if err != nil || num < minReportDiskMetricsInterval || num > maxReportDiskMetricsInterval {
			log.GetLogger().Warnf("invalid report disk metrics interval, skip reset")
		} else {
			interval = time.Duration(num) * time.Second
		}
	}
	log.GetLogger().Infof("start report mirror disk space ticker, interval: %v", interval)
	reportMirroredDiskSpaceTicker := time.NewTicker(interval)
	for {
		select {
		case <-reportMirroredDiskSpaceTicker.C:
			collectMirroredDiskSpace()
		case <-stopCh:
			log.GetLogger().Warnf("stop channel received, stop ticker")
			reportMirroredDiskSpaceTicker.Stop()
			return
		}
	}
}

func collectMirroredDiskSpace() {
	view := &workermanager.View{
		Type: workermanager.DiskUsageRequestMetric,
	}
	availSpace, err := utils.GetMirrorDiskSpace()
	if err != nil {
		return
	}
	mirroredDiskSpaceView := workermanager.MirroredDiskSpaceView{
		NodeIP:        nodeIP,
		FreeDiskSpace: availSpace,
	}
	view.Object = mirroredDiskSpaceView

	data, err := json.Marshal(view)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal mirror disk space view: %s", err.Error())
		return
	}
	requestURI := wmhealth.GetWorkerManagerBaseURL() + "/worker-manager/v1/functions/worker/metrics"
	ctx, canceled := context.WithTimeout(context.Background(), reportMetricsTimeout)
	defer canceled()
	request, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Warnf("failed to new request for mirror disk space view, err %s", err.Error())
		return
	}

	utils.FillInWorkerManagerRequestHeaders(request)

	resp, err := utils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Warnf("failed to report mirror disk space view, err %s", err.Error())
		return
	}
	defer resp.Body.Close()
	log.GetLogger().Infof("succeed to report mirror disk space view, view type is %s, "+
		"data: %+v", view.Type, mirroredDiskSpaceView)
	return
}
