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

// Package functionlb load balance
package functionlb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/instance"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/loadbalancer/service"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/model"
	"functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	// InvokePath Http endpoint
	InvokePath = "/invoke"

	// RetryInterval - Millisecond
	RetryInterval = 1000

	workerInsKeyLen = 2
	tenantCnt       = 3

	// AppID is app ID
	AppID          = "frontendinvoke"
	proxyAppID     = "function-task"
	requestTimeout = "timeout"

	// NodeEtcdKeyLen -
	NodeEtcdKeyLen = 13
	defaultBodyMap = 100

	backpressureConcurrentNum = 6

	// ensure request successful
	maxBackPressureTimesWithWorker = 0xFFFF

	defaultConcurrency = 1000

	// specifies the expiration time when ETCD query fails in the singleflight cache
	singleFlightTTL                     = 10 * time.Second
	urnLens                             = 7
	base                                = 10
	defaultBackPressureTimesWithTimeout = 5
)

var (
	// ErrEmptyURN -
	ErrEmptyURN = errors.New("empty service name")
	// ErrLenURN represents that the URN length is incorrect
	ErrLenURN = errors.New("incorrect length of the service name")
	// ErrEmptyWorkerInstanceName -
	ErrEmptyWorkerInstanceName = errors.New("empty worker instance az/name")
	// ErrServiceNotAvailable -
	ErrServiceNotAvailable = errors.New("worker service is not available")
	// ErrNoWorkerInstancePool -
	ErrNoWorkerInstancePool = errors.New("no worker instance pool was found")
	// ErrNoWorkerInstanceConnection -
	ErrNoWorkerInstanceConnection = errors.New("no worker instance connection was found")
	// ErrNoNonNativeWorkerInstance -
	ErrNoNonNativeWorkerInstance = errors.New("no non-native worker instance was found")
	// ErrNoWorker ErrNoWorkerInstance -
	ErrNoWorker = errors.New("no available worker or proxy was found by router")
	// ErrNoNodeLabelUnavailable -
	ErrNoNodeLabelUnavailable = errors.New("selected node label is not available")

	backPressureTimesWithTimeout int
)

func init() {
	backPressureTimesWithTimeout = defaultBackPressureTimesWithTimeout
	timesLimitStr := os.Getenv("BACKPRESSURE_LIMIT_TIMES")
	if timesLimitStr == "" {
		return
	}
	timesLimit, err := strconv.Atoi(timesLimitStr)
	if err != nil {
		log.GetLogger().Warnf("failed to parse env BACKPRESSURE_LIMIT_TIMES, err: %s", err.Error())
		return
	}
	backPressureTimesWithTimeout = timesLimit
}

// Workers -
type Workers struct {
	// key: worker name
	// value: instance of *Worker
	WorkerMap     *sync.Map
	bpWorkers     *BackPressureWorkers
	stateInstance *Worker
	// used for all workers modifying routing instances, also used in back pressure workers
	instancesMutex *sync.Mutex
}

// Node struct stores an node configs
type Node struct {
	IP       string
	Port     string
	Status   string
	P2pPort  string
	NodeIP   string
	NodePort string
}

// Nodes map
type Nodes struct {
	// NodeList key is nodeID, value is Node pointer
	NodeList sync.Map
}

// InstanceInfo -
type InstanceInfo struct {
	Az                string
	Urn               string
	WorkerInstanceKey string
	DeployNode        string
	FuncKeyWithRes    string
	InsResource       types.InstanceResource
}

// Start -
func (ws *Workers) Start(stopCh <-chan struct{}) {
	ws.bpWorkers = initBackPressureWorkers(ws.instancesMutex)
	go ws.bpWorkers.checkPeriodically(stopCh)
}

// DoRequest -
func (ws *Workers) DoRequest(reqInfo *model.FunctionRequestInfo, req *fhttp.Request, resp *fhttp.Response,
	insResource types.InstanceResource) error {
	worker, proxyIns, err := ws.selectWorker(reqInfo, insResource)
	if err != nil {
		log.GetLogger().Errorf("request %s, traceID %s, failed %s", string(req.Host()),
			reqInfo.TraceID, err.Error())
		return err
	}

	hasInstances := false
	if reqInfo.StateID == "" {
		worker.WorkerInstancesConn.Range(func(key, value interface{}) bool {
			hasInstances = true
			return false
		})
	}

	initProxyRequest(req)
	// if worker has no instances, send to proxy
	if hasInstances {
		err = worker.nativeInvoke(reqInfo, req, resp, 0)
	} else {
		if commonUtils.IsMCPSessionReq(reqInfo.MCPFunc, req.Header) {
			log.GetLogger().Warnf("failed to find mcp pod instance %s with sessionID %s, traceID :%s",
				req.Header.Peek(constants.HeaderSSEPodName),
				req.Header.Peek(constants.HeaderSSESessionID), reqInfo.TraceID)
			reqInfo.InnerCode = snerror.MCPSessionInstanceInvalid
			return snerror.New(snerror.MCPSessionInstanceInvalid, snerror.ErrText(snerror.MCPSessionInstanceInvalid))
		}

		err = proxyIns.nativeInvoke(reqInfo, req, resp, 0)
	}
	if err != nil && (err.Error() == requestTimeout ||
		err.Error() == snerror.ErrText(snerror.MCPSessionInstanceInvalid)) {
		return err
	}

	// need retry to other proxy
	if hasInstances && err != nil {
		log.GetLogger().Warnf("failed to traverse worker instances and try to traverse proxy instance, "+
			"traceID %s", reqInfo.TraceID)
		// traverse other proxy nodes
		err = proxyIns.nativeInvoke(reqInfo, req, resp, 0)
	}

	for i := 0; i < maxBackPressureTimesWithWorker; i++ {
		if err == nil && reqInfo.InnerCode == constants.BackpressureCode {
			// retry request to back pressure worker and add back pressure worker to LB
			ws.processRequestWithBackPressureWorker(worker, reqInfo, resp)
			err = worker.nativeInvoke(reqInfo, req, resp, i+1)
		} else {
			break
		}
	}

	if err != nil {
		log.GetLogger().Errorf("DoRequest failed, traceID: %s, error: %s", reqInfo.TraceID, err.Error())
		return err
	}

	log.GetLogger().Debugf("DoRequest finished, traceID: %s", reqInfo.TraceID)

	return nil
}

func (ws *Workers) isProxyExist(workerInstance *WorkerInstance) bool {
	proxyIns := ws.getProxyIns()
	if proxyIns == nil || len(proxyIns.WorkerInstances) == 0 {
		return false
	}
	isExist := false
	proxyIns.mtx.RLock()
	wik, ok := proxyIns.WorkerInstances[workerInstance.Host]
	proxyIns.mtx.RUnlock()
	if !ok {
		log.GetLogger().Errorf("proxy[%s:%s] not existed", workerInstance.Host, workerInstance.Port)
		return false
	}
	wik.mux.RLock()
	if !wik.heartbeatOk {
		wik.mux.RUnlock()
		log.GetLogger().Errorf("proxy[%s:%s] heartbeat nok", workerInstance.Host, workerInstance.Port)
		return false
	}
	wik.mux.RUnlock()
	proxyIns.WorkerInstancesConn.Range(func(key, value interface{}) bool {
		conn, ok := value.(*WorkerInstanceConnection)
		if !ok {
			log.GetLogger().Warnf("the type of WorkerInstancesConn error")
			return false
		}
		if conn.WorkerInstance.Host == workerInstance.Host {
			isExist = true
			return false
		}
		return true
	})

	return isExist
}

func (ws *Workers) getProxyIns() *Worker {
	workerIf, ok := ws.WorkerMap.Load(proxyURN)
	if !ok {
		log.GetLogger().Errorf("there is no proxy")
		return nil
	}
	worker, ok := workerIf.(*Worker)
	if !ok {
		log.GetLogger().Errorf("assert proxy failed")
		return nil
	}
	return worker
}

// GetStateIns -
func (ws *Workers) GetStateIns() *Worker {
	return ws.stateInstance
}

func getTenantID(urn string) (string, error) {
	strs := strings.Split(urn, ":")
	if len(strs) != urnutils.URNLenWithVersion {
		log.GetLogger().Errorf("get incorrect length of %s", urn)
		return "", ErrLenURN
	}
	return strs[tenantCnt], nil
}

// get busproxy (functiontask) instance
func processURN(urn string, insResource types.InstanceResource) (string, string, error) {
	tenantID, err := getTenantID(urn)
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant from URN %s: %s", urn, err.Error())
		return "", "", err
	}
	workerType := util.NormalWorkerIns
	if tenantID == "0" {
		log.GetLogger().Infof("busproxy instance URN is: %s", urn)
		proxyInsKey, err := util.GetProxyInsKey(urn)
		if err != nil {
			log.GetLogger().Errorf("failed to get proxyInsKey %s from URN %s: %s", proxyInsKey, urn, err.Error())
			return "", "", err
		}
		log.GetLogger().Infof("get proxyInsKey %s from URN %s", proxyInsKey, urn)
		urn = proxyInsKey
		workerType = util.ProxyIns
	}

	if insResource.CPU != "" && insResource.Memory != "" {
		urn = convertURNWithResource(urn, insResource)
	}

	return urn, workerType, nil
}

// ConvertURNWithResource -
func convertURNWithResource(urn string, insResource types.InstanceResource) string {
	return fmt.Sprintf("%s:%s-%s", urn, insResource.CPU, insResource.Memory)
}

func getInnerCode(resp *fhttp.Response) int {
	innerCode := resp.Header.Peek(constants.HeaderInnerCode)
	innerCodeStr, err := strconv.Atoi(string(innerCode))
	if err != nil {
		log.GetLogger().Warnf("failed to parse inner code <%s>, error %s", string(innerCode), err.Error())
		innerCodeStr = 0
	}
	return innerCodeStr
}

const proxyURN = proxyAppID + ":" + urnutils.DefaultURNVersion

func (ws *Workers) selectWorker(reqInfo *model.FunctionRequestInfo,
	insResource types.InstanceResource) (*Worker, *Worker, error) {
	proxyIns := ws.getProxyIns()
	if proxyIns == nil || len(proxyIns.WorkerInstances) == 0 {
		return nil, nil, ErrNoWorker
	}
	// req with stateID use proxyURN
	if reqInfo.StateID != "" {
		log.GetLogger().Infof("get state trigger call, traceID:%s, stateID: %s, stateKey: %s", reqInfo.TraceID,
			reqInfo.StateID, reqInfo.StateKey)
		ins := ws.GetStateIns()
		return ins, ins, nil
	}

	urn := convertURNWithResource(reqInfo.URN, insResource)
	funcKey := urnutils.GetFuncKey(reqInfo.TenantID, reqInfo.Name, reqInfo.Version)
	funcKeyWithRes := urnutils.GetFuncKeyWithResource(funcKey, insResource.CPU, insResource.Memory)

	// Check whether worker instances exist
	worker, err := ws.getWorker(urn, "", util.NormalWorkerIns, funcKeyWithRes)
	if err != nil {
		return nil, nil, err
	}
	return worker, proxyIns, nil
}

// getWorker -
func (ws *Workers) getWorker(urn, instance, workerType, funcKeyWithRes string) (*Worker, error) {
	if urn == "" {
		log.GetLogger().Errorf("empty worker az/name when added instance %s", instance)
		return nil, ErrEmptyURN
	}
	var worker *Worker
	workerIf, ok := ws.WorkerMap.Load(urn)
	if !ok {
		lbType, lbInstance := ws.createLB(workerType, funcKeyWithRes)
		workerIf, loaded := ws.WorkerMap.LoadOrStore(urn, &Worker{
			WorkerInstancesConn: &sync.Map{},
			urn:                 urn,
			nodeLB:              loadbalance.NewNodeRR(),
			lbType:              lbType,
			workerType:          workerType,
			bpWorkers:           ws.bpWorkers,
			funcKeyWithRes:      funcKeyWithRes,
			lbWrapper:           createLBWrapper(lbInstance),
		})
		worker, ok = workerIf.(*Worker)
		if !ok {
			return nil, errors.New("failed to get worker")
		}
		if loaded == false {
			worker.lbWrapper.Start()
		}
	} else {
		tmpWorker, ok := workerIf.(*Worker)
		if !ok {
			return nil, errors.New("failed to get worker")
		}
		worker = tmpWorker
	}
	if worker == nil {
		return nil, fmt.Errorf("get a nil worker")
	}
	return worker, nil
}

func (ws *Workers) createLB(workerType, funcKeyWithRes string) (loadbalance.LBType, loadbalance.LoadBalance) {
	var lbType loadbalance.LBType
	var lbInstance loadbalance.LoadBalance
	if workerType == util.NormalWorkerIns {
		concurrency := defaultConcurrency
		isLongRun := false
		tenantID, funcName, funcVersion := urnutils.ParseFuncKey(funcKeyWithRes)
		info, exist := metadata.LoadMetaData(funcName, tenantID, funcVersion)
		isCaeFunc := false
		if exist {
			isCaeFunc = commonUtils.IsCAEFunc(info.FuncMetaDataParts.BusinessType)
			concurrency = info.InstanceMetaData.ConcurrentNum
			isLongRun = info.FuncMetaDataParts.Timeout > constants.GetSyncRequestTimeout()
		}
		lbType = loadbalance.Decentralized
		lbInstance = service.CreateLoadBalancer(funcKeyWithRes, concurrency, isLongRun, isCaeFunc)
	} else {
		lbType = loadbalance.ConsistentHashGeneric
		lbInstance = loadbalance.LBFactory(lbType)
	}
	return lbType, lbInstance
}

func (ws *Workers) processRequestWithBackPressureWorker(worker *Worker, params *model.FunctionRequestInfo,
	resp *fhttp.Response) {
	nodeID := string(resp.Header.Peek(constants.HeaderBackPressureNodeID))
	instanceID := string(resp.Header.Peek(constants.HeaderBackPressureInstanceID))
	if nodeID == "" || instanceID == "" {
		log.GetLogger().Errorf("back pressure with no nodeID %s or instanceID %s", nodeID, instanceID)
		return
	}
	log.GetLogger().Infof("backPressureReq | %s | %s | %s | %s", params.TraceID, nodeID,
		urnutils.ExtractPodName(instanceID), urnutils.FuncURNtoShort(params.AnonymizeURN))
	params.BackPressure = true
	params.TargetNode = nodeID
	params.TargetInstance = instanceID
	resp.Header.Del(constants.HeaderBackPressureInstanceID)
	resp.Header.Del(constants.HeaderBackPressureNodeID)

	workerInstanceKey := util.BuildWorkerInstanceKey(nodeID, constants.HTTPort, instanceID)
	ws.instancesMutex.Lock()
	defer ws.instancesMutex.Unlock()
	exist, err := worker.CheckAndAddWorkerInstance(constant.NativeAz, workerInstanceKey, nodeID)
	if exist || err != nil {
		return
	}

	callback := deleteCallback{
		urnWithRes: worker.urn,
		function:   ws.deleteBackupPressureWorker,
	}
	ws.bpWorkers.add(workerInstanceKey, callback)
}

// AddWorkerInstance add instance to workers
func (ws *Workers) AddWorkerInstance(instanceInfo InstanceInfo) error {
	newURN, workerType, err := processURN(instanceInfo.Urn, instanceInfo.InsResource)
	if err != nil {
		return err
	}
	worker, err := ws.getWorker(newURN, instanceInfo.WorkerInstanceKey, workerType, instanceInfo.FuncKeyWithRes)
	if err != nil {
		return err
	}
	ws.instancesMutex.Lock()
	defer ws.instancesMutex.Unlock()
	exist, err := worker.CheckAndAddWorkerInstance(instanceInfo.Az,
		instanceInfo.WorkerInstanceKey, instanceInfo.DeployNode)
	if err != nil {
		log.GetLogger().Errorf("failed to add instance %s to worker %s: %s",
			instanceInfo.WorkerInstanceKey, newURN, err.Error())
		return err
	}
	if exist {
		ws.bpWorkers.delete(instanceInfo.WorkerInstanceKey)
	}
	return nil
}

// DeleteWorkerInstance delete instance from workers
func (ws *Workers) DeleteWorkerInstance(az, urn, instance string,
	insResource types.InstanceResource) error {
	if az == "" || urn == "" {
		log.GetLogger().Errorf("empty az/name when deleted instance %s from worker %s", instance, urn)
		return ErrEmptyURN
	}

	tenantID, err := getTenantID(urn)
	if err != nil {
		log.GetLogger().Errorf("failed to get tenant from URN %s: %s", urn, err.Error())
		return err
	}

	urnWithRes, _, err := processURN(urn, insResource)
	if err != nil {
		return err
	}
	workerIf, ok := ws.WorkerMap.Load(urnWithRes)
	if !ok {
		return nil
	}

	worker, ok := workerIf.(*Worker)
	if !ok {
		return nil
	}

	ws.instancesMutex.Lock()
	defer ws.instancesMutex.Unlock()
	ws.deleteWorkInstance(worker, az, instance, urnWithRes, tenantID == constants.PlatformTenantID)
	return nil
}

func (ws *Workers) deleteWorkInstance(worker *Worker, az, instance, urnWithRes string, nodeLoss bool) {
	worker.deleteWorkerInstance(az, instance, nodeLoss)
	// worker get no instance, gc
	if len(worker.WorkerInstances) == 0 {
		ws.WorkerMap.Delete(urnWithRes)
		worker.lbWrapper.Stop()
	}
}

func (ws *Workers) deleteBackupPressureWorker(az, instance, urnWithRes string) {
	workerIf, ok := ws.WorkerMap.Load(urnWithRes)
	if !ok {
		return
	}
	worker, ok := workerIf.(*Worker)
	if !ok {
		return
	}
	ws.deleteWorkInstance(worker, az, instance, urnWithRes, false)
}

// String return Join strings
func (ws *Workers) String() string {
	var workers []string
	ws.WorkerMap.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			log.GetLogger().Warnf("failed to assert type string. key: %T", key)
			return true
		}
		value, ok := v.(*Worker)
		if !ok {
			log.GetLogger().Warnf("failed to assert type *Worker. value: %T", value)
			return true
		}
		workers = append(workers, key+":"+value.String())
		return true
	})

	return strings.Join(workers, ",")
}

// UpdateFuncMeta -
func (ws *Workers) UpdateFuncMeta(funcKey string, info *metadata.FunctionMetaData) {
	ws.WorkerMap.Range(func(k, v interface{}) bool {
		worker, ok := v.(*Worker)
		if !ok {
			log.GetLogger().Warnf("failed to assert type Worker")
			return true
		}
		key := urnutils.GetFuncKeyFromFuncKeyWithResource(worker.funcKeyWithRes)
		if key == funcKey {
			log.GetLogger().Infof("update key %s, concurrency %d", key, info.InstanceMetaData.ConcurrentNum)
			worker.lbWrapper.SetConcurrency(info.InstanceMetaData.ConcurrentNum)
		}
		return true
	})
}

// Worker -
type Worker struct {
	// key: worker instance name
	// value: instance of *WorkerInstanceConnection
	WorkerInstancesConn *sync.Map

	bpWorkers *BackPressureWorkers

	lbWrapper *lbWrapper

	stateSingleFlight *singleflight.SingleFlight

	// key: worker instance name
	// value: state of this instance
	WorkerInstances map[string]*WorkerInstanceKeeper

	urn string

	lbType loadbalance.LBType

	// including proxy instance and normal worker instance
	workerType string

	nodeLB *loadbalance.NodeRR

	requestNum uint64

	funcKeyWithRes string
	// used for function bus WorkerInstances access
	mtx sync.RWMutex
}

// WorkerInstanceKeeper worker instance keeper
type WorkerInstanceKeeper struct {
	*WorkerInstance
	heartBeatClient *fhttp.Client
	stopCh          chan struct{}
	heartbeatOk     bool
	mux             sync.RWMutex
}

// ResetHeartbeatTasks reset all worker instance heartbeat tasks
func ResetHeartbeatTasks() {
	functionTaskWorker := FunctionLB.getProxyIns()
	if functionTaskWorker == nil {
		return
	}
	functionTaskWorker.mtx.RLock()
	for key, wik := range functionTaskWorker.WorkerInstances {
		if key == wik.InstanceID {
			wik.restartHeartbeat()
		}
	}
	functionTaskWorker.mtx.RUnlock()
}

// restartHeartbeat restart heartbeat task
func (wik *WorkerInstanceKeeper) restartHeartbeat() {
	log.GetLogger().Infof("restart worker instance[%s:%s] heartbeat task\n", wik.Host, wik.Port)
	wik.stopHeartbeat()
	wik.stopCh = make(chan struct{})
	go wik.startHeartbeat(constant.FConfig.HeartbeatConfig)
}

// stopHeartbeat stop heartbeat task
func (wik *WorkerInstanceKeeper) stopHeartbeat() {
	log.GetLogger().Infof("stopWorkerInsHeartbeat | %s | %s", wik.Host, wik.Port)
	nodeIP := splitWorkerInstanceKey(wik.WorkerInstanceKey)
	instance.GetWorkerInstanceManager().DeleteWorkerInstanceStatus(nodeIP)
	close(wik.stopCh)
}

// startHeartbeat start heartbeat task
func (wik *WorkerInstanceKeeper) startHeartbeat(heartbeatConfig *constant.HeartbeatConfig) {
	log.GetLogger().Infof("start worker instance[%s:%s] heartbeat task", wik.Host, wik.Port)
	instance.GetWorkerInstanceManager().AddWorkerInstanceStatus(
		splitWorkerInstanceKey(wik.WorkerInstanceKey), true)
	var url string
	if constant.FConfig.HTTPSConfig.HTTPSEnable {
		url = fmt.Sprintf("%s://%s:%s/heartbeat", "https", wik.Host, wik.Port)
	} else {
		url = fmt.Sprintf("%s://%s:%s/heartbeat", "http", wik.Host, wik.Port)
	}
	timeout := time.Duration(heartbeatConfig.HeartbeatTimeout)
	threshold := heartbeatConfig.HeartbeatTimeoutThreshold
	interval := heartbeatConfig.HeartbeatInterval
	count := int32(0)
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	defer ticker.Stop()
	for {
		select {
		case <-wik.stopCh:
			log.GetLogger().Infof("worker instance[%s:%s] heartbeat exit", wik.Host, wik.Port)
			return
		case <-ticker.C:
			go wik.doHeartbeat(url, timeout, &count, int32(threshold))
		}
	}
}

func (wik *WorkerInstanceKeeper) doHeartbeat(url string, timeout time.Duration, count *int32, threshold int32) {
	req := fhttp.AcquireRequest()
	resp := fhttp.AcquireResponse()
	req.SetRequestURI(url)
	req.Header.SetMethod(fhttp.MethodGet)
	addAuthorizationHeader(req)
	log.GetLogger().Debugf("worker instance[%s:%s] send heartbeat", wik.Host, wik.Port)
	err := wik.heartBeatClient.DoTimeout(req, resp, time.Second*timeout)
	code := resp.Header.StatusCode()
	fhttp.ReleaseRequest(req)
	fhttp.ReleaseResponse(resp)
	if err != nil || code != statuscode.Ok {
		log.GetLogger().Warnf("worker instance[%s:%s] heartbeat nok", wik.Host, wik.Port)
		curCount := atomic.AddInt32(count, 1)
		if curCount >= threshold && wik.heartbeatOk {
			wik.mux.Lock()
			wik.handleHeartbeatNotOK()
			wik.mux.Unlock()
			log.GetLogger().Warnf("worker instance[%s:%s] heartbeat failed", wik.Host, wik.Port)
		}
	} else {
		log.GetLogger().Debugf("worker instance[%s:%s] heartbeat ok", wik.Host, wik.Port)
		atomic.StoreInt32(count, 0)
		if !wik.heartbeatOk {
			wik.mux.Lock()
			wik.handleHeartbeatOK()
			wik.mux.Unlock()
		}
	}
}

func (wik *WorkerInstanceKeeper) handleHeartbeatNotOK() {
	if wik.heartbeatOk == true {
		nodeIP := splitWorkerInstanceKey(wik.WorkerInstanceKey)
		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus(nodeIP, false)
		log.GetLogger().Warnf("worker instance[%s:%s] heartbeat failed, record nodeIP", wik.Host, wik.Port)
		wik.heartbeatOk = false
	}
}

func (wik *WorkerInstanceKeeper) handleHeartbeatOK() {
	if wik.heartbeatOk == false {
		nodeIP := splitWorkerInstanceKey(wik.WorkerInstanceKey)
		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus(nodeIP, true)
		log.GetLogger().Warnf("worker instance[%s:%s] heartbeat ok, update status true", wik.Host, wik.Port)
		wik.heartbeatOk = true
	}
}

func splitWorkerInstanceKey(workerInstanceKey string) string {
	insKeySlice := strings.Split(workerInstanceKey, "@")
	nodeIPPortSlice := strings.Split(insKeySlice[0], ":")
	if len(nodeIPPortSlice) != workerInsKeyLen {
		err := fmt.Errorf("wrong key : %s", workerInstanceKey)
		log.GetLogger().Errorf("WorkerInstanceKey is split error %s.", err)
		return ""
	}
	return nodeIPPortSlice[0]
}

// GetStateSingleFlight -
func (w *Worker) GetStateSingleFlight() *singleflight.SingleFlight {
	return w.stateSingleFlight
}

func (w *Worker) getTargetNodeConnection(node string) (*WorkerInstanceConnection, error) {
	c, exist := w.WorkerInstancesConn.Load(node)
	if !exist {
		return nil, ErrNoWorkerInstancePool
	}
	workerInstanceConnection, ok := c.(*WorkerInstanceConnection)
	if !ok {
		log.GetLogger().Warnf("failed to assert type. workerInstanceConnection: %T", workerInstanceConnection)
		return nil, ErrNoWorkerInstancePool
	}
	return workerInstanceConnection, nil
}

func (w *Worker) getNodeConnection(node string) (*WorkerInstanceConnection, error) {
	next := w.nodeLB.Next(node)
	return w.getTargetNodeConnection(next)
}

func (w *Worker) getBackpressureConn(req *fhttp.Request,
	params *model.FunctionRequestInfo) (*WorkerInstanceConnection, error) {
	var wic *WorkerInstanceConnection
	var nodeWic *WorkerInstanceConnection
	var err error
	nodeWic, err = w.getTargetNodeConnection(params.TargetNode)
	if constant.FConfig.LoadBalanceConfig.DisableBackpressureBalance != 0 {
		req.Header.Set(constants.HeaderBackPressureNodeID, params.TargetNode)
		return nodeWic, err
	}
	wic, err = w.getWorkerInstanceConn(params.URN, params.TraceID, params.TargetInstance, params.HashMove)
	if nodeWic != nil && wic != nil && wic.Host == nodeWic.Host {
		req.Header.Set(constants.HeaderBackPressureNodeID, params.TargetNode)
	}
	return wic, err
}

func (w *Worker) getConnection(req *fhttp.Request,
	params *model.FunctionRequestInfo) (*WorkerInstanceConnection, error) {
	var wic *WorkerInstanceConnection
	var err error
	if params.StateID != "" {
		wic, err = w.getStateInstanceConn(params)
		return wic, err
	}
	if commonUtils.IsWebSocketFunc(params.BusinessType, params.EnableRemoteDebug) {
		wssInstanceConn := w.getWssInstanceConn(req, params)
		if wssInstanceConn == nil {
			return nil, ErrNoWorkerInstanceConnection
		}
		return wssInstanceConn, nil
	}
	if commonUtils.IsMCPSessionReq(params.MCPFunc, req.Header) {
		mcpInstanceConn := w.getMcpInstanceConn(req)
		if mcpInstanceConn == nil {
			params.InnerCode = snerror.MCPSessionInstanceInvalid
			return nil, snerror.New(snerror.MCPSessionInstanceInvalid,
				snerror.ErrText(snerror.MCPSessionInstanceInvalid))
		}
		return mcpInstanceConn, nil
	}
	if params.BackPressure {
		if params.TargetNode != "" {
			wic, err = w.getBackpressureConn(req, params)
		} else {
			wic, err = w.getNodeConnection(params.CurrentNode)
		}
	} else {
		wic, err = w.getWorkerInstanceConn(params.URN, params.TraceID, "", params.HashMove)
	}
	if wic != nil {
		params.CurrentNode = wic.Host
		// trigger back pressure for at most one time to avoid storm
		if params.StartNode != "" {
			req.Header.Set(constants.HeaderBackpressure, constant.FalseStr)
		} else {
			params.StartNode = wic.Host
		}
		ifProxyExist := FunctionLB.isProxyExist(wic.WorkerInstance)
		if !ifProxyExist {
			if w.workerType != util.StateIns {
				w.lbWrapper.Done(wic.WorkerInstance.InstanceID, 0)
			}
			err = errors.New("failed to connect busProxy, IP: " + wic.WorkerInstance.Host)
			return nil, err
		}
		return wic, err
	}

	return nil, err
}

func (w *Worker) clearBackPressure(params *model.FunctionRequestInfo) {
	params.BackPressure = false
	params.TargetNode = ""
	params.TargetInstance = ""
}

// doInvoke return (needBreak, needSleep, error), caller should first check error
func (w *Worker) doInvoke(req *fhttp.Request, resp *fhttp.Response,
	params *model.FunctionRequestInfo) (bool, bool, error) {
	if params.TryCount >= constant.FConfig.InvokeMaxRetryTimes {
		log.GetLogger().Warnf("failed to request worker for %d times, traceID %s",
			constant.FConfig.InvokeMaxRetryTimes, params.TraceID)
		return false, false, ErrServiceNotAvailable
	}
	if params.TryCount != 0 && w.workerType == util.ProxyIns {
		params.HashMove = true
	}
	wic, err := w.getConnection(req, params)
	if err != nil {
		if err.Error() == snerror.ErrText(snerror.MCPSessionInstanceInvalid) {
			log.GetLogger().Warnf("failed to find mcp pod instance %s with sessionID %s, traceID :%s",
				req.Header.Peek(constants.HeaderSSEPodName),
				req.Header.Peek(constants.HeaderSSESessionID), params.TraceID)
			return false, false, err
		}
		return false, false, err
	}
	if wic == nil {
		return false, false, nil
	}
	log.GetLogger().Infof("sendReq | %s | routeType %s | %s | %s", wic.Host, w.workerType, params.TraceID,
		urnutils.FuncURNtoShort(params.AnonymizeURN))
	params.RequestCount++
	err = wic.DoRequest(params, req, resp)

	if w.workerType != util.StateIns {
		w.lbWrapper.Done(wic.WorkerInstance.InstanceID, params.LastBusCost)
	}
	if commonUtils.IsWebSocketFunc(params.BusinessType, params.EnableRemoteDebug) {
		w.releaseWSRequest(params)
	}

	if err != nil && constant.FConfig.RetryConfig.InstanceExceptionRetry {
		log.GetLogger().Errorf("retry %d, connection of worker instance %s is not healthy: %s, traceID %s",
			params.TryCount, wic, err.Error(), params.TraceID)
		if err.Error() == requestTimeout {
			return false, false, err
		}
		params.TryCount++
		req.Header.Set(constants.HeaderRetryFlag, constant.TrueStr)
		if params.TryCount >= constant.FConfig.InvokeMaxRetryTimes {
			return false, false, err
		}
		return false, true, nil
	}
	params.InnerCode = getInnerCode(resp)
	// back pressure to make full use of workers, choose next available worker self
	if checkBackpressureWithoutWorker(params.InnerCode, resp) {
		w.clearBackPressure(params)
		params.BackPressure = true
		return false, false, nil
	}

	// need retry
	if !w.checkInnerCode(params.InnerCode, params) {
		log.GetLogger().Warnf("request in proxy %s for function %s failed: %d, traceID: %s, retry count: %d",
			wic.Host, params.AnonymizeURN, params.InnerCode, params.TraceID, params.TryCount)
		params.TryCount++
		w.clearBackPressure(params)
		return false, true, nil
	}

	if params.InnerCode != snerror.InnerResponseSuccessCode && params.InnerCode != constants.BackpressureCode {
		log.GetLogger().Warnf("function %s request in proxy %s failed, response: %d, cost: %s, traceID: %s",
			params.AnonymizeURN, wic.Host, params.InnerCode, params.LastBusCost, params.TraceID)
	}
	return true, false, nil
}

func checkBackpressureWithoutWorker(innerCode int, resp *fhttp.Response) bool {
	return innerCode == constants.BackpressureCode &&
		len(resp.Header.Peek(constants.HeaderBackPressureNodeID)) == 0
}

// If the invocation fails, it will traverse all instances in the list and retry until succeeds
func (w *Worker) nativeInvoke(reqInfo *model.FunctionRequestInfo, req *fhttp.Request,
	resp *fhttp.Response, backPressureNums int) error {
	reqInfo.TryCount = 0
	defer atomic.AddUint64(&w.requestNum, ^uint64(0))
	requestNum := atomic.AddUint64(&w.requestNum, 1)
	if requestNum > backpressureConcurrentNum || w.workerType == util.ProxyIns {
		req.Header.Set(constants.HeaderBackpressure, constant.FalseStr)
	} else if commonUtils.IsMCPSessionReq(reqInfo.MCPFunc, req.Header) {
		req.Header.Set(constants.HeaderBackpressure, constant.FalseStr)
	} else {
		req.Header.Set(constants.HeaderBackpressure, constant.TrueStr)
	}
	req.Header.Set(constants.HeaderBackpressureNums, strconv.Itoa(backPressureNums))
	if backPressureNums > backPressureTimesWithTimeout {
		start := time.Now()
		timeout := reqInfo.BackPressureDeadline.Sub(start)
		if timeout <= 0 {
			log.GetLogger().Errorf("request has reached backpressure deadline %s", reqInfo.TraceID)
			return errors.New("timeout")
		}
	}
	for {
		needBreak, needSleep, err := w.doInvoke(req, resp, reqInfo)
		if err != nil {
			return err
		}
		if needBreak {
			return nil
		}
		if needSleep {
			time.Sleep(RetryInterval * time.Millisecond)
		}
	}
}

// If retry is needed, false is returned. Otherwise, true is returned.
func (w *Worker) checkInnerCode(innerCode int, params *model.FunctionRequestInfo) bool {
	switch innerCode {
	case snerror.WorkerExitErrCode, snerror.UserFuncIsUpdatedCode, snerror.RefreshSilentFunc, snerror.ClientExitErrCode:
		return false
	default:
		return true
	}
}

func (w *Worker) getWorkerInstanceConn(urn, traceID, targetIns string,
	hashMove bool) (*WorkerInstanceConnection, error) {
	workerInstance := w.nextWorkerInstance(urn, traceID, targetIns, hashMove)
	if workerInstance == "" {
		log.GetLogger().Errorf("no worker instance connection was found")
		return nil, ErrNoWorkerInstanceConnection
	}

	workerInstanceIf, ok := w.WorkerInstancesConn.Load(workerInstance)
	if !ok {
		return nil, ErrNoWorkerInstancePool
	}
	wic, ok := workerInstanceIf.(*WorkerInstanceConnection)
	if !ok {
		return nil, ErrNoWorkerInstanceConnection
	}

	return wic, nil
}

func (w *Worker) getStateInstanceConn(params *model.FunctionRequestInfo) (*WorkerInstanceConnection, error) {
	instanceInterface, exist := w.WorkerInstancesConn.Load(params.StateID)
	var err error
	if !exist {
		// queries and updates the cache to etcd
		instanceInterface, err = w.GetStateSingleFlight().Do(params.StateID, func() (interface{}, error) {
			return w.queryStateInsFromETCD(params)
		})
		if err != nil || instanceInterface == nil {
			return nil, err
		}
		w.WorkerInstancesConn.Store(params.StateID, instanceInterface)
	}
	instance, ok := instanceInterface.(*WorkerInstanceConnection)
	if !ok {
		return nil, errors.New("failed to convert workerInstanceConnection")
	}
	return instance, nil
}

func (w *Worker) getWssInstanceConn(req *fhttp.Request,
	params *model.FunctionRequestInfo) *WorkerInstanceConnection {
	return NewWorkerInstanceConnection(params.WSReqInfo.DemandNodeIP, params.WSReqInfo.DemandNodePort,
		string(req.Header.Peek(constants.HeaderDemandWssInstance)), params.WSReqInfo.DemandNodeIP)
}

func (w *Worker) getMcpInstanceConn(req *fhttp.Request) *WorkerInstanceConnection {
	instance := req.Header.Peek(constants.HeaderSSEPodName)
	if len(instance) == 0 {
		return nil
	}
	instanceKey := fmt.Sprintf("defaultaz-#-%s", instance)
	workerInstanceIf, ok := w.WorkerInstancesConn.Load(instanceKey)
	if !ok {
		return nil
	}
	wic, ok := workerInstanceIf.(*WorkerInstanceConnection)
	if !ok {
		return nil
	}
	return wic
}

func (w *Worker) releaseWSRequest(params *model.FunctionRequestInfo) {
	log.GetLogger().Infof("begin release WS request, traceID: %s, instanceKey",
		params.TraceID, params.WSReqInfo.InstanceKey)
	req := &schedulerapi.ReleaseInstanceReq{
		TraceID:        params.TraceID,
		FuncKeyWithRes: params.WSReqInfo.FuncKeyWithRes,
		InstanceKey:    params.WSReqInfo.InstanceKey,
		ProcessorID:    params.WSReqInfo.ProcessorID,
		LeaseVersion:   params.WSReqInfo.LeaseVersion,
		IsWebSocketReq: true,
	}
	processorKey := urnutils.GetProcessorKey(
		req.InstanceKey, req.ProcessorID, req.LeaseVersion)
	client := remotescheduler.GetInstance().GetScheduler(req.FuncKeyWithRes)
	if client == nil {
		log.GetLogger().Warnf("failed to release ws instance, no faas-scheduler find,"+
			" traceID %s funcKeyWithRes %s processorKey %s",
			req.TraceID, urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), processorKey)
		return
	}
	err := releaseInstance(client, req)
	if err != nil {
		log.GetLogger().Warnf("failed to release instance, err %s,"+
			" traceID %s funcKeyWithRes %s processorKey %s",
			err.Error(), req.TraceID, urnutils.AnonymizeTenantKey(req.FuncKeyWithRes), processorKey)
		return
	}
	log.GetLogger().Infof("release WS request finished, traceID: %s", params.TraceID)
}

func releaseInstance(scheduler common.RPCClient, releaseReq *schedulerapi.ReleaseInstanceReq) error {
	return scheduler.ReleaseInstance(releaseReq)
}

func (w *Worker) queryStateInsFromETCD(params *model.FunctionRequestInfo) (*WorkerInstanceConnection, error) {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient.Client == nil {
		log.GetLogger().Warnf("failed to init state ETCD client")
		return nil, errors.New("failed to init state ETCD client")
	}
	stateWorkerKey := etcdkey.GetStateETCDPath(params.StateID)
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	values, err := stateClient.GetValues(defaultEtcdCtx, stateWorkerKey)
	if err != nil {
		time.AfterFunc(singleFlightTTL, func() {
			w.GetStateSingleFlight().Remove(params.StateID)
		})
		return nil, err
	}
	if len(values) == 0 {
		log.GetLogger().Warnf("stateKey: %s or stateID: %s does not exist, traceID: %s", params.StateKey,
			params.StateID, params.TraceID)
		params.InnerCode = snerror.NoSuchInstanceName
		return nil, snerror.New(snerror.NoSuchInstanceName, fmt.Sprintf(snerror.ErrText(snerror.NoSuchInstanceName),
			params.StateKey, params.StateID))
	}
	// parse workerInstanceConnection
	curStates := make(map[string]string, 1)
	err = json.Unmarshal([]byte(values[0]), &curStates)
	if err != nil {
		return nil, err
	}
	instance := curStates[params.StateID]
	taskWorkerKey := etcdkey.GetTaskETCDPath(instance)
	defaultEtcdCtx = etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	values, err = stateClient.GetValues(defaultEtcdCtx, taskWorkerKey)
	if err != nil {
		time.AfterFunc(singleFlightTTL, func() {
			w.GetStateSingleFlight().Remove(params.StateID)
		})
		return nil, err
	}
	if len(values) == 0 {
		return nil, errors.New(snerror.InternalErrorMsg)
	}
	node := &Node{}
	err = json.Unmarshal([]byte(values[0]), node)
	if err != nil {
		return nil, err
	}
	return NewWorkerInstanceConnection(node.NodeIP, node.NodePort, instance, ""), nil
}

// CheckAndAddWorkerInstance return value : exist and system error
func (w *Worker) CheckAndAddWorkerInstance(az, workerInstanceKey string, deployNode string) (bool, error) {
	if az == "" || workerInstanceKey == "" {
		return false, ErrEmptyWorkerInstanceName
	}

	infos, err := util.WorkerInstanceKeyConv(workerInstanceKey)
	if err != nil {
		return false, err
	}

	if az == constant.NativeAz {
		workerInstanceConn := NewWorkerInstanceConnection(infos.IP, infos.Port, infos.InstanceID, deployNode)
		if workerInstanceConn == nil {
			return false, errors.New("failed to connect a new worker instance")
		}
		workerInstanceConn.WorkerInstance.WorkerInstanceKey = workerInstanceKey
		// if ownerIP changed
		old, exist := w.WorkerInstancesConn.Load(infos.InstanceID)
		if exist {
			w.checkAndUpdateIP(old, infos)
		}
		w.WorkerInstancesConn.Store(infos.InstanceID, workerInstanceConn)
		w.WorkerInstancesConn.LoadOrStore(infos.IP, NewWorkerInstanceConnection(infos.IP, infos.Port,
			infos.IP, deployNode))
		if len(w.WorkerInstances) == 0 {
			w.WorkerInstances = make(map[string]*WorkerInstanceKeeper)
		}

		// NOTE: We cannot distinguish stateful and non-stateful function, so set value as empty
		if _, ok := w.WorkerInstances[infos.InstanceID]; !ok {
			w.addToLbByWorkerType(infos)
			w.nodeLB.Add(infos.IP)
			wik := &WorkerInstanceKeeper{
				WorkerInstance:  workerInstanceConn.WorkerInstance,
				heartBeatClient: util.GetHeartbeatClient(),
				heartbeatOk:     true,
				stopCh:          make(chan struct{}),
			}
			if w.urn == proxyURN {
				go wik.startHeartbeat(constant.FConfig.HeartbeatConfig)
				w.mtx.Lock()
				w.WorkerInstances[infos.InstanceID] = wik
				w.WorkerInstances[infos.IP] = wik
				w.mtx.Unlock()
			} else {
				w.WorkerInstances[infos.InstanceID] = wik
				w.WorkerInstances[infos.IP] = wik
			}
			return false, nil
		}
		return true, nil
	}

	return false, nil
}

func (w *Worker) addToLbByWorkerType(infos *util.WorkerInstance) {
	if w.workerType == util.NormalWorkerIns {
		w.addToLB(infos.InstanceID + util.WorkerInstanceAZSep + infos.IP)
	} else {
		w.addToLB(infos.InstanceID)
	}
}

// checkAndUpdateIP If the old IP address is different from the new, delete the old and add the new.
func (w *Worker) checkAndUpdateIP(old interface{}, infos *util.WorkerInstance) {
	workerInstanceConnection, ok := old.(*WorkerInstanceConnection)
	if ok {
		oldHost := workerInstanceConnection.Host
		if oldHost != infos.IP {
			w.nodeLB.Del(oldHost)
			w.nodeLB.Add(infos.IP)
		}
	}
}

func (w *Worker) deleteWorkerInstance(az, workerInstanceKey string, nodeLost bool) {
	infos, err := util.WorkerInstanceKeyConv(workerInstanceKey)
	if err != nil {
		return
	}

	if az == constant.NativeAz {
		w.WorkerInstancesConn.Delete(infos.InstanceID)
		if nodeLost {
			w.WorkerInstancesConn.Delete(infos.IP)
		}
		if wki, ok := w.WorkerInstances[infos.InstanceID]; ok {
			if w.urn == proxyURN {
				wki.stopHeartbeat()
				w.mtx.Lock()
				delete(w.WorkerInstances, infos.InstanceID)
				delete(w.WorkerInstances, infos.IP)
				w.mtx.Unlock()
			} else {
				delete(w.WorkerInstances, infos.InstanceID)
				delete(w.WorkerInstances, infos.IP)
			}
			w.nodeLB.Del(infos.IP)
		}

		w.delFromLbByWorkerType(infos)

		log.GetLogger().Infof("delNativeWorkerIns | %s | %s | %s", infos.IP, infos.Port,
			urnutils.ExtractPodName(infos.InstanceID))
	}
}

func (w *Worker) delFromLbByWorkerType(infos *util.WorkerInstance) {
	if w.workerType == util.NormalWorkerIns {
		w.delFromLB(infos.InstanceID + util.WorkerInstanceAZSep + infos.IP)
	} else {
		w.delFromLB(infos.InstanceID)
	}
}

func (w *Worker) nextWorkerInstance(urn, traceID, targetIns string, hashMove bool) string {
	req := &loadbalance.Request{
		Name:           urn,
		TraceID:        traceID,
		Timestamp:      time.Now(),
		TargetInstance: targetIns,
		RequestNum:     atomic.LoadUint64(&w.requestNum),
	}
	workerInstanceIf := w.lbWrapper.NextWithRequest(req, hashMove)
	if workerInstanceIf == nil {
		return ""
	}
	workerInstance, ok := workerInstanceIf.(service.ScheduleResult)
	if ok {
		return workerInstance.InstanceKey
	}

	workerInstanceStr, ok := workerInstanceIf.(string)
	if ok {
		return workerInstanceStr
	}

	log.GetLogger().Errorf("can't get next worker instance from type %T traceID %s", workerInstanceIf, traceID)
	return ""
}

func (w *Worker) addToLB(workerInstance string) {
	w.lbWrapper.Add(workerInstance)
}

func (w *Worker) delFromLB(workerInstance string) {
	w.lbWrapper.Remove(workerInstance)
}

// String return instances
func (w *Worker) String() string {
	var instances string
	for instance := range w.WorkerInstances {
		instances += instance + "|"
	}

	if len(instances) == 0 {
		return ""
	}

	return instances[0 : len(instances)-1]
}

// NewWorkerInstanceConnection -
func NewWorkerInstanceConnection(host, port, instanceID string, deployNode string) *WorkerInstanceConnection {
	if host == "" || port == "" {
		return nil
	}
	log.GetLogger().Infof("newWorkerInsConn | %s | %s | %s | %s", host, port,
		urnutils.ExtractPodName(instanceID), deployNode)
	workerInstanceConn := &WorkerInstanceConnection{
		WorkerInstance: &WorkerInstance{
			Host:       host,
			Port:       port,
			InstanceID: instanceID,
			Node:       deployNode,
		},
		conn: util.GetGlobalClient(),
	}

	return workerInstanceConn
}

// WorkerInstance -
type WorkerInstance struct {
	Host              string
	Port              string
	InstanceID        string
	Node              string
	WorkerInstanceKey string
}

func initProxyRequest(proxyReq *fhttp.Request) {
	proxyReq.SetRequestURI(InvokePath)
	setLubanBody(proxyReq)
	proxyReq.Header.SetMethod("POST")
}

// makeProxyRequest -
func makeProxyRequest(proxyReq *fhttp.Request, host, port string) {
	proxyReq.SetHost(host + ":" + port)
	if constant.FConfig.HTTPSConfig.HTTPSEnable {
		proxyReq.URI().SetScheme("https")
	} else {
		proxyReq.URI().SetScheme("http")
	}
	proxyReq.Header.ResetConnectionClose()

	addAuthorizationHeader(proxyReq)
}

func addAuthorizationHeader(proxyReq *fhttp.Request) {
	// authorization
	authorization, timestamp := localauth.SignLocally(constant.FConfig.LocalAuth.AKey,
		constant.FConfig.LocalAuth.SKey, util.AppID, constant.FConfig.LocalAuth.Duration)
	proxyReq.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	proxyReq.Header.Set(constants.HeaderAuthorization, authorization)
}

func setLubanBody(req *fhttp.Request) {
	lubanID := req.Header.Peek(constants.HeaderLuBanGTraceID)
	if len(lubanID) == 0 {
		return
	}
	bodyMap := make(map[string]interface{}, defaultBodyMap)
	if err := json.Unmarshal(req.Body(), &bodyMap); err != nil {
		return
	}
	bodyMap[constants.HeaderLuBanNTraceID] = string(req.Header.Peek(constants.HeaderLuBanNTraceID))
	bodyMap[constants.HeaderLuBanGTraceID] = string(lubanID)
	bodyMap[constants.HeaderLuBanSpanID] = string(req.Header.Peek(constants.HeaderLuBanSpanID))
	bodyMap[constants.HeaderLuBanEvnID] = string(req.Header.Peek(constants.HeaderLuBanEvnID))
	bodyMap[constants.HeaderLuBanEventID] = string(req.Header.Peek(constants.HeaderLuBanEventID))
	bodyMap[constants.HeaderLuBanDomainID] = string(req.Header.Peek(constants.HeaderLuBanDomainID))
	rsp, err := json.Marshal(bodyMap)
	if err != nil {
		return
	}
	req.SetBody(rsp)
}

// WorkerInstanceConnection -
type WorkerInstanceConnection struct {
	*WorkerInstance

	conn *fhttp.Client
}

// DoRequest -
func (wic *WorkerInstanceConnection) DoRequest(reqInfo *model.FunctionRequestInfo, req *fhttp.Request,
	resp *fhttp.Response) error {
	log.GetLogger().Debugf("entry]Send req, stateKey: %s, stateID: %s, instanceID: %s, address: %s:%s, traceID:%s",
		reqInfo.StateKey, reqInfo.StateID, wic.InstanceID, wic.Host, wic.Port, reqInfo.TraceID)
	makeProxyRequest(req, wic.Host, wic.Port)
	start := time.Now()
	timeout := reqInfo.Deadline.Sub(start)
	if timeout <= 0 {
		log.GetLogger().Errorf("request has reached deadline %s", reqInfo.TraceID)
		return errors.New("timeout")
	}
	err := wic.conn.DoTimeout(req, resp, timeout)
	if err != nil {
		resp.Header.Set(constants.HeaderInnerCode, strconv.Itoa(statuscode.InternalError))
		resp.SetStatusCode(http.StatusInternalServerError)
		return err
	}
	cost := time.Since(start)
	reqInfo.LastBusCost = cost
	reqInfo.AllBusCost += cost
	log.GetLogger().Debugf("response from %s:%s:%s with http status code %d, traceID:%s",
		wic.Host, wic.Port, wic.InstanceID, resp.StatusCode(), reqInfo.TraceID)
	return nil
}

// String return Host and Port
func (wic *WorkerInstanceConnection) String() string {
	return wic.Host + ":" + wic.Port
}

// WorkerInstanceReadyCheck -
type WorkerInstanceReadyCheck struct {
	Host       string
	HealthPort string

	ready bool
	done  chan struct{}
}

// AddNode -
func (n *Nodes) AddNode(node *Node, nodeKey string) {
	nodeID := getNodeIDFromKey(nodeKey)
	if nodeID == "" {
		return
	}
	n.updateNodeList(node, nodeID)
}

func (n *Nodes) updateNodeList(node *Node, nodeID string) {
	n.NodeList.Store(nodeID, &Node{
		IP:       node.IP,
		Port:     node.Port,
		Status:   node.Status,
		P2pPort:  node.P2pPort,
		NodeIP:   node.NodeIP,
		NodePort: node.NodePort,
	})
}

func getNodeIDFromKey(key string) string {
	strs := strings.Split(key, "/")
	if len(strs) != NodeEtcdKeyLen {
		return ""
	}
	return strs[12]
}

// RemoveNode remove node from node map
func (n *Nodes) RemoveNode(nodeKey string) {
	nodeID := getNodeIDFromKey(nodeKey)
	if nodeID == "" {
		return
	}
	n.NodeList.Delete(nodeID)
}

// GetNodeNum get node num from nodeList sync map
func (n *Nodes) GetNodeNum() int {
	length := 0
	n.NodeList.Range(func(key, value interface{}) bool {
		length++
		return true
	})
	return length
}
