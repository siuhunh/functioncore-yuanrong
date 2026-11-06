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

// Package servicediscovery is used for service discovery
package servicediscovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/vpa"
	coretypes "functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/proxyview"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/remotescheduler"
	"functioncore/pkg/functionbus/utils"
)

const (
	nameOfFunctionTask        = "function-task"
	singleFlightKeyTTL        = 10 * time.Second
	recoverRetryTime          = 10
	recoverRetryInterval      = 15 * time.Second
	getFuncMetaTimeout        = 5 * time.Second
	functionClusterKeyLen     = 6
	functionClusterKeyIdx     = 5
	clusterFrontendKeyLen     = 7
	clusterFrontendClusterIdx = 4
	clusterFrontendIPIdx      = 5
)

var (
	sf         = singleflight.NewSingleFlight()
	selfNodeIP = os.Getenv(constants.HostIPEnvKey)
)

// Provider saves the Instance info
type Provider struct {
	InstanceID              string
	IP                      string
	Port                    string
	Cluster                 string
	P2pPort                 string
	Status                  string
	NodeIP                  string
	OwnerIP                 string
	NodePort                string
	Applier                 string
	StateID                 string
	CPU                     int
	Memory                  int
	PreviousWillDeleteState bool
	WillDelete              bool
	BusinessType            string
	IsDirectFunc            bool
	HasInitializer          bool
	PodUID                  string
	ContainerIDs            vpa.ContainerInfo
	Resource                vpa.PodResourceInfo
	ViewResource            vpa.ViewResourceInfo
}

// InstEvent for type of event
type InstEvent int32

// InstInfo include pod info
type InstInfo struct {
	FunctionName    string // FunctionName
	FunctionVersion string // FunctionVersion
	PodName         string // PodName is instanceID
}

// Registry for etcd client
type Registry struct {
	Providers             map[string]map[string]*Provider // a function(one level index) has many instances(two level index,pod), one instance has a Provider which may have bind many state
	ProvidersMu           sync.RWMutex
	ProvidersHasSynced    bool
	States                sync.Map
	FuncMeta              map[string]*FunctionMetaInfo // FuncMeta store the poolSize of functions
	funcMetaLock          sync.RWMutex
	FuncAvailableClusters map[string][]string // function available clusters
	funcClusterLock       sync.RWMutex
	ClusterFrontends      map[string][]string // cluster frontend instance
	ClusterFrontendsLock  sync.RWMutex
	callBack              func(string, interface{}, bool) // callBack for Registry to handle some specific events
	addWorker             []operateWorker                 // callBack for Registry to handle some specific events
	deleteWorker          operateWorker                   // callBack for Registry to handle some specific events
	enableNode            operateNode                     // callBack for Registry to handle some specific events
	disableNode           operateNode                     // callBack for Registry to handle some specific events
	recoverLongRun        recoverLongRun
	cbFinishedRecovering  []cbFinishedRecovering // callback after finished recovering all worker info from ETCD
	CAEFuncVersion        sync.Map               // CAEFuncVersion store cae function version
	funcVersionMu         sync.Mutex
	providerSyncCount     int64
}

type recoverLongRun func(router types.InstanceRouter)
type cbFinishedRecovering func(status bool)

// InstanceEvent saves worker instance value
type InstanceEvent struct {
	IP             string               `json:"ip"`
	Port           string               `json:"port"`
	Status         string               `json:"status"`
	Cluster        string               `json:"cluster"`
	P2pPort        string               `json:"p2pPort"`
	NodeIP         string               `json:"nodeIP"`
	OwnerIP        string               `json:"ownerIP"`
	NodePort       string               `json:"nodePort"`
	Applier        string               `json:"applier"`
	CPU            int                  `json:"cpu" valid:"optional"`
	Memory         int                  `json:"memory" valid:"optional"`
	StateID        string               `json:"stateID" valid:"optional"`
	WillDelete     bool                 `json:"willDelete" valid:"optional"`
	BusinessType   string               `json:"businessType" valid:"optional"`
	IsDirectFunc   bool                 `json:"isDirectFunc"`
	HasInitializer bool                 `json:"hasInitializer"`
	PodUID         string               `json:"podUID,omitempty"`
	ContainerIDs   vpa.ContainerInfo    `json:"containerIDs,omitempty"`
	Resource       vpa.PodResourceInfo  `json:"resource,omitempty"`
	ViewResource   vpa.ViewResourceInfo `json:"viewResource,omitempty"`
}

// FunctionMetaInfo saves functionMeta Info
type FunctionMetaInfo struct {
	Source etcd3.EtcdType `json:"-"`
	// "functionVersionUrn":"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-mytest-functionb:$latest"
	FuncMetaData     FuncMetaData     `json:"funcMetaData" valid:",optional"`
	InstanceMetaData InstanceMetaData `json:"instanceMetaData" valid:",optional"`
	ResourceMetaData ResourceMetaData `json:"resourceMetaData" valid:"optional"`
	ExtendedMetaData ExtendedMetaData `json:"extendedMetaData" valid:"optional"`
}

// FuncMetaData define meta data of functions
type FuncMetaData struct {
	FunctionVersionURN       string `json:"functionVersionUrn"`
	IsStatefulFunction       bool   `json:"isStatefulFunction" valid:"option"`
	IsBridgeFunction         bool   `json:"isBridgeFunction" valid:"optional"`
	Timeout                  int64  `json:"timeout" valid:"optional"`
	IsCustomContainer        bool   `json:"-"`
	BusinessType             string `json:"businessType"`
	EnableRemoteDebug        bool   `json:"enableRemoteDebug"`
	MicCircuitBreakerEnabled bool   `json:"microservice_circuit_breaker_enabled" valid:"optional"`
	EnableWebsocket          bool   `json:"enable_websocket" valid:"optional"`
}

// UnmarshalJSON -
func (f *FuncMetaData) UnmarshalJSON(data []byte) error {
	type Alias FuncMetaData
	alias := &struct {
		Runtime string `json:"runtime"`
		*Alias
	}{
		Alias: (*Alias)(f),
	}
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	f.IsCustomContainer = alias.Runtime == "custom image"
	return nil
}

// InstanceMetaData define instance meta data of functions
type InstanceMetaData struct {
	ConcurrentNum int  `json:"concurrentNum"`
	IdleMode      bool `json:"idleMode"`
}

// ExtendedMetaData define extended meta data of function
type ExtendedMetaData struct {
	Initializer     Initializer `json:"initializer"`
	EnableWebsocket bool        `json:"enable_websocket" valid:"optional"`
}

// Initializer define initializer config of function
type Initializer struct {
	InitializerTimeout int64 `json:"initializer_timeout"`
}

// ResourceMetaData define instance meta data of resource for cpu and memory
type ResourceMetaData struct {
	CPU                int   `json:"cpu"`
	Memory             int   `json:"memory"`
	GpuMemory          int64 `json:"gpu_memory" valid:"optional"`
	EnableTmpExpansion bool  `json:"enable_tmp_expansion" valid:"optional"`
}

type operateWorker func(
	funcKeyWithResource, instanceID string, provider *Provider, isSynced bool)

type operateNode func(nodeIP string)

const (
	mapInitSize = 16
	// InstOffline is instance event start
	InstOffline       InstEvent = 0
	instanceKeyMinLen           = 11

	validPodPathCount   = 13
	validStatePathCount = 13
	validFuncPathCount  = 11
	instanceInfoCount   = 4
)

// index of items
const (
	tenantPosition       = 6
	functionNamePosition = 8
	versionPosition      = 10
	instancePosition     = 12
	instanceSeparator    = "-"
	instanceCPUIndex     = 3
	instanceMemoryIndex  = 4
)

var (
	registry *Registry
	once     sync.Once
)

// GetRegistry get singleton registry
func GetRegistry() *Registry {
	if registry == nil {
		once.Do(func() {
			registry = newRegistry()
		})
	}
	return registry
}

// newRegistry creates new registry
func newRegistry() *Registry {
	return &Registry{
		Providers:             make(map[string]map[string]*Provider, mapInitSize),
		ProvidersHasSynced:    false,
		callBack:              nil,
		FuncMeta:              make(map[string]*FunctionMetaInfo, mapInitSize),
		FuncAvailableClusters: make(map[string][]string, mapInitSize),
		ClusterFrontends:      make(map[string][]string, mapInitSize),
	}
}

// RegisterAddWorker -
func (r *Registry) RegisterAddWorker(addWorker ...operateWorker) {
	r.addWorker = addWorker
}

// RegisterDeleteWorker -
func (r *Registry) RegisterDeleteWorker(deleteWorker operateWorker) {
	r.deleteWorker = deleteWorker
}

// RegisterEnableNode -
func (r *Registry) RegisterEnableNode(enableNode operateNode) {
	r.enableNode = enableNode
}

// RegisterDisableNode -
func (r *Registry) RegisterDisableNode(disableNode operateNode) {
	r.disableNode = disableNode
}

// RegisterRecoverLongRun register recover long run callback
func (r *Registry) RegisterRecoverLongRun(recover recoverLongRun) {
	r.recoverLongRun = recover
}

// RegisterCallbackFinishedRecovering register finished recovering callback
func (r *Registry) RegisterCallbackFinishedRecovering(cb ...cbFinishedRecovering) {
	r.cbFinishedRecovering = cb
}

// UpdateProviderOnObserve updates provider
func (r *Registry) UpdateProviderOnObserve(ev *etcd3.Event) {
	items := strings.Split(ev.Key, "/")
	if !r.updateProviders(ev.Value, items, ev.Source) {
		log.GetLogger().Errorf("failed to Update provider key: %s", ev.Key)
	}
}

// DeleteProviderOnObserve deletes provider
func (r *Registry) DeleteProviderOnObserve(ev *etcd3.Event) {
	items := strings.Split(ev.Key, "/")
	r.deleteProviders(items, ev.Source)
}

// AddProviderSyncCount add provider sync count
func (r *Registry) AddProviderSyncCount() {
	atomic.AddInt64(&r.providerSyncCount, 1)
}

// SyncedProviderOnObserve mark providers sync successfully for the first time
// The reason for adding this function is the scaling instance only use returned instance info
// to create broker and avoid the problem of etcd write slowly
func (r *Registry) SyncedProviderOnObserve(event *etcd3.Event) {
	if atomic.AddInt64(&r.providerSyncCount, -1) > 0 {
		log.GetLogger().Infof("waiting other provider synced")
		return
	}
	log.GetLogger().Infof("providers sync successfully for the first time, time: %s", string(event.Value))
	r.ProvidersMu.Lock()
	r.ProvidersHasSynced = true
	r.ProvidersMu.Unlock()
	go r.RecoverLongRunWithRetry()
}

// UpdateSchedulerOnObserve updates scheduler
func (r *Registry) UpdateSchedulerOnObserve(ev *etcd3.Event) {
	scheduler := types.FaaSServiceInfo{}
	err := json.Unmarshal(ev.Value, &scheduler)
	if err != nil {
		log.GetLogger().Warnf("failed to Unmarshal etcd event"+
			"to FaaSServiceInfo, ev.key: %s, ev.Value: %s", ev.Key, string(ev.Value))
		return
	}
	manager := remotescheduler.GetInstanceManager()
	if manager != nil {
		manager.AddScheduler(scheduler.InstanceID, &scheduler)
	}
}

// DeleteSchedulerOnObserve delete scheduler
func (r *Registry) DeleteSchedulerOnObserve(ev *etcd3.Event) {
	scheduler := types.FaaSServiceInfo{}
	err := json.Unmarshal(ev.PrevValue, &scheduler)
	if err != nil {
		log.GetLogger().Warnf("failed to Unmarshal etcd event"+
			"to FaaSServiceInfo, ev.key: %s, ev.PrevValue: %s", ev.Key, string(ev.PrevValue))
		return
	}
	manager := remotescheduler.GetInstanceManager()
	if manager != nil {
		manager.DeleteScheduler(scheduler.InstanceID)
	}
}

// RecoverLongRunWithRetry recover terminating long run instance with retry
func (r *Registry) RecoverLongRunWithRetry() {
	for i := 0; i < recoverRetryTime; i++ {
		err := r.RecoverLongRun()
		if err == nil {
			log.GetLogger().Infof("recover finished")
			break
		}
		log.GetLogger().Warnf("failed to recover, retry: %d, err: %s", i, err.Error())
		time.Sleep(recoverRetryInterval)
	}
	for _, cb := range r.cbFinishedRecovering {
		cb(true)
	}
}

// RecoverLongRun recover terminating long run instance
func (r *Registry) RecoverLongRun() error {
	ctx, cancel := context.WithTimeout(context.Background(), recoverRetryInterval)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		wmhealth.GetWorkerManagerBaseURL()+"/worker-manager/v1/functions/longrun/"+selfNodeIP, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to new request, err: %s", err.Error())
		return err
	}
	utils.FillInWorkerManagerRequestHeaders(req)
	resp, err := utils.GetWorkerManagerClient().Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to list from wm, err: %s", err.Error())
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status code %d", resp.StatusCode)
	}
	var data map[string]types.InstanceRouter
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		log.GetLogger().Errorf("failed to decode, err: %s", err)
		return err
	}
	for _, v := range data {
		go func(v types.InstanceRouter) {
			r.recoverLongRun(v)
		}(v)
	}
	return nil
}

// WorkerPathFilter filters worker path
// /sn/workers/business/yrk/tenant/i1fe5394/function/app-storage-list/version/$latest/defaultaz/default-#-yhagi1fe-app-storage-list-l.s.t-980699502
// |    seg1    | seg2  |  seg3 |seg4| seg5 |  seg6  |  seg7  |    seg8        | seg9  | seg10 |  seg11  |   seg12
func (r *Registry) WorkerPathFilter(path string) bool {
	items := strings.Split(path, "/")
	if len(items) != validPodPathCount {
		return false
	}

	if items[2] != "workers" || items[7] != "function" || items[9] != "version" {
		return false
	}
	return true
}

// FaaSSchedulerPathFilter /sn/faas-scheduler/instances/cluster001/7.218.100.25/faas-scheduler-59ddbc4b75-8xdjf
func (r *Registry) FaaSSchedulerPathFilter(key string) bool {
	return strings.Contains(key, common.WatchPathSchedulerPrefix)
}

// UpdateStateOnObserve updates state
func (r *Registry) UpdateStateOnObserve(ev *etcd3.Event) {
	items := strings.Split(ev.Key, "/")
	r.updateState(ev.Value, items)
}

// DeleteStateOnObserve updates state
func (r *Registry) DeleteStateOnObserve(ev *etcd3.Event) {
	items := strings.Split(ev.Key, "/")
	r.deleteState(ev.PrevValue, items)
}

// StatePathFilter filters state path
// /sn/stateworkers/business/yrk/tenant/i1fe5394/function/app-storage-list/version/$latest/defaultaz
// |    seg1    | seg2       |  seg3 |seg4| seg5 |  seg6  |   seg7 |  seg8          |seg9   | seg10 | seg11  |
func (r *Registry) StatePathFilter(path string) bool {
	items := strings.Split(path, "/")
	if len(items) != validStatePathCount { // 13 new statePath len
		return false
	}
	if items[2] != "stateworkers" || items[7] != "function" || items[9] != "version" {
		return false
	}
	return true
}

// UpdateFuncOnObserver -
func (r *Registry) UpdateFuncOnObserver(ev *etcd3.Event) {
	functionRepoInfo := FunctionMetaInfo{}
	err := json.Unmarshal(ev.Value, &functionRepoInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal function update event%s : %s", ev.Value, err)
		return
	}
	functionRepoInfo.Source = ev.Source
	funcVersionURN, err := urnutils.GetFunctionInfo(functionRepoInfo.FuncMetaData.FunctionVersionURN)
	if err != nil {
		log.GetLogger().Errorf("URN %s in wrong format: %s", functionRepoInfo.FuncMetaData.
			FunctionVersionURN, err.Error())
		return
	}
	if functionRepoInfo.InstanceMetaData.ConcurrentNum == 0 &&
		functionRepoInfo.FuncMetaData.BusinessType != constants.BusinessTypeCAE {
		log.GetLogger().Warnf("invalid ConcurrentNum %d, urn: %s", functionRepoInfo.InstanceMetaData.ConcurrentNum,
			functionRepoInfo.FuncMetaData.FunctionVersionURN)
		return
	}
	if funcVersionURN.Version == "" {
		log.GetLogger().Errorf("no version in FunctionVersionURN %s", functionRepoInfo.
			FuncMetaData.FunctionVersionURN)
		return
	}

	funcKey := utils.GetFuncKey(funcVersionURN.TenantID, funcVersionURN.Name, funcVersionURN.Version)
	if !r.checkAndUpdate(ev.Source, funcKey, &functionRepoInfo) {
		return
	}
	log.GetLogger().Debugf("update funcMeta of %s/%s/%s with businessType %s from %s etcd",
		urnutils.Anonymize(funcVersionURN.TenantID), funcVersionURN.Name, funcVersionURN.Version,
		functionRepoInfo.FuncMetaData.BusinessType, ev.Source)
	if r.callBack != nil {
		r.callBack(common.UpdFunctionEvent, funcKey, false)
	}
	if functionRepoInfo.FuncMetaData.BusinessType == constants.BusinessTypeCAE {
		log.GetLogger().Debugf("update CAE function cache, funcName:%s, version:%s",
			funcVersionURN.Name, funcVersionURN.Version)
		r.updateCAEFuncVersion(funcVersionURN.TenantID, funcVersionURN.Name, funcVersionURN.Version)
	}
}

// UpdateFuncAvailableCluster update function available clusters
func (r *Registry) UpdateFuncAvailableCluster(ev *etcd3.Event) {
	clusters := make([]string, 0)
	err := json.Unmarshal(ev.Value, &clusters)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal function cluster update event: %s, reason: %s", ev.Value, err)
		return
	}
	key := ev.Key
	parts := strings.Split(key, "/")
	if len(parts) != functionClusterKeyLen {
		log.GetLogger().Warnf("invalied function cluster key:%s, ignore it", key)
		return
	}
	funcURN := parts[functionClusterKeyIdx]
	r.funcClusterLock.Lock()
	funcURN, err = urnutils.ConvertFgUrnToSnUrn(funcURN)
	if err != nil {
		log.GetLogger().Warnf("funcURN:%s convert failed:%s, ignore update", funcURN, err.Error())
		return
	}
	log.GetLogger().Infof("add funcURN:%s available cluster:%#v", funcURN, clusters)
	r.FuncAvailableClusters[funcURN] = clusters
	r.funcClusterLock.Unlock()
}

// DeleteFuncAvailableCluster delete function available clusters
func (r *Registry) DeleteFuncAvailableCluster(ev *etcd3.Event) {
	key := ev.Key
	parts := strings.Split(key, "/")
	if len(parts) != functionClusterKeyLen {
		log.GetLogger().Warnf("invalied function cluster key:%s, ignore it", key)
		return
	}
	funcURN := parts[functionClusterKeyIdx]
	var err error
	funcURN, err = urnutils.ConvertFgUrnToSnUrn(funcURN)
	if err != nil {
		log.GetLogger().Warnf("funcURN:%s convert failed:%s, ignore delete", funcURN, err.Error())
		return
	}
	log.GetLogger().Infof("delete funcURN:%s available cluster", funcURN)
	r.funcClusterLock.Lock()
	delete(r.FuncAvailableClusters, funcURN)
	r.funcClusterLock.Unlock()
}

// UpdateFrontendInstances update frontend instance info
func (r *Registry) UpdateFrontendInstances(ev *etcd3.Event) {
	key := ev.Key
	parts := strings.Split(key, "/")
	if len(parts) != clusterFrontendKeyLen {
		log.GetLogger().Warnf("invalied cluster frontend key:%s, ignore it", key)
		return
	}
	status := string(ev.Value)
	if status != "active" {
		r.ClusterFrontendsLock.Lock()
		cluster := parts[clusterFrontendClusterIdx]
		newIPs := make([]string, 0)
		ips := r.ClusterFrontends[cluster]
		for _, val := range ips {
			if val != parts[clusterFrontendIPIdx] {
				newIPs = append(newIPs, val)
			}
		}
		if len(newIPs) == 0 {
			delete(r.ClusterFrontends, cluster)
		} else {
			r.ClusterFrontends[cluster] = newIPs
		}
		r.ClusterFrontendsLock.Unlock()
		return
	}
	r.ClusterFrontendsLock.Lock()
	cluster := parts[clusterFrontendClusterIdx]
	ips := r.ClusterFrontends[cluster]
	if ips == nil {
		ips = make([]string, 0)
		r.ClusterFrontends[cluster] = ips
	}
	r.ClusterFrontends[cluster] = append(r.ClusterFrontends[cluster], parts[clusterFrontendIPIdx])
	r.ClusterFrontendsLock.Unlock()
}

// DeleteFrontendInstances delete frontend instance info
func (r *Registry) DeleteFrontendInstances(ev *etcd3.Event) {
	key := ev.Key
	parts := strings.Split(key, "/")
	if len(parts) != clusterFrontendKeyLen {
		log.GetLogger().Warnf("invalied cluster frontend key:%s, ignore it", key)
		return
	}
	r.ClusterFrontendsLock.Lock()
	cluster := parts[clusterFrontendClusterIdx]
	newIPs := make([]string, 0)
	ips := r.ClusterFrontends[cluster]
	for _, val := range ips {
		if val != parts[clusterFrontendIPIdx] {
			newIPs = append(newIPs, val)
		}
	}
	if len(newIPs) == 0 {
		delete(r.ClusterFrontends, cluster)
	} else {
		r.ClusterFrontends[cluster] = newIPs
	}
	r.ClusterFrontendsLock.Unlock()
}

func (r *Registry) checkAndUpdate(source etcd3.EtcdType, funcKey string, data *FunctionMetaInfo) bool {
	if source == etcd3.CAEMeta {
		r.funcMetaLock.Lock()
		old, ok := r.FuncMeta[funcKey]
		if ok && old.Source == etcd3.Meta {
			r.funcMetaLock.Unlock()
			log.GetLogger().Infof("function from meta etcd exists, skip update from cae, key %s", funcKey)
			return false
		}
		r.FuncMeta[funcKey] = data
		r.funcMetaLock.Unlock()
	} else {
		r.funcMetaLock.Lock()
		r.FuncMeta[funcKey] = data
		r.funcMetaLock.Unlock()
		sf.Remove(funcKey)
	}
	return true
}

func (r *Registry) updateCAEFuncVersion(tenantID, funcName, funcVersion string) {
	r.funcVersionMu.Lock()
	defer r.funcVersionMu.Unlock()

	funcKey := fmt.Sprintf("%s/%s", tenantID, funcName)
	data, exist := r.CAEFuncVersion.Load(funcKey)

	if exist {
		versionMap, ok := data.(map[string]struct{})
		if !ok {
			log.GetLogger().Warnf("failed to assert type, type: %T, funcKey: %s", data, funcKey)
			return
		}
		versionMap[funcVersion] = struct{}{}
		r.CAEFuncVersion.Store(funcKey, versionMap)
	} else {
		versionMap := make(map[string]struct{})
		versionMap[funcVersion] = struct{}{}
		r.CAEFuncVersion.Store(funcKey, versionMap)
	}
}

func checkSynced(actual interface{}, functionRepoInfo FunctionMetaInfo) bool {
	checkedActual, ok := actual.(*FunctionMetaInfo)
	if !ok {
		log.GetLogger().Warnf("failed to assert type")
		return false
	}
	return checkedActual.ResourceMetaData.Memory == functionRepoInfo.ResourceMetaData.Memory &&
		checkedActual.ResourceMetaData.CPU == functionRepoInfo.ResourceMetaData.CPU &&
		checkedActual.InstanceMetaData.ConcurrentNum == functionRepoInfo.InstanceMetaData.ConcurrentNum &&
		checkedActual.FuncMetaData.IsStatefulFunction == functionRepoInfo.FuncMetaData.IsStatefulFunction
}

// DeleteFuncOnObserver -
func (r *Registry) DeleteFuncOnObserver(ev *etcd3.Event) {
	path := ev.Key
	items := strings.Split(path, "/")
	if len(items) != validFuncPathCount {
		return
	}
	// /sn/functions/business/yrk/tenant/i1fe5394/function/0-ttl-testfuncsnjavastatettl001/version/$latest
	tenantID := items[6]     // 6 is tenantID
	funcName := items[8]     // 8 is functionName
	funcVersion := items[10] // 10 is functionVersion
	funcKey := tenantID + "/" + funcName + "/" + funcVersion
	isCAEFunc := r.judgeCAEFunc(funcKey)
	if ev.Source == etcd3.CAEMeta {
		r.funcMetaLock.Lock()
		old, ok := r.FuncMeta[funcKey]
		if ok && old.Source == etcd3.Meta {
			r.funcMetaLock.Unlock()
			log.GetLogger().Infof("function from meta etcd exists, skip delete from cae, key %s", funcKey)
			return
		}
		delete(r.FuncMeta, funcKey)
		r.funcMetaLock.Unlock()
	} else {
		r.funcMetaLock.Lock()
		delete(r.FuncMeta, funcKey)
		r.funcMetaLock.Unlock()
	}
	sf.Remove(funcKey)
	if r.callBack != nil {
		r.callBack(common.DelFunctionEvent, funcKey, isCAEFunc)
	}
	r.deleteCAEFuncVersion(tenantID, funcName, funcVersion)
}

func (r *Registry) judgeCAEFunc(funcKey string) bool {
	r.funcMetaLock.Lock()
	defer r.funcMetaLock.Unlock()
	funcMeta, ok := r.FuncMeta[funcKey]
	return ok && funcMeta.FuncMetaData.BusinessType == constants.BusinessTypeCAE
}

func (r *Registry) deleteCAEFuncVersion(tenantID, funcName, funcVersion string) {
	r.funcVersionMu.Lock()
	defer r.funcVersionMu.Unlock()

	log.GetLogger().Debugf("delete cae function cache, funcName:%s, funcVersion:%s", funcName, funcVersion)
	funcKey := fmt.Sprintf("%s/%s", tenantID, funcName)
	data, exist := r.CAEFuncVersion.Load(funcKey)
	if exist {
		versionMap, ok := data.(map[string]struct{})
		if !ok {
			log.GetLogger().Warnf("failed to assert type, type: %T, funcKey: %s", data, funcKey)
			return
		}
		delete(versionMap, funcVersion)
		if len(versionMap) == 0 {
			r.CAEFuncVersion.Delete(funcKey)
		} else {
			r.CAEFuncVersion.Store(funcKey, versionMap)
		}
	}
}

// FuncPathFilter filters function path
// /sn/functions/business/yrk/tenant/i1fe5394/function/0-ttl-testfuncsnjavastatettl001/version/$latest
// |seg1|  seg2 |  seg3 |seg4| seg5 |  seg6  |   seg7 |  seg8                          |seg9  | seg10 |
func (r *Registry) FuncPathFilter(path string) bool {
	items := strings.Split(path, "/")
	if len(items) != validFuncPathCount {
		return false
	}
	if items[2] != "functions" || items[7] != "function" || items[9] != "version" {
		return false
	}
	return true
}

// ObserverDefaultFilter default observer filter, do nothing
func (r *Registry) ObserverDefaultFilter(_ string) bool {
	return true
}

func (r *Registry) updateState(data []byte, items []string) {
	newStates := make(map[string]string, mapInitSize)
	err := json.Unmarshal(data, &newStates)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal data: %s", err)
		return
	}
	for stateID, node := range newStates {
		r.States.Store(stateID, node)
		log.GetLogger().Debugf("update state %s in node %s", stateID, node)
	}
}

func (r *Registry) deleteState(data []byte, items []string) {
	newStates := make(map[string]string, mapInitSize)
	err := json.Unmarshal(data, &newStates)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal data: %s", err)
		return
	}
	for stateID, node := range newStates {
		r.States.Delete(stateID)
		if !constants.IsStateID(stateID) {
			continue
		}
		if r.callBack != nil {
			r.callBack(common.DelStateIDEvent, stateID, false)
			r.callBack(common.DelStateIDEventStateQueue, newStates, false)
		}
		log.GetLogger().Debugf("delete stateID %s in node %s", stateID, node)
	}

}

func (r *Registry) updateProviders(data []byte, items []string, eventEtcdType etcd3.EtcdType) bool {
	event, ok := convertEvent(data)
	if !ok {
		log.GetLogger().Warnf("event data of %#v convert failed", items)
		return false
	}
	return r._updateProviders(event, items, eventEtcdType)
}

func isCAERouterEtcdOnline() bool {
	caeRouterEtcdClient := etcd3.GetCaeRouterEtcdClient()
	return caeRouterEtcdClient != nil && caeRouterEtcdClient.GetEtcdStatusNow()
}

func (r *Registry) _updateProviders(event InstanceEvent, items []string, eventEtcdType etcd3.EtcdType) bool {
	instanceID := extractInstanceFromPodPath(items)
	functionKey := getFuncKey(items)

	if commonutils.IsCAEFunc(event.BusinessType) && isCAERouterEtcdOnline() && eventEtcdType == etcd3.Router {
		log.GetLogger().Infof("Router etcd update event for CAE function when CAE router etcd online, "+
			"funcKey:%s, instanceId: %s", functionKey, instanceID)
		return true
	}
	r.ProvidersMu.Lock()
	defer r.ProvidersMu.Unlock()
	if _, ok := r.Providers[functionKey]; !ok {
		r.Providers[functionKey] = make(map[string]*Provider, mapInitSize)
	}
	// If functiontask restarts while worker-manager is dealing scaling up request,
	// the sync process cannot find updating etcd event of the worker,
	// which will cause free worker with no request.
	var provider *Provider
	var ok bool
	if provider, ok = r.Providers[functionKey][instanceID]; !ok {
		provider = makeProvider(event, instanceID, functionKey)
		r.Providers[functionKey][instanceID] = provider
	}
	// record will be deleted
	if provider != nil && event.WillDelete != provider.WillDelete {
		r.Providers[functionKey][instanceID].WillDelete = event.WillDelete
	}

	if !strings.HasPrefix(functionKey, common.TaskTenantID+"/") {
		insResource := r.getInstanceResource(provider)
		if insResource == nil {
			log.GetLogger().Warnf("failed to get instance resource info")
			return false
		}
		funcKeyWithResource := utils.GetFuncKeyWithResource(functionKey, *insResource)
		for _, cb := range r.addWorker {
			cb(funcKeyWithResource, instanceID, provider, r.ProvidersHasSynced)
		}
	} else {
		if r.enableNode != nil {
			r.enableNode(event.NodeIP)
		}
		proxyview.UpdateProxyAvailableStatus(event.NodeIP, true)
	}
	log.GetLogger().Debugf("succeed to update providers, functionKey: %s instanceID: %s nodeIP:"+
		" %s ownerIP: %s cpu %d memory %d, willDelete tag: %v", urnutils.AnonymizeTenantKey(functionKey),
		instanceID, event.NodeIP, event.OwnerIP, event.CPU, event.Memory, event.WillDelete)
	return true
}

func (r *Registry) deleteProviders(items []string, eventEtcdType etcd3.EtcdType) {
	info := getInstanceInfo(items)
	if info != nil && r.callBack != nil {
		r.callBack(common.DelInstanceEvent, info, false)
	}
	functionKey := getFuncKey(items)
	instanceID := extractInstanceFromPodPath(items)

	r.ProvidersMu.Lock()
	defer r.ProvidersMu.Unlock()
	if _, ok := r.Providers[functionKey]; !ok {
		log.GetLogger().Warnf("functionKey: %s not exists in Providers", functionKey)
		return
	}
	if _, ok := r.Providers[functionKey][instanceID]; !ok {
		log.GetLogger().Warnf("functionKey: %s and instanceID: %s not exists in Providers",
			functionKey, instanceID)
		return
	}
	provider := r.Providers[functionKey][instanceID]

	if commonutils.IsCAEFunc(provider.BusinessType) && isCAERouterEtcdOnline() && eventEtcdType == etcd3.Router {
		log.GetLogger().Infof("Router etcd delete event for CAE function when CAE router etcd online. "+
			"funcKey: %s, instanceID: %s", functionKey, instanceID)
		return
	}

	if !strings.HasPrefix(functionKey, common.TaskTenantID+"/") {
		insResource := r.getInstanceResource(provider)
		funcKeyWithResource := utils.GetFuncKeyWithResource(functionKey, *insResource)
		if r.deleteWorker != nil {
			r.deleteWorker(funcKeyWithResource, instanceID, provider, r.ProvidersHasSynced)
		}
	} else {
		if r.disableNode != nil {
			r.disableNode(provider.NodeIP)
		}
	}
	log.GetLogger().Infof("succeed to delete or disable worker,"+
		"functionKey: %s, instanceID: %s, nodeIP: %s, ownerIP: %s",
		urnutils.AnonymizeTenantKey(functionKey), instanceID, provider.NodeIP, provider.OwnerIP)
	delete(r.Providers[functionKey], instanceID)
	log.GetLogger().Debugf("succeed to remove pod, instanceID: %s", instanceID)
	if len(r.Providers[functionKey]) == 0 {
		delete(r.Providers, functionKey)
	}
}

func (r *Registry) getInstanceResource(provider *Provider) *types.InstanceResource {
	if commonutils.IsCAEFunc(provider.BusinessType) {
		return &types.InstanceResource{
			CPU:    "0",
			Memory: "0",
		}
	}
	return &types.InstanceResource{
		CPU:    strconv.Itoa(provider.CPU),
		Memory: strconv.Itoa(provider.Memory),
	}
}

func convertEvent(data []byte) (InstanceEvent, bool) {
	var event InstanceEvent
	err := json.Unmarshal(data, &event)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal data: %s", err)
		return InstanceEvent{}, false
	}

	if !checkWatchEvent(event) {
		return InstanceEvent{}, false
	}
	return event, true
}

func makeProvider(event InstanceEvent, instanceID, functionKey string) *Provider {
	provider := new(Provider)
	provider.InstanceID = instanceID
	provider.IP = event.IP
	provider.Port = event.Port
	provider.Cluster = event.Cluster
	provider.P2pPort = event.P2pPort
	provider.Status = event.Status
	provider.NodeIP = event.NodeIP
	provider.OwnerIP = event.OwnerIP
	provider.NodePort = event.NodePort
	provider.Applier = event.Applier
	provider.BusinessType = event.BusinessType
	provider.IsDirectFunc = event.IsDirectFunc
	provider.HasInitializer = event.HasInitializer
	provider.PodUID = event.PodUID
	provider.ContainerIDs = event.ContainerIDs
	provider.Resource = event.Resource
	provider.ViewResource = event.ViewResource
	// if worker is functionTask instance, return
	if strings.HasPrefix(functionKey, common.TaskTenantID+"/") {
		return provider
	}
	provider.CPU = event.CPU
	provider.Memory = event.Memory
	provider.StateID = event.StateID
	provider.WillDelete = event.WillDelete
	return provider
}

func setInfoFromInstanceID(provider *Provider, instanceID string) {
	items := strings.Split(instanceID, instanceSeparator)
	if len(items) <= instanceMemoryIndex {
		log.GetLogger().Warnf("invalid worker instanceID")
		return
	}
	cpuStr, memoryStr := items[instanceCPUIndex], items[instanceMemoryIndex]
	cpu, err := strconv.Atoi(cpuStr)
	if err != nil {
		log.GetLogger().Warnf("failed to convert the cpu type to int, cpuStr: %s", cpuStr)
		return
	}
	memory, err := strconv.Atoi(memoryStr)
	if err != nil {
		log.GetLogger().Warnf("failed to convert the memory type to int, memoryStr: %s", memoryStr)
		return
	}
	provider.CPU = cpu
	provider.Memory = memory
}

func checkWatchEvent(event InstanceEvent) bool {
	if !utils.CheckIP(event.NodeIP) {
		log.GetLogger().Warnf("invalid watch event ip %s", event.NodeIP)
		return false
	}

	if !utils.CheckPort(event.NodePort) {
		log.GetLogger().Warnf("invalid watch event port %s", event.NodePort)
		return false
	}

	if !utils.CheckPort(event.P2pPort) {
		log.GetLogger().Warnf("invalid watch event p2pPort %s", event.P2pPort)
		return false
	}

	if event.CPU < 0 || event.Memory < 0 {
		log.GetLogger().Warnf("invalid cpu %d or memory %d", event.CPU, event.Memory)
		return false
	}

	return true
}

func getInstanceInfo(items []string) map[string]string {
	if len(items) != validPodPathCount {
		return nil
	}

	// The length is 4 because there are only four data records.
	instanceInfo := make(map[string]string, instanceInfoCount)
	instanceInfo["TenantID"] = items[tenantPosition]
	instanceInfo["FunctionName"] = items[functionNamePosition]
	instanceInfo["FunctionVersion"] = items[versionPosition]
	instanceInfo["InstanceIDD"] = items[instancePosition]
	return instanceInfo
}

func getFuncKey(items []string) string {
	if len(items) < instanceKeyMinLen { // 11
		return ""
	}
	funcKey := items[tenantPosition] + "/" + items[functionNamePosition] + "/" + items[versionPosition]
	return funcKey
}

func extractInstanceFromPodPath(items []string) string {
	// The 12rd segment represent instance name
	if len(items) > instancePosition {
		return items[instancePosition]
	}
	return ""
}

// GetResourceValue to get the CPU/memory information of metadata
func (r *Registry) GetResourceValue(functionKey string) (*ResourceMetaData, error) {
	if obj, err := r.getValueFromCache(functionKey); err == nil {
		return &obj.ResourceMetaData, nil
	}
	obj, err := r.storeMetaFromEtcd(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to store metadata to cache from etcd, err: %s", err.Error())
		return nil, err
	}
	return &obj.ResourceMetaData, nil
}

// GetStatefulFunctionConcurrentNum is used to obtain the number of concurrent stateful functions for statequeuemap.
func (r *Registry) GetStatefulFunctionConcurrentNum(tenantID, functionName, serviceID string) (int, error) {
	funcName, functionVersion := utils.GetFuncNameAndVersion(functionName)
	log.GetLogger().Debugf("GetStatefulFunctionConcurrentNum funcName is %s, funcVersion is: %s",
		funcName, functionVersion)
	if functionVersion == "" {
		functionVersion = "latest"
	}
	functionKey := utils.GetFuncKey(tenantID, serviceID+"@"+funcName, functionVersion)
	instanceMetadata, err := r.GetFunctionInstanceMetadata(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get metadata for %s, err: %s", functionKey, err.Error())
		return 1, err
	}
	return instanceMetadata.ConcurrentNum, err
}

// GetFunctionInstanceMetadata -
func (r *Registry) GetFunctionInstanceMetadata(functionKey string) (InstanceMetaData, error) {
	if obj, err := r.getValueFromCache(functionKey); err == nil {
		return obj.InstanceMetaData, nil
	}
	obj, err := r.storeMetaFromEtcd(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to store metadata to cache from etcd for %s, err: %s",
			functionKey, err.Error())
		return InstanceMetaData{}, err
	}
	return obj.InstanceMetaData, nil
}

// GetFunctionMetadata gets function metadata
func (r *Registry) GetFunctionMetadata(functionKey string) (*FunctionMetaInfo, error) {
	if obj, err := r.getValueFromCache(functionKey); err == nil {
		return obj, nil
	}
	obj, err := r.storeMetaFromEtcd(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to store metadata to cache from etcd for %s, err: %s",
			functionKey, err.Error())
		return nil, err
	}
	return obj, nil
}

// GetFuncMetaInfoFromEtcd -
func (r *Registry) GetFuncMetaInfoFromEtcd(funcKeyWithResource string) (*workermanager.FuncMetaInfo, error) {
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	cachedFunctionMetaInfo, err := r.storeMetaFromEtcd(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to store metadata to cache from etcd for %s, err: %s",
			functionKey, err.Error())
		return nil, err
	}
	return transCachedFuncMetaInfoToFuncMetaInfo(cachedFunctionMetaInfo), nil
}

func transCachedFuncMetaInfoToFuncMetaInfo(cachedFunctionMetaInfo *FunctionMetaInfo) *workermanager.FuncMetaInfo {
	return &workermanager.FuncMetaInfo{
		BusinessType:             cachedFunctionMetaInfo.FuncMetaData.BusinessType,
		IsStatefulFunction:       cachedFunctionMetaInfo.FuncMetaData.IsStatefulFunction,
		ConcurrentNum:            cachedFunctionMetaInfo.InstanceMetaData.ConcurrentNum,
		Timeout:                  cachedFunctionMetaInfo.FuncMetaData.Timeout,
		InitializerTimeout:       cachedFunctionMetaInfo.ExtendedMetaData.Initializer.InitializerTimeout,
		IdleMode:                 cachedFunctionMetaInfo.InstanceMetaData.IdleMode,
		EnableRemoteDebug:        cachedFunctionMetaInfo.FuncMetaData.EnableRemoteDebug,
		MicCircuitBreakerEnabled: cachedFunctionMetaInfo.FuncMetaData.MicCircuitBreakerEnabled,
		IsBridgeFunction:         cachedFunctionMetaInfo.FuncMetaData.IsBridgeFunction,
		EnableWebsocket:          cachedFunctionMetaInfo.ExtendedMetaData.EnableWebsocket,
	}
}

func buildGetFuncMetaInfoReq(ctx context.Context, funcKeyWithResource string) (*http.Request, error) {
	url := wmhealth.GetWorkerManagerBaseURL() + "/worker-manager/v1/functions/funcmeta"
	funcMetaBaseInfo, err := utils.GetFuncMetaBaseInfoByFuncKeyWithResource(funcKeyWithResource)
	if err != nil {
		return nil, err
	}
	baseInfoBytes, err := json.Marshal(funcMetaBaseInfo)
	if err != nil {
		return nil, err
	}
	return http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(baseInfoBytes))
}

func processGetFuncMetaInfoResp(resp *http.Response) (*workermanager.FuncMetaBaseInfoResp, error) {
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("getFuncMetaInfoResp status code %d nok", resp.StatusCode)
	}
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	funcMetaInfoResp := &workermanager.FuncMetaBaseInfoResp{}
	err = json.Unmarshal(respBody, funcMetaInfoResp)
	if err != nil {
		return nil, err
	}
	return funcMetaInfoResp, nil
}

// GetFuncMetaInfo Get func meta from local cache first, if not available, query func meta from worker-manager.
// If an exception occurred during the query process, functiontask query func meta from etcd again.
// Worker-manager return func meta from local cache firstly, and get func meta from faas etcd or cae etcd if not exist.
func (r *Registry) GetFuncMetaInfo(funcKeyWithResource string) (*workermanager.FuncMetaInfo, error) {
	functionKey := utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource)
	if cachedFunctionMetaInfo, err := r.getValueFromCache(functionKey); err == nil {
		return transCachedFuncMetaInfoToFuncMetaInfo(cachedFunctionMetaInfo), nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), getFuncMetaTimeout)
	defer cancel()
	req, err := buildGetFuncMetaInfoReq(ctx, functionKey)
	if err != nil {
		log.GetLogger().Warnf("buildGetFuncMetaInfoReq of %s err:%s", funcKeyWithResource, err.Error())
		return r.GetFuncMetaInfoFromEtcd(funcKeyWithResource)
	}
	resp, err := utils.GetWorkerManagerClient().Do(req)
	if err != nil {
		log.GetLogger().Warnf("get funcMetaInfoResp of %s from worker-manager err:%s",
			funcKeyWithResource, err.Error())
		return r.GetFuncMetaInfoFromEtcd(funcKeyWithResource)
	}
	defer resp.Body.Close()
	funcMetaInfoResp, err := processGetFuncMetaInfoResp(resp)
	if err != nil {
		log.GetLogger().Warnf("process GetFuncMetaInfoResp of %s err:%s", funcKeyWithResource, err.Error())
		return r.GetFuncMetaInfoFromEtcd(funcKeyWithResource)
	}
	if len(funcMetaInfoResp.ErrMsg) > 0 {
		if strings.Contains(funcMetaInfoResp.ErrMsg, snerror.ErrText(snerror.FuncMetaNotFound)) {
			return nil, errors.New(snerror.ErrText(snerror.FuncMetaNotFound))
		}
		return r.GetFuncMetaInfoFromEtcd(funcKeyWithResource)
	}
	return &funcMetaInfoResp.FuncMetaInfo, nil
}

// GetDirectFunctionMetadata gets direct function metadata
func (r *Registry) GetDirectFunctionMetadata(functionKey string) (*FunctionMetaInfo, error) {
	if obj, err := r.getValueFromCache(functionKey); err == nil {
		return obj, nil
	}
	obj, err := r.getMetaFromEtcd(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get direct function metadata from etcd for %s, err: %s",
			functionKey, err.Error())
		return nil, err
	}
	return obj, nil
}

func (r *Registry) getValueFromCache(functionKey string) (*FunctionMetaInfo, error) {
	r.funcMetaLock.RLock()
	obj, exist := r.FuncMeta[functionKey]
	r.funcMetaLock.RUnlock()
	if !exist {
		return nil, errors.New("not find metadata from cache")
	}
	return obj, nil
}

// GetFunctionFuncMetaData -
func (r *Registry) GetFunctionFuncMetaData(functionKey string) (FuncMetaData, error) {
	if obj, err := r.getValueFromCache(functionKey); err == nil {
		return obj.FuncMetaData, nil
	}
	obj, err := r.storeMetaFromEtcd(functionKey)
	if err != nil {
		log.GetLogger().Errorf("failed to store metadata to cache from etcd for %s, err: %s",
			functionKey, err.Error())
		return FuncMetaData{}, err
	}
	return obj.FuncMetaData, nil
}

func (r *Registry) getMetaFromEtcd(functionKey string) (*FunctionMetaInfo, error) {
	obj, err := r.storeMetaFromEtcd(functionKey)
	if err == nil {
		return obj, nil
	}
	tenantID, funcName, funcVersion := utils.ParseFuncKey(functionKey)
	funcStorageKey := etcd3.HandleCellPrefix(fmt.Sprintf(constants.EtcdFuncKey, tenantID, funcName, funcVersion))
	resp, err := utils.GetValueFromEtcd(funcStorageKey, etcd3.Meta)
	if err != nil {
		log.GetLogger().Warnf("failed to query from etcd, funcKey: %s, err: %s",
			urnutils.AnonymizeTenantKey(functionKey), err.Error())
		return nil, err
	}
	return r.storeFunctionMeta(resp, functionKey)
}

func (r *Registry) storeMetaFromEtcd(functionKey string) (*FunctionMetaInfo, error) {
	tenantID, funcName, funcVersion := utils.ParseFuncKey(functionKey)
	functionRepoInfo, err := sf.Do(functionKey, func() (interface{}, error) {
		functionStorageKey := etcd3.HandleCellPrefix(fmt.Sprintf(constants.SilentFuncKey, tenantID, funcName, funcVersion))
		resp, err := utils.GetValueFromEtcd(functionStorageKey, etcd3.Meta)
		if err != nil {
			if !errors.Is(err, utils.ErrETCDNotFound) {
				time.AfterFunc(singleFlightKeyTTL, func() {
					sf.Remove(functionKey)
				})
			}
			log.GetLogger().Warnf("failed to query from etcd, funcKey: %s, err: %s",
				urnutils.AnonymizeTenantKey(functionKey), err.Error())
			return nil, err
		}
		return r.storeFunctionMeta(resp, functionKey)
	})
	if err != nil {
		return nil, err
	}
	log.GetLogger().Debugf("get concurrentNum of %s is %d, get IsBridgeFunction of %s is %t",
		urnutils.AnonymizeTenantKey(functionKey), functionRepoInfo.(*FunctionMetaInfo).InstanceMetaData.ConcurrentNum,
		functionRepoInfo.(*FunctionMetaInfo).FuncMetaData.IsBridgeFunction)
	return functionRepoInfo.(*FunctionMetaInfo), nil
}

// resolve code duplicate
func (r *Registry) storeFunctionMeta(respContext []byte, functionKey string) (*FunctionMetaInfo, error) {
	functionRepoInfo := &FunctionMetaInfo{}
	err := json.Unmarshal(respContext, functionRepoInfo)
	if err != nil {
		return nil, err
	}

	functionRepoInfo.Source = etcd3.Meta
	r.funcMetaLock.Lock()
	r.FuncMeta[functionKey] = functionRepoInfo
	r.funcMetaLock.Unlock()
	return functionRepoInfo, nil
}

// CreateAliasOnObserve creates alias
func CreateAliasOnObserve(ev *etcd3.Event) {
	alias := aliasroute.Alias{}
	err := json.Unmarshal(ev.Value, &alias)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal alias on observe")
		return
	}
	aliasroute.AliasRoute.AddAlias(&alias)
}

// DeleteAliasOnObserve deletes alias
func DeleteAliasOnObserve(ev *etcd3.Event) {
	aliasURN := aliasroute.BuildURNFromAliasKey(ev.Key)
	aliasroute.AliasRoute.RemoveAlias(aliasURN)
}

// RegisterCallBack register callback function of registry
func (r *Registry) RegisterCallBack(callback func(eventType string, eventInfo interface{}, isDelCAEFunc bool)) {
	r.callBack = callback
}

// IsFunctionExist uses to query whether a function exists
func (r *Registry) IsFunctionExist(tenantID, service, functionName, version string) bool {
	functionName, version = utils.ExtractFuncNameAndVersion(functionName, version)
	// service is 0@service
	fullName := fmt.Sprintf("%s@%s", service, functionName)

	// judge whether CAE function. if CAE, version is arbitrary
	funcKey := fmt.Sprintf("%s/%s", tenantID, fullName)
	if _, exist := r.CAEFuncVersion.Load(funcKey); exist {
		return true
	}
	funcKey = fmt.Sprintf("%s/%s/%s", tenantID, fullName, version)
	r.funcMetaLock.RLock()
	_, exist := r.FuncMeta[funcKey]
	r.funcMetaLock.RUnlock()
	if exist {
		return exist
	}
	meta, err := r.getMetaFromEtcd(funcKey)
	if err != nil {
		return false
	}
	return meta != nil
}

// IsStatefulFunction uses to check whether a function is a stateful function
func (r *Registry) IsStatefulFunction(tenantID, service, functionName, version string) bool {
	functionName, version = utils.ExtractFuncNameAndVersion(functionName, version)
	// service is 0@service
	fullName := fmt.Sprintf("%s@%s", service, functionName)
	var funcKey string
	funcKeyWithoutVersion := fmt.Sprintf("%s/%s", tenantID, fullName)
	// if CAE function, select an arbitrary version
	if _, exist := r.CAEFuncVersion.Load(funcKeyWithoutVersion); exist {
		funcVersion, ok := r.selectCAEFuncVersion(tenantID, fullName)
		if !ok {
			return false
		}
		funcKey = fmt.Sprintf("%s/%s/%s", tenantID, fullName, funcVersion)
	} else {
		funcKey = fmt.Sprintf("%s/%s/%s", tenantID, fullName, version)
	}
	r.funcMetaLock.RLock()
	funcMeta, exist := r.FuncMeta[funcKey]
	r.funcMetaLock.RUnlock()
	if !exist {
		var err error
		funcMeta, err = r.storeMetaFromEtcd(funcKey)
		if err != nil {
			log.GetLogger().Warnf("funcKey %s is not exist", urnutils.AnonymizeTenantKey(funcKey))
			return false
		}
	}
	return funcMeta.FuncMetaData.IsStatefulFunction
}

func (r *Registry) selectCAEFuncVersion(tenantID, funcName string) (string, bool) {
	r.funcVersionMu.Lock()
	defer r.funcVersionMu.Unlock()

	funcKey := fmt.Sprintf("%s/%s", tenantID, funcName)
	data, exist := r.CAEFuncVersion.Load(funcKey)
	if !exist {
		log.GetLogger().Warnf("funcKey %s does not exist in CAEFuncVersion", funcKey)
		return "", false
	}
	versionMap, ok := data.(map[string]struct{})
	if !ok {
		log.GetLogger().Warnf("failed to assert type, type: %T, funcKey: %s", data, funcKey)
		return "", false
	}
	for version := range versionMap {
		return version, true
	}
	log.GetLogger().Warnf("versionMap is empty, funcKey: %s", funcKey)
	return "", false
}

// IsStateInsExist check whether the state instance exists in etcd
func (r *Registry) IsStateInsExist(stateID string) bool {
	_, exist := r.States.Load(stateID)
	return exist
}

// GetNodeIDByStateID -
func (r *Registry) GetNodeIDByStateID(stateID string) string {
	nodeID, exist := r.States.Load(stateID)
	if !exist {
		return ""
	}
	return nodeID.(string)
}

// GetDynamicCPUByStateID -
func (r *Registry) GetDynamicCPUByStateID(stateID string) string {
	dynamicCPU, exist := r.States.Load(constants.CPUKey + stateID)
	if !exist {
		return ""
	}
	return dynamicCPU.(string)
}

// GetDynamicMemoryByStateID -
func (r *Registry) GetDynamicMemoryByStateID(stateID string) string {
	dynamicMemory, exist := r.States.Load(constants.MemoryKey + stateID)
	if !exist {
		return ""
	}
	return dynamicMemory.(string)
}

// StateRecovery recovery state if etcd data is not equal to redis data
func (r *Registry) StateRecovery(stateRecoveryConfig coretypes.StateRecoveryConfig) {
	localExistInETCD := stateRecoveryConfig.LocalExistInETCD
	realExistInETCD := stateRecoveryConfig.RealExistInETCD

	// it happened when etcd watch not timely, the thing only need to do is sync local storage of etcd
	if localExistInETCD != realExistInETCD {
		// means it should exist in local storage of etcd
		if realExistInETCD {
			r.States.Store(stateRecoveryConfig.StateID, stateRecoveryConfig.RealValue)
			log.GetLogger().Debugf("state recovery store, stateID: %v, realValue: %v",
				stateRecoveryConfig.StateID, stateRecoveryConfig.RealValue)
		} else {
			// means it should not exist in local storage of etcd
			r.States.Delete(stateRecoveryConfig.StateID)
			log.GetLogger().Debugf("state recovery delete, stateID: %v", stateRecoveryConfig.StateID)
		}
	}
}

// GetAvailableClusters get available clusters with read lock
func (r *Registry) GetAvailableClusters(funcUrn string) []string {
	r.funcClusterLock.RLock()
	defer r.funcClusterLock.RUnlock()
	clusters := r.FuncAvailableClusters[funcUrn]
	copied := make([]string, len(clusters))
	copy(copied, clusters)
	return copied
}
