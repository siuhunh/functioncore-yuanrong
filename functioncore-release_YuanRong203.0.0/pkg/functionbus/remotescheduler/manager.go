/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package remotescheduler -
package remotescheduler

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functionbus/common"
)

const (
	// InstanceSplicer instance splicer
	InstanceSplicer            = "&&"
	keyElementNum              = 2
	instanceKeyIndex           = 0
	processIDIndex             = 1
	base                       = 10
	bitSize32                  = 32
	defaultInstanceReleaseTime = 3
	defaultInstanceLeaseTime   = 3
	defaultCheckInstancePeriod = 10
)

var (
	virtualNodeNum  = 100
	instanceManager *Manager

	// the key of instanceMap: instanceID&&processID, value: &instanceStatus{}
	instanceMap sync.Map
	// the key of idleInstanceMap: funcName , value: List[instanceID&&processID]
	idleInstanceMap     = make(map[string][]string)
	idleInstanceMapLock sync.RWMutex

	// instanceReleaseTime default 3 Second
	instanceReleaseTime = defaultInstanceReleaseTime * time.Second
	// checkInstancePeriod default 10 Second
	checkInstancePeriod = defaultCheckInstancePeriod * time.Second
	// instanceLeaseTime default 3 Second
	instanceLeaseTime = defaultInstanceLeaseTime * time.Second

	instanceRecords     = schedulerapi.RecoverInstancesRes{InstancesRes: make(map[string]*schedulerapi.CheckInstanceReq)}
	instanceRecordsLock = new(sync.RWMutex)
)

type instanceStatus struct {
	funcKeyWithRes string
	instanceID     string
	nodeIP         string
	processID      uint32
	leaseVersion   uint64
	restThread     uint32
	idleFlag       bool
	lastUsedTime   time.Time
	sync.Mutex
}

// Manager -
type Manager struct {
	disable bool
	lb      loadbalance.NoLockLoadBalance
	sync.RWMutex
}

// CreateInstanceManager -
func CreateInstanceManager(stopCh <-chan struct{}) {
	if common.FConfig.RemoteSchedulerConfig != nil && common.FConfig.RemoteSchedulerConfig.Disable {
		instanceManager = &Manager{disable: true}
		return
	}
	if common.FConfig.RemoteSchedulerConfig != nil {
		if common.FConfig.RemoteSchedulerConfig.InstanceReleaseTime > 0 {
			instanceReleaseTime = time.Duration(common.FConfig.RemoteSchedulerConfig.InstanceReleaseTime) * time.Second
		}
		if common.FConfig.RemoteSchedulerConfig.CheckInstancePeriod > 0 {
			checkInstancePeriod = time.Duration(common.FConfig.RemoteSchedulerConfig.CheckInstancePeriod) * time.Second
		}
		if common.FConfig.RemoteSchedulerConfig.InstanceLeaseTime > 0 {
			instanceLeaseTime = time.Duration(common.FConfig.RemoteSchedulerConfig.InstanceLeaseTime) * time.Second
		}
	}
	if virtualNum, err := strconv.Atoi(os.Getenv("VIRTUAL_NODE_NUM")); err == nil {
		virtualNodeNum = virtualNum
	}
	instanceManager = &Manager{
		lb: loadbalance.NewConsistentHashWithVirtualNode(virtualNodeNum),
	}
	// Start a scheduled task to renew the lease regularly
	go renewInstancesLeaseScheduledTask(stopCh)
	// Start a scheduled task to check the lease regularly
	go checkInstanceLeaseScheduledTask(stopCh)
}

// GetInstanceManager -
func GetInstanceManager() *Manager {
	return instanceManager
}

// AddScheduler -
func (manager *Manager) AddScheduler(key string, info *types.FaaSServiceInfo) {
	if manager.disable {
		return
	}
	manager.Lock()
	defer manager.Unlock()
	client := createClient(key, info.IP, info.Port)
	node := &loadbalance.Node{
		Obj: client,
		Key: key,
	}
	err := manager.lb.Add(node)
	if err != nil {
		log.GetLogger().Errorf("failed to add scheduler, scheduler %s, err: %s", key, err.Error())
		return
	}
	client.start()
	log.GetLogger().Infof("success to add scheduler %s, %+v", key, info)
}

// DeleteScheduler -
func (manager *Manager) DeleteScheduler(key string) {
	if manager.disable {
		return
	}
	manager.Lock()
	defer manager.Unlock()
	var client *clientAdapter
	var ok bool
	node := manager.lb.Delete(key)
	if node != nil {
		client, ok = node.Obj.(*clientAdapter)
		if !ok {
			log.GetLogger().Errorf("failed to assert node.Obj to clientAdapter for key %s", key)
			return
		}
	}
	if !ok || client == nil {
		log.GetLogger().Errorf("failed to delete scheduler %s, scheduler not exist", key)
		return
	}
	client.stop()
	log.GetLogger().Infof("success to delete scheduler %s", key)
}

// getScheduler -
func (manager *Manager) getScheduler(funcKeyWithRes string) *clientAdapter {
	if manager.disable {
		return nil
	}
	manager.RLock()
	defer manager.RUnlock()
	node := manager.lb.Next(funcKeyWithRes)
	if node == nil {
		return nil
	}
	client, ok := node.Obj.(*clientAdapter)
	if !ok {
		log.GetLogger().Errorf("failed to get scheduler, type is not valid")
		return nil
	}
	if !client.isEnable() {
		return nil
	}
	return client
}

func renewInstancesLeaseScheduledTask(stopCh <-chan struct{}) {
	if instanceManager == nil || instanceManager.disable {
		return
	}
	renewInstancesLeaseTicker := time.NewTicker(instanceLeaseTime)
	for {
		select {
		case <-renewInstancesLeaseTicker.C:
			traceID := "renewInstance_scheduledTask_" + uuid.New().String()
			renewInstancesLease(traceID)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("renewInstanceScheduledtask | stop channel is closed")
			}
			return
		}
	}
}

func renewInstancesLease(traceID string) {
	if instanceManager == nil {
		return
	}
	instanceMap.Range(func(key, value interface{}) bool {
		status, ok := value.(*instanceStatus)
		if !ok {
			instanceMap.Delete(key)
			return true
		}

		client := instanceManager.getScheduler(status.funcKeyWithRes)
		if client == nil {
			log.GetLogger().Warnf("renewInstanceScheduledtask | renew instance lease %s with no client, %s",
				key, traceID)
			return true
		}

		instanceIdleTime := time.Now().Sub(status.lastUsedTime)
		if !status.idleFlag || instanceIdleTime < instanceReleaseTime {
			go func() {
				rsp, err := client.RetainInstance(&schedulerapi.RetainInstanceReq{
					TraceID:        traceID,
					FuncKeyWithRes: status.funcKeyWithRes,
					InstanceKey:    status.instanceID,
					ProcessorID:    status.processID,
					LeaseVersion:   status.leaseVersion,
				})
				if err != nil {
					log.GetLogger().Warnf("renewInstanceScheduledtask | failed to renew instance lease %s, "+
						"traceid: %s, error: %s", status.instanceID, traceID, err.Error())
					removeIdleInstance(status, key.(string))
					deleteProcessorRecord(status.funcKeyWithRes, status.instanceID, status.processID)
					instanceMap.Delete(key)
					return
				}
				if rsp != nil && rsp.Error.ErrorCode != 0 {
					log.GetLogger().Warnf("renewInstanceScheduledtask | failed to renew instance lease %s, "+
						"traceid: %s, error: %s", status.instanceID, traceID, rsp.Error.ErrorMessage)
					removeIdleInstance(status, key.(string))
					deleteProcessorRecord(status.funcKeyWithRes, status.instanceID, status.processID)
					instanceMap.Delete(key)
					return
				}
				log.GetLogger().Debugf("renewInstanceScheduledtask | renew instance lease successfully!"+
					"FunctionName: %s, instance %s, processId: %d, traceid: %s", status.funcKeyWithRes,
					status.instanceID, status.processID, traceID)
			}()
			return true
		}
		releaseInstanceWhileRenewFail(key, traceID, status, client)
		return true
	})
}

func releaseInstanceWhileRenewFail(key interface{}, traceID string, status *instanceStatus,
	client *clientAdapter) {
	req := &schedulerapi.ReleaseInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: status.funcKeyWithRes,
		InstanceKey:    status.instanceID,
		ProcessorID:    status.processID,
		LeaseVersion:   status.leaseVersion,
	}

	err := client.ReleaseInstance(req)
	if err != nil {
		log.GetLogger().Warnf("renewInstanceScheduledtask | failed to"+
			"release instance %s, traceID: %s", status.instanceID, traceID)
	}
	removeIdleInstance(status, key.(string))
	deleteProcessorRecord(status.funcKeyWithRes, status.instanceID, status.processID)
	instanceMap.Delete(key)
	log.GetLogger().Infof("renewInstanceScheduledtask | release instance long time no use "+
		"release FunctionName: %s, instance %s, processId: %d, traceID: %s", status.funcKeyWithRes,
		status.instanceID, status.processID, traceID)
}

func removeIdleInstance(status *instanceStatus, key string) {
	if !status.idleFlag {
		return
	}
	funcKeyWithRes := status.funcKeyWithRes
	idleInstanceMapLock.Lock()
	defer idleInstanceMapLock.Unlock()
	instances, ok := idleInstanceMap[funcKeyWithRes]
	if !ok || len(instances) == 0 {
		return
	}
	var tmp []string
	for _, e := range instances {
		if e != key {
			tmp = append(tmp, e)
		}
	}
	if len(tmp) > 0 {
		sort.Strings(tmp)
		idleInstanceMap[funcKeyWithRes] = tmp
	} else {
		delete(idleInstanceMap, funcKeyWithRes)
	}
}

func checkInstanceLeaseScheduledTask(stopCh <-chan struct{}) {
	if instanceManager == nil || instanceManager.disable {
		return
	}
	checkInstancesTicker := time.NewTicker(checkInstancePeriod)
	traceID := "scheduled_checkInstanceTask_" + uuid.New().String()
	log.GetLogger().Debugf("checkInstanceScheduledtask "+
		"| start check instance scheduleTask, traceid: %s", traceID)
	for {
		select {
		case <-checkInstancesTicker.C:
			checkInstanceLease(traceID)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("checkInstanceScheduledtask | stop channel is closed")
			}
			return
		}
	}
}

func checkInstanceLease(traceID string) {
	if instanceManager == nil {
		return
	}
	checkFinishedMap := make(map[string]bool)
	instanceMap.Range(func(key, value interface{}) bool {
		status, ok := value.(*instanceStatus)
		if !ok {
			instanceMap.Delete(key)
			return true
		}
		finished, loaded := checkFinishedMap[status.funcKeyWithRes]
		if loaded && finished {
			return true
		}
		checkFinishedMap[status.funcKeyWithRes] = true
		instancesRes := getInstancesRecord()
		instanceReq, ok := instancesRes.InstancesRes[status.funcKeyWithRes]
		if !ok || instanceReq == nil {
			return true
		}
		client := instanceManager.getScheduler(status.funcKeyWithRes)
		if client == nil {
			log.GetLogger().Warnf("checkInstanceScheduledtask | check instance %s with no client, %s",
				key, traceID)
			return true
		}
		err := client.CheckInstance(instanceReq)
		if err != nil {
			log.GetLogger().Warnf("checkInstanceScheduledtask | failed to checkInstance "+
				"with funcKeyWithRes %s, err: %s, traceid: %s",
				urnutils.AnonymizeTenantKey(status.funcKeyWithRes), err.Error(), traceID)
			return true
		}
		log.GetLogger().Debugf("checkInstanceScheduledtask |  succeed to checkInstance with "+
			"funcKeyWithRes： %s,  traceid %s",
			urnutils.AnonymizeTenantKey(status.funcKeyWithRes), traceID)
		return true
	})
}

// AcquireInstance get instanceID&&processID by funcKeyWithRes
func AcquireInstance(funcKeyWithRes string, traceID string) (string, string, string) {
	if instanceManager == nil || instanceManager.disable {
		return "", "", ""
	}
	idleInstanceMapLock.Lock()
	instances, ok := idleInstanceMap[funcKeyWithRes]
	var targetInstance string
	var key string
	var nodeIP string
	if ok && len(instances) > 0 {
		targetInstance, key, nodeIP = getInstance(funcKeyWithRes, instances)
	}
	idleInstanceMapLock.Unlock()
	if targetInstance != "" {
		log.GetLogger().Infof("succeed to acquire instance %s "+
			"for funcKeyWithRes %s, nodeIP: %s, traceID: %s from: %v",
			targetInstance, funcKeyWithRes, nodeIP, traceID, instances)
		return targetInstance, key, nodeIP
	}
	client := instanceManager.getScheduler(funcKeyWithRes)
	if client == nil {
		return "", "", ""
	}
	req := &schedulerapi.AcquireInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: funcKeyWithRes,
	}
	rsp, err := client.AcquireInstance(req)
	if err != nil {
		log.GetLogger().Errorf("functionbus failed to acquires "+
			"function %s instance from faas-scheduler, error: %s ", funcKeyWithRes, err.Error())
		return "", "", ""
	}
	status := &instanceStatus{
		funcKeyWithRes: funcKeyWithRes,
		instanceID:     rsp.InstanceKey,
		nodeIP:         rsp.NodeIP,
		processID:      rsp.ProcessorID,
		leaseVersion:   rsp.LeaseVersion,
		restThread:     rsp.RestThread,
		lastUsedTime:   time.Now(),
	}
	key = fmt.Sprintf("%s%s%v", rsp.InstanceKey, InstanceSplicer, rsp.ProcessorID)
	instanceMap.Store(key, status)
	storeInstanceRecord(status)
	log.GetLogger().Infof("succeed to acquire instance %s for funcKeyWithRes %s, traceid: %s",
		key, funcKeyWithRes, traceID)
	return rsp.InstanceKey, key, rsp.NodeIP
}

func storeInstanceRecord(status *instanceStatus) {
	processStatus := &schedulerapi.ProcessStatus{ProcessorID: status.processID,
		RestThread: status.restThread, LeaseVersion: status.leaseVersion}
	createOrUpdateInstancesRecord(status.funcKeyWithRes, status.instanceID, processStatus)
}

func getInstance(funcKeyWithRes string, instances []string) (string, string, string) {
	for {
		if len(instances) == 0 {
			break
		}
		target := instances[len(instances)-1]
		val, ok := instanceMap.Load(target)
		if !ok {
			instances = instances[:len(instances)-1]
			if len(instances) > 0 {
				idleInstanceMap[funcKeyWithRes] = instances
			} else {
				delete(idleInstanceMap, funcKeyWithRes)
			}
			continue
		}
		instancesNew := instances[:len(instances)-1]
		if len(instancesNew) > 0 {
			idleInstanceMap[funcKeyWithRes] = instancesNew
		} else {
			delete(idleInstanceMap, funcKeyWithRes)
		}
		status, ok := val.(*instanceStatus)
		if !ok {
			instanceMap.Delete(target)
			continue
		}
		list := strings.Split(target, InstanceSplicer)
		if len(list) != keyElementNum {
			log.GetLogger().Warnf("invalid instance key %s", target)
			continue
		}
		status.Lock()
		status.idleFlag = false
		status.Unlock()
		return list[instanceKeyIndex], target, status.nodeIP
	}
	return "", "", ""
}

// ReleaseInstance release instance by instanceID&&processID
func ReleaseInstance(funcKeyWithRes string, key string, errFlag bool, traceID string) {
	if instanceManager == nil || instanceManager.disable {
		return
	}
	val, ok := instanceMap.Load(key)
	if !ok {
		log.GetLogger().Warnf("failed to load instance %s", key)
		return
	}
	// if errFlag true Release immediately
	if errFlag {
		releaseInstance(funcKeyWithRes, key, traceID)
		return
	}
	status, ok := val.(*instanceStatus)
	if !ok {
		instanceMap.Delete(key)
		return
	}
	status.Lock()
	status.idleFlag = true
	status.lastUsedTime = time.Now()
	status.Unlock()
	addIdleInstance(funcKeyWithRes, key)
	log.GetLogger().Debugf("succeed to release instance %s for funcKeyWithRes %s", key, funcKeyWithRes)
}

func releaseInstance(funcKeyWithRes string, key string, traceID string) {
	if instanceManager == nil {
		return
	}
	client := instanceManager.getScheduler(funcKeyWithRes)
	if client == nil {
		log.GetLogger().Warnf("failed to release instance %s with no client, %s", key, traceID)
		return
	}
	list := strings.Split(key, InstanceSplicer)
	if len(list) != keyElementNum {
		log.GetLogger().Warnf("invalid instance key %s", key)
		return
	}
	processorID, err := strconv.ParseUint(list[processIDIndex], base, bitSize32)
	if err != nil {
		log.GetLogger().Warnf("failed to parse ProcessorID %s, %s", list[processIDIndex], traceID)
		return
	}
	val, ok := instanceMap.Load(key)
	if !ok {
		log.GetLogger().Warnf("failed to load instance %s, %s", key, traceID)
		return
	}
	status, ok := val.(*instanceStatus)
	if !ok {
		instanceMap.Delete(key)
		return
	}
	req := &schedulerapi.ReleaseInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: funcKeyWithRes,
		InstanceKey:    list[instanceKeyIndex],
		ProcessorID:    uint32(processorID),
		LeaseVersion:   status.leaseVersion,
	}
	err = client.ReleaseInstance(req)
	if err != nil {
		log.GetLogger().Warnf("failed to release instance %s, %s", list[instanceKeyIndex], traceID)
	}
	deleteProcessorRecord(status.funcKeyWithRes, status.instanceID, status.processID)
	instanceMap.Delete(key)
}

func addIdleInstance(funcKeyWithRes string, key string) {
	idleInstanceMapLock.Lock()
	defer idleInstanceMapLock.Unlock()
	instances, ok := idleInstanceMap[funcKeyWithRes]
	if !ok || len(instances) == 0 {
		idleInstanceMap[funcKeyWithRes] = []string{key}
		return
	}
	// Avoid duplicate data writes
	for _, e := range instances {
		if e == key {
			return
		}
	}
	instances = append(instances, key)
	sort.Strings(instances)
	idleInstanceMap[funcKeyWithRes] = instances
}

// CreateOrUpdateInstancesRecord create or update instances record
func createOrUpdateInstancesRecord(funcKeyWithRes string,
	instanceKey string, processStatus *schedulerapi.ProcessStatus) {
	if instanceKey == "" || funcKeyWithRes == "" {
		return
	}
	instanceRecordsLock.Lock()
	defer instanceRecordsLock.Unlock()
	instanceReq, ok := instanceRecords.InstancesRes[funcKeyWithRes]
	if !ok || len(instanceReq.InstancesStatusMap) == 0 {
		statusMap := make(map[uint32]*schedulerapi.ProcessStatus)
		statusMap[processStatus.ProcessorID] = &schedulerapi.ProcessStatus{
			ProcessorID:  processStatus.ProcessorID,
			LeaseVersion: processStatus.LeaseVersion,
			RestThread:   processStatus.RestThread,
		}
		instancesStatus := &schedulerapi.InstancesStatus{
			InstanceKey:        instanceKey,
			ProcessesStatusMap: statusMap,
		}
		instancesStatusMap := make(map[string]*schedulerapi.InstancesStatus)
		instancesStatusMap[instanceKey] = instancesStatus
		instanceReq = &schedulerapi.CheckInstanceReq{
			FuncKeyWithRes:     funcKeyWithRes,
			InstancesStatusMap: instancesStatusMap,
		}
		instanceRecords.InstancesRes[funcKeyWithRes] = instanceReq
		return
	}
	originInstancesStatus, ok := instanceReq.InstancesStatusMap[instanceKey]
	if !ok || len(originInstancesStatus.ProcessesStatusMap) == 0 {
		processesStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
		processesStatusMap[processStatus.ProcessorID] = &schedulerapi.ProcessStatus{
			ProcessorID:  processStatus.ProcessorID,
			LeaseVersion: processStatus.LeaseVersion,
			RestThread:   processStatus.RestThread,
		}
		instancesStatus := &schedulerapi.InstancesStatus{
			InstanceKey:        instanceKey,
			ProcessesStatusMap: processesStatusMap,
		}
		instanceReq.InstancesStatusMap[instancesStatus.InstanceKey] = instancesStatus
		return
	}

	originProcessStatus, ok := originInstancesStatus.ProcessesStatusMap[processStatus.ProcessorID]
	if !ok || processStatus.LeaseVersion >= originProcessStatus.LeaseVersion {
		originInstancesStatus.ProcessesStatusMap[processStatus.ProcessorID] = processStatus
	}
}

func deleteProcessorRecord(funcKeyWithRes string, instanceKey string, processorID uint32) {
	if funcKeyWithRes == "" || instanceKey == "" {
		return
	}
	instanceRecordsLock.Lock()
	defer instanceRecordsLock.Unlock()
	instanceReq, ok := instanceRecords.InstancesRes[funcKeyWithRes]
	if !ok || instanceReq == nil {
		return
	}
	instancesStatus, ok := instanceReq.InstancesStatusMap[instanceKey]
	if !ok || instancesStatus == nil {
		return
	}
	delete(instancesStatus.ProcessesStatusMap, processorID)
	if len(instancesStatus.ProcessesStatusMap) == 0 {
		delete(instanceReq.InstancesStatusMap, instanceKey)
	}

	if len(instanceReq.InstancesStatusMap) == 0 {
		delete(instanceRecords.InstancesRes, funcKeyWithRes)
	}
}

func getInstancesRecord() schedulerapi.RecoverInstancesRes {
	instanceRecordsLock.RLock()
	defer instanceRecordsLock.RUnlock()
	instancesRecord := schedulerapi.RecoverInstancesRes{InstancesRes: make(map[string]*schedulerapi.CheckInstanceReq)}
	for funcKeyWithRes, CheckInstanceReq := range instanceRecords.InstancesRes {
		instancesRecord.InstancesRes[funcKeyWithRes] = getCheckInstanceReqSnapshot(CheckInstanceReq)
	}
	return instancesRecord
}

func getCheckInstanceReqSnapshot(originInstanceReq *schedulerapi.CheckInstanceReq) *schedulerapi.CheckInstanceReq {
	checkInstanceReq := &schedulerapi.CheckInstanceReq{
		FuncKeyWithRes:     originInstanceReq.FuncKeyWithRes,
		InstancesStatusMap: make(map[string]*schedulerapi.InstancesStatus),
	}
	for instanceKey, instancesStatus := range originInstanceReq.InstancesStatusMap {
		checkInstanceReq.InstancesStatusMap[instanceKey] = &schedulerapi.InstancesStatus{
			InstanceKey:        instanceKey,
			ProcessesStatusMap: getProcessesStatusMapSnapshot(instancesStatus.ProcessesStatusMap),
		}
	}
	return checkInstanceReq
}

func getProcessesStatusMapSnapshot(
	originMap map[uint32]*schedulerapi.ProcessStatus) map[uint32]*schedulerapi.ProcessStatus {
	processesStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
	for instanceKey, processStatus := range originMap {
		processesStatusMap[instanceKey] = &schedulerapi.ProcessStatus{
			ProcessorID:  processStatus.ProcessorID,
			RestThread:   processStatus.RestThread,
			LeaseVersion: processStatus.LeaseVersion,
		}
	}
	return processesStatusMap
}
