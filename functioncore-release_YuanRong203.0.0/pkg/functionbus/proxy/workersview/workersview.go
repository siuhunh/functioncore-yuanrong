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

// Package workersview monitor remote worker and choose remote proxy
package workersview

import (
	"sort"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/bridgemetrics"
	"functioncore/pkg/functionbus/proxy/caemetrics"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/resourceusage"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

// WorkersView the struct for monitor remote worker
type WorkersView struct {
	observers sync.Map
	// workers on these nodes are reachable
	enabledNodes      sync.Map
	toDeleteInstances toDeleteInstanceSlice
}

var workersview *WorkersView = nil

var localNodeIP string

type toDeleteInstance struct {
	id             string
	instanceStatus internal.InstanceStatus
}

type toDeleteInstanceSlice []toDeleteInstance

// Sort -
func (x toDeleteInstanceSlice) Sort() {
	sort.Slice(x, func(i, j int) bool {
		if i < 0 || i >= len(x) || j < 0 || j >= len(x) {
			return false
		}
		return x[i].id < x[j].id
	})
}

// Exist -
func (x toDeleteInstanceSlice) Exist(id string) bool {
	pos := sort.Search(len(x), func(i int) bool {
		if i < 0 || i >= len(x) {
			return false
		}
		return x[i].id >= id
	})
	return pos >= 0 && pos < len(x) && x[pos].id == id
}

// GetWorkersView get singleton of workersview
func GetWorkersView() *WorkersView {
	if workersview == nil {
		localNodeIP = internal.SelfNodeIP
		workersview = &WorkersView{
			observers:    sync.Map{},
			enabledNodes: sync.Map{},
		}
	}
	return workersview
}

// ObserveWorkers observe workers add
func (wv *WorkersView) ObserveWorkers(funcKeyWithResource string, callback func(bool)) {
	wv.observers.Store(funcKeyWithResource, callback)
}

// UnobserveWorkers observe workers add or delete
func (wv *WorkersView) UnobserveWorkers(funcKeyWithResource string) {
	wv.observers.Delete(funcKeyWithResource)
}

func (wv *WorkersView) onObserved(funcKeyWithResource string) {
	if cb, ok := wv.observers.Load(funcKeyWithResource); ok {
		// remote workers update, remote=true
		if cb, ok := cb.(func(bool)); ok {
			cb(true)
		}
	}
}

// RecoverLongRun recover terminating long-run instance
func (wv *WorkersView) RecoverLongRun(ins types.InstanceRouter) {
	log.GetLogger().Infof("recovering longrun func %s %s %s %t %t", ins.FunctionKeyWithResource, ins.InstanceID,
		ins.WorkerAddr, ins.IsMinInstance, ins.IsDirectFunc)
	config := &workerbroker.BrokerConfig{
		FuncKeyWithResource: ins.FunctionKeyWithResource,
		WorkerID:            ins.InstanceID,
		WorkerAddr:          ins.WorkerAddr,
		RemoteIP:            internal.SelfNodeIP,
		IsMinInstance:       ins.IsMinInstance,
		IsDirectFunc:        ins.IsDirectFunc,
		HasInitializer:      ins.HasInitializer,
	}
	workerbroker.GetInstance().CreateWorkerBroker(config)
}

func createBrokerConfig(funcKeyWithResource, instanceID string,
	provider *servicediscovery.Provider, isSynced bool) *workerbroker.BrokerConfig {
	config := &workerbroker.BrokerConfig{
		FuncKeyWithResource: funcKeyWithResource,
		WorkerID:            instanceID,
		WorkerAddr:          provider.IP + ":" + provider.P2pPort,
		RemoteIP:            provider.OwnerIP,
		IsMinInstance:       provider.Applier == constants.WorkerManagerApplier,
		StateID:             provider.StateID,
		CreateEventChan:     true,
		IsDirectFunc:        provider.IsDirectFunc,
		HasInitializer:      provider.HasInitializer,
		BusinessType:        provider.BusinessType,
		PodUID:              provider.PodUID,
		ContainerIDs:        provider.ContainerIDs,
		Resource:            provider.Resource,
		ViewResource:        provider.ViewResource,
	}
	return config
}

// AddWorker add when worker added
func (wv *WorkersView) AddWorker(
	funcKeyWithResource, instanceID string, provider *servicediscovery.Provider, isSynced bool) {
	if funcKeyWithResource == "" || instanceID == "" || provider.NodeIP == "" || provider.OwnerIP == "" {
		return
	}

	config := createBrokerConfig(funcKeyWithResource, instanceID, provider, isSynced)
	if provider.OwnerIP != localNodeIP {
		// the code has been deprecated, the ownerIP and nodeIP values are the same in the current version
		if provider.NodeIP == localNodeIP {
			go workerbroker.GetInstance().CreateWorkerBroker(config)
		} else {
			// worker is not owned by this proxy
			getRandomWeightMap().addWorker(funcKeyWithResource, instanceID, provider)
		}
		return
	}

	// if is cae func and local worker, need round-robin
	getRandomWeightMap().addWorker(funcKeyWithResource, instanceID, provider)

	internal.SaveOwnedInstances(instanceID, funcKeyWithResource, config.IsMinInstance)
	log.GetLogger().Infof("addWorker | %s | %s", urnutils.ExtractPodName(instanceID),
		urnutils.FuncKeyWithResToShort(funcKeyWithResource))
	// instance of owner is this node
	if provider.NodeIP == localNodeIP {
		config.RemoteIP = ""
		// local worker
		go workerbroker.GetInstance().CreateWorkerBroker(config)
	} else {
		// remote worker
		common.NotifyRemote(provider.NodeIP, &common.FunctionProperty{
			TenantID:   common.BusProxyTenantID,
			InstanceID: instanceID,
		}, &common.NotifyEvent{
			NotifyType:          common.ChangeState,
			Enable:              true,
			FuncKeyWithResource: funcKeyWithResource,
			WorkerAddress:       provider.IP + ":" + provider.P2pPort,
			WorkerOwnerIP:       provider.OwnerIP,
			IsDirectFunc:        provider.IsDirectFunc,
			HasInitializer:      provider.HasInitializer,
		})
		workerbroker.GetInstance().CreateRemoteBroker(funcKeyWithResource, instanceID, provider.NodeIP,
			provider.StateID)
	}
}

// DeleteWorker delete when worker deleted
func (wv *WorkersView) DeleteWorker(
	funcKeyWithResource, instanceID string, provider *servicediscovery.Provider, isSynced bool) {
	if funcKeyWithResource == "" || instanceID == "" || provider.NodeIP == "" || provider.OwnerIP == "" {
		return
	}

	getRandomWeightMap().delWorker(funcKeyWithResource, instanceID, provider.OwnerIP)
	if provider.OwnerIP != localNodeIP {
		// worker is not owned by this proxy
		workerbroker.GetInstance().DeleteRemoteBroker(funcKeyWithResource, instanceID)
		return
	}

	log.GetLogger().Infof("delete worker %s %s", instanceID, funcKeyWithResource)

	internal.DeleteOwnedInstances(instanceID)

	resourceusage.GetNodeResourceUsageInstance().
		RemoveByInstance(utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource), instanceID)

	caemetrics.GetCaeMetricsHandler().
		RemoveByInstance(utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource), instanceID)

	bridgemetrics.GetBridgeMetricsHandler().
		RemoveByInstance(utils.GetFuncKeyFromFuncKeyWithResource(funcKeyWithResource), instanceID)

	workerAddr := provider.IP + ":" + provider.P2pPort
	workerbroker.GetInstance().WaitEventSignal(instanceID)
	workerbroker.GetInstance().DeleteWorkerBroker(funcKeyWithResource, instanceID, workerAddr, false)
}

// EnableNode enable node then workers on this node are reachable
func (wv *WorkersView) EnableNode(nodeIP string) {
	wv.enabledNodes.Store(nodeIP, nil)
}

// DisableNode disable node then workers on this node are unreachable
func (wv *WorkersView) DisableNode(nodeIP string) {
	wv.enabledNodes.Delete(nodeIP)
}

// Start -
func (wv *WorkersView) Start(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("nil stopCh")
		return
	}

	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case _, ok := <-t.C:
			if !ok {
				log.GetLogger().Errorf("ticker closed unexpectedly")
				return
			}
			wv.syncWorkers()
		case <-stopCh:
			return
		}
	}
}

func (wv *WorkersView) syncWorkers() {
	var toDeleteInstances toDeleteInstanceSlice
	internal.OwnedInstances.Lock()
	for instanceID, instanceStatus := range internal.OwnedInstances.InstanceMap {
		if !workerevaluator.GetInstance().IsExist(instanceID) {
			toDeleteInstances = append(toDeleteInstances, toDeleteInstance{
				id:             instanceID,
				instanceStatus: instanceStatus,
			})
		}
	}
	internal.OwnedInstances.Unlock()
	wv.toDeleteInstances.Sort()
	for _, instance := range toDeleteInstances {
		if wv.toDeleteInstances.Exist(instance.id) {
			log.GetLogger().Infof("scale down broken worker, instanceID %s, funcKeyWithResource %s, "+
				"isBrokenConnection %t", instance.id, instance.instanceStatus.FuncKeyWithResource,
				instance.instanceStatus.IsBrokenConnection)
			go action.ScaleDown(action.ScaleDownParam{WorkerID: instance.id},
				utils.GetFuncKeyFromFuncKeyWithResource(instance.instanceStatus.FuncKeyWithResource),
				instance.instanceStatus.IsBrokenConnection)
		}
	}
	wv.toDeleteInstances = toDeleteInstances
}
