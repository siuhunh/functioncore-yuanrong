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

// Package remotescheduler contains remote scheduler client logic
package remotescheduler

import (
	"os"
	"strconv"
	"sync"
	"time"

	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
)

var (
	virtualNodeNum = 100
	instance       *Manager
)

// Manager -
type Manager struct {
	lb loadbalance.NoLockLoadBalance
	// remote scheduler of statefulSet
	stateful map[string]common.RPCClient
	sync.RWMutex
}

// CreateInstance -
func CreateInstance() {
	if virtualNum, err := strconv.Atoi(os.Getenv("VIRTUAL_NODE_NUM")); err == nil {
		virtualNodeNum = virtualNum
	}
	instance = &Manager{
		lb:       loadbalance.NewConsistentHashWithVirtualNode(virtualNodeNum),
		stateful: make(map[string]common.RPCClient),
	}
}

// GetInstance -
func GetInstance() *Manager {
	return instance
}

// AddScheduler -
func (manager *Manager) AddScheduler(key string, info *types.FaaSServiceInfo) {
	manager.Lock()
	defer manager.Unlock()

	config := common.RPCClientConfig{
		IP:             info.IP,
		Port:           info.Port,
		ServerKey:      key,
		RequestTimeout: time.Duration(constant.FConfig.LoadBalanceConfig.AcquireInstanceTimeout) * time.Millisecond,
	}

	client, err := common.CreateRPCClient(config)
	if err != nil {
		log.GetLogger().Errorf("failed to CreateRPCClient, scheduler %s, err: %s", key, err.Error())
		return
	}

	node := &loadbalance.Node{
		Obj: client,
		Key: key,
	}

	switch info.RunningMode {
	case "stateful":
		manager.stateful[key] = client
	default:
		err = manager.lb.Add(node)
	}

	if err != nil {
		log.GetLogger().Errorf("failed to add scheduler, scheduler %s, err: %s", key, err.Error())
		return
	}
	client.Start()
	log.GetLogger().Infof("success to add scheduler %s, %+v", key, info)
}

// DeleteScheduler -
func (manager *Manager) DeleteScheduler(key string, info *types.FaaSServiceInfo) {
	manager.Lock()
	defer manager.Unlock()

	var client common.RPCClient
	var ok bool

	switch info.RunningMode {
	case "stateful":
		client, ok = manager.stateful[key]
		if ok {
			delete(manager.stateful, key)
		}
		log.GetLogger().Infof("DeleteScheduler meet stateful, ok is %t, client is %+v", ok, client)
	default:
		node := manager.lb.Delete(key)
		if node != nil {
			log.GetLogger().Infof("DeleteScheduler get node success, node.Key is %s", node.Key)
			client, ok = node.Obj.(common.RPCClient)
			if !ok {
				log.GetLogger().Errorf("node.Obj assert common.RPCClient failed for key %s", key)
				return
			}
			log.GetLogger().Infof("DeleteScheduler get client success, client.serverKey is %s, "+
				"enable is %s, client is %+v", client.ServerKey(), client.IsEnable(), client)
		}
	}

	if !ok || client == nil {
		log.GetLogger().Errorf("failed to delete scheduler %s, scheduler not exist", key)
		return
	}

	client.Stop()
	log.GetLogger().Infof("success to DeleteScheduler %s", key)
}

// GetScheduler -
func (manager *Manager) GetScheduler(funcKeyWithRes string) common.RPCClient {
	manager.RLock()
	defer manager.RUnlock()

	node := manager.lb.Next(funcKeyWithRes)
	if node == nil {
		return nil
	}

	client, ok := node.Obj.(common.RPCClient)
	if !ok {
		log.GetLogger().Errorf("failed to GetScheduler, type is not valid")
		return nil
	}

	if !client.IsEnable() {
		return nil
	}

	return client
}

// GetStatefulScheduler get stateful scheduler by scheduler name
func (manager *Manager) GetStatefulScheduler(scheduler string) common.RPCClient {
	manager.RLock()
	defer manager.RUnlock()
	client, ok := manager.stateful[scheduler]
	if !ok {
		log.GetLogger().Debugf("can't find stateful scheduler for %s", scheduler)
		return nil
	}
	if !client.IsEnable() {
		log.GetLogger().Debugf("stateful scheduler for %s is disable", scheduler)
		return nil
	}
	return client
}
