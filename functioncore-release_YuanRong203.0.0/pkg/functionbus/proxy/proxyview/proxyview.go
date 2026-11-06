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

// Package proxyview is used to saving available status of proxy
package proxyview

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
)

type proxyTimer struct {
	punishTimer *time.Timer
}

const recoverStatusTime = 3 * time.Second

// unavailableProxy stores the nodeIP that cannot connect
var unavailableProxy = make(map[string]*proxyTimer)
var mutex sync.RWMutex

// UpdateProxyAvailableStatus updates proxy available status
// if failed to connect to remote proxy, update status to be false.
// if functiontask information is added to ETCD or reconnect successfully again, the data would be deleted.
func UpdateProxyAvailableStatus(nodeIP string, status bool) {
	log.GetLogger().Infof("update proxy %s available status is %t", nodeIP, status)
	mutex.Lock()
	value, ok := unavailableProxy[nodeIP]

	// ok = false status = true, proxy is not existed in map and the latest status is connection, no processing
	// ok = false status = false, proxy is not existed in map and the latest status is disconnection, add to the map
	// ok = true status = false, proxy is existed in map and the latest status is disconnection, no processing
	// ok = true status = true, proxy is existed in map and the latest status is connection, deleted the data
	if !ok {
		if !status {
			value = &proxyTimer{punishTimer: time.AfterFunc(recoverStatusTime, deleteStatusTimeFunc(nodeIP))}
			unavailableProxy[nodeIP] = value
		} else {
			mutex.Unlock()
			return
		}
	} else {
		if status {
			value.punishTimer.Stop()
			delete(unavailableProxy, nodeIP)
		}
	}
	mutex.Unlock()
}

func deleteStatusTimeFunc(nodeIP string) func() {
	return func() {
		deleteProxy(nodeIP)
	}
}

func deleteProxy(nodeIP string) {
	log.GetLogger().Infof("delete proxy %s from unavailableProxy map", nodeIP)
	mutex.Lock()
	delete(unavailableProxy, nodeIP)
	mutex.Unlock()
}

// GetProxyAvailableStatus get proxy available status
func GetProxyAvailableStatus(nodeIP string) bool {
	mutex.RLock()
	_, ok := unavailableProxy[nodeIP]
	if !ok {
		mutex.RUnlock()
		log.GetLogger().Debugf("get proxy %s available status is true", nodeIP)
		return true
	}
	mutex.RUnlock()
	log.GetLogger().Debugf("get proxy %s available status is false", nodeIP)

	return false
}
