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

// Package internal contains internal structs and variables
package internal

import (
	"errors"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
)

var (
	errNodeIP   = errors.New("failed to get node ip from environment")
	errPodIP    = errors.New("failed to get pod ip from environment")
	errNodeName = errors.New("failed to get node name from environment")
	// OwnedInstances indicates the instance map of the current function.
	OwnedInstances = struct {
		InstanceMap map[string]InstanceStatus
		sync.RWMutex
	}{
		InstanceMap: make(map[string]InstanceStatus),
		RWMutex:     sync.RWMutex{},
	}
)

// InstanceStatus the struct for instance status
type InstanceStatus struct {
	FuncKeyWithResource string
	IsBrokenConnection  bool
	IsMinInstance       bool
}

// SaveOwnedInstances -
func SaveOwnedInstances(instanceID string, funcKeyWithResource string, isMinInstance bool) {
	OwnedInstances.Lock()
	defer OwnedInstances.Unlock()
	OwnedInstances.InstanceMap[instanceID] = InstanceStatus{
		FuncKeyWithResource: funcKeyWithResource,
		IsMinInstance:       isMinInstance,
	}
}

// DeleteOwnedInstances -
func DeleteOwnedInstances(instanceID string) {
	OwnedInstances.Lock()
	defer OwnedInstances.Unlock()
	delete(OwnedInstances.InstanceMap, instanceID)
}

// IsOwnedInstancesExist -
func IsOwnedInstancesExist(instanceID string) bool {
	OwnedInstances.RLock()
	defer OwnedInstances.RUnlock()
	_, ok := OwnedInstances.InstanceMap[instanceID]
	return ok
}

// UpdateOwnedInstances -
func UpdateOwnedInstances(instanceID string, IsBrokenConnection bool) {
	OwnedInstances.Lock()
	defer OwnedInstances.Unlock()
	data, ok := OwnedInstances.InstanceMap[instanceID]
	if !ok {
		log.GetLogger().Infof("instanceID %s is not exist", instanceID)
		return
	}
	if data.IsMinInstance {
		data.IsBrokenConnection = IsBrokenConnection
		OwnedInstances.InstanceMap[instanceID] = data
		log.GetLogger().Infof("IsBrokenConnection is %t, reserved instance %s will be deleted",
			IsBrokenConnection, instanceID)
	}
}

// GetEnvs init global values from environment
func GetEnvs() error {
	if SelfNodeIP == "" || net.ParseIP(SelfNodeIP) == nil {
		return errNodeIP
	}

	if SelfPodIP == "" || net.ParseIP(SelfPodIP) == nil {
		return errPodIP
	}

	if HostName == "" {
		return errNodeName
	}

	if valueStr := os.Getenv("REQUESTS_LIMIT"); valueStr != "" {
		if valueInt, err := strconv.Atoi(valueStr); err == nil && valueInt > 0 && valueInt < math.MaxInt32 {
			log.GetLogger().Infof("set RequestsLimit from environment: %d", valueInt)
			RequestsLimit = int32(valueInt)
		}
	}

	if valueStr := os.Getenv("MAX_STORE_RESPONSE_SIZE"); valueStr != "" {
		if valueInt, err := strconv.Atoi(valueStr); err == nil && valueInt > 0 {
			log.GetLogger().Infof("set MaxStoreResponseSize from environment: %d", valueInt)
			MaxStoreResponseSize = valueInt
		}
	}

	if valueStr := os.Getenv("DISABLE_BACK_PRESSURE"); valueStr == "true" {
		log.GetLogger().Infof("set EnableBackPressure from environment: %s", valueStr)
		DisableBackPressure = true
	}

	return nil
}

// SelfNodeIP - node IP
var SelfNodeIP = os.Getenv(constants.HostIPEnvKey)

// SelfPodIP - pod IP
var SelfPodIP = os.Getenv(constants.PodIPEnvKey)

// HostName - local task hostName
var HostName = os.Getenv(constants.HostNameEnvKey)

// IdleTimeForWorker workers lasted for at most 60s from status-idle to request-to-scale
var IdleTimeForWorker = time.Duration(60) * time.Second

// IdleTimeForCaeWorker cae workers lasted for at most 600s from status-idle to request-to-scale
var IdleTimeForCaeWorker = time.Duration(600) * time.Second

// RequestsLimit concurrent requests limit for single proxy
var RequestsLimit int32 = 200000

// RequestQueueTimeout request queued timeout. Reserve 20s for network delay all the way
var RequestQueueTimeout = time.Duration(40) * time.Second

// MaxStoreResponseSize -
var MaxStoreResponseSize = 100 * 1024

// DisableBackPressure -
var DisableBackPressure = false
