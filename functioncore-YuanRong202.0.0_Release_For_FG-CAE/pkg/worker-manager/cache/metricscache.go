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

// Package cache is the cache for worker
package cache

import (
	"encoding/json"
	"strings"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/metrics"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
)

const (
	defaultPodNameLen = 6
	memSizeLocation   = 2
)

// MetricsCache metrics cache
var MetricsCache = &metricsCache{
	functionInstanceCurrentCount: make(map[string]int, initCacheSize),
	ReserveInstanceTimestamp:     make(map[string]ReserveInstanceInfo, initCacheSize),
}

// ReserveInstanceInfo -
type ReserveInstanceInfo struct {
	FunctionURN string
	PodName     string
	DelSignal   bool
	FirstStart  bool
}

type metricsCache struct {
	functionInstanceCurrentCount map[string]int
	ReserveInstanceTimestamp     map[string]ReserveInstanceInfo
	lock                         sync.RWMutex
	ReserveLock                  sync.Mutex
}

// UpdateFuncInstanceCurrentCount -
func (c *metricsCache) UpdateFuncInstanceCurrentCount(funcInfo *metrics.FunctionInfo, count int) {
	c.lock.Lock()
	funcKey := tools.GetFuncKey(funcInfo.TenantID, funcInfo.ServiceID, funcInfo.FunctionName, funcInfo.Version)
	if count == 0 {
		delete(c.functionInstanceCurrentCount, funcKey)
		c.lock.Unlock()
		log.GetLogger().Debugf("succeeded to clear funcKey %s metrics cache", urnutils.AnonymizeTenantKey(funcKey))
		return
	}
	c.functionInstanceCurrentCount[funcKey] = count
	c.lock.Unlock()
	log.GetLogger().Debugf("succeeded to update funcKey %s value to %d", urnutils.AnonymizeTenantKey(funcKey), count)
}

// IncFuncInstanceCurrentCount -
func (c *metricsCache) IncFuncInstanceCurrentCount(funcKey string) {
	c.lock.Lock()
	count, exist := c.functionInstanceCurrentCount[funcKey]
	if !exist {
		c.functionInstanceCurrentCount[funcKey] = 0
	}
	c.functionInstanceCurrentCount[funcKey] = count + 1
	c.lock.Unlock()
}

// ClearByFunctionService -
func (c *metricsCache) ClearByFunctionService(functionService *faasfunction.FunctionService) {
	c.lock.Lock()
	conf := functionService.Spec.Configuration
	funcKey := tools.GetFuncKey(conf.TenantID, "", conf.FunctionName, conf.FunctionVersion)
	if _, exist := c.functionInstanceCurrentCount[funcKey]; exist {
		delete(c.functionInstanceCurrentCount, funcKey)
		log.GetLogger().Debugf("succeeded to clear funcKey %s metrics cache", urnutils.AnonymizeTenantKey(funcKey))
	}
	c.lock.Unlock()
}

// IncFuncInstanceCurrentCount -
func IncFuncInstanceCurrentCount(funcKey string) {
	MetricsCache.IncFuncInstanceCurrentCount(funcKey)
}

// MarshalFuncInsCurrentCount -
func MarshalFuncInsCurrentCount() ([]byte, error) {
	MetricsCache.lock.RLock()
	countMap := MetricsCache.functionInstanceCurrentCount
	data, err := json.Marshal(countMap)
	MetricsCache.lock.RUnlock()
	return data, err
}

// AddReserveInstanceTimestamp add reserved instance and add timestamp info
func AddReserveInstanceTimestamp(funcInfo *metrics.FunctionInfo, podName string, recover bool) {
	MetricsCache.ReserveLock.Lock()
	defer MetricsCache.ReserveLock.Unlock()
	for name := range MetricsCache.ReserveInstanceTimestamp {
		if name == podName {
			return
		}
	}
	log.GetLogger().Debugf("receive add reserved instance: %s timestamp request", podName)
	if funcInfo.TenantID == "" && funcInfo.FunctionName == "" && funcInfo.Version == "" {
		log.GetLogger().Warn("Add reserved instance timestamp functionInfo is nil")
		return
	}
	functionName, memSize := handleFunctionName(funcInfo.FunctionName, podName)
	if functionName == "" {
		log.GetLogger().Warn("failed to handle functionName")
		return
	}
	functionURN := funcInfo.TenantID + "@" + functionName + "@" + funcInfo.Version + "@" + memSize
	info := ReserveInstanceInfo{
		FunctionURN: functionURN,
		PodName:     podName,
		FirstStart:  recover,
	}
	MetricsCache.ReserveInstanceTimestamp[podName] = info
}

// DeleteReserveInstanceTimestamp add reserved instance and del timestamp info
func DeleteReserveInstanceTimestamp(podName string) {
	log.GetLogger().Debugf("receive del reserved instance: %s timestamp request", podName)
	MetricsCache.ReserveLock.Lock()
	delete(MetricsCache.ReserveInstanceTimestamp, podName)
	MetricsCache.ReserveLock.Unlock()
}

// CheckReserveInstanceExist check reserved instance is or not exist
func CheckReserveInstanceExist(podName string) bool {
	MetricsCache.ReserveLock.Lock()
	defer MetricsCache.ReserveLock.Unlock()
	info, ok := MetricsCache.ReserveInstanceTimestamp[podName]
	if !ok {
		return false
	}
	if info.DelSignal {
		return false
	}
	return true
}

func handleFunctionName(functionName string, podName string) (string, string) {
	funcName := ""
	memSize := ""
	if strings.HasPrefix(functionName, "0@") && strings.Contains(podName, "-") {
		splits := strings.Split(podName, "-")
		if len(splits) != defaultPodNameLen {
			return funcName, memSize
		}
		memSize = splits[memSizeLocation]
		funcName = strings.TrimPrefix(functionName, "0@")
	}
	return funcName, memSize
}
