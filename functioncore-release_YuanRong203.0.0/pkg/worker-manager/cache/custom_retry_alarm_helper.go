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

// Package cache is the cache for worker
package cache

import "sync"

var helper = &CustomRetryAlarmHelper{
	retryMap:   make(map[string]struct{}),
	retryMutex: sync.Mutex{},
}

// GetCustomRetryAlarmHelper -
func GetCustomRetryAlarmHelper() *CustomRetryAlarmHelper {
	return helper
}

// CustomRetryAlarmHelper -
type CustomRetryAlarmHelper struct {
	retryMap   map[string]struct{}
	retryMutex sync.Mutex
}

// AddAlarm -
func (c *CustomRetryAlarmHelper) AddAlarm(funcSvcID string) {
	c.retryMutex.Lock()
	defer c.retryMutex.Unlock()
	c.retryMap[funcSvcID] = struct{}{}
}

// TryClearAlarmIfExist -
func (c *CustomRetryAlarmHelper) TryClearAlarmIfExist(funcSvcID string) bool {
	c.retryMutex.Lock()
	defer c.retryMutex.Unlock()
	_, ok := c.retryMap[funcSvcID]
	if ok {
		delete(c.retryMap, funcSvcID)
		return true
	}
	return false
}
