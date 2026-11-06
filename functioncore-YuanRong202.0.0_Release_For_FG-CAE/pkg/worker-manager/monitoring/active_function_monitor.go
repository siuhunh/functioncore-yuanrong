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

package monitoring

import (
	"context"
	"strconv"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/worker-manager/cache"
)

const (
	// DynamicKeepAliveTimeValidTimeWindow if the downloaded data's update time is generated one hour ago
	// it will be seen as outdated and default keep alive time will be used for all worker
	DynamicKeepAliveTimeValidTimeWindow = 60 * 60
	// DynamicKeepAliveTimeRedisKey redis key for adjusting keep-alive time dynamically
	DynamicKeepAliveTimeRedisKey = "FunctionGraph:DynamicKeepAliveTime"
	// LastUpdateTimeKey time of updating keepalive metrics
	LastUpdateTimeKey = "lastUpdateTime"

	int64Base          = 10
	int64BitSize       = 64
	defaultDialTimeout = 5 * time.Second
)

func getAndMonitorActiveFunction(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Warn("stop channel is nil")
		return
	}
	go initInactiveFunctionCronJob(stopCh)
}

func initInactiveFunctionCronJob(stopCh <-chan struct{}) {
	ticker := time.NewTicker(time.Minute)
	for {
		select {
		case <-ticker.C:
			updateInactiveFunctionList()
		case <-stopCh:
			log.GetLogger().Infof("graceful exit")
			ticker.Stop()
			return
		}
	}
}

func updateInactiveFunctionList() {
	if redisclient.GetRedisCmd() == nil || redisclient.GetRedisCmd().GetClient() == nil {
		log.GetLogger().Warnf("redis is not ready when get inactive function info")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()
	inactiveFunctionMap, err := redisclient.GetRedisCmd().HGetAll(ctx, DynamicKeepAliveTimeRedisKey).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to get inactive function redis key: %s, err: %s",
			DynamicKeepAliveTimeRedisKey, err.Error())
		return
	}
	latestUpdateTime, ok := inactiveFunctionMap[LastUpdateTimeKey]
	if !ok {
		log.GetLogger().Warnf("bad format for inactiveFunctionMap which downloaded from Redis")
		return
	}
	latestUpdateTimeInt, err := strconv.ParseInt(latestUpdateTime, int64Base, int64BitSize)
	if err != nil {
		log.GetLogger().Warnf("failed to parse lastUpdateTime, err: %s", err.Error())
		return
	}
	gap := time.Now().Unix() - latestUpdateTimeInt
	if gap > DynamicKeepAliveTimeValidTimeWindow {
		log.GetLogger().Warnf("reset inactiveFunctionList because lastUpdateData is one hour ago")
		cache.GetActivenessCaches().FunctionListLock.Lock()
		cache.GetActivenessCaches().InactiveFunctionList = make(map[string]string)
		cache.GetActivenessCaches().FunctionListLock.Unlock()
		return
	}
	cache.GetActivenessCaches().FunctionListLock.Lock()
	cache.GetActivenessCaches().InactiveFunctionList = inactiveFunctionMap
	cache.GetActivenessCaches().FunctionListLock.Unlock()
	log.GetLogger().Infof("updated inactiveFunctionList: %v", inactiveFunctionMap)
}
