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

// Package instance -
package instance

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
)

const (
	redisPersistenceTTL = 15 * time.Minute
	redisRetryTimes     = 3
	redisRetryInterval  = 100 * time.Millisecond
	redisPersistenceKey = "/sn/autosacler/persistence"
	defaultVersion      = "0"
)

var (
	redisAlarmCount = 0
)

// ASPersistence set Persistent data of instances
type ASPersistence struct {
	// Version instances struct version
	Version   string                           `json:"version"`
	Timestamp int64                            `json:"timestamp"`
	Instances map[string]*config.InstanceCache `json:"instances"`
}

func (asp *ASPersistence) updateFuncInsCacheFromRedis() {
	for k, v := range asp.Instances {
		cache.GetInstance().FunctionInstanceCache.Store(k, v)
	}
}

func (asp *ASPersistence) setInstances() {
	asp.Instances = make(map[string]*config.InstanceCache)
	cache.GetInstance().FunctionInstanceCache.Range(func(key, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to string: %+v", key)
			return false
		}
		v, ok := value.(*config.InstanceCache)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to InstanceCache: %v", value)
			return false
		}
		asp.Instances[k] = v
		return true
	})
}

// SaveInsCacheToRedis save FunctionInstanceCache to redis
func SaveInsCacheToRedis() {
	redisPersistenceVersion := defaultVersion
	conf, err := config.GetConfig()
	if err == nil {
		redisPersistenceVersion = conf.RedisPersistenceVersion
	} else {
		log.GetLogger().Warnf("failed to get autoscaler config, error: %s", err.Error())
	}
	asp := ASPersistence{
		Version:   redisPersistenceVersion,
		Timestamp: time.Now().UnixMilli(),
	}
	asp.setInstances()

	aspBytes, err := json.Marshal(asp)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal autoscaler persistence, error: %s", err.Error())
		return
	}

	if ok := redisclient.SetWithRetry(redisPersistenceKey, aspBytes, redisPersistenceTTL); ok {
		redisAlarmCount = 0
		alarm.GetClient().ClearAlarm(alarm.RedisConnection)
		return
	}
	redisAlarmCount++
	if redisAlarmCount >= redisRetryTimes {
		alarm.GetClient().ReportAlarm(alarm.RedisConnection,
			fmt.Sprintf("error msg=%s,", "failed to save ins cache to redis"))
	}
}

// RecoverPersistence -
func RecoverPersistence() {
	log.GetLogger().Infof("started to recover FuncInsCache from redis")
	redisPersistenceVersion := defaultVersion
	conf, err := config.GetConfig()
	if err == nil {
		redisPersistenceVersion = conf.RedisPersistenceVersion
	} else {
		log.GetLogger().Warnf("failed to get autoscaler config, error: %s", err.Error())
	}
	var asp ASPersistence
	if redisclient.GetRedisCmd() == nil {
		log.GetLogger().Warnf("autoscaler redis is not ready")
		UpdateFuncInsCache()
		return
	}
	aspJSON, err := redisclient.GetRedisCmd().Get(context.TODO(), redisPersistenceKey).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to get persistence from redis, errors: %s", err.Error())
		UpdateFuncInsCache()
		return
	}
	if err = json.Unmarshal([]byte(aspJSON), &asp); err != nil {
		log.GetLogger().Warnf("failed to unmarshal aspJSON, %s", err.Error())
		UpdateFuncInsCache()
		return
	}
	if asp.Version != redisPersistenceVersion {
		UpdateFuncInsCache()
		return
	}
	asp.updateFuncInsCacheFromRedis()
	UpdateFuncInsCache()
	log.GetLogger().Infof("finished to recover FuncInsCache from redis")
}

// UpdateFuncInsCache update FunctionMetaCache
func UpdateFuncInsCache() {
	log.GetLogger().Info("start to update funcInsCache")

	poolLen := defaultPoolLen

	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get config when update instance cache, err: %s", err.Error())
	} else {
		poolLen = int(cfg.InstanceQueryPoolLen)
	}
	createQueryPool().WithPoolLen(poolLen).StartWorking()

	log.GetLogger().Info("succeed to update funcInsCache")
}
