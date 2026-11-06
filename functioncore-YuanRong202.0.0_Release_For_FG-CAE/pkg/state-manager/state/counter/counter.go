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

// Package counter count tenant states num
package counter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/utils"
)

const (
	counterMapRedisKeyTTL  = 24 * time.Hour
	redisContextTimeout    = 2000 * time.Millisecond
	lockContextTimeout     = 6 * time.Millisecond
	stateCounterMapKey     = "state_counter_map"
	stateCounterMapLockKey = "state_counter_lock"
	defaultCounterMapSize  = 128
)

// Counter define counter struct
type Counter struct {
	config          *config.FConfig
	counterMap      map[string]map[string]*tenantCount
	localMap        map[string]*tenantCount
	counterMapMutex sync.Mutex
	localMapMutex   sync.Mutex
}

type tenantCount struct {
	Count int64 `json:"count" valid:"optional"`
	Limit int64 `json:"limit" valid:"optional"`
}

var (
	counter = &Counter{}
)

// NewCounter generate a new counter
func NewCounter(c *config.FConfig, stopChan <-chan struct{}) error {
	counter.config = c
	if err := counter.restoreCounterMap(); err != nil {
		log.GetLogger().Errorf("failed to restore counter map, %s", err.Error())
		return err
	}
	go persistenceCounterMapTask(stopChan)
	log.GetLogger().Infof("succeeded to new state counter")
	return nil
}

func (c *Counter) persistenceCounterMap() {
	err := getCounterMap()
	if err != nil {
		log.GetLogger().Errorf("failed to get counter map: %s when persist", err.Error())
		return
	}
	c.counterMapMutex.Lock()
	for tenant, value := range c.localMap {
		if c.counterMap[tenant] == nil {
			c.counterMap[tenant] = map[string]*tenantCount{}
		}
		c.counterMap[tenant][c.config.StateCounterCfg.ClusterID] = value
	}
	data, err := json.Marshal(c.counterMap)
	c.counterMapMutex.Unlock()
	if err != nil {
		log.GetLogger().Errorf("failed to marshal counterMap, %s", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	if utils.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready when persistence counter map")
		return
	}
	if _, err := utils.GetRedisCmd().Set(ctx, stateCounterMapKey, string(data),
		counterMapRedisKeyTTL).Result(); err != nil {
		log.GetLogger().Errorf("failed to set counterMap to redis, %s", err.Error())
	}
}

func (c *Counter) restoreCounterMap() error {
	err := getCounterMap()
	if err != nil {
		log.GetLogger().Warnf("failed to get counter map from redis: %s, init default local cache", err.Error())
	}
	localMap := make(map[string]*tenantCount, defaultCounterMapSize)
	for tenant, valueMap := range c.counterMap {
		if value, ok := valueMap[c.config.StateCounterCfg.ClusterID]; ok {
			localMap[tenant] = value
		}
	}
	c.localMap = localMap
	return nil
}

func getCounterMap() error {
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	if utils.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready when get counter map")
		return nil
	}
	counterMapJSON, err := utils.GetRedisCmd().Get(ctx, stateCounterMapKey).Result()
	if err != nil && err != redis.Nil {
		log.GetLogger().Errorf("failed to get counterMapJSON from redis, %s", err.Error())
		return err
	}
	counterMap := make(map[string]map[string]*tenantCount, defaultCounterMapSize)
	if counterMapJSON != "" {
		if err := json.Unmarshal([]byte(counterMapJSON), &counterMap); err != nil {
			log.GetLogger().Errorf("failed to unmarshal counterMapJSON, %s", err.Error())
			return err
		}
	}
	counter.counterMapMutex.Lock()
	counter.counterMap = counterMap
	counter.counterMapMutex.Unlock()
	return nil
}

func persistenceCounterMapTask(stopChan <-chan struct{}) {
	ticker := time.NewTicker(time.Duration(counter.config.StateCounterCfg.PersistenceInterval) * time.Minute)
	for {
		select {
		case <-ticker.C:
			if utils.GetRedisCmd() == nil {
				log.GetLogger().Errorf("redis is not ready when persistence counter map")
				continue
			}
			if getDistributedLock() {
				counter.persistenceCounterMap()
				releaseDistributedLock()
			}
		case _, ok := <-stopChan:
			if !ok {
				if getDistributedLock() {
					counter.persistenceCounterMap()
					releaseDistributedLock()
				}
				ticker.Stop()
				log.GetLogger().Debugf("stop persistence counterMap")
				return
			}
		}
	}
}

func getTenantCounter(tenantID string) *tenantCount {
	counter.counterMapMutex.Lock()
	defer counter.counterMapMutex.Unlock()
	if _, exists := counter.localMap[tenantID]; !exists {
		counter.localMap[tenantID] = &tenantCount{
			Limit: config.TenantStateLocalLimit,
		}
	}
	return counter.localMap[tenantID]
}

// Incr counter num increase 1
func Incr(tenantID string) error {
	tenantCounter := getTenantCounter(tenantID)
	if err := tenantCounter.incr(tenantID); err != nil {
		log.GetLogger().Errorf("Failed to incr state count, tenantID: %s, error: %s", tenantID, err.Error())
		return snerror.New(snerror.TenantStateNumExceedLimit,
			fmt.Sprintf(snerror.ErrText(snerror.TenantStateNumExceedLimit), tenantCounter.Limit))
	}
	return nil
}

// Decr counter num minus 1
func Decr(tenantID string) {
	tenantCounter := getTenantCounter(tenantID)
	tenantCounter.decr()
}

func (t *tenantCount) incr(tenantID string) error {
	counter.localMapMutex.Lock()
	defer counter.localMapMutex.Unlock()
	t.Count++
	if t.Count > t.Limit {
		err := getCounterMap()
		if err != nil {
			t.Count--
			return err
		}
		var total int64
		counter.counterMapMutex.Lock()
		if counterMap, ok := counter.counterMap[tenantID]; ok {
			for _, counters := range counterMap {
				total += counters.Limit
			}
		}
		counter.counterMapMutex.Unlock()
		if config.TenantStateLimit-total > config.TenantStateLocalLimit && getDistributedLock() {
			t.Limit += config.TenantStateLocalLimit
			counter.persistenceCounterMap()
			releaseDistributedLock()
			return nil
		}
		t.Count--
		return errors.New("failed to incr tenant state count")
	}
	return nil
}

func (t *tenantCount) decr() {
	counter.localMapMutex.Lock()
	defer counter.localMapMutex.Unlock()
	if t.Count == 0 {
		return
	}
	t.Count--
	if t.Limit-t.Count > config.TenantStateLocalLimit && getDistributedLock() {
		t.Limit -= config.TenantStateLocalLimit
		counter.persistenceCounterMap()
		releaseDistributedLock()
	}
	return
}

func getDistributedLock() bool {
	ok, err := utils.GetRedisCmd().SetNX(context.TODO(), stateCounterMapLockKey, "state possession",
		lockContextTimeout).Result()
	if err != nil {
		log.GetLogger().Warnf("getDistributedLock failed, failed to compete redis lock, err: %s", err.Error())
		return false
	}
	if !ok {
		log.GetLogger().Warnf("getDistributedLock failed, failed to compete redis lock")
		return false
	}
	log.GetLogger().Debug("succeeded to compete redis lock")
	return true
}

func releaseDistributedLock() {
	_, err := utils.GetRedisCmd().Del(context.TODO(), stateCounterMapLockKey).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to release distributed lock: %s", err.Error())
	}
	return
}
