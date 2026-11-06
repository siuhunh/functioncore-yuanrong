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
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

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
	err := getCountMapByNewHashKey()
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
	clusterStateCount := map[string]*tenantCount{}
	for tenant, tenantClusterState := range c.counterMap {
		clusterStateCount[tenant] = tenantClusterState[c.config.StateCounterCfg.ClusterID]
	}
	data, err := json.Marshal(clusterStateCount)
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
	if _, err := utils.GetRedisCmd().Set(ctx, c.config.StateCounterCfg.ClusterID+config.HashTag, string(data),
		counterMapRedisKeyTTL).Result(); err != nil {
		log.GetLogger().Errorf("failed to set counterMap to redis, %s", err.Error())
	}
}

func (c *Counter) restoreCounterMap() error {
	err := getCountMapByNewHashKey()
	if err != nil {
		log.GetLogger().Warnf("failed to get counter map from redis: %s, init default local cache", err.Error())
	}
	localMap := make(map[string]*tenantCount, defaultCounterMapSize)
	for tenant, valueMap := range c.counterMap {
		if value, ok := valueMap[c.config.StateCounterCfg.ClusterID]; ok {
			localMap[tenant] = value
		}
	}
	counter.localMapMutex.Lock()
	c.localMap = localMap
	counter.localMapMutex.Unlock()
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

func getCountMapByNewHashKey() error {
	if utils.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready when get counter map")
		return nil
	}
	if config.FConfigs == nil || config.FConfigs.StateCounterCfg.SiteClusters == nil {
		return fmt.Errorf("FConfig is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()

	counterMapRaw, err := utils.GetRedisCmd().MGet(ctx,
		config.TransToClusterTenantHashTagKey(config.FConfigs.StateCounterCfg.SiteClusters)...).Result()
	if err != nil && err != redis.Nil {
		log.GetLogger().Errorf("failed to get counterMapJSON from redis, %s", err.Error())
		return err
	}
	var mGetFailedSlice []string
	counter.counterMapMutex.Lock()
	if counter.counterMap == nil {
		counter.counterMap = map[string]map[string]*tenantCount{}
	}
	for clusterIndex, val := range counterMapRaw {
		if val == nil {
			mGetFailedSlice = append(mGetFailedSlice, config.FConfigs.StateCounterCfg.SiteClusters[clusterIndex])
			continue
		}
		value, ok := val.(string)
		if !ok || value == "" {
			mGetFailedSlice = append(mGetFailedSlice, config.FConfigs.StateCounterCfg.SiteClusters[clusterIndex])
			continue
		}
		clusterCounterMap := make(map[string]*tenantCount, defaultCounterMapSize)
		if err := json.Unmarshal([]byte(value), &clusterCounterMap); err != nil {
			log.GetLogger().Errorf("failed to unmarshal clusterHashKeyJSON, %s", err.Error())
			mGetFailedSlice = append(mGetFailedSlice, config.FConfigs.StateCounterCfg.SiteClusters[clusterIndex])
			continue
		}
		clusterID := config.FConfigs.StateCounterCfg.SiteClusters[clusterIndex]
		for tenant, stateCount := range clusterCounterMap {
			if counter.counterMap[tenant] == nil {
				counter.counterMap[tenant] = map[string]*tenantCount{}
			}
			if stateCount == nil {
				log.GetLogger().Warnf("stateCount of tenant:%s-cluster:%s is nil, "+
					"need to get again from old key", tenant, clusterID)
				mGetFailedSlice = append(mGetFailedSlice, clusterID)
				continue
			}
			counter.counterMap[tenant][clusterID] = stateCount
		}
	}
	counter.counterMapMutex.Unlock()
	if len(mGetFailedSlice) != 0 {
		refreshStateCacheByStateCountKey(mGetFailedSlice)
	}
	return nil
}

func refreshStateCacheByStateCountKey(clusterNeedToRefresh []string) {
	log.GetLogger().Infof("cluster: %v could not get state count by new key, search state_count_map key.",
		clusterNeedToRefresh)
	if len(clusterNeedToRefresh) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	if utils.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready when get counter map")
		return
	}
	counterMapJSON, err := utils.GetRedisCmd().Get(ctx, stateCounterMapKey).Result()
	if err != nil && err != redis.Nil {
		log.GetLogger().Errorf("failed to get counterMapJSON from redis, %s", err.Error())
		return
	}
	counterMap := make(map[string]map[string]*tenantCount, defaultCounterMapSize)
	if counterMapJSON != "" {
		if err := json.Unmarshal([]byte(counterMapJSON), &counterMap); err != nil {
			log.GetLogger().Errorf("failed to unmarshal counterMapJSON, %s", err.Error())
			return
		}
	}

	counter.counterMapMutex.Lock()
	for tenant, clusterTenantCount := range counterMap {
		for _, clusterID := range clusterNeedToRefresh {
			if counter.counterMap[tenant] == nil {
				counter.counterMap[tenant] = map[string]*tenantCount{}
			}
			if clusterTenantCount[clusterID] == nil {
				log.GetLogger().Warnf("clusterTenantCount[clusterID] of tenant:%s-cluster:%s is nil, use default",
					tenant, clusterID)
				counter.counterMap[tenant][clusterID] = &tenantCount{
					Limit: config.TenantStateLocalLimit,
				}
			} else {
				counter.counterMap[tenant][clusterID] = clusterTenantCount[clusterID]
			}
		}
	}
	counter.counterMapMutex.Unlock()

	log.GetLogger().Infof("refresh cluster: %v state limit succeed.",
		clusterNeedToRefresh)
	return
}

// ResetRealStateCountByETCD -
func ResetRealStateCountByETCD(realCount map[string]int64) {
	log.GetLogger().Infof("start to reset the real tenant count: %v", realCount)
	if config.FConfigs == nil || config.FConfigs.StateCounterCfg.ClusterID == "" {
		log.GetLogger().Errorf("FConfig is nil")
		return
	}
	curClusterID := config.FConfigs.StateCounterCfg.ClusterID
	counter.counterMapMutex.Lock()
	if counter.counterMap == nil {
		counter.counterMap = map[string]map[string]*tenantCount{}
	}
	tmpClusterStateCount := map[string]*tenantCount{}
	for tenant, count := range realCount {
		if counter.counterMap[tenant] == nil {
			counter.counterMap[tenant] = map[string]*tenantCount{}
		}
		val, ok := counter.counterMap[tenant][curClusterID]
		if !ok || val == nil {
			counter.counterMap[tenant][curClusterID] = &tenantCount{Limit: config.TenantStateLocalLimit}
		}
		counter.counterMap[tenant][curClusterID].Count = count
		tmpClusterStateCount[tenant] = counter.counterMap[tenant][curClusterID]
		// reset tenant cache in localMap by counterMap pointer
		counter.localMapMutex.Lock()
		if counter.localMap == nil {
			counter.localMap = make(map[string]*tenantCount, defaultCounterMapSize)
		}
		counter.localMap[tenant] = counter.counterMap[tenant][curClusterID]
		counter.localMapMutex.Unlock()
	}
	counter.counterMapMutex.Unlock()
	// delete unExist tenant in localMap cache
	counter.localMapMutex.Lock()
	for tenant := range counter.localMap {
		if _, ok := realCount[tenant]; !ok {
			delete(counter.localMap, tenant)
		}
	}
	counter.localMapMutex.Unlock()
	// if realCount is nil, clean all localMap cache in previous step,
	// because there is no any tenant stateCount in current cluster,
	// this function will only do once when process initStateCacheMap, so return here.
	if len(tmpClusterStateCount) == 0 {
		log.GetLogger().Warnf("reset local tenant countMap cache end, there is no tenant cache.")
		return
	}
	data, err := json.Marshal(tmpClusterStateCount)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal counterMap, %s", err.Error())
		return
	}
	log.GetLogger().Infof("the cluster: %s real tenant count: %s", curClusterID, string(data))
	setClusterIDHashTagInRedis(curClusterID+config.HashTag, string(data))
	log.GetLogger().Infof("reset the cluster: %s real tenant count: %s succeed", curClusterID, string(data))
}

func setClusterIDHashTagInRedis(clusterKey, clusterText string) {
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	if utils.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready when persistence counter map")
		return
	}
	if _, err := utils.GetRedisCmd().Set(ctx, clusterKey, clusterText,
		counterMapRedisKeyTTL).Result(); err != nil {
		log.GetLogger().Errorf("failed to set counterMap to redis, %s", err.Error())
	}
	return
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
		err := getCountMapByNewHashKey()
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
		if atomic.LoadInt64(&config.TenantStateLimit)-total > config.TenantStateLocalLimit && getDistributedLock() {
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
