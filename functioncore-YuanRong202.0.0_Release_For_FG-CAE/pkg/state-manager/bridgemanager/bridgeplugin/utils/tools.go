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

package utils

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	stateutils "functioncore/pkg/state-manager/utils"
)

const (
	separator                   = ":"
	stateHashPrefix             = "state-hash"
	maxRedisRetry               = 5
	defaultRetryDelay           = 3
	stateIDPrefix               = "stateID"
	redisTimeout                = 5 * time.Second
	stateWorkersPartsLens       = 13
	stateIDIdxInStateWorkersKey = 12
)

func slot(tenantID, serviceID string) string {
	return "{" + tenantID + separator + serviceID + "}"
}

func geneStateHashKey(tenantID, serviceID, funcName string) string {
	return strings.Join([]string{stateHashPrefix, slot(tenantID, serviceID), funcName}, separator)
}

func genStateIDKey(tenantID, serviceID, stateID string) string {
	return strings.Join([]string{slot(tenantID, serviceID), stateIDPrefix, stateID}, separator)
}

// TimeCost record the method execution cost time
func TimeCost(funcName string) func() {
	start := time.Now()
	return func() {
		tc := time.Since(start)
		log.GetLogger().Debugf("[bridgeplugin TimeCost] func: %s , time cost = %v", funcName, tc)
	}
}

// DeleteStateInRedis delete state info in redis
func DeleteStateInRedis(tenantID, serviceID, functionName, stateID string) (int, error) {
	var err error
	var changeCount int
	stateHashKey := geneStateHashKey(tenantID, serviceID, functionName)
	for i := 0; i < maxRedisRetry; i++ {
		redisClient := stateutils.GetRedisCmd()
		if redisClient == nil {
			log.GetLogger().Debugf("[bridgeplugin] redis is not ready when delete state")
			return 0, errors.New("redis is not ready when delete state")
		}
		intCmd := redisClient.HDel(context.TODO(), stateHashKey, stateID)
		var values int64
		values, err = intCmd.Result()
		if err == nil {
			changeCount = int(values)
			break
		}
		time.Sleep(defaultRetryDelay * time.Second)
		log.GetLogger().Warnf("[bridgeplugin] failed to delete state in redis with stateID: %s, "+
			"this is %v time to retry", stateID, i+1)
	}
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to delete state in redis, error (http response is error): %s,"+
			" stateID: %s", err.Error(), stateID)
		return 0, err
	}
	log.GetLogger().Debugf("[bridgeplugin] finished deleting state in redis, stateID: %s", stateID)
	return changeCount, nil
}

// CleanStateInRedis clean all state info in redis
func CleanStateInRedis(tenantID, serviceID, functionName, stateID string) {
	var err error
	stateHashKey := geneStateHashKey(tenantID, serviceID, functionName)
	stateIDKey := genStateIDKey(tenantID, serviceID, stateID)
	if _, err = delOptInRedis(stateHashKey, stateID); err != nil {
		log.GetLogger().Warnf("delete stateHashKey: %s, state: %s in redis error: %s",
			stateHashKey, stateID, err.Error())
	}
	if _, err = delOptInRedis(stateIDKey, stateID); err != nil {
		log.GetLogger().Warnf("delete stateIDKey: %s, state: %s in redis error: %s",
			stateIDKey, stateID, err.Error())
	}
	return
}

func delOptInRedis(key, stateID string) (int, error) {
	var err error
	var changeCount int
	for i := 0; i < maxRedisRetry; i++ {
		redisClient := stateutils.GetRedisCmd()
		if redisClient == nil {
			log.GetLogger().Debugf("[bridgeplugin] redis is not ready when delete state")
			return 0, errors.New("redis is not ready when delete state")
		}
		ctx, cancel := context.WithTimeout(context.TODO(), redisTimeout)
		intCmd := redisClient.Del(ctx, key)
		cancel()
		var values int64
		values, err = intCmd.Result()
		if err == nil {
			changeCount = int(values)
			break
		}
		time.Sleep(defaultRetryDelay * time.Second)
		log.GetLogger().Warnf("[bridgeplugin] failed to delete state in redis with stateID: %s, "+
			"this is %v time to retry", stateID, i+1)
	}
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to delete state in redis, error (http response is error): %s,"+
			" stateID: %s, key: %s", err.Error(), stateID, key)
		return 0, err
	}
	log.GetLogger().Debugf("[bridgeplugin] finished deleting state in redis, stateID: %s, key: %s",
		stateID, key)
	return changeCount, nil
}

// IsClientInsFuncUseInfoEmpty judge whether the element in map funcUseInfo is zero
func IsClientInsFuncUseInfoEmpty(clientInstInfo *bgtypes.ClientInstance) bool {
	isEmpty := true
	funcUseInfo := clientInstInfo.FuncUseInfo
	if funcUseInfo == nil {
		return true
	}
	clientInstInfo.FuncUseInfoLock.RLock()
	for _, value := range funcUseInfo {
		if value != 0 {
			isEmpty = false
			break
		}
	}
	clientInstInfo.FuncUseInfoLock.RUnlock()
	return isEmpty
}

// GetValueFromMetaEtcd get data from meta etcd
func GetValueFromMetaEtcd(key string) ([]byte, error) {
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient.Client == nil {
		return nil, errors.New(InitMetaETCDError)
	}
	var (
		values []string
		err    error
	)
	for i := 1; i <= defaultRetryDelay; i++ {
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		values, err = metaClient.GetValues(defaultEtcdCtx, key)
		if err == nil {
			log.GetLogger().Infof("Succeed to get value from etcd, key: %s", key)
			break
		}
		if err != context.DeadlineExceeded {
			return nil, err
		}
		log.GetLogger().Errorf("get value from etcd with key %s timeout, try time %d", key, i)
		time.Sleep(defaultRetryDelay * time.Second)
	}

	if len(values) == 0 {
		log.GetLogger().Errorf("failed to get value from etcd, key: %s", key)
		return nil, errors.New("the value got from etcd is empty")
	}

	return []byte(values[0]), err
}

// ParseStateIDFromStateWorkersKey parse stateID from stateWorkers key
func ParseStateIDFromStateWorkersKey(key string) (string, error) {
	parts := strings.Split(key, "/")
	if len(parts) != stateWorkersPartsLens {
		return "", fmt.Errorf("stateWorkersKey %s is illegal", key)
	}
	return parts[stateIDIdxInStateWorkersKey], nil
}
