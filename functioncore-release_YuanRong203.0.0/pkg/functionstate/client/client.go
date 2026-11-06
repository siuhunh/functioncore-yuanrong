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

// Package client implements StateClient for functionstate
package client

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/functionbus/servicediscovery"
)

const (
	expiredTickerTime         = 60 * 60
	defaultExpiredMapSize     = 10
	limitSizeOfScanArray      = 2
	redisReconnectionInternal = 10 * time.Second
	defaultStateTTL           = 7 * 24 * 3600
	stateHashKeyPart          = 4
)

var (
	subExpiredKeys = struct {
		sync.RWMutex
		m map[string]int64
	}{m: make(map[string]int64, defaultExpiredMapSize)}

	errNotExists = errors.New("stateID recycled or not exists")
)

// StateClientConfig config for StateClient initialization
type StateClientConfig struct {
	ServerAddr             string
	ServerMode             string
	Password               string
	EnableTLS              bool
	ConcurrentConns        int
	AKey                   string
	SKey                   string
	ExpiredCb              func(string)
	GetRealTimeStateServer func() (string, redisclient.TimeoutConf, error)
	TimeoutConf            redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
}

// StateClient object to use state operations
type StateClient struct {
	redisGoClient *RedisGoClient
	c             *redisclient.Client
	config        StateClientConfig
	stopChan      chan struct{}
}

// SaveOption option for save request
type SaveOption struct {
	TTL int `unit:"seconds"`
}

// GetRedisClient -
func (client *StateClient) GetRedisClient() *redisclient.Client {
	return client.c
}

// CreateStateClient method to create StateClient object
func CreateStateClient(config StateClientConfig, stopCh <-chan struct{}) (*StateClient, error) {
	client := &StateClient{
		config:   config,
		stopChan: make(chan struct{}),
	}
	alarm.GetClient().RegisterAlarmType(alarm.RedisConnection)
	if err := client.initClient(stopCh, config); err != nil {
		log.GetLogger().Errorf("connection error: %s", err.Error())
		alarm.GetClient().ReportAlarm(alarm.RedisConnection, fmt.Sprintf("functionTask failed to "+
			"connect redis, error msg=%s,", err.Error()))
	}
	go client.checkRedisConnectivity(stopCh, config)
	log.GetLogger().Infof("connected to server %s successfully", config.ServerAddr)
	return client, nil
}

// DeleteStateClient method to delete StateClient object
func DeleteStateClient(client *StateClient) {
	if client == nil {
		return
	}

	log.GetLogger().Infof("stopped connection to server %s", client.config.ServerAddr)
	close(client.stopChan)
}

// DeleteExpiredKey delete expired key from subExpiredKeys map
func (client *StateClient) DeleteExpiredKey() {
	now := time.Now().Unix()
	subExpiredKeys.Lock()
	for stateID, timestamp := range subExpiredKeys.m {
		if timestamp > 0 && now > timestamp {
			// stateID in map is {tenantID:0@serviceID}:stateID:0486039f-b1fe-5738-b61b-xxxx
			strList := strings.Split(stateID, ":")
			if len(strList) == stateHashKeyPart {
				stateIDExist := servicediscovery.GetRegistry().IsStateInsExist(strList[stateHashKeyPart-1])
				log.GetLogger().Debugf("stateIDExist, stateID: %s, exist: %t", strList[stateHashKeyPart-1], stateIDExist)
				if stateIDExist {
					// Avoid frequent expire test
					subExpiredKeys.m[stateID] = now + defaultStateTTL
					continue
				}
			}
			log.GetLogger().Infof("State of %s will be delete when expire", stateID)
			// do callback to notify register
			go client.config.ExpiredCb(stateID)
			// expired stateID will delete in map
			delete(subExpiredKeys.m, stateID)
		}
	}
	subExpiredKeys.Unlock()
}

func (client *StateClient) checkExpiring() {
	tick := time.NewTicker(expiredTickerTime * time.Second)
	for {
		select {
		case <-tick.C:
			client.DeleteExpiredKey()
		case _, ok := <-client.stopChan:
			if !ok {
				tick.Stop()
				log.GetLogger().Infof("stopChan was closed")
				return
			}
		}
	}
}

func (client *StateClient) initClient(stopCh <-chan struct{}, config StateClientConfig) error {
	var err error
	clientConfig := config
	// without alarm switch on
	client.c, err = redisclient.New(redisclient.NewRedisClientParam{
		ServerMode: clientConfig.ServerMode,
		ServerAddr: clientConfig.ServerAddr,
		Password:   clientConfig.Password,
		Timeout:    clientConfig.TimeoutConf,
	}, stopCh, redisclient.SetEnableTLS(clientConfig.EnableTLS),
		redisclient.SetGetRealTimeServerAddrFunc(clientConfig.GetRealTimeStateServer))
	if err != nil {
		log.GetLogger().Errorf("failed to new a redis client, err:%s", err.Error())
		return err
	}
	client.redisGoClient = CreateRedisGoClientConfig(RedisGoClientConfig{
		ServerAddr: strings.Split(client.config.ServerAddr, ","),
		Password:   client.config.Password,
	})
	// check expired message of key in redis by time ticker
	go client.checkExpiring()
	return nil
}

func (client *StateClient) checkRedisConnectivity(stopCh <-chan struct{}, config StateClientConfig) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	ticker := time.NewTicker(redisReconnectionInternal)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debug("functionTask check redis connection start")
			if client.GetRedisClient() != nil {
				_, err := client.GetRedisClient().Ping(context.TODO()).Result()
				if err == nil {
					log.GetLogger().Debug("functionTask periodically check redis available")
					continue
				}
				client.GetRedisClient().Close()
			}
			if err := client.initClient(stopCh, config); err != nil {
				log.GetLogger().Error("failed to init redis client")
				continue
			}
			log.GetLogger().Infof("functionTask redis connect succeed, clear the redis alarm")
			alarm.GetClient().ClearAlarm(alarm.RedisConnection)
		case <-stopCh:
			log.GetLogger().Errorf("functionTask process exit")
			ticker.Stop()
			return
		}
	}
}

// IsNotExist check if err message is equal errNotExists message
func IsNotExist(err error) bool {
	return err.Error() == errNotExists.Error()
}

// DoArg represents cmd and args for redis.Do
type DoArg []interface{}

// Transaction method to run a transaction.
func (client *StateClient) Transaction(doArgs ...DoArg) error {
	if client.c == nil {
		return errors.New("redis is not ready when transaction")
	}
	pipe := client.c.TxPipeline()
	for _, doArg := range doArgs {
		pipe.Do(context.TODO(), doArg...)
	}
	cmders, err := pipe.Exec(context.TODO())
	if err != nil {
		log.GetLogger().Error("in transaction", zap.Error(err), zap.Int("cmders", len(cmders)))
		for _, cmder := range cmders {
			log.GetLogger().Errorf("cmder name: %s, args: %+v, err: %s, fullname: %s",
				cmder.Name(), cmder.Args(), cmder.Err().Error(), cmder.FullName())
		}
		return err
	}
	return nil
}

// SaveState method to save state with option
func (client *StateClient) SaveState(id, state string, option *SaveOption) error {
	if err := client.SetKeyWithTTL(id, state, option.TTL); err != nil {
		log.GetLogger().Error("failed to save state", zap.String("key", id), zap.Error(err))
		return err
	}
	log.GetLogger().Debug("save state successfully", zap.String("key", id), zap.Int("ttl", option.TTL))
	// if expired callback is not set, we will not subscribe state id expired message
	if client.config.ExpiredCb == nil {
		log.GetLogger().Warn("expired callback function is nil", zap.String("key", id))
		return nil
	}

	SubscribeExpiredKey(id, option.TTL)

	return nil
}

// RestoreState method to restore state
func (client *StateClient) RestoreState(id string) (string, error) {
	if client.c == nil {
		return "", errors.New("redis is not ready when restoreState")
	}
	exist, err := client.c.Exists(context.TODO(), id).Result()
	if err != nil {
		return "", err
	}
	if exist == int64(0) {
		return "", errNotExists
	}
	return client.c.Get(context.TODO(), id).Result()
}

// DeleteState method to delete state
func (client *StateClient) DeleteState(id string) error {
	if client.c == nil {
		return errors.New("redis is not ready when DeleteState")
	}
	_, err := client.c.Del(context.TODO(), id).Result()
	if err != nil {
		return err
	}
	log.GetLogger().Infof("delete key successfully", zap.String("key", id))
	// if expired callback is not set, we will not subscribe state id expired message
	if client.config.ExpiredCb == nil {
		log.GetLogger().Warn("expired callback function is nil", zap.String("key", id))
		return nil
	}
	// unsubscribe state id expired message
	subExpiredKeys.Lock()
	delete(subExpiredKeys.m, id)
	subExpiredKeys.Unlock()

	return nil
}

// SetKeyWithTTL set key and expire key with ttl
func (client *StateClient) SetKeyWithTTL(key, value string, ttl int) error {
	if client.c == nil {
		return errors.New("redis is not ready when setKeyWithTTL")
	}
	_, err := client.c.Set(context.TODO(), key, value, time.Duration(ttl)*time.Second).Result()
	if err != nil {
		log.GetLogger().Error("failed to set key with ttl", zap.String("key", key), zap.Error(err))
		return err
	}
	return nil
}

// SubscribeExpiredKey subscribe expired key.
func SubscribeExpiredKey(id string, ttl int) {
	// subscribe stateID in map
	// map key is stateID, value is (current timestamp + ttl)
	timestamp := int64(ttl) + time.Now().Unix()
	subExpiredKeys.Lock()
	subExpiredKeys.m[id] = timestamp
	subExpiredKeys.Unlock()
}

// ResetExpiredTTL reset timer with expired ttl int seconds
func (client *StateClient) ResetExpiredTTL(key string, ttlInt int) error {
	oldTTL, err := client.GetTTL(key)
	if err != nil {
		log.GetLogger().Errorf("getting key %s ttl error: %s", key, err.Error())
		return err
	}
	if oldTTL <= 0 {
		log.GetLogger().Errorf("stateID %s didn't exist", key)
		return errNotExists
	}
	// reset ttl of id in redis
	if err := client.SetTTL(key, ttlInt); err != nil {
		log.GetLogger().Errorf("setting key %s to ttl error: %s", key, err.Error())
		return err
	}
	log.GetLogger().Debugf("resetting key %s to expired ttl %d successfully", key, ttlInt)
	// if expired callback is not set, we will not subscribe state id expired message
	if client.config.ExpiredCb == nil {
		log.GetLogger().Warn("expired callback function is nil")
		return nil
	}
	// update timestamp of subscribed key in map
	timestamp := int64(ttlInt) + time.Now().Unix()
	subExpiredKeys.Lock()
	subExpiredKeys.m[key] = timestamp
	subExpiredKeys.Unlock()

	return nil
}

// SAdd add key value of set
func (client *StateClient) SAdd(key string, value string) (int, error) {
	if client.c == nil {
		return 0, errors.New("redis is not ready when SAdd")
	}
	values, err := client.c.SAdd(context.TODO(), key, value).Result()
	if err != nil {
		log.GetLogger().Error("sadd command exec error", zap.Error(err))
		return 0, err
	}
	return int(values), nil
}

// SRem remove key value of set
func (client *StateClient) SRem(key string, value string) (int, error) {
	if client.c == nil {
		return 0, errors.New("redis is not ready when SRem")
	}
	values, err := client.c.SRem(context.TODO(), key, value).Result()
	if err != nil {
		log.GetLogger().Error("srem command exec error", zap.Error(err))
		return 0, err
	}
	return int(values), nil
}

// SMembers list all key's value of set
func (client *StateClient) SMembers(key string) ([]string, error) {
	if client.c == nil {
		return nil, errors.New("redis is not ready when SMembers")
	}
	values, err := client.c.SMembers(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Error("smembers command exec error", zap.Error(err))
		return nil, err
	}
	return values, nil
}

// SCard get nums of key's value of set
func (client *StateClient) SCard(key string) (int, error) {
	if client.c == nil {
		return 0, errors.New("redis is not ready when SCard")
	}
	values, err := client.c.SCard(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to exec SCard command, key: %s, error: %s", key, err.Error())
		return 0, err
	}
	return int(values), nil
}

// HSetNX set filed value of hash
func (client *StateClient) HSetNX(key, filed, value string) (bool, error) {
	if client.c == nil {
		return false, errors.New("redis is not ready when HSetNX")
	}
	values, err := client.c.HSetNX(context.TODO(), key, filed, value).Result()
	if err != nil {
		log.GetLogger().Error("HSetNX command exec error", zap.Error(err))
		return false, err
	}
	return values, nil
}

// HDel delete fields from hash
func (client *StateClient) HDel(key string, fields ...string) (int, error) {
	if client.c == nil {
		return 0, errors.New("redis is not ready when HDel")
	}
	values, err := client.c.HDel(context.TODO(), key, fields...).Result()
	if err != nil {
		log.GetLogger().Error("HDel command exec error", zap.Error(err))
		return 0, err
	}
	return int(values), nil
}

// HLen get len of hash
func (client *StateClient) HLen(key string) (int, error) {
	if client.c == nil {
		return 0, errors.New("redis is not ready when HLen")
	}
	values, err := client.c.HLen(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Error("HLen command exec error", zap.Error(err))
		return 0, err
	}
	return int(values), nil
}

// HGetAll get all of hash
func (client *StateClient) HGetAll(key string) (map[string]string, error) {
	if client.c == nil {
		return nil, errors.New("redis is not ready when HGetAll")
	}
	values, err := client.c.HGetAll(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Error("redis HGetAll command exec error",
			zap.String("key", key),
			zap.Error(err))
		return nil, err
	}
	return values, nil
}

// HExists filed exists of hash
func (client *StateClient) HExists(key, field string) (bool, error) {
	if client.c == nil {
		return false, errors.New("redis is not ready when HExists")
	}
	values, err := client.c.HExists(context.TODO(), key, field).Result()
	if err != nil {
		return false, err
	}
	return values, nil
}

// Exists if key exists in redis return true
func (client *StateClient) Exists(key string) (bool, error) {
	if client.c == nil {
		return false, errors.New("redis is not ready when Exists")
	}
	exist, err := client.c.Exists(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Error("exists command exec error", zap.Error(err))
		return false, err
	}
	if exist == int64(0) {
		return false, nil
	}
	return true, nil
}

// GetTTL get ttl of key
func (client *StateClient) GetTTL(key string) (time.Duration, error) {
	if client.c == nil {
		return 0, errors.New("redis is not ready when GetTTL")
	}
	value, doErr := client.c.TTL(context.TODO(), key).Result()
	if doErr != nil {
		log.GetLogger().Errorf("get key %s ttl error: %s", key, doErr.Error())
		return 0, doErr
	}
	return value, nil
}

// SetTTL set ttl of key if key exist
func (client *StateClient) SetTTL(key string, ttlInt int) error {
	if client.c == nil {
		return errors.New("redis is not ready when SetTTL")
	}
	_, err := client.c.Expire(context.TODO(), key, time.Duration(ttlInt)*time.Second).Result()
	if err != nil {
		return err
	}
	return nil
}

// Scan use redis's scan command
func (client *StateClient) Scan(pattern string) ([]string, error) {
	if client.config.ServerMode == "single" {
		result, err := client.scan(pattern)
		if err != nil {
			log.GetLogger().Error("single mode redis scan command exec error",
				zap.String("pattern", pattern),
				zap.Error(err))
			return nil, err
		}
		return result, nil
	}

	return client.redisGoClient.Scan(pattern)
}

// scan single redis mode will use this method
func (client *StateClient) scan(pattern string) ([]string, error) {
	var cursor uint64
	var result []string
	if client.c == nil {
		return nil, errors.New("redis is not ready when scan")
	}
	for {
		var keys []string
		var err error
		// we scan with our cursor offset, starting at 0
		keys, cursor, err = client.c.Scan(context.TODO(), cursor, pattern, limitSizeOfScanArray).Result()
		if err != nil {
			log.GetLogger().Errorf("scan keys failed err: %s", err.Error())
			return nil, err
		}
		result = append(result, keys...)
		// we need to stop for loop if cursor is 0
		if cursor == 0 {
			break
		}
	}
	return result, nil
}

// SIsMember if key and value exists in set return true
func (client *StateClient) SIsMember(key string, value string) (bool, error) {
	isExists := false
	if client.c == nil {
		return false, errors.New("redis is not ready when SIsMember")
	}
	isExists, err := client.c.SIsMember(context.TODO(), key, value).Result()
	if err != nil {
		log.GetLogger().Error("SISMEMBER command exec error", zap.Error(err))
		return false, err
	}
	return isExists, nil
}
