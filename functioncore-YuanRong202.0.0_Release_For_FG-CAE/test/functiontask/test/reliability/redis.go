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

// Package reliability implements unit tests for reliability
package reliability

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
)

var testRedisConfig = &Config{
	ServerAddr:      "10.162.202.103:6379",
	ConcurrentCount: 4,
}

// default configs.
const (
	// timeout : allow TCP reconnection for 3 times(1, 2, 4)
	connTimeout   = 8 * time.Second
	readTimeout   = 8 * time.Second
	writeTimeout  = 8 * time.Second
	idleTimeout   = 300 * time.Second
	maxRetryTimes = 3
)

// Config for ExtFutureStore.
type Config struct {
	ServerAddr      string
	Password        string
	ConcurrentCount int
	RedisMaxIdle    int
	RedisMaxActive  int
}

func max(x int, y int) int {
	if x < y {
		return y
	}
	return x
}

// Storage represents a redis storage.
type Storage struct {
	cluster          *redis.Client
	config           *Config
	redisConnChannel chan string
}

// NewRedisStorage returns a new redis storage.
func NewRedisStorage(conf *Config) (*Storage, error) {
	store := new(Storage)
	store.config = conf
	store.redisConnChannel = make(chan string, max(conf.RedisMaxActive, store.config.ConcurrentCount))
	err := store.start()
	if err != nil {
		log.GetLogger().Errorf("connect error, err: %s", err)
		return nil, err
	}
	return store, nil
}

// Close the future store in external storage.
func (store *Storage) Close() error {
	close(store.redisConnChannel)
	return store.cluster.Close()
}

func (store *Storage) start() error {
	store.cluster = redis.NewClient(
		&redis.Options{
			Addr:         store.config.ServerAddr,
			Password:     store.config.Password,
			DialTimeout:  connTimeout,
			ReadTimeout:  readTimeout,
			WriteTimeout: writeTimeout,
			MinIdleConns: store.config.RedisMaxIdle,
			IdleTimeout:  idleTimeout,
			MaxRetries:   maxRetryTimes,
		})

	_, err := store.cluster.Ping(context.TODO()).Result()
	if err != nil {
		log.GetLogger().Errorf("dail redis server error, err: %s", err)
		return err
	}
	return nil
}

// Set the value by key.
func (store *Storage) Set(key string, value string, expireTime int) common.TaskError {
	_, err := store.cluster.Set(context.TODO(), key, value, time.Duration(expireTime)).Result()
	if err != nil {
		log.GetLogger().Errorf("set Error, err: %s", err)
		return common.NewTaskError(common.EStateClient, err.Error())
	}
	return nil
}

// Get the value by key.
func (store *Storage) Get(key string) (string, common.TaskError) {
	value, err := store.cluster.Get(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Errorf("get Error, err: %s", err)
		return "", common.NewTaskError(common.EStateClient, err.Error())
	}
	return value, nil
}

// Delete the key.
func (store *Storage) Delete(key string) common.TaskError {
	_, err := store.cluster.Del(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Errorf("delete Error, err: %s", err.Error())
		return common.NewTaskError(common.EStateClient, err.Error())
	}
	return nil
}

// Exists check key's existence.
func (store *Storage) Exists(key string) (bool, common.TaskError) {
	_, err := store.cluster.Exists(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Errorf("exists Error, err: %s", err)
		return false, common.NewTaskError(common.EStateClient, err.Error())
	}
	return true, nil
}

// Scan scans keys with certain pattern.
func (store *Storage) Scan(pattern string) (map[string]string, common.TaskError) {
	return nil, nil
}

// GetNumsByServiceID get nums of stateID by serviceID.
func (store *Storage) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	return 0, nil
}
