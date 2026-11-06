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

package client

import (
	"context"
	"errors"
	"sync"

	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/logger/log"
)

// scanDefaultCount scan default count every time in redis
const scanDefaultCount = 10

// RedisGoClientConfig config for RedisGoClient initialization
type RedisGoClientConfig struct {
	ServerAddr []string
	Password   string
}

// RedisGoClient is client of redis cluster client
type RedisGoClient struct {
	client *redis.ClusterClient
	ctx    context.Context
}

// CreateRedisGoClientConfig method to create RedisGoClient object
func CreateRedisGoClientConfig(config RedisGoClientConfig) *RedisGoClient {
	clusterClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    config.ServerAddr,
		Password: config.Password,
	})
	return &RedisGoClient{
		client: clusterClient,
		ctx:    context.Background(),
	}
}

// Scan use redis's scan command
func (client *RedisGoClient) Scan(pattern string) ([]string, error) {
	var result []string
	var resMu sync.Mutex
	err := client.client.ForEachMaster(context.TODO(), func(ctx context.Context, client *redis.Client) error {
		var cursor uint64
		cursor = 0
		var tmp []string
		var err error
		for {
			tmp, cursor, err = client.Scan(ctx, cursor, pattern, scanDefaultCount).Result()
			resMu.Lock()
			result = append(result, tmp...)
			resMu.Unlock()
			if cursor == 0 {
				break
			}
		}
		return err
	})

	if err != nil {
		log.GetLogger().Errorf("dialing server error: %s", err.Error())
		return nil, errors.New("internal system error")
	}

	return result, nil
}
