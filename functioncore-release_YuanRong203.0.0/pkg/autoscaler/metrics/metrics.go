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

// Package metrics defines metrics function for autoscaler
package metrics

import (
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/utils"
)

var (
	client = &MonitoringClient{}
)

// MonitoringClient -
type MonitoringClient struct {
	c              *redisclient.Client
	metricConfig   config.MetricConfig
	redisCFG       config.MetricServerConfig
	redisConnected bool
}

// CreateMonitoringClient method to create a monitoring client
func CreateMonitoringClient(redisConfig config.MetricServerConfig, stopCh <-chan struct{}) error {
	client.redisCFG = redisConfig
	err := client.initRedis(redisConfig, stopCh)
	if err != nil {
		return err
	}
	return nil
}

func (mc *MonitoringClient) initRedis(redisConfig config.MetricServerConfig, stopCh <-chan struct{}) error {
	var err error
	var pwd []byte
	pwd, err = localauth.Decrypt(redisConfig.Password)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt redis password, %s", err.Error())
		return err
	}
	mc.c, err = redisclient.New(redisclient.NewRedisClientParam{
		ServerMode: redisConfig.ServerMode,
		ServerAddr: redisConfig.ServerAddr,
		Password:   string(pwd),
		Timeout:    redisConfig.TimeoutConf,
	}, stopCh, redisclient.SetEnableTLS(redisConfig.EnableTLS), redisclient.AlarmSwitchOn())
	if err != nil {
		utils.ClearByteMemory(pwd)
		log.GetLogger().Errorf("failed to create a new redis client: %s", err.Error())
		return err
	}
	redisclient.SetRedisCmd(mc.c)
	return nil
}

// GetMonitoringClient -
func GetMonitoringClient() *redisclient.Client {
	if client == nil {
		return nil
	}
	return client.c
}
