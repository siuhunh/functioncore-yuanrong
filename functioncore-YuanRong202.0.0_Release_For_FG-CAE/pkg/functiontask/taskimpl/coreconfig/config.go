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

// Package coreconfig defines configurations for function task.
package coreconfig

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"time"

	logc "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functiontask/task/config"
)

const (
	// TaskConfigEnv -
	TaskConfigEnv = "FUNCTION_TASK_CONFIG"
)

// Config defines configurations for function task.
type Config struct {
	config.Config

	StateServerAddr       string                  `json:"state_server_addr"`
	TimeoutConf           redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
	StateServerMode       string                  `json:"state_server_mode"`
	StateServerPassword   string                  `json:"state_server_password"`
	StateServerEnableTLS  bool                    `json:"state_server_enable_tls"`
	RedisMaxIdle          int                     `json:"redis_max_idle"`
	RedisMaxActive        int                     `json:"redis_max_active"`
	FunctionNameSeparator string                  `json:"functionNameSeparator"`
}

// Const for function task.
const (
	NameOfFunctionTask = "function-task"
	TenantID           = "0"
	logFileName        = "functiontask"
	// DefaultTimeout define a defaultTimeout
	DefaultTimeout = 6000
	MaxTimeout     = 3600 * 24 * 100
	configPath     = "/home/sn/config/conf.json"
)

var (
	// NodeID for function task.
	NodeID = os.Getenv("NODE_ID")
	// HostIP for function task.
	HostIP string
	// PodIP for function task.
	PodIP string
	// Logger defines is a global logger used by function task.
	Logger log.FormatLogger
)

func confCheck(conf *Config) {
	utils.ValidateTimeout(&conf.CallTimeout, DefaultTimeout)
	conf.CallTimeout = int64(time.Duration(conf.CallTimeout) * time.Second)
	if conf.FutureTTL > 0 {
		log.GetLogger().Infof("set future_ttl, FutureTTL: %d", conf.FutureTTL)
		conf.FutureTTL = conf.FutureTTL * time.Second
	} else {
		log.GetLogger().Infof("set default future_ttl, DefaultFutureTTL: %d ", config.DefaultFutureTTL)
		conf.FutureTTL = config.DefaultFutureTTL
	}
	if conf.ExternalFutureTTL > 0 && conf.ExternalFutureTTL <= MaxTimeout {
		log.GetLogger().Infof("set external_future_ttL, ExternalFutureTTL: %d", conf.ExternalFutureTTL)
	} else {
		log.GetLogger().Infof("set external_future_ttl to default value, DefaultExternalFutureTTL: %d",
			config.DefaultExternalFutureTTL)
		conf.ExternalFutureTTL = config.DefaultExternalFutureTTL
	}
	if conf.CallTimeout < int64(config.DefaultCallTimeout) {
		log.GetLogger().Infof("invalid call_timeout set to default, CallTimeout: %d, DefaultCallTimeout: %d",
			conf.CallTimeout/int64(time.Second), config.DefaultCallTimeout/time.Second)
		conf.CallTimeout = int64(config.DefaultCallTimeout)
	}
	if conf.MaxSingleWaitFutureCnt <= 0 {
		log.GetLogger().Infof("invalid max_wait_future_cnt set to default, MaxSingleWaitFutureCnt: %s, "+
			"DefaultMaxSingleWaitFutureCnt: %d", string(conf.MaxSingleWaitFutureCnt),
			config.DefaultMaxSingleWaitFutureCnt)
		conf.MaxSingleWaitFutureCnt = config.DefaultMaxSingleWaitFutureCnt
	}
	if conf.EtcdDiffSyncTimeout <= 0 {
		log.GetLogger().Infof("the etcd diff sync timeout is invalid, use default %d seconds",
			config.DefaultEtcdDiffSyncTimeout)
		conf.EtcdDiffSyncTimeout = config.DefaultEtcdDiffSyncTimeout
	}
	if conf.UpdateCacheFilePeriod <= 0 {
		log.GetLogger().Infof("the update cache file period is invalid, use default %d minutes",
			config.DefaultUpdateCacheFilePeriod)
		conf.UpdateCacheFilePeriod = config.DefaultUpdateCacheFilePeriod
	}
	if conf.FlushCacheThreshold <= 0 {
		log.GetLogger().Infof("the flush cache threshold is invalid, use default %d",
			config.DefaultFlushCacheThreshold)
		conf.FlushCacheThreshold = config.DefaultFlushCacheThreshold
	}
	if conf.CacheUpdateBufSize <= 0 {
		log.GetLogger().Infof("the cache update buf size is invalid, use default %d",
			config.DefaultCacheUpdateBufSize)
		conf.CacheUpdateBufSize = config.DefaultCacheUpdateBufSize
	}
	if conf.CacheFilePartitionSize <= 0 {
		log.GetLogger().Infof("the cache file partition size is invalid, use default %d",
			config.DefaultCacheFilePartitionSize)
		conf.CacheFilePartitionSize = config.DefaultCacheFilePartitionSize
	}
}

// Init reads configurations from config file.
func Init() (*Config, error) {
	err := initTaskLog()
	if err != nil {
		return nil, err
	}
	conf := new(Config)
	datas := os.Getenv(TaskConfigEnv)
	if err = json.Unmarshal([]byte(datas), conf); err != nil {
		return nil, err
	}
	HostIP = os.Getenv("HOST_IP")
	if net.ParseIP(HostIP) == nil {
		return nil, errors.New("get HostIP error")
	}
	PodIP = os.Getenv("POD_IP")
	if net.ParseIP(PodIP) == nil {
		return nil, errors.New("get podIP error")
	}
	confCheck(conf)
	conf.StateServerAddr, conf.TimeoutConf, err = GetHotloadStateServerConf()
	if err != nil {
		return nil, err
	}
	config.Conf = &conf.Config
	return conf, nil
}

// GetHotloadStateServerConf get real-time config info from config.json
func GetHotloadStateServerConf() (string, redisclient.TimeoutConf, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	conf := new(Config)
	err = decoder.Decode(&conf)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	log.GetLogger().Infof("get serverAddr: %s, timeoutConf: %v", conf.StateServerAddr, conf.TimeoutConf)
	if conf.TimeoutConf.IdleTimeout <= 0 || conf.TimeoutConf.WriteTimeout <= 0 || conf.TimeoutConf.
		ReadTimeout <= 0 || conf.TimeoutConf.DialTimeout <= 0 {
		return conf.StateServerAddr, redisclient.GetDefaultTimeoutConf(), nil
	}
	log.GetLogger().Infof("get serverAddr: %s, timeoutConf: %v", conf.StateServerAddr, conf.TimeoutConf)
	return conf.StateServerAddr, conf.TimeoutConf, nil
}

func initTaskLog() error {
	coreInfo, err := logc.GetCoreInfoFromEnv()
	if err != nil {
		return err
	}
	runLog, err := log.InitRunLogWithConfig(logFileName, true, coreInfo)
	if err != nil {
		Logger = log.NewConsoleLogger()
		Logger.Errorf("init logger error: %s", err.Error())
		return err
	}
	Logger = runLog
	Logger.Info("success init function-task log")
	return nil
}
