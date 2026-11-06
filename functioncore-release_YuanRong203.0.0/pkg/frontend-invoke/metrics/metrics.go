/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package metrics report to redis for the monitor market
package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/frontend-invoke/constant"
)

const (
	reportTickerTime = 1 * time.Minute
	metricsKeyTTL    = 60 * time.Minute

	redisRetryTimes    = 3
	redisRetryInterval = 100 * time.Millisecond

	bufferSize = 10000

	listLimit = 3

	logLimitFrequency = 1000000
)

var (
	metricsChan = make(chan RequestStatistics, bufferSize)

	requestInfoRedisKey = fmt.Sprintf("/sn/metrics/requestinfo/%s/%s",
		os.Getenv("CLUSTER_ID"), os.Getenv("POD_NAME"))

	statistics = &totalStatistics{}

	reachMaxChanCount = 0
)

// RequestStatistics -
type RequestStatistics struct {
	ErrorFlag     bool
	TotalDelay    float64
	FrontendDelay float64
	BusDelay      float64
	RuntimeDelay  float64
}

// totalStatistics -
type totalStatistics struct {
	lock          sync.RWMutex
	count         int64
	errCount      int64
	totalDelay    float64
	frontendDelay float64
	busDelay      float64
	runtimeDelay  float64
}

// RequestInfoMetrics request info metrics
type RequestInfoMetrics struct {
	TimeStamp     int64   `json:"timeStamp,omitempty"`
	Count         int64   `json:"count,omitempty"`
	ErrCount      int64   `json:"errCount,omitempty"`
	TotalDelay    float64 `json:"totalDelay,omitempty"`
	FrontendDelay float64 `json:"frontendDelay,omitempty"`
	BusDelay      float64 `json:"busDelay,omitempty"`
	RuntimeDelay  float64 `json:"runtimeDelay,omitempty"`
}

// PublishMetrics -
func PublishMetrics(metrics RequestStatistics) {
	if len(metricsChan) < bufferSize {
		metricsChan <- metrics
		return
	}
	if reachMaxChanCount%logLimitFrequency == 0 {
		reachMaxChanCount = 1
		log.GetLogger().Warnf("metricsChan reaches capacity and will discard the metric statistics")
		return
	}
	reachMaxChanCount++
}

// subscribeMetrics -
func subscribeMetrics(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	for {
		select {
		case msg, ok := <-metricsChan:
			if !ok {
				log.GetLogger().Errorf("metrics channel is closed")
				return
			}
			statistics.lock.Lock()
			statistics.count = statistics.count + 1
			if msg.ErrorFlag {
				statistics.errCount = statistics.errCount + 1
			}
			statistics.totalDelay += msg.TotalDelay
			statistics.frontendDelay += msg.FrontendDelay
			statistics.busDelay += msg.BusDelay
			statistics.runtimeDelay += msg.RuntimeDelay
			statistics.lock.Unlock()
		case <-stopChan:
			log.GetLogger().Warnf("Received signal to quit")
			return
		}
	}
}

// StartReportMetrics -
func StartReportMetrics(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	err := initRedisClient(stopChan)
	if err != nil {
		log.GetLogger().Errorf("failed to new redis client and skip to report metrics, err: %s", err.Error())
		return
	}
	log.GetLogger().Infof("start to report metrics to server")
	go subscribeMetrics(stopChan)
	reportTick := time.NewTicker(reportTickerTime)
	for {
		select {
		case <-reportTick.C:
			if statistics.count <= 0 {
				log.GetLogger().Infof("no request info and skip reporting the metrics")
				continue
			}
			reportRequestInfoMetrics(requestInfoRedisKey, resetMetrics(), metricsKeyTTL)
		case _, ok := <-stopChan:
			if !ok {
				reportTick.Stop()
				log.GetLogger().Infof("stop report metrics to redis server")
				return
			}
		}
	}
}

func resetMetrics() []byte {
	now := time.Now()
	statistics.lock.Lock()
	metrics := RequestInfoMetrics{
		TimeStamp:     now.Unix() - int64(now.Second()),
		Count:         statistics.count,
		ErrCount:      statistics.errCount,
		TotalDelay:    statistics.totalDelay / float64(statistics.count),
		FrontendDelay: statistics.frontendDelay / float64(statistics.count),
		BusDelay:      statistics.busDelay / float64(statistics.count),
		RuntimeDelay:  statistics.runtimeDelay / float64(statistics.count),
	}
	statistics.count = 0
	statistics.errCount = 0
	statistics.totalDelay = 0
	statistics.frontendDelay = 0
	statistics.busDelay = 0
	statistics.runtimeDelay = 0
	statistics.lock.Unlock()
	value, err := json.Marshal(metrics)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal requestInfo metrics, err: %s", err.Error())
	}
	return value
}

func initRedisClient(stopCh <-chan struct{}) error {
	var err error
	var pwd []byte
	pwd, err = localauth.Decrypt(constant.FConfig.RedisConfig.Password)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt redis password, %s", err.Error())
		return err
	}
	redisConf := redisclient.NewRedisClientParam{
		ServerMode: constant.FConfig.RedisConfig.ServerMode,
		ServerAddr: constant.FConfig.RedisConfig.ServerAddr,
		Password:   string(pwd),
		Timeout:    constant.FConfig.RedisConfig.TimeoutConf,
	}
	redisCmd, err := redisclient.New(redisclient.NewRedisClientParam{
		ServerMode: constant.FConfig.RedisConfig.ServerMode,
		ServerAddr: constant.FConfig.RedisConfig.ServerAddr,
		Password:   string(pwd),
		Timeout:    constant.FConfig.RedisConfig.TimeoutConf,
	}, stopCh, redisclient.SetEnableTLS(constant.FConfig.RedisConfig.EnableTLS), redisclient.AlarmSwitchOn())
	if err != nil {
		log.GetLogger().Errorf("failed to new a redis client and will "+
			"retry to reconnect later, err: %s", err.Error())
	} else {
		redisclient.SetRedisCmd(redisCmd)
	}
	go redisclient.CheckRedisConnectivity(&redisConf, stopCh)
	return nil
}

func reportRequestInfoMetrics(redisKey string, redisValue []byte, expireTime time.Duration) {
	for i := 0; i < redisRetryTimes; i++ {
		if redisclient.GetRedisCmd() == nil {
			log.GetLogger().Warnf("[reportCPUFlagsMatchingRatio]redis is not ready")
			continue
		}
		err := redisclient.ZADDMetricsToRedis(redisKey, redisValue, listLimit, expireTime)
		if err == nil {
			log.GetLogger().Infof("succeed to report metrics key: %s, value:%s", redisKey, string(redisValue))
			return
		}
		log.GetLogger().Errorf("failed to set key: %s, err: %s, retry time %d", redisKey, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
}
