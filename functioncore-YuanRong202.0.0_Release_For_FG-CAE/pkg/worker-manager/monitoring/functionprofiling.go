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

package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/logger/log"
)

const (
	functionProfilingInterval = time.Hour
	redisContextTimeout       = 100 * time.Millisecond
	datasetTimeWindowLength   = 2

	functionProfilingKey = "FunctionProfiling"
)

var (
	funcProfilingData  *FunctionProfilingData
	funcProfilingMutex = &sync.RWMutex{}
)

// FunctionProfilingData get from redis -> profiler
type FunctionProfilingData struct {
	DataSetTimeWindow    [datasetTimeWindowLength]int `json:"dataset_time_window,omitempty"`
	ProcessingTimeWindow [datasetTimeWindowLength]int `json:"processing_time_window,omitempty"`
	ProfilingContent     map[string]*ProfilingContent `json:"profiling_content,omitempty"`
}

// ProfilingContent item
type ProfilingContent struct {
	PodQuantity   int64                  `json:"pod_quantity,omitempty"`
	CPU           *ProfilingResourceItem `json:"cpu,omitempty"`
	Memory        *ProfilingResourceItem `json:"memory,omitempty"`
	QPS           *ProfilingResourceItem `json:"qps,omitempty"`
	ExecutionTime *ProfilingResourceItem `json:"execution_time,omitempty"`
}

// ProfilingResourceItem detail
type ProfilingResourceItem struct {
	Average int64 `json:"average,omitempty"`
	Max     int64 `json:"max,omitempty"`
	Min     int64 `json:"min,omitempty"`
	Std     int64 `json:"std,omitempty"`
}

func getAndMonitorFunctionProfiling(stopCh <-chan struct{}) {
	if err := getFunctionProfilingDataFromRedis(); err != nil {
		log.GetLogger().Errorf("failed to get functionProfilingDataString when start, %s", err.Error())
	}

	go monitorFunctionProfiling(stopCh)
}

func monitorFunctionProfiling(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Warn("stop channel is nil")
		return
	}

	ticker := time.NewTicker(functionProfilingInterval)
	log.GetLogger().Infof("start to monitor function profiling")
	for {
		select {
		case <-ticker.C:
			err := getFunctionProfilingDataFromRedis()
			if err != nil {
				log.GetLogger().Warnf("failed to get function profiling data, err: %s", err.Error())
				continue
			}
		case <-stopCh:
			log.GetLogger().Infof("monitor function profiling graceful exit")
			ticker.Stop()
			return
		}
	}
}

// GetFunctionProfilingDataByFuncName get func data by funcName
func GetFunctionProfilingDataByFuncName(funcName string) *ProfilingContent {
	funcProfilingMutex.RLock()
	defer funcProfilingMutex.RUnlock()
	if funcProfilingData == nil {
		return nil
	}

	if funcProfilingData.ProfilingContent == nil {
		return nil
	}

	if profilingContent, ok := funcProfilingData.ProfilingContent[funcName]; ok {
		log.GetLogger().Debugf("get function profiling data by funcName success! funcName is : %s", funcName)
		return profilingContent
	}

	return nil
}

func getFunctionProfilingDataFromRedis() error {
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	key := functionProfilingKey
	if client.c == nil {
		log.GetLogger().Warnf("redis is not ready")
		return errors.New("redis is not ready")
	}
	functionProfilingDataString, err := client.c.Get(ctx, key).Result()
	if err != nil && err != redis.Nil {
		log.GetLogger().Warnf("failed to get key: %s", key)
		return err
	}

	functionProfilingData := &FunctionProfilingData{}
	if functionProfilingDataString != "" {
		if err = json.Unmarshal([]byte(functionProfilingDataString), &functionProfilingData); err != nil {
			log.GetLogger().Errorf("failed to unmarshal functionProfilingDataString, %s", err.Error())
			return err
		}
	}

	funcProfilingMutex.Lock()
	funcProfilingData = functionProfilingData
	funcProfilingMutex.Unlock()
	return nil
}
