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

// Package invokemetric save and report function invoke result
package invokemetric

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
)

var (
	irQueue      = make(chan *invokeResult, getInvokeResultCap())
	metricMap    = make(map[MetricKey]Metric, 100)
	mutex        sync.Mutex
	podName      = os.Getenv("POD_NAME")
	needSave     bool
	saveInternal = 10 * time.Second
)

const (
	defaultInvokeResultCap = 1000
	fssMetricKey           = "fss-function-metrics"
	funcInfoMinLen         = 3
	fullFuncNameMinLen     = 3
	scaleDownRatio         = 2
	nameSpaceIndex         = 0
	fullFuncNameIndex      = 1
	versionIndex           = 2
	packageIndex           = 1
	funcNameIndex          = 2
)

type invokeResult struct {
	duration int64
	funcKey  string
	errCode  uint32
}

// MetricKey the min unit of summary metric
type MetricKey struct {
	Namespace string
	Package   string
	FuncName  string `json:"Functionname"`
	Version   string
	Timestamp int64
	PodName   string
}

// Metric summary function invoke result
type Metric struct {
	MetricKey          MetricKey `json:"Metrixkey"`
	Count              int
	Duration           int64
	MaxDuration        int64 `json:"maxDuration"`
	MinDuration        int64 `json:"minDuration"`
	FailCount          int
	RejectCount        int
	SystemErrorCount   int
	FunctionErrorCount int
}

func getInvokeResultCap() int {
	irCap, err := strconv.Atoi(os.Getenv("INVOKE_RESULT_CAP"))
	if err == nil && irCap > 0 {
		return irCap
	}
	return defaultInvokeResultCap
}

// MarshalBinary implement encoding.BinaryMarshaler for redis.Z
func (m Metric) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func getMetricKey(funcKey string) *MetricKey {
	funcInfo := strings.Split(funcKey, "/")
	if len(funcInfo) >= funcInfoMinLen {
		fullFuncName := strings.Split(funcInfo[fullFuncNameIndex], "@")
		if len(fullFuncName) >= fullFuncNameMinLen {
			return &MetricKey{
				Namespace: funcInfo[nameSpaceIndex],
				Package:   fullFuncName[packageIndex],
				FuncName:  fullFuncName[funcNameIndex],
				Version:   funcInfo[versionIndex],
				PodName:   podName,
			}
		}
	}
	log.GetLogger().Errorf("failed to get metric key with func: %s", funcKey)
	return nil
}

func handleInvokeResult(stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	for {
		select {
		case ir := <-irQueue:
			mergeMetric(ir)
		case <-stopCh:
			needSave = false
			return
		}
	}
}

func mergeMetric(ir *invokeResult) {
	metricKey := getMetricKey(ir.funcKey)
	if metricKey == nil {
		return
	}
	mutex.Lock()
	metric := metricMap[*metricKey]
	metric.MetricKey = *metricKey
	metric.Count++
	if ir.errCode != proxyerrors.StateOKCode && ir.errCode != proxyerrors.StateAcceptedCode {
		metric.FailCount++
		if snerror.IsUserErr(ir.errCode) {
			metric.FunctionErrorCount++
		} else {
			metric.SystemErrorCount++
		}
	}
	metric.Duration += ir.duration
	if ir.duration != 0 && (ir.duration < metric.MinDuration || metric.MinDuration == 0) {
		metric.MinDuration = ir.duration
	}
	if ir.duration > metric.MaxDuration {
		metric.MaxDuration = ir.duration
	}
	metricMap[*metricKey] = metric
	mutex.Unlock()
}

func saveInvokeResult(c *redisclient.Client, stopCh <-chan struct{}) {
	if c == nil || stopCh == nil {
		needSave = false
		return
	}
	ticker := time.NewTicker(saveInternal)
	for {
		select {
		case <-stopCh:
			ticker.Stop()
			return
		case <-ticker.C:
			saveToRedis(c)
		}
	}
}

func saveToRedis(c *redisclient.Client) {
	mutex.Lock()
	if len(metricMap) == 0 {
		mutex.Unlock()
		return
	}
	oldMap := metricMap
	metricMap = make(map[MetricKey]Metric, len(metricMap)/scaleDownRatio)
	mutex.Unlock()
	members := make([]*redis.Z, 0, len(oldMap))
	t := time.Now().Second()
	for _, v := range oldMap {
		v.MetricKey.Timestamp = int64(t)
		members = append(members, &redis.Z{Score: float64(t), Member: v})
	}
	for i := 0; i < common.DefaultRetryTimes; i++ {
		if c == nil {
			log.GetLogger().Errorf("redis is not ready when save to redis")
			continue
		}
		_, err := c.ZAdd(context.TODO(), fssMetricKey, members...).Result()
		if err == nil {
			return
		}
		log.GetLogger().Errorf("failed to ZAdd key: %s, err: %s , retry time %d",
			fssMetricKey, err.Error(), i)
		time.Sleep(common.DefaultRetryInterval)
	}
}

// ReportInvokeResultMetric report function invoke result to redis
func ReportInvokeResultMetric(c *redisclient.Client, stopCh <-chan struct{}) {
	if c == nil {
		return
	}
	go handleInvokeResult(stopCh)
	go saveInvokeResult(c, stopCh)
	needSave = true
}

// SaveMetric push function invoke result to queue
func SaveMetric(funcKey string, duration int64, errCode uint32) {
	if needSave {
		irQueue <- &invokeResult{
			funcKey:  funcKey,
			duration: duration,
			errCode:  errCode,
		}
	}
}
