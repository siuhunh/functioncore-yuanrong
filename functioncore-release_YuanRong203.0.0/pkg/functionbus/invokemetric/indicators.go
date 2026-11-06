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

package invokemetric

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
)

const (
	fssInnerInvokeIndicators = "fss-inner-invoke-failure-indicators"
	redisRetryTimes          = 3
	redisRetryInterval       = 100 * time.Millisecond
	maxCachedRecordNum       = 1000
	reportInterval           = 10 * time.Second
	indicatorKeyTTL          = 60 * time.Minute

	funcNameLen = 3
)

var (
	indicatorMap   = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
	indicatorMutex sync.Mutex
	redisClient    *redisclient.Client
)

// IndicatorKey key of err summary metric
type IndicatorKey struct {
	CallerPodName string `json:"callerPodName"`
	CalleePodName string `json:"calleePodName"`
	ErrorCode     uint32 `json:"errorCode"`
	Timestamp     int64
}

// InnerInvokeIndicators - data for inner invoke indicators reporting
type InnerInvokeIndicators struct {
	IndicatorKey    IndicatorKey `json:"indicatorKey"`
	ErrType         string       `json:"errType"`
	CallerFuncName  string       `json:"callerFuncName"`
	CalleeFuncName  string       `json:"calleeFuncName"`
	CalleeNodeIP    string       `json:"calleeNodeIP"`
	CalleeClusterID string       `json:"calleeClusterID"`
	Count           int64
}

// MarshalBinary implement encoding.BinaryMarshaler for redis.Z
func (in InnerInvokeIndicators) MarshalBinary() ([]byte, error) {
	return json.Marshal(in)
}

func reportToRedisWithRetry(data map[IndicatorKey]InnerInvokeIndicators) {
	if redisClient == nil {
		log.GetLogger().Warnf("redis client is nil, try get redis client")
		redisClient = redisclient.GetRedisCmd()
	}
	if redisClient == nil {
		log.GetLogger().Errorf("failed to report InnerInvokeIndicators, redis client is nil")
	}

	if len(data) == 0 {
		return
	}

	members := make([]redis.Z, 0, len(data))
	t := time.Now().Unix()
	for _, v := range data {
		v.IndicatorKey.Timestamp = t
		members = append(members, redis.Z{Score: float64(t), Member: v})
	}

	for i := 0; i < redisRetryTimes; i++ {
		err := reportToRedis(members)
		if err == nil {
			log.GetLogger().Debugf("succeed to report indicators, key: %s, value num:%d",
				fssInnerInvokeIndicators, len(members))
			return
		}
		log.GetLogger().Errorf("failed to report key: %s, err: %s, retry time %d", fssInnerInvokeIndicators, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
}

func reportToRedis(members []redis.Z) error {
	if err := zddWithTimeout(members); err != nil {
		return err
	}
	if err := expireWithTimeout(); err != nil {
		return err
	}

	return nil
}

func zddWithTimeout(members []redis.Z) error {
	if redisClient == nil {
		log.GetLogger().Errorf("failed to report redis, client is nil")
		return errors.New("client is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
	defer cancel()
	if err := redisClient.ZAdd(ctx, fssInnerInvokeIndicators, members...).Err(); err != nil {
		log.GetLogger().Errorf("failed to ZAdd key %s to redis, err: %s",
			fssInnerInvokeIndicators, err.Error())
		return err
	}
	return nil
}

func expireWithTimeout() error {
	if redisClient == nil {
		log.GetLogger().Errorf("failed to report redis, client is nil")
		return errors.New("client is nil")
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisclient.DefaultRedisContextTimeout)
	defer cancel()
	if err := redisClient.Expire(ctx, fssInnerInvokeIndicators, indicatorKeyTTL).Err(); err != nil {
		log.GetLogger().Errorf("failed to Expire key %s, err: %s",
			fssInnerInvokeIndicators, err.Error())
		return err
	}
	return nil
}

// RecordIndicators record inner invoke failure
func RecordIndicators(traceID string, req *types.TCPRequest, dst *common.FunctionProperty,
	resp *commontypes.TCPResponse) {
	summaryInfo := snerror.SummaryInfo{}
	err := json.Unmarshal([]byte(resp.Summary), &summaryInfo)
	if err != nil {
		log.GetLogger().Warnf("unmarshal summary info error! error: %s.", err.Error())
	} else {
		log.GetLogger().Debugf("summaryInfo: %+v.", summaryInfo)
	}

	indicatorKey := IndicatorKey{
		CallerPodName: req.UserData[constants.SrcPodNameFlag],
		CalleePodName: summaryInfo.PodName,
		ErrorCode:     resp.ErrorCode,
	}

	indicator := InnerInvokeIndicators{
		ErrType:         getErrType(resp.ErrorCode),
		CallerFuncName:  getFuncNameKeyFromUrn(req.UserData[constants.SrcFuncUrnFlag]),
		CalleeFuncName:  buildIndicatorFuncKey(dst.TenantID, dst.FunctionName, dst.FunctionVersion),
		CalleeNodeIP:    summaryInfo.NodeIP,
		CalleeClusterID: summaryInfo.ClusterID,
	}

	log.GetLogger().Infof("record invoke err: traceID:%s, key:%+v, indicator: %+v.", traceID, indicatorKey, indicator)
	mergeIndicator(indicatorKey, indicator)
	judgeReportAfterMerge()
}

func judgeReportAfterMerge() {
	indicatorMutex.Lock()
	if len(indicatorMap) >= maxCachedRecordNum {
		oldMap := indicatorMap
		indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
		indicatorMutex.Unlock()
		reportToRedisWithRetry(oldMap)
	} else {
		indicatorMutex.Unlock()
	}
}

func getFuncNameKeyFromUrn(urn string) string {
	functionInfo, err := urnutils.GetFunctionInfo(urn)
	if err != nil {
		log.GetLogger().Warnf("failed to parse from urn: %s", urn)
		return ""
	}
	return buildIndicatorFuncKey(functionInfo.TenantID, functionInfo.Name, functionInfo.Version)
}

func buildIndicatorFuncKey(tenantID, funcName, version string) string {
	funcNameList := strings.Split(funcName, "@")
	if len(funcNameList) != funcNameLen {
		log.GetLogger().Errorf("invalid funcName: %s.", funcName)
		return fmt.Sprintf("%s#%s#%s", tenantID, funcName, version)
	}
	return fmt.Sprintf("%s#%s#%s#%s", tenantID, funcNameList[packageIndex], funcNameList[funcNameIndex], version)
}

func mergeIndicator(key IndicatorKey, indicator InnerInvokeIndicators) {
	indicatorMutex.Lock()
	cachedIndicator, exist := indicatorMap[key]
	if !exist {
		cachedIndicator = indicator
		cachedIndicator.IndicatorKey = key
	}

	cachedIndicator.Count++
	indicatorMap[key] = cachedIndicator
	indicatorMutex.Unlock()
}

func reportInvokeErrToRedisPeriodically(c *redisclient.Client, stopCh <-chan struct{}) {
	log.GetLogger().Infof("start to report inner invoke failure metrics periodically")
	redisClient = c
	ticker := time.NewTicker(reportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debugf("timer begin report indicatorsCache to redis.")
			indicatorMutex.Lock()
			oldMap := indicatorMap
			indicatorMap = make(map[IndicatorKey]InnerInvokeIndicators, maxCachedRecordNum)
			indicatorMutex.Unlock()
			reportToRedisWithRetry(oldMap)
		case <-stopCh:
			return
		}
	}
}

func getErrType(errorCode uint32) string {
	if errorCode == 0 {
		return "sysOK"
	}
	if errorCode < snerror.UserErrorMax {
		return "usrErr"
	}
	return "sysErr"
}
