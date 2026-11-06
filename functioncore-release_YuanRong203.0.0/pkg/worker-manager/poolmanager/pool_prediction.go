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

// Package poolmanager manages pool
package poolmanager

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
)

const (
	// set used pod record to redis
	oneMinute                         = 60
	usedPodRecordRedisProjectName     = "PoolPrediction"
	usedPodRecordRedisKey             = "UsedRecord"
	usedPodRecordRedisKeySuffixFormat = "2006_01_02"
	usedPodRecordRedisIndexFormat     = "15_04"
	poolPredictionResultRedisKey      = "PredictionResult"
	usedPodRecordChanLength           = 1024
)

type podUsedRecordChanElement struct {
	poolStandard standard
	usedNumber   int64
}

// PodUsedRecord pod used record for pool prediction
type PodUsedRecord struct {
	usedPodRecordVal   map[standard]int64
	usedPodRecordMutex sync.Mutex
	usedPodRecordJSON  string
	UsedPodRecordChan  chan *podUsedRecordChanElement
}

func newPodUsedRecord() *PodUsedRecord {
	return &PodUsedRecord{
		usedPodRecordVal:  make(map[standard]int64),
		usedPodRecordJSON: "",
		UsedPodRecordChan: make(chan *podUsedRecordChanElement, usedPodRecordChanLength),
	}
}

// ProcessPoolProactiveModeUpdate process pool proactive mode changed during watching config file
func (p *PoolMgr) ProcessPoolProactiveModeUpdate() {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, fail to call back function to update pool status")
		return
	}
	enableProactiveModePoolList := faasConfig.ProactiveEnableList
	p.poolListLock.RLock()
	defer p.poolListLock.RUnlock()
	for poolStandard, pool := range p.poolList {
		isOpened, ok := enableProactiveModePoolList[string(poolStandard)]
		if ok && isOpened {
			pool.scalingConfig.setScalingConfigForPool(true)
		} else {
			pool.scalingConfig.setScalingConfigForPool(false)
		}
	}
}

func (sc *ScalingConfig) setScalingConfigForPool(proactiveEnable bool) {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, fail to set cooldown time for %s", sc.standard)
		return
	}
	atomic.StoreInt32(&sc.scalingUpTriggerCounter, 0)
	atomic.StoreInt32(&sc.scalingUpTriggerThreshold, faasConfig.PoolMgrProactiveConfig.ScalingUpTriggerTimeThreshold)
	atomic.StoreInt32(&sc.enableFusionPolicy, faasConfig.PoolMgrProactiveConfig.EnableFusionPolicy)
	atomic.StoreInt32(&sc.cooldownPeriod, 0)
	sc.scalingMutex.Lock()
	sc.scalingPlan = nil
	sc.scalingMutex.Unlock()
	if proactiveEnable {
		sc.setElasticScalingCfg(faasConfig.ProactiveElasticScalingConfig)
		atomic.StoreInt32(&sc.proactiveMode, 1)
		log.GetLogger().Infof("pool prediction hot load config changed, succeed to config %s to proactive mode",
			sc.standard)
	} else {
		sc.setElasticScalingCfg(config.GetElasticScalingConfig())
		atomic.StoreInt32(&sc.proactiveMode, 0)
		log.GetLogger().Infof("pool prediction hot load config changed, succeed to config %s to reactive mode",
			sc.standard)
	}
}

func (p *PoolMgr) generateScalingPlanPeriodically(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config")
		return
	}
	interval := faasConfig.PoolMgrProactiveConfig.PoolPredictionResultDownloadInterval
	tick := time.NewTicker(time.Duration(interval) * time.Minute)
	for {
		select {
		case <-tick.C:
			predictionResult, err := p.getPoolPredictionResultFromRedis()
			if err != nil {
				log.GetLogger().Warnf("fail to generate scaling plan. %s", err.Error())
				continue
			}
			err = p.generateScalingPlan(predictionResult)
			if err != nil {
				log.GetLogger().Warnf("fail to generate scaling plan. %s", err.Error())
			}
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed when resetting used pod record")
			}
			tick.Stop()
			log.GetLogger().Infof("stop uploading and resetting for pod used record")
			return
		}
	}
}

func (p *PoolMgr) getPoolPredictionResultFromRedis() (map[standard][]float64, error) {
	envClusterID := os.Getenv("CLUSTER_ID")
	if envClusterID == "" {
		envClusterID = "default"
	}
	redisKeyArray := []string{usedPodRecordRedisProjectName, poolPredictionResultRedisKey, envClusterID}
	redisKey := strings.Join(redisKeyArray, ":")
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Errorf("redis is not ready when get pool prediction result")
		return nil, errors.New("redis is not ready when get pool prediction result")
	}
	predictionData, err := redisClient.Get(context.TODO(), redisKey).Result()
	if err != nil {
		log.GetLogger().Errorf("fail to download prediction result from Redis and use previous result: %s",
			err.Error())
		return nil, err
	}
	predictionResult := struct {
		StartTime  int64                  `json:"start_time"`
		Confidence map[int64]float64      `json:"confidence"`
		Result     map[standard][]float64 `json:"result"`
	}{}
	err = json.Unmarshal([]byte(predictionData), &predictionResult)
	if err != nil {
		log.GetLogger().Errorf("fail to deserialize prediction result：%s", err.Error())
		return nil, err
	}
	timestamp := time.Now().Unix() / oneMinute
	if predictionResult.StartTime == 0 || predictionResult.Result == nil {
		return nil, errors.New("fail to deserialize redis json to prediction type")
	}
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config")
		return nil, err
	}
	proactiveConfig := faasConfig.PoolMgrProactiveConfig
	if timestamp-predictionResult.StartTime >= int64(proactiveConfig.PoolPredictionResultLength) {
		log.GetLogger().Warnf(
			"prediction result can not cover current timeline, current time line: %d, prediction timeline: %d",
			timestamp,
			predictionResult.StartTime,
		)
		return nil, errors.New("prediction result can not cover current timeline")
	}
	alignPredictionResult(predictionResult.Result, predictionResult.StartTime, timestamp)
	return predictionResult.Result, nil
}

func (p *PoolMgr) generateScalingPlan(predictionResults map[standard][]float64) error {
	if predictionResults == nil {
		return errors.New("no scaling plan generated because of invalid prediction result ")
	}
	for poolStandard, predictionResult := range predictionResults {
		pool, ok := p.getPoolByStandard(poolStandard)
		if !ok || pool == nil {
			continue
		}
		curPoolScalingConfig := pool.scalingConfig
		curPoolScalingConfig.scalingMutex.Lock()
		// model should return result for all size pools, but some of them may be invalid
		// if prediction result is invalid, skip generate scaling plan
		if predictionResult == nil || len(predictionResult) == 0 {
			curPoolScalingConfig.scalingPlan = nil
			log.GetLogger().Infof("no available prediction result for pool %s", poolStandard)
		} else {
			intScalingPlan := smoothScalingPlan(predictionResult, curPoolScalingConfig.replicas)
			curPoolScalingConfig.scalingPlan = intScalingPlan
			log.GetLogger().Infof("update scaling plan for pool %s, scaling plan: %v",
				poolStandard, intScalingPlan)
		}
		curPoolScalingConfig.scalingMutex.Unlock()
	}
	return nil
}

// updateUsedPodRecord record minimum value for one specific pool in one minute
func (p *PodUsedRecord) updateUsedPodRecord(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	for {
		select {
		case usedPodChanElement, ok := <-p.UsedPodRecordChan:
			if !ok {
				log.GetLogger().Warnf("UsedPodRecord channel is closed")
				return
			}
			p.usedPodRecordMutex.Lock()
			val, ok := p.usedPodRecordVal[usedPodChanElement.poolStandard]
			if ok && val < usedPodChanElement.usedNumber {
				p.usedPodRecordVal[usedPodChanElement.poolStandard] = usedPodChanElement.usedNumber
				log.GetLogger().Infof("update records for %s, current: %d",
					usedPodChanElement.poolStandard, p.usedPodRecordVal[usedPodChanElement.poolStandard])
			}
			p.usedPodRecordMutex.Unlock()
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed when updating used pod record")
			}
			log.GetLogger().Infof("stop updating pod used record in pool manager")
			return
		}
	}
}

func (p *PodUsedRecord) uploadAndResetUsedPodRecordEveryMinute(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	faasConfig, err := config.GetConfig()
	if err != nil || faasConfig == nil || faasConfig.PoolMgrProactiveConfig == nil {
		log.GetLogger().Errorf("fail to get faas config or its fields is nil")
		return
	}
	proactiveConfig := faasConfig.PoolMgrProactiveConfig
	tick := time.NewTicker(time.Duration(proactiveConfig.UsedPodRecordResetAndUploadInterval) * time.Minute)
	for {
		select {
		case <-tick.C:
			p.resetUsedPodRecord()
			p.uploadUsedPodRecordJSON()
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed when resetting used pod record")
			}
			tick.Stop()
			log.GetLogger().Infof("stop uploading and resetting for pod used record")
			return
		}
	}
}

func (p *PodUsedRecord) resetUsedPodRecord() {
	p.usedPodRecordMutex.Lock()
	defer p.usedPodRecordMutex.Unlock()
	usedPodRecordJSON, err := json.Marshal(p.usedPodRecordVal)
	// promise the current data is reset successfully
	for poolMemorySize := range p.usedPodRecordVal {
		p.usedPodRecordVal[poolMemorySize] = 0
	}
	if err != nil {
		log.GetLogger().Errorf("fail to serialize used pod record for pool prediction")
		p.usedPodRecordJSON = ""
		return
	}
	p.usedPodRecordJSON = string(usedPodRecordJSON)
	log.GetLogger().Infof("update records for %s", p.usedPodRecordJSON)
	return
}

func (p *PodUsedRecord) uploadUsedPodRecordJSON() {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config, fail to upload used records to Redis, err: %s",
			err.Error())
		return
	}
	if p.usedPodRecordJSON == "" || !faasConfig.EnableUploadUsedPodRecordToRedis {
		return
	}
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.GetLogger().Errorf("fail to get UTC timestamp: %s", err.Error())
		return
	}
	timestamp := time.Now().In(loc)
	usedPodRecordTimeStampIndex := timestamp.Format(usedPodRecordRedisIndexFormat)
	podUsedRecordClusterID := os.Getenv("CLUSTER_ID")
	if podUsedRecordClusterID == "" {
		podUsedRecordClusterID = "default"
	}
	redisKeyArray := []string{
		usedPodRecordRedisProjectName,
		usedPodRecordRedisKey,
		podUsedRecordClusterID,
		timestamp.Format(usedPodRecordRedisKeySuffixFormat),
	}
	redisKey := strings.Join(redisKeyArray, ":")
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Errorf("redis is not ready when upload used pod record")
		return
	}
	resInt, err := redisClient.HSet(
		context.TODO(),
		redisKey,
		usedPodRecordTimeStampIndex,
		p.usedPodRecordJSON,
	).Result()
	if err != nil {
		log.GetLogger().Errorf("fail to upload records to Redis: %s", err.Error())
		return
	}
	if resInt != 1 {
		log.GetLogger().Warnf("the filed has been existing in Redis")
		return
	}
	p.setRedisExpirationTime(redisKey)
	log.GetLogger().Infof("upload records to redis successfully, redis hset key: %s %s",
		redisKey, usedPodRecordTimeStampIndex)
	return
}

func (p *PodUsedRecord) setRedisExpirationTime(redisKey string) {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config")
		return
	}
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Errorf("redis is not ready when set redis expiration time")
		return
	}
	resBool, err := redisClient.Expire(
		context.TODO(),
		redisKey,
		time.Duration(faasConfig.PoolMgrProactiveConfig.UsedPodRecordExpiration)*time.Hour,
	).Result()
	if err != nil {
		log.GetLogger().Errorf("fail to set the redis key expiration time in pool prediction ")
		return
	}
	if !resBool {
		log.GetLogger().Warnf("redis key doesn't exist when setting expiration time in pool prediction")
	}
	log.GetLogger().Infof("upload records to redis successfully")
	return
}

func smoothScalingPlan(scalingPlan []float64, curReplica int32) []int64 {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config")
		return nil
	}
	proactiveConfig := faasConfig.PoolMgrProactiveConfig
	startIndexRecord := 0
	timeWindowLoopIndex := 0
	var replicaSeries []float64
	replicaSeries = append(replicaSeries, float64(curReplica))
	var maxVal float64 = -1
	for i := 0; i < len(scalingPlan); i++ {
		if maxVal < scalingPlan[i] {
			maxVal = scalingPlan[i]
		}
		timeWindowLoopIndex++
		if timeWindowLoopIndex == proactiveConfig.ScalingPlanTimeWindow {
			replicaSeries = append(replicaSeries, maxVal*proactiveConfig.ScalingPlanFactor)
			timeWindowLoopIndex = 0
			startIndexRecord = i + 1
			maxVal = -1
		}
	}
	// record the last max value within the last window
	for i := startIndexRecord; i < len(scalingPlan); i++ {
		replicaSeries = append(replicaSeries, maxVal*proactiveConfig.ScalingPlanFactor)
	}
	return avoidReplicaFluctuating(scalingPlan, replicaSeries)
}

func avoidReplicaFluctuating(plan []float64, series []float64) []int64 {
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config")
		return nil
	}
	proactiveConfig := faasConfig.PoolMgrProactiveConfig
	scalingStep := float64(proactiveConfig.MaxScalingStep)
	for i := 1; i < len(series); i++ {
		if series[i]-series[i-1] > scalingStep {
			series[i] = series[i-1] + scalingStep
		}
		if series[i-1]-series[i] > scalingStep {
			series[i] = series[i-1] - scalingStep
		}
	}
	intPlan := make([]int64, len(plan))
	for i := 0; i < len(plan); i++ {
		seriesIndex := i/proactiveConfig.ScalingPlanTimeWindow + 1
		if seriesIndex >= len(series) {
			return nil
		}
		plan[i] = series[seriesIndex]
		intPlan[i] = int64(math.Ceil(plan[i]))
	}
	return intPlan
}

func alignPredictionResult(predictionResult map[standard][]float64, predictionTimestamp, curTimestamp int64) {
	if predictionResult == nil {
		return
	}
	for key, val := range predictionResult {
		if val[0] == -1 {
			continue
		}
		for i := curTimestamp - predictionTimestamp; i > 0; i-- {
			predictionResult[key] = predictionResult[key][1:]
		}
		for i := predictionTimestamp - curTimestamp; i > 0; i-- {
			predictionResult[key] = append([]float64{val[0]}, predictionResult[key]...)
		}
	}
}
