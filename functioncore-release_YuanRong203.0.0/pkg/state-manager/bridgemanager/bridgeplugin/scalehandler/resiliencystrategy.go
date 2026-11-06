/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package scalehandler -
package scalehandler

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/state-manager/config"
)

const (
	// CacheBridgeType -
	CacheBridgeType = "cache"
)

const (
	// ScaleRuleKeyOfCPU -
	ScaleRuleKeyOfCPU = "cpu"
	// ScaleRuleKeyOfMemory -
	ScaleRuleKeyOfMemory = "memory"
	// ScaleRuleKeyOfConnections -
	ScaleRuleKeyOfConnections = "connections"
)

const (
	defaultMetricsWantValue = 65

	percent = 100

	threshold = 1e-9
)

// ResiliencyStrategyFactory -
type ResiliencyStrategyFactory struct {
}

// CreateResiliencyStrategy -
func (factory ResiliencyStrategyFactory) CreateResiliencyStrategy(funcSvcID string, curInsCount int,
	bridgeType string) ResiliencyStrategy {

	defaultStrategy := &CacheStrategy{
		funcSvcID:          funcSvcID,
		curInsCount:        curInsCount,
		scaleUpTolerance:   0.1,
		scaleDownTolerance: 0.1,
		scaleRulesItems: map[string]int64{
			ScaleRuleKeyOfCPU:         defaultMetricsWantValue,
			ScaleRuleKeyOfMemory:      defaultMetricsWantValue,
			ScaleRuleKeyOfConnections: defaultMetricsWantValue,
		},
	}

	cfg, cfgErr := config.GetConfig()
	if cfgErr != nil {
		log.GetLogger().Errorf("failed to get config when createAvgDataByResourceDataByPod")
		return defaultStrategy
	}

	switch bridgeType {
	case CacheBridgeType:
		return factory.getSpecificCacheStrategy(curInsCount, funcSvcID, cfg)
	default:
		return defaultStrategy
	}
}

func (factory ResiliencyStrategyFactory) getSpecificCacheStrategy(
	curInsCount int, funcSvcID string, cfg *config.FConfig) ResiliencyStrategy {
	strategy := &CacheStrategy{
		funcSvcID:          funcSvcID,
		curInsCount:        curInsCount,
		scaleUpTolerance:   cfg.ScaleData.ScaleUpTolerance,
		scaleDownTolerance: cfg.ScaleData.ScaleDownTolerance,
		scaleRulesItems:    cfg.ScaleData.ScaleRules[CacheBridgeType],
	}
	if cfg.BridgeManagerConfig.BridgeFuncWhiteList == nil {
		return strategy
	}
	scaleCfg := cfg.BridgeManagerConfig.BridgeFuncWhiteList[funcSvcID]

	if scaleCfg.ScaleUpTolerance != 0 {
		strategy.scaleUpTolerance = scaleCfg.ScaleUpTolerance
	}
	if scaleCfg.ScaleDownTolerance != 0 {
		strategy.scaleDownTolerance = scaleCfg.ScaleDownTolerance
	}
	if scaleCfg.ScaleRules != nil {
		strategy.scaleRulesItems = scaleCfg.ScaleRules[CacheBridgeType]
	}
	log.GetLogger().Debugf("getSpecificStrategy for %s, bridgeType: %s, scaleUpTolerance: %f, ScaleDownTolerance: %f",
		strategy.funcSvcID, CacheBridgeType, strategy.scaleUpTolerance, strategy.scaleDownTolerance)
	return strategy
}

// ResiliencyStrategy -
type ResiliencyStrategy interface {
	pushData(metrics resourceusage.BridgeFuncMetrics)
	getExpectedInsCount() (int64, string, error)
}

// CacheStrategy -
type CacheStrategy struct {
	funcSvcID          string
	curInsCount        int
	scaleRulesItems    map[string]int64
	scaleUpTolerance   float64
	scaleDownTolerance float64

	dataNum          int
	cpuUsageTotal    float64
	cpuLimit         float64
	memoryUsageTotal float64
	memoryLimit      float64
	connectionTotal  int64
	connectionLimit  int64
}

func (cacheStrategy *CacheStrategy) pushData(metrics resourceusage.BridgeFuncMetrics) {
	cacheStrategy.dataNum++

	for scaleRuleType := range cacheStrategy.scaleRulesItems {
		switch scaleRuleType {
		case ScaleRuleKeyOfCPU:
			cacheStrategy.cpuUsageTotal += metrics.CPUUsage
			cacheStrategy.cpuLimit = metrics.CPUCapacity
		case ScaleRuleKeyOfMemory:
			cacheStrategy.memoryUsageTotal += metrics.MemoryUsage
			cacheStrategy.memoryLimit = metrics.MemoryCapacity
		case ScaleRuleKeyOfConnections:
			log.GetLogger().Debugf("the CustomReportMetrics is %+v", metrics.CustomReportMetrics)
			connectionTotal, connectionLimit := calConnectionNums(metrics.CustomReportMetrics)
			cacheStrategy.connectionTotal += connectionTotal
			cacheStrategy.connectionLimit += connectionLimit
		default:
			log.GetLogger().Errorf("get unexpect data of type: %s", scaleRuleType)
		}
	}
}

func calConnectionNums(customReportMetrics resourceusage.CustomReportMetrics) (int64, int64) {
	connectionTotal, connectionLimit := int64(0), int64(0)
	for _, item := range customReportMetrics {
		log.GetLogger().Debugf("item is %+v, item.Val is %s", item, item.Val)
		customMetricsItem := &CacheMetrics{}
		err := json.Unmarshal([]byte(item.Val), customMetricsItem)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal customMetricsItem, val is %+v", item.Val)
			break
		}
		connectionTotal += customMetricsItem.NumActive
		connectionLimit += customMetricsItem.MaxTotal
	}
	return connectionTotal, connectionLimit
}

// CacheMetrics -
type CacheMetrics struct {
	NumActive int64 `json:"numActive" valid:",optional"`
	MaxTotal  int64 `json:"maxTotal" valid:",optional"`
	MaxIdle   int64 `json:"maxIdle" valid:",optional"`
	NumIdle   int64 `json:"numIdle" valid:",optional"`
}

func createAndMakeResiliencyStrategy(funcSvcID string, curInsCount int,
	resourceDataByPod map[resourceusage.PodName]resourceusage.BridgeResourceList) ResiliencyStrategy {
	now := time.Now().Unix()

	var resiliencyStrategy ResiliencyStrategy
	for _, resourceData := range resourceDataByPod {
		for _, resourceItem := range resourceData {
			if now-resourceItem.TimeStamp > noUseTime {
				log.GetLogger().Debugf("ignore data because old data timeStamp is: %d", resourceItem.TimeStamp)
				continue
			}

			log.GetLogger().Debugf("get data success %+v", resourceItem)
			bridgeType := resourceItem.BridgeType

			if resiliencyStrategy == nil {
				resiliencyStrategy = ResiliencyStrategyFactory{}.CreateResiliencyStrategy(funcSvcID, curInsCount, bridgeType)
			}

			resiliencyStrategy.pushData(resourceItem)
		}
	}

	return resiliencyStrategy
}

func (cacheStrategy *CacheStrategy) getExpectedInsCount() (int64, string, error) {
	expectedInsCount, actualScaleRes := int64(0), ""

	for scaleRuleType := range cacheStrategy.scaleRulesItems {
		// it means connections exist in config and no user connection in any pods and have data of user connection config
		// in this situation, need scale down to half
		if scaleRuleType == ScaleRuleKeyOfConnections &&
			cacheStrategy.connectionTotal == 0 && cacheStrategy.connectionLimit != 0 {
			log.GetLogger().Infof("func %s with no connections, so need to scale down to half, connectionLimit: %d",
				cacheStrategy.funcSvcID, cacheStrategy.connectionLimit)
			return int64(math.Floor(float64(cacheStrategy.curInsCount) / float64(2))), ScaleRuleKeyOfConnections, nil
		}

		resourceExpectedCnt, err := cacheStrategy.calResourceExpectedScaleNums(scaleRuleType)
		if err != nil {
			log.GetLogger().Warnf("funcSvc: %s resource %s get expected scale nums failed, err %s, skipping",
				scaleRuleType, cacheStrategy.funcSvcID, err.Error())
			continue
		}

		if resourceExpectedCnt > expectedInsCount {
			expectedInsCount = resourceExpectedCnt
			actualScaleRes = scaleRuleType
		}
	}

	log.GetLogger().Infof("Expect to scale %s to %d instances by resource %s",
		cacheStrategy.funcSvcID, expectedInsCount, actualScaleRes)

	return expectedInsCount, actualScaleRes, nil
}

func (cacheStrategy *CacheStrategy) calResourceExpectedScaleNums(scaleRuleType string) (int64, error) {
	// means no data, so do not scale up or scale down
	if cacheStrategy.dataNum == 0 {
		return 0, fmt.Errorf("no data to calculate scale up or scale down, funcSvc: %s",
			cacheStrategy.funcSvcID)
	}

	averageResUsedPercentage := float64(0)
	targetResUsedPercentage := float64(0)
	switch scaleRuleType {
	case ScaleRuleKeyOfCPU:
		cpuTotalAverage := cacheStrategy.cpuUsageTotal / float64(cacheStrategy.dataNum)
		if cacheStrategy.cpuLimit == 0 {
			return 0, fmt.Errorf("no cpuLimit data to calculate, funcSvc: %s", cacheStrategy.funcSvcID)
		}
		averageResUsedPercentage = (cpuTotalAverage * percent) / cacheStrategy.cpuLimit
		targetResUsedPercentage = float64(cacheStrategy.scaleRulesItems[ScaleRuleKeyOfCPU])
	case ScaleRuleKeyOfMemory:
		memoryTotalAverage := cacheStrategy.memoryUsageTotal / float64(cacheStrategy.dataNum)
		if cacheStrategy.memoryLimit == 0 {
			return 0, fmt.Errorf("no memoryLimit data to calculate, funcSvc: %s", cacheStrategy.funcSvcID)
		}
		averageResUsedPercentage = (memoryTotalAverage * percent) / cacheStrategy.memoryLimit
		targetResUsedPercentage = float64(cacheStrategy.scaleRulesItems[ScaleRuleKeyOfMemory])
	case ScaleRuleKeyOfConnections:
		connectionsTotalAverage := float64(cacheStrategy.connectionTotal)
		if cacheStrategy.connectionLimit == 0 {
			return 0, fmt.Errorf("no connection data to calculate, funcSvc: %s", cacheStrategy.funcSvcID)
		}
		averageResUsedPercentage = (connectionsTotalAverage * percent) / float64(cacheStrategy.connectionLimit)
		targetResUsedPercentage = float64(cacheStrategy.scaleRulesItems[ScaleRuleKeyOfConnections])
	default:
		log.GetLogger().Errorf("get unexpect data of type: %s", scaleRuleType)
	}

	log.GetLogger().Debugf("cpuUsage: %f,memoryUsage: %f, connections: %d, connectionsLimit : %d",
		cacheStrategy.cpuUsageTotal, cacheStrategy.memoryUsageTotal, cacheStrategy.connectionTotal,
		cacheStrategy.connectionLimit)

	ratio := averageResUsedPercentage / targetResUsedPercentage

	// diff < 0 想要扩容， > threshold 需要扩容
	// diff > 0 想要缩容， > threshold 需要缩容
	diff := 1 - ratio

	resourceExpectedCnt := int64(0)
	if (diff < 0 && math.Abs(diff)-cacheStrategy.scaleUpTolerance > threshold) ||
		(diff > 0 && diff-cacheStrategy.scaleDownTolerance > threshold) {
		resourceExpectedCnt = int64(math.Ceil(float64(cacheStrategy.curInsCount) * ratio))
	} else {
		resourceExpectedCnt = int64(cacheStrategy.curInsCount)
	}

	log.GetLogger().Infof("scaleRuleType: %s,funcSvc: %s, averageUsedPercentage: %f, targetUsedPercentage:%f, "+
		"ratio: %f, scaleUpTolerance:%f, scaleDownTolerance:%f, diff:%f, expectedInsCount:%d, curInsCount:%d", scaleRuleType,
		cacheStrategy.funcSvcID, averageResUsedPercentage, targetResUsedPercentage, ratio, cacheStrategy.scaleUpTolerance,
		cacheStrategy.scaleDownTolerance, diff, resourceExpectedCnt, cacheStrategy.curInsCount)

	return resourceExpectedCnt, nil
}
