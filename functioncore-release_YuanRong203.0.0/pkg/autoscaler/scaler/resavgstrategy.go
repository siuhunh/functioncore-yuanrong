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

// Package scaler -
package scaler

import (
	"fmt"
	"math"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/functiontypes"
)

const (
	oneMinute = 60
)

// ResourceAvgStrategy strategy for using avg metrics.
type ResourceAvgStrategy struct {
	resources     []ResourceConfig
	functionSvcID string
	config        config.MetricConfig
}

func createResourceAvgStrategy(functionSvcID string, config config.MetricConfig) *ResourceAvgStrategy {
	return &ResourceAvgStrategy{
		resources:     []ResourceConfig{},
		functionSvcID: functionSvcID,
		config:        config,
	}
}

func (s *ResourceAvgStrategy) withCPUResource() *ResourceAvgStrategy {
	s.resources = append(s.resources, createCPUResourceConfig(s.config))
	return s
}

func (s *ResourceAvgStrategy) withMemoryResource() *ResourceAvgStrategy {
	s.resources = append(s.resources, createMEMResourceConfig(s.config))
	return s
}

func (s *ResourceAvgStrategy) withExtendStrategies(extendScaleStrategies []functiontypes.
	ExtendScaleStrategy) *ResourceAvgStrategy {
	if len(extendScaleStrategies) == 0 {
		return s
	}
	s.resources = append(s.resources, createExtendStrategiesResourceConfig(extendScaleStrategies, s.config)...)
	return s
}

func (s *ResourceAvgStrategy) calExpectedScaleNums() (int64, int64, error) {
	curInsCount, _ := cache.GetInstance().GetFuncInstanceCurrentCount(s.functionSvcID)

	var (
		expectedInsCount           int64
		expectedForPredictInsCount int64
		actualScaleRes             string
	)
	needPredict := s.needPredict()

	if curInsCount == 0 && !needPredict {
		log.GetLogger().Errorf("funcSvc: %s get curInsCount is 0 and not need predict", s.functionSvcID)
		return 0, 0, fmt.Errorf("funcSvc: %s get curInsCount is 0 and not need predict", s.functionSvcID)
	}

	for _, resource := range s.resources {
		resourceExpectedCnt, err := s.calResourceExpectedScaleNums(curInsCount, resource)
		if err != nil {
			log.GetLogger().Warnf("funcSvc: %s resource %s for %s get expected scale nums failed, err %s, skipping",
				resource.getResourceName(), resource.getMetricType(), s.functionSvcID, err.Error())
			continue
		}

		if resourceExpectedCnt > expectedInsCount {
			expectedInsCount = resourceExpectedCnt
			actualScaleRes = resource.getResourceName()
		}
	}

	if needPredict {
		expectedForPredictInsCount = s.calPredictionScaleNums()

		if !s.needUsePredictResult() {
			log.GetLogger().Infof("not need use predict result, predict scale num is: %d, funcSvcID: %s",
				expectedForPredictInsCount, s.functionSvcID)
			expectedForPredictInsCount = 0
		}
	}

	log.GetLogger().Infof("Expect to scale %s to %d instances by resource %s, to %d instance by predict, "+
		"needPredict: %t", s.functionSvcID, expectedInsCount, actualScaleRes, expectedForPredictInsCount, needPredict)

	return expectedInsCount, expectedForPredictInsCount, nil
}

func (s *ResourceAvgStrategy) calResourceExpectedScaleNums(curInsCount int64,
	resourceCfg ResourceConfig) (int64, error) {
	var err error
	averageResUsedPercentage, expectedInsCount, totalUsed, totalLimit := float64(0), int64(0), float64(0), 0

	// get pod metrics to scale, cpu or mem need limit data
	if resourceCfg.isCPUOrMem() {
		totalUsed, totalLimit, err = s.calResourceUsedWithLimit(totalUsed, totalLimit, resourceCfg)
		if err != nil {
			return 0, err
		}
		averageResUsedPercentage = (totalUsed * resourceCfg.getUsedLimitRate() * percent) / float64(totalLimit)
	} else if resourceCfg.isFileSystemMetric() || resourceCfg.isNetMetric() {
		averageResUsedPercentage, err = s.calResourceUsedFromCaeMetrics(resourceCfg)
		log.GetLogger().Debugf("is file system metric, averageResUsedPercentage is %f", averageResUsedPercentage)
		if err != nil {
			return 0, err
		}
	} else {
		averageResUsedPercentage, err = s.calResourceUsedWithoutLimit(resourceCfg)
		if err != nil {
			return 0, err
		}
	}

	targetResUsedPercentage := float64(resourceCfg.getTargetResourceUtilPercentage())
	ratio := averageResUsedPercentage / targetResUsedPercentage
	scaleUpTolerance := resourceCfg.getScaleUpTolerance()
	scaleDownTolerance := resourceCfg.getScaleDownTolerance()
	diff := 1 - ratio
	// diff < 0 想要扩容， > threshold 需要扩容
	// diff > 0 想要缩容， > threshold 需要缩容
	if (diff < 0 && math.Abs(diff)-scaleUpTolerance > threshold) || (diff > 0 && diff-scaleDownTolerance > threshold) {
		expectedInsCount = int64(math.Ceil(float64(curInsCount) * ratio))
	} else {
		expectedInsCount = curInsCount
	}
	log.GetLogger().Infof("funcSvc: %s of %s, averageUsedPercentage: %f, targetUsedPercentage:%f, ratio: %f, "+
		"scaleUpTolerance:%f, scaleDownTolerance:%f, diff:%f, expectedInsCount:%d, curInsCount:%d",
		s.functionSvcID, resourceCfg.getResourceName(), averageResUsedPercentage, targetResUsedPercentage, ratio,
		scaleUpTolerance, scaleDownTolerance, diff, expectedInsCount, curInsCount)
	return expectedInsCount, nil

}

func (s *ResourceAvgStrategy) calResourceExpectedScaleNumsByPrediction(resConfig ResourceConfig) (int64, error) {
	var (
		resUsage         float64
		resLimit         int64
		expectedInsCount int64
	)

	functionInfo, exist := cache.GetInstance().GetFunctionMeta(s.functionSvcID)
	if !exist {
		return 0, fmt.Errorf("funcSvc: %s can't get functionInfo by prediction, so do not scale up", s.functionSvcID)
	}

	now := time.Now().Unix()
	predictResourceList := cache.GetInstance().GetPredictMetricsOutputCache(s.functionSvcID)

	for _, resource := range predictResourceList {
		if resource.Timestamp-now <= oneMinute && resource.Timestamp-now > 0 {
			resUsage, resLimit = resConfig.getResourceStatus(&resourceusage.Resource{
				CPUUsage:    resource.CPUUtil,
				CPULimit:    int64(functionInfo.ResourceMetaData.CPU),
				MemoryUsage: resource.MemoryUtil,
				MemoryLimit: int64(functionInfo.ResourceMetaData.Memory),
			})
		}
	}

	if resUsage == 0 {
		return 0, fmt.Errorf("funcSvc: %s can't get resUsage %s by prediction, so do not scale up",
			s.functionSvcID, resConfig.getResourceName())
	}

	if resLimit == 0 {
		return 0, fmt.Errorf("funcSvc: %s can't get resLimit %s by prediction, so do not scale up",
			s.functionSvcID, resConfig.getResourceName())
	}

	resUsedPercentage := (resUsage * resConfig.getUsedLimitRate() * percent) / float64(resLimit)
	targetResUsedPercentage := float64(resConfig.getTargetResourceUtilPercentage())
	if targetResUsedPercentage == 0 {
		return 0, fmt.Errorf("funcSvc: %s can't get targetResUsedPercentage %s by prediction, so do not scale up",
			s.functionSvcID, resConfig.getResourceName())
	}

	expectedInsCount = int64(math.Ceil(resUsedPercentage / targetResUsedPercentage))

	log.GetLogger().Infof("calResourceExpectedScaleNumsByPrediction, funcSvc: %s, resUsage: %f, resLimit: %d, "+
		"resUsedPercentage:%f, targetResUsedPercentage: %f, expectedInsCount:%d, resource type: %s", s.functionSvcID,
		resUsage, resLimit, resUsedPercentage, targetResUsedPercentage, expectedInsCount, resConfig.resourceName)
	return expectedInsCount, nil
}

func (s *ResourceAvgStrategy) calResourceUsedWithLimit(totalUsed float64, totalLimit int,
	resConfig ResourceConfig) (float64, int, error) {
	now := time.Now().Unix()
	// 从 redis 中拿到了当前函数的所有 pod 的资源使用情况
	functionMetric := cache.GetInstance().GetResourceMetricCache(s.functionSvcID)
	var newestPodName resourceusage.PodName
	newestTimeDiff := math.MaxInt64
	for _, podList := range functionMetric {
		for podName, resourceList := range podList {
			lastestData := resourceList.GetAvgDataInTime(now, noUseTime)
			// if resourceList len is zero or item data is zero will return nil, if not check will panic
			if lastestData == nil {
				continue
			}
			timeDiff := now - lastestData.ReportTime
			if lastestData != nil && (timeDiff <= noUseTime) {
				resUsage, resLimit := resConfig.getResourceStatus(lastestData)
				totalUsed = totalUsed + resUsage
				totalLimit = totalLimit + int(resLimit)
			}
			if int(timeDiff) < newestTimeDiff {
				newestTimeDiff = int(timeDiff)
				newestPodName = podName
			}
		}
	}
	if newestTimeDiff > noUseTime {
		log.GetLogger().Warnf("funcSvc: %s get redis data expired newestTimeDiff is %d greater than 15s, "+
			"newestPodName is %s", s.functionSvcID, newestTimeDiff, newestPodName)
	}
	if totalLimit == 0 {
		return 0, 0, fmt.Errorf("funcSvc: %s can't get total %s limit, so do not scale up",
			s.functionSvcID, resConfig.getResourceName())
	}
	if resConfig.getTargetResourceUtilPercentage() == 0 {
		return 0, 0, fmt.Errorf("funcSvc: %s target %s util percentage is 0, so do not scale up",
			s.functionSvcID, resConfig.getResourceName())
	}
	return totalUsed, totalLimit, nil
}

func (s *ResourceAvgStrategy) calResourceUsedWithoutLimit(resConfig ResourceConfig) (float64, error) {
	if resConfig.getTargetResourceUtilPercentage() == 0 {
		return 0, fmt.Errorf("funcSvc: %s target %s for %s util percentage is 0, so do not scale up",
			s.functionSvcID, resConfig.getResourceName(), resConfig.getMetricType())
	}

	now := time.Now().Unix()
	functionMetric := cache.GetInstance().GetResourceMetricCache(s.functionSvcID)

	totalUsed, dataNum := float64(0), float64(0)

	for _, podList := range functionMetric {
		for _, resourceList := range podList {
			used := getTotalUsedResourceUsedWithOutLimit(resConfig, resourceList, now)
			if used > 0 {
				totalUsed += used
				dataNum++
			}
		}
	}

	if dataNum == 0 {
		return 0, nil
	}
	return totalUsed / dataNum, nil
}

func (s *ResourceAvgStrategy) calResourceUsedFromCaeMetrics(resConfig ResourceConfig) (float64, error) {
	if resConfig.getTargetResourceUtilPercentage() == 0 {
		return 0, fmt.Errorf("funcSvc: %s target %s for %s util percentage is 0, so do not scale up",
			s.functionSvcID, resConfig.getResourceName(), resConfig.getMetricType())
	}

	now := time.Now().Unix()
	caeFunctionMetric := cache.GetInstance().GetCaeMetricsCache(s.functionSvcID)

	caeMetricsTotalUsed, caeMetricDataNum := float64(0), float64(0)

	for _, podList := range caeFunctionMetric {
		for _, resourceList := range podList {
			usedFromCaeMetrics := getTotalUsedResourceFromCaeMetrics(resConfig, resourceList, now)
			log.GetLogger().Debugf("used id %f", usedFromCaeMetrics)
			if usedFromCaeMetrics > 0 {
				caeMetricsTotalUsed += usedFromCaeMetrics
				caeMetricDataNum++
			}
		}
	}

	if caeMetricDataNum == 0 {
		return 0, nil
	}
	return caeMetricsTotalUsed / caeMetricDataNum, nil
}

func getTotalUsedResourceUsedWithOutLimit(resConfig ResourceConfig, resourceList resourceusage.ResourceList,
	now int64) float64 {

	if resConfig.isSystemMetric() {
		latestData := resourceList.GetAvgExtendSystemDataInTime(now, noUseTime)
		// if resourceList len is zero or item data is zero will return nil, if not check will panic
		if latestData == nil {
			return 0
		}

		return resConfig.getExtendSystemMetrics(latestData)
	}

	// if is custom metric, need parse json and get data next step
	return 0
}

func getTotalUsedResourceFromCaeMetrics(resConfig ResourceConfig, caeResourceList caemetrics.List,
	now int64) float64 {

	if resConfig.isSystemMetric() {
		latestData := caeResourceList.GetLatestData()
		log.GetLogger().Debugf("latestData is %+v", latestData)
		// if resourceList len is zero or item data is zero will return nil, if not check will panic
		if latestData == nil {
			return 0
		}

		if now-latestData.ReportTime > caeMetricsUseTime {
			log.GetLogger().Debugf("data is too old, so ignore, %+v", latestData)
			return 0
		}

		return resConfig.getExtendSystemCaeMetrics(latestData)
	}

	// if is custom metric, need parse json and get data next step
	return 0
}

/*
	needPredict

if PredictEnable and user config need predict, need predict
if PredictEnable and in white list, need predict
if PredictEnable and white list is empty, not predict
if not PredictEnable not need predict
*/
func (s *ResourceAvgStrategy) needPredict() bool {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config err when need predict, err: %s", err.Error())
		return false
	}

	if cfg.PredictConfig.PredictEnable &&
		cache.CheckPredictWhiteList(cfg.PredictConfig.PredictFuncWhiteList, s.functionSvcID) {
		return true
	}

	return false
}

func (s *ResourceAvgStrategy) needUsePredictResult() bool {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config err when need use predict result, err: %s", err.Error())
		return false
	}

	return cfg.PredictConfig.PredictUseResult
}

func (s *ResourceAvgStrategy) calPredictionScaleNums() int64 {
	var (
		predictionCount int64
		actualScaleRes  string
	)

	for _, resource := range s.resources {
		// predict only cpu and memory now
		if resource.metricType != MetricTypeSystem {
			continue
		}

		if resource.resourceName != CPU && resource.resourceName != MEM {
			continue
		}

		// scale up depend on prediction
		count, err := s.calResourceExpectedScaleNumsByPrediction(resource)
		if err != nil {
			log.GetLogger().Warnf("calPredictionScaleNums, funcSvc: %s resource %s get expected scale nums by "+
				"prediction failed, err %s, skipping", resource.getResourceName(), s.functionSvcID, err.Error())
			continue
		}

		if count > predictionCount {
			predictionCount = count
			actualScaleRes = resource.getResourceName()
		}
	}

	log.GetLogger().Infof("predict to scale %s to %d instances by resource %s", s.functionSvcID,
		predictionCount, actualScaleRes)
	return predictionCount
}
