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
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/resourceusage"
)

// ResourceAvgStrategy strategy for using avg metrics.
type ResourceAvgStrategy struct {
	resources     []ResourceConfig
	functionSvcID string
}

func (s *ResourceAvgStrategy) calExpectedScaleNums() (int64, error) {
	curInsCount := cache.GetInstance().GetFuncInstanceCurrentCount(s.functionSvcID)
	var expectedInsCount int64
	var actualScaleRes string
	if curInsCount == 0 {
		log.GetLogger().Errorf("funcSvc: %s get curInsCount is 0", s.functionSvcID)
		return 0, fmt.Errorf("funcSvc: %s get curInsCount is 0", s.functionSvcID)
	}

	for _, resource := range s.resources {
		resourceExpectedCnt, err := s.calResourceExpectedScaleNums(curInsCount, resource)

		if err != nil {
			log.GetLogger().Warnf("funcSvc: %s resource %s get expected scale nums failed, err %s, skipping",
				resource.getResourceName(), s.functionSvcID, err.Error())
			continue
		}

		if resourceExpectedCnt > expectedInsCount {
			expectedInsCount = resourceExpectedCnt
			actualScaleRes = resource.getResourceName()
		}
	}
	log.GetLogger().Infof("Expect to scale %s to %d instances by resource %s", s.functionSvcID,
		expectedInsCount, actualScaleRes)
	return expectedInsCount, nil
}

func (s *ResourceAvgStrategy) calResourceExpectedScaleNums(curInsCount int64,
	resourceCfg ResourceConfig) (int64, error) {
	var totalUsed float64
	var totalLimit int
	var expectedInsCount int64
	totalUsed, totalLimit, err := s.calResourceUsed(totalUsed, totalLimit, resourceCfg)
	if err != nil {
		return 0, err
	}
	averageResUsedPercentage := (totalUsed * resourceCfg.getUsedLimitRate() * percent) / float64(totalLimit)
	targetResUsedPercentage := float64(resourceCfg.getTargetResourceUtilPercentage())
	ratio := averageResUsedPercentage / targetResUsedPercentage
	scaleUpTolerance := resourceCfg.getScaleUpTolerance()
	scaleDownTolerance := resourceCfg.getScaleDownTolerance()
	diff := 1 - ratio
	if (diff < 0 && math.Abs(diff-scaleUpTolerance) > threshold) ||
		(diff > 0 && math.Abs(diff-scaleDownTolerance) > threshold) {
		expectedInsCount = int64(math.Ceil(float64(curInsCount) * ratio))
	} else {
		expectedInsCount = curInsCount
	}
	log.GetLogger().Infof("funcSvc: %s, averageUsedPercentage: %f, targetUsedPercentage:%f, ratio: %f, "+
		"scaleUpTolerance:%f, scaleDownTolerance:%f, diff:%f, expectedInsCount:%d, curInsCount:%d",
		s.functionSvcID, averageResUsedPercentage, targetResUsedPercentage, ratio, scaleUpTolerance, scaleDownTolerance,
		diff, expectedInsCount, curInsCount)
	return expectedInsCount, nil

}

func (s *ResourceAvgStrategy) calResourceUsed(totalUsed float64, totalLimit int,
	resConfig ResourceConfig) (float64, int, error) {
	now := time.Now().Unix()
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
