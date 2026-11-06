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

// Package strategy contains strategies for judging the scaling of a function's worker instances.
package strategy

import (
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/config"
)

// UsedTimeSliceStrategy -
type UsedTimeSliceStrategy struct{}

// JudgeScaleDown implements autoscaler.ScaleStrategy
func (s *UsedTimeSliceStrategy) JudgeScaleDown(sc *autoscaler.ScaleStrategyContext,
) ([]v1alpha1.WorkerInstance, error) {
	capacity := time.Duration(len(sc.GetWorkers())) * sc.GetSlideWindowInterval()
	instances, current, err := s.getTotalUsedTimeSlice(sc)
	if err != nil {
		log.GetLogger().Errorf("failed to get total max current request num for funcSvc %s: %s",
			sc.GetFuncSvcName(), err.Error())
		return nil, err
	}
	nonReservedWorkers := getNonReservedWorkers(sc.GetWorkers())
	nonReservedWorkers = filterWorker(nonReservedWorkers, instances)
	scaleDownNum := int((capacity - current) / sc.GetSlideWindowInterval())
	// Must be >= to 0 and <= to number of nonReservedWorkers
	scaleDownNum = max(min(scaleDownNum, len(nonReservedWorkers)), 0)
	log.GetLogger().Debugf("[JudgeScaleDown] capacity: %d, current: %d, scaleDownNum: %d",
		capacity, current, scaleDownNum)

	if scaleDownNum <= 0 {
		// Current function does not need to scale down. Do nothing.
		return nil, nil
	}

	// Scale down workers in arbitrary order
	return nonReservedWorkers[:scaleDownNum], nil
}

func (s *UsedTimeSliceStrategy) getTotalUsedTimeSlice(sc *autoscaler.ScaleStrategyContext,
) ([]string, time.Duration, error) {
	validTimestamp := time.Now().Add(-radicalValidTime).Unix()
	instances, sum, err := accumulate(sc,
		func(sum *workermanager.ReportMetrics, val workermanager.ReportMetrics) {
			if config.EnableRadicalAutoscaler() {
				if validTimestamp < val.SecondsTimestamp {
					sum.UsedTimeSlice = max(sum.UsedTimeSlice, val.UsedTimeSlice)
				}
			} else {
				sum.UsedTimeSlice = max(sum.UsedTimeSlice, val.UsedTimeSlice)
			}
		},
		func(sum *workermanager.ReportMetrics, val workermanager.ReportMetrics) {
			sum.UsedTimeSlice += val.UsedTimeSlice
		})
	if err != nil {
		return nil, 0, err
	}
	return instances, time.Duration(sum.UsedTimeSlice) * time.Second, nil
}
