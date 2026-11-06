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
	"sort"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

// autoscaler will be triggered every 5 minutes, a few seconds slightly after metrics report.
// so we should use 6 minute as valid time in order to calculate all metrics in last 5 minutes
const radicalValidTime = 6 * time.Minute

// if autoscaler set to RadicalAutoscaler, metrics num should more than 5 before trigger scale down
const minMetricsNum = 5

// Selector is an autoscaler strategy selector
type Selector struct {
	u *UsedTimeSliceStrategy
	m *MaxRequestNumStrategy
}

// NewSelector returns a Selector
func NewSelector() *Selector {
	return &Selector{
		u: &UsedTimeSliceStrategy{},
		m: &MaxRequestNumStrategy{},
	}
}

// SelectStrategy implements autoscaler.ScaleStrategySelector
func (s *Selector) SelectStrategy(sc *autoscaler.ScaleStrategyContext) autoscaler.ScaleStrategy {
	concurrency := sc.GetSpec().Concurrency.ReservedValue
	if concurrency == 1 {
		return s.u
	}
	return s.m
}

func getNonReservedWorkers(workers map[string]v1alpha1.WorkerInstance) (res []v1alpha1.WorkerInstance) {
	for _, worker := range workers {
		if worker.Applier != constants.WorkerManagerApplier {
			res = append(res, worker)
		}
	}
	return
}

type accumulateFn func(sum *workermanager.ReportMetrics, val workermanager.ReportMetrics)

func accumulate(sc *autoscaler.ScaleStrategyContext, perWorker, final accumulateFn,
) ([]string, workermanager.ReportMetrics, error) {
	type metric struct {
		sum   workermanager.ReportMetrics
		count int
	}
	workers := sc.GetWorkers()
	m := make(map[string]*metric, len(workers))
	err := sc.RangeSlideWindow(func(offset int, instanceID string, list []workermanager.ReportMetrics) {
		log.GetLogger().Debugf("range slide window for funcSvc %s, %d, %s, %+v", sc.GetFuncSvcName(), offset,
			instanceID, list)

		_, ok := workers[instanceID]
		if !ok {
			// Cannot find the worker instance. It might have exited.
			return
		}

		for _, reportMetrics := range list {
			// only record the maximum MaxCurrentRequestNums in the time window
			x, ok := m[instanceID]
			if !ok {
				x = &metric{}
				m[instanceID] = x
			}
			perWorker(&x.sum, reportMetrics)
			x.count++
		}
	})
	if err != nil {
		log.GetLogger().Errorf("failed to range slide window, funcSvc %s, %s", sc.GetFuncSvcName(), err.Error())
		return nil, workermanager.ReportMetrics{}, err
	}

	var sum workermanager.ReportMetrics
	var instances []string
	for instanceID := range workers {
		metric, ok := m[instanceID]
		if !ok {
			continue
		}
		final(&sum, metric.sum)
		// for function with 1:1 concurrency, we only scale down instances that has no request (UsedTimeSlice = 0);
		// and functions with higher concurrency will use MaxCurrentRequestNums instead of UsedTimeSlice,
		// so here we check if UsedTimeSlice == 0
		if enoughMetrics(sc, metric.count) && metric.sum.UsedTimeSlice == 0 {
			instances = append(instances, instanceID)
		}
	}
	return instances, sum, nil
}

func enoughMetrics(sc *autoscaler.ScaleStrategyContext, count int) bool {
	if config.EnableRadicalAutoscaler() {
		return count >= minMetricsNum
	}
	return count >= sc.GetSlideWindowSlotNum()
}

func max(nums ...int) int {
	num := nums[0]
	for i := 1; i < len(nums); i++ {
		if nums[i] > num {
			num = nums[i]
		}
	}
	return num
}

func min(nums ...int) int {
	num := nums[0]
	for i := 1; i < len(nums); i++ {
		if nums[i] < num {
			num = nums[i]
		}
	}
	return num
}

func filterWorker(workers []v1alpha1.WorkerInstance, instances []string) []v1alpha1.WorkerInstance {
	sort.Strings(instances)
	res := make([]v1alpha1.WorkerInstance, 0, utils.DefaultSliceSize)
	for _, worker := range workers {
		index := sort.SearchStrings(instances, worker.InstanceID)
		if index >= 0 && index < len(instances) && instances[index] == worker.InstanceID {
			res = append(res, worker)
		}
	}
	return res
}
