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
	"errors"
	"time"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/config"
)

// MaxRequestNumStrategy -
type MaxRequestNumStrategy struct{}

// JudgeScaleDown implements autoscaler.ScaleStrategy
func (s *MaxRequestNumStrategy) JudgeScaleDown(sc *autoscaler.ScaleStrategyContext,
) ([]v1alpha1.WorkerInstance, error) {
	concurrency := int(sc.GetSpec().Concurrency.ReservedValue)
	if concurrency == 0 {
		return nil, errors.New("zero concurrency")
	}

	capacity := concurrency * len(sc.GetWorkers())
	instances, current, err := s.getTotalMaxCurrentRequestNum(sc)
	if err != nil {
		log.GetLogger().Errorf("failed to get total max current request num for funcSvc %s: %s",
			sc.GetFuncSvcName(), err.Error())
		return nil, err
	}
	nonReservedWorkers := getNonReservedWorkers(sc.GetWorkers())
	log.GetLogger().Debugf("workers len %d,nonReservedWorkers len %d, instance len %d", len(sc.GetWorkers()),
		len(nonReservedWorkers), len(instances))
	nonReservedWorkers = filterWorker(nonReservedWorkers, instances)
	scaleDownNum := (capacity - current) / concurrency
	// Must be >= to 0 and <= to number of nonReservedWorkers
	maxRequestScaleDownNum := max(min(scaleDownNum, len(nonReservedWorkers)), 0)
	log.GetLogger().Debugf("[JudgeScaleDown] capacity: %d, current: %d, scaleDownNum: %d",
		capacity, current, maxRequestScaleDownNum)

	if maxRequestScaleDownNum <= 0 {
		// Current function does not need to scale down. Do nothing.
		return nil, nil
	}

	// According to the request waiting to be processed, the number needs to be reduced
	pendingRequests := autoscaler.GetFuncAllPendingRequest(sc.GetFuncSvcName())
	pendingRequestsScaleDownNum := (capacity - pendingRequests) / concurrency
	log.GetLogger().Debugf("[JudgeScaleDown] capacity: %d, pendingRequests: %d, pendingRequestsScaleDownNum: %d",
		capacity, pendingRequests, pendingRequestsScaleDownNum)
	// Get the lesser
	finialScaleDownNum := min(maxRequestScaleDownNum, pendingRequestsScaleDownNum)
	if finialScaleDownNum <= 0 {
		return nil, nil
	}
	log.GetLogger().Infof("[JudgeScaleDown] final mark %d workers to be delete, funcSvcName is %s, capacity is"+
		" %d, current is %d, concurrency is %d, workers num is %d ,nonReservedWorkers num is %d, "+
		"pendingRequests is %d, maxRequestScaleDownNum is %d, pendingRequestsScaleDownNum is %d, worker is %v, "+
		"instance is %v, nonReservedWorkers is %v",
		finialScaleDownNum, sc.GetFuncSvcName(), capacity, current, concurrency, len(sc.GetWorkers()),
		len(nonReservedWorkers), pendingRequests, maxRequestScaleDownNum, pendingRequestsScaleDownNum,
		sc.GetWorkerInstanceIDs(), instances, getWorkerInstanceIDs(nonReservedWorkers))
	// Scale down workers in arbitrary order
	return nonReservedWorkers[:finialScaleDownNum], nil
}

// getWorkerInstanceIDs return cached worker's instance id
func getWorkerInstanceIDs(workers []v1alpha1.WorkerInstance) []string {
	workersInstanceIDs := make([]string, 0)
	for _, instance := range workers {
		workersInstanceIDs = append(workersInstanceIDs, instance.InstanceID)
	}
	return workersInstanceIDs
}

func (s *MaxRequestNumStrategy) getTotalMaxCurrentRequestNum(sc *autoscaler.ScaleStrategyContext,
) ([]string, int, error) {
	validTimestamp := time.Now().Add(-radicalValidTime).Unix()
	instances, sum, err := accumulate(sc,
		func(sum *workermanager.ReportMetrics, val workermanager.ReportMetrics) {
			if config.EnableRadicalAutoscaler() {
				if validTimestamp < val.SecondsTimestamp {
					sum.MaxCurrentRequestNums = max(sum.MaxCurrentRequestNums, val.MaxCurrentRequestNums)
				}
			} else {
				sum.MaxCurrentRequestNums = max(sum.MaxCurrentRequestNums, val.MaxCurrentRequestNums)
			}
		},
		func(sum *workermanager.ReportMetrics, val workermanager.ReportMetrics) {
			sum.MaxCurrentRequestNums += val.MaxCurrentRequestNums
		})
	if err != nil {
		return nil, 0, err
	}
	return instances, sum.MaxCurrentRequestNums, nil
}
