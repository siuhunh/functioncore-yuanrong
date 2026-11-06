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

package autoscaler

import (
	"context"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

// ScaleStrategy -
type ScaleStrategy interface {
	JudgeScaleDown(sc *ScaleStrategyContext) ([]v1alpha1.WorkerInstance, error)
}

// ScaleStrategySelector -
type ScaleStrategySelector interface {
	SelectStrategy(sc *ScaleStrategyContext) ScaleStrategy
}

// ScaleStrategyContext contains some utility functions which may be helpful when calculating the scaling number.
// Cached values generate at the beginning of each judge.
type ScaleStrategyContext struct {
	autoScaler *AutoScaler
	ctx        context.Context

	// some cached values
	workers map[string]v1alpha1.WorkerInstance
	spec    faasfunction.FunctionServiceSpec
}

func newScaleStrategyContext(ctx context.Context, as *AutoScaler, workers map[string]v1alpha1.WorkerInstance,
) (*ScaleStrategyContext, error) {
	spec, err := as.fa.GetSpec()
	if err != nil {
		log.GetLogger().Errorf("failed to get spec for funcSvc %s: %s", as.fa.FuncSvcName(), err.Error())
		return nil, err
	}

	return &ScaleStrategyContext{
		autoScaler: as,
		ctx:        ctx,
		workers:    workers,
		spec:       spec,
	}, nil
}

// GetSpec returns a cached FunctionServiceSpec
func (s *ScaleStrategyContext) GetSpec() faasfunction.FunctionServiceSpec {
	return s.spec
}

// RangeSlideWindow ranges the slide window
func (s *ScaleStrategyContext) RangeSlideWindow(rangeFn SlideWindowRangeFn) error {
	return s.autoScaler.window.do(s.ctx, &slideWindowRange{rangeFn: rangeFn})
}

// GetWorkers returns cached workers
func (s *ScaleStrategyContext) GetWorkers() map[string]v1alpha1.WorkerInstance {
	return s.workers
}

// GetWorkerInstanceIDs return cached workers instance ID
func (s *ScaleStrategyContext) GetWorkerInstanceIDs() []string {
	workerInstanceIDs := make([]string, 0)
	for _, instance := range s.workers {
		workerInstanceIDs = append(workerInstanceIDs, instance.InstanceID)
	}
	return workerInstanceIDs
}

// GetFuncSvcName returns the function service name
func (s *ScaleStrategyContext) GetFuncSvcName() string {
	return s.autoScaler.fa.FuncSvcName()
}

// GetSlideWindowSlotNum returns the slide window slot num
func (s *ScaleStrategyContext) GetSlideWindowSlotNum() int {
	return s.autoScaler.window.ring.len()
}

// GetSlideWindowInterval returns the slide window interval
func (s *ScaleStrategyContext) GetSlideWindowInterval() time.Duration {
	return s.autoScaler.window.interval
}
