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
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

func TestUsedTimeSlice(t *testing.T) {
	// total cap = 2 * 1min
	// current max = 40sec + 20sec
	// scale down num should be 1
	mock := mockScaleStrategyContext{
		GetSpec: func(*autoscaler.ScaleStrategyContext) faasfunction.FunctionServiceSpec {
			return faasfunction.FunctionServiceSpec{Concurrency: faasfunction.ConcurrencySpec{ReservedValue: 100}}
		},
		RangeSlideWindow: func(_ *autoscaler.ScaleStrategyContext, rangeFn autoscaler.SlideWindowRangeFn) error {
			rangeFn(0, "instanceA", []workermanager.ReportMetrics{
				{UsedTimeSlice: 30, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(1, "instanceA", []workermanager.ReportMetrics{
				{UsedTimeSlice: 40, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(2, "instanceA", []workermanager.ReportMetrics{
				{UsedTimeSlice: 20, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(3, "instanceA", []workermanager.ReportMetrics{
				{UsedTimeSlice: 30, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(4, "instanceA", []workermanager.ReportMetrics{
				{UsedTimeSlice: 20, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(0, "instanceB", []workermanager.ReportMetrics{
				{UsedTimeSlice: 15, SecondsTimestamp: time.Now().Unix()},
				{UsedTimeSlice: 20, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(0, "instanceC", []workermanager.ReportMetrics{
				{UsedTimeSlice: 0, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(1, "instanceC", []workermanager.ReportMetrics{
				{UsedTimeSlice: 0, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(2, "instanceC", []workermanager.ReportMetrics{
				{UsedTimeSlice: 0, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(3, "instanceC", []workermanager.ReportMetrics{
				{UsedTimeSlice: 0, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(4, "instanceC", []workermanager.ReportMetrics{
				{UsedTimeSlice: 0, SecondsTimestamp: time.Now().Unix()},
			})
			return nil
		},
		GetWorkers: func(*autoscaler.ScaleStrategyContext) map[string]v1alpha1.WorkerInstance {
			return map[string]v1alpha1.WorkerInstance{
				"instanceA": {InstanceID: "instanceA"},
				"instanceB": {InstanceID: "instanceB"},
				"instanceC": {InstanceID: "instanceC"},
			}
		},
		GetFuncSvcName: func(*autoscaler.ScaleStrategyContext) string {
			return ""
		},
		GetSlideWindowSlotNum: func(*autoscaler.ScaleStrategyContext) int {
			return 2
		},
		GetSlideWindowInterval: func(*autoscaler.ScaleStrategyContext) time.Duration {
			return 1 * time.Minute
		},
	}
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	mock.apply(patches)

	strategy := UsedTimeSliceStrategy{}
	instances, err := strategy.JudgeScaleDown(&autoscaler.ScaleStrategyContext{})
	assert.NoError(t, err)
	// only need to scale down instanceC
	assert.Equal(t, 1, len(instances))
}
