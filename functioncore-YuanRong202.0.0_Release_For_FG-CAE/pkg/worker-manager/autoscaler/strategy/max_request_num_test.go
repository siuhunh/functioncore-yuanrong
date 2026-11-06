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

package strategy

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

type mockScaleStrategyContext struct {
	GetSpec                func(*autoscaler.ScaleStrategyContext) faasfunction.FunctionServiceSpec
	RangeSlideWindow       func(_ *autoscaler.ScaleStrategyContext, rangeFn autoscaler.SlideWindowRangeFn) error
	GetWorkers             func(*autoscaler.ScaleStrategyContext) map[string]v1alpha1.WorkerInstance
	GetFuncSvcName         func(*autoscaler.ScaleStrategyContext) string
	GetSlideWindowSlotNum  func(*autoscaler.ScaleStrategyContext) int
	GetSlideWindowInterval func(*autoscaler.ScaleStrategyContext) time.Duration
}

func (m *mockScaleStrategyContext) apply(patches *gomonkey.Patches) {
	var sc *autoscaler.ScaleStrategyContext
	typ := reflect.TypeOf(sc)
	patches.ApplyMethod(typ, "GetSpec", m.GetSpec)
	patches.ApplyMethod(typ, "RangeSlideWindow", m.RangeSlideWindow)
	patches.ApplyMethod(typ, "GetWorkers", m.GetWorkers)
	patches.ApplyMethod(typ, "GetFuncSvcName", m.GetFuncSvcName)
	patches.ApplyMethod(typ, "GetSlideWindowSlotNum", m.GetSlideWindowSlotNum)
	patches.ApplyMethod(typ, "GetSlideWindowInterval", m.GetSlideWindowInterval)
}

func TestMaxRequestNum(t *testing.T) {
	// total cap = 100 * 2
	// current max = 70 + 30
	// scale down num should be 1
	mock := mockScaleStrategyContext{
		GetSpec: func(*autoscaler.ScaleStrategyContext) faasfunction.FunctionServiceSpec {
			return faasfunction.FunctionServiceSpec{Concurrency: faasfunction.ConcurrencySpec{ReservedValue: 100}}
		},
		RangeSlideWindow: func(_ *autoscaler.ScaleStrategyContext, rangeFn autoscaler.SlideWindowRangeFn) error {
			rangeFn(0, "instanceA", []workermanager.ReportMetrics{
				{MaxCurrentRequestNums: 40, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(1, "instanceA", []workermanager.ReportMetrics{
				{MaxCurrentRequestNums: 70, SecondsTimestamp: time.Now().Unix()},
			})
			rangeFn(0, "instanceB", []workermanager.ReportMetrics{
				{MaxCurrentRequestNums: 30, SecondsTimestamp: time.Now().Unix()},
				{MaxCurrentRequestNums: 20, SecondsTimestamp: time.Now().Unix()},
				{MaxCurrentRequestNums: 30, SecondsTimestamp: time.Now().Unix()},
				{MaxCurrentRequestNums: 20, SecondsTimestamp: time.Now().Unix()},
				{MaxCurrentRequestNums: 30, SecondsTimestamp: time.Now().Unix()},
			})
			return nil
		},
		GetWorkers: func(*autoscaler.ScaleStrategyContext) map[string]v1alpha1.WorkerInstance {
			return map[string]v1alpha1.WorkerInstance{
				"instanceA": {InstanceID: "instanceA"},
				"instanceB": {InstanceID: "instanceB"},
			}
		},
		GetFuncSvcName: func(*autoscaler.ScaleStrategyContext) string {
			return ""
		},
		GetSlideWindowSlotNum: func(*autoscaler.ScaleStrategyContext) int {
			return 2
		},
	}
	patches := gomonkey.NewPatches()
	defer patches.Reset()
	mock.apply(patches)

	strategy := MaxRequestNumStrategy{}
	instances, err := strategy.JudgeScaleDown(&autoscaler.ScaleStrategyContext{})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(instances))
}
