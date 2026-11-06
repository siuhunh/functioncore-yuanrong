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

package workerevaluator

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/logger/log"
)

type MockRequestDispatcher struct {
	functionKey      string
	finishedRequests int
	instanceMetrics  map[string]WorkerMetrics
	sync.RWMutex
}

func CreateMockRequestDispatcher(functionKey string) *MockRequestDispatcher {
	mrd := &MockRequestDispatcher{
		functionKey:      functionKey,
		finishedRequests: 0,
		instanceMetrics:  make(map[string]WorkerMetrics, 0),
	}
	return mrd
}

func (mrd *MockRequestDispatcher) OnObserved(content interface{}, recovering bool) {
	metrics, ok := content.(*WorkerMetrics)
	if !ok {
		log.GetLogger().Errorf("convert interface to *WorkerMetrics failed")
		return
	}
	mrd.Lock()
	if atomic.LoadUint32(&metrics.Enable) == 0 {
		delete(mrd.instanceMetrics, metrics.ID)
	} else {
		mrd.instanceMetrics[metrics.ID] = *metrics
	}
	mrd.finishedRequests += len(metrics.Latencies)
	mrd.Unlock()
}

func TestWorkerEvaluator_Observe(t *testing.T) {
	functionKey := "testA"
	mrd := CreateMockRequestDispatcher(functionKey)
	GetInstance().Observe(functionKey, mrd)
	obj, ok := GetInstance().observers.Load(functionKey)
	if !ok {
		t.Errorf("no functionKey:%s observer in map", functionKey)
	}
	if tmpMrd, ok := obj.(*MockRequestDispatcher); !ok {
		t.Errorf("functionKey:%s observer is error in map", functionKey)
		return
	} else {
		if mrd != tmpMrd {
			t.Errorf("don't equal")
		}
	}
}

func TestWorkerEvaluator_UnObserve(t *testing.T) {
	functionKey := "testA"
	mrd := CreateMockRequestDispatcher(functionKey)
	GetInstance().Observe(functionKey, mrd)
	pod1 := &WorkerMetrics{
		FuncKeyWithResource: "testA",
		Enable:              1,
	}
	GetInstance().metrics.Store("testpool", pod1)
	GetInstance().UnObserve(functionKey)
	if _, ok := GetInstance().observers.Load(functionKey); ok {
		t.Errorf("delete functionKey:%s observer failed", functionKey)
	}
	assert.Equal(t, uint32(1), atomic.LoadUint32(&pod1.Enable))
	GetInstance().metrics.Delete("testpool")
}

func TestOnObserved(t *testing.T) {
	convey.Convey("TestOnObserved", t, func() {
		functionKey := "testMockObserver"
		mrd := CreateMockRequestDispatcher(functionKey)
		GetInstance().metrics.Store("test", &WorkerMetrics{Enable: 1, FuncKeyWithResource: functionKey})
		instanceNum := GetInstance().GetWorkerNum()
		convey.So(instanceNum, convey.ShouldEqual, 1)
		GetInstance().Observe(functionKey, mrd)
		convey.So(len(mrd.instanceMetrics), convey.ShouldEqual, 1)
		GetInstance().UnObserve(functionKey)
		_, ok := GetInstance().observers.Load(functionKey)
		convey.So(ok, convey.ShouldBeFalse)
	})
}
