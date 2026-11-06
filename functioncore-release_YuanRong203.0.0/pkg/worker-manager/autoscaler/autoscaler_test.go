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
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

type rangeDataKey struct {
	judgeTimes int
	offset     int
	instanceID string
}

type suit struct {
	t              *testing.T
	funcSvcName    string
	workers        map[string]v1alpha1.WorkerInstance
	windowSlotNum  int
	windowInterval time.Duration
	spec           faasfunction.FunctionServiceSpec
	rangeData      map[rangeDataKey][]workermanager.ReportMetrics

	judgeTimes int
}

func (s *suit) SelectStrategy(sc *ScaleStrategyContext) ScaleStrategy {
	return s
}

func (s *suit) JudgeScaleDown(sc *ScaleStrategyContext) ([]v1alpha1.WorkerInstance, error) {
	s.judgeTimes++
	fmt.Println("judge", s.judgeTimes)
	assert.Equal(s.t, "aa", sc.GetFuncSvcName())
	assert.Equal(s.t, s.workers, sc.GetWorkers())
	assert.Equal(s.t, s.windowSlotNum, sc.GetSlideWindowSlotNum())
	assert.Equal(s.t, s.windowInterval, sc.GetSlideWindowInterval())
	assert.Equal(s.t, s.spec, sc.GetSpec())
	assert.Equal(s.t, len(s.workers), len(sc.GetWorkerInstanceIDs()))
	sc.RangeSlideWindow(func(offset int, instanceID string, list []workermanager.ReportMetrics) {
		key := rangeDataKey{
			judgeTimes: s.judgeTimes,
			offset:     offset,
			instanceID: instanceID,
		}
		val, ok := s.rangeData[key]
		if !ok {
			assert.Fail(s.t, "rangeDataKey does not exist: %+v", key)
			return
		}
		assert.Equal(s.t, val, list)
	})
	return nil, nil
}

func (s *suit) FuncSvcName() string {
	return s.funcSvcName
}

func (s *suit) GetWorkerInstances() (map[string]v1alpha1.WorkerInstance, error) {
	return s.workers, nil
}

func (s *suit) GetSpec() (faasfunction.FunctionServiceSpec, error) {
	return s.spec, nil
}

func (suit) DeleteInstances(instances []v1alpha1.WorkerInstance) {
}

func (s *suit) WillDeleteInstances(instances []v1alpha1.WorkerInstance) {
}

func TestAutoScaler(t *testing.T) {
	windowSlotNum := 5
	windowInterval := 1 * time.Second
	cfg := Config{
		WindowSlotInterval: windowInterval,
		WindowSlotNum:      windowSlotNum,
		JudgeInterval:      1 * time.Second,
		Executor:           NewExecutor(1*time.Millisecond, 16),
	}
	patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{}
	})
	defer patch.Reset()
	now := time.Now().UTC().Unix()
	metrics := []workermanager.ReportMetrics{
		{
			SecondsTimestamp: now - 1,
		},
		{
			SecondsTimestamp: now - 2,
		},
	}
	s := &suit{
		t:           t,
		funcSvcName: "aa",
		workers: map[string]v1alpha1.WorkerInstance{
			"instanceA": {InstanceID: "instanceA"},
		},
		windowSlotNum:  windowSlotNum,
		windowInterval: windowInterval,
		spec:           faasfunction.FunctionServiceSpec{},
		rangeData: map[rangeDataKey][]workermanager.ReportMetrics{
			{judgeTimes: 1, offset: 2, instanceID: "instanceB"}: metrics[:1],
			{judgeTimes: 1, offset: 1, instanceID: "instanceB"}: metrics[1:],
			{judgeTimes: 2, offset: 1, instanceID: "instanceB"}: metrics[:1],
			{judgeTimes: 2, offset: 0, instanceID: "instanceB"}: metrics[1:],
		},
	}
	as := NewAutoScalerWithConfig(s, s, cfg)
	as.AddMetrics(context.TODO(), workermanager.WorkerView{
		InstanceID: "instanceB",
		Metrics:    metrics,
	})

	time.Sleep(2800 * time.Millisecond)
	as.Close()
	cfg.Executor.Close()
}

type fakeFunctionAction struct{}

// FuncSvcName implements autoscaler.FunctionAction
func (fa *fakeFunctionAction) FuncSvcName() string {
	return ""
}

// GetWorkerInstances implements autoscaler.FunctionAction
func (fa *fakeFunctionAction) GetWorkerInstances() (map[string]v1alpha1.WorkerInstance, error) {
	return map[string]v1alpha1.WorkerInstance{}, nil
}

// GetSpec implements autoscaler.FunctionAction
func (fa *fakeFunctionAction) GetSpec() (faasfunction.FunctionServiceSpec, error) {
	return faasfunction.FunctionServiceSpec{}, nil
}

// DeleteInstances implements autoscaler.FunctionAction
func (fa *fakeFunctionAction) DeleteInstances(workers []v1alpha1.WorkerInstance) {}

func (fa *fakeFunctionAction) WillDeleteInstances(workers []v1alpha1.WorkerInstance) {}

// TestAutoScaler_RemoveWorkerInstanceMetrics -
func TestAutoScaler_RemoveWorkerInstanceMetrics(t *testing.T) {
	convey.Convey("TestAutoScaler_RemoveWorkerInstanceMetrics", t, func() {
		convey.Convey("AutoScaler is nil", func() {
			var as *AutoScaler
			convey.So(as.RemoveWorkerInstanceMetrics(context.Background(), ""), convey.ShouldBeNil)
		})
		convey.Convey("RemoveWorkerInstanceMetricsOK", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&Executor{}), "Async", func(_ *Executor,
				ctx context.Context, fn ExecutorFn) error {
				return nil
			})
			as := &AutoScaler{
				fa:     &fakeFunctionAction{},
				window: &slideWindow{},
			}
			convey.So(as.RemoveWorkerInstanceMetrics(context.Background(), ""), convey.ShouldBeNil)
			patch.Reset()
		})
	})
}

func TestJudge(t *testing.T) {
	convey.Convey("Test judge error", t, func() {
		as := NewAutoScaler(&fakeFunctionAction{}, &suit{})
		ctx := context.TODO()
		as.judge(ctx)
	})
	convey.Convey("Test judge error", t, func() {
		flag := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&fakeFunctionAction{}), "GetWorkerInstances",
			func(fa *fakeFunctionAction) (map[string]v1alpha1.WorkerInstance, error) {
				flag = 1
				return nil, errors.New("aa")
			})
		patch.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		defer patch.Reset()
		as := NewAutoScaler(&fakeFunctionAction{}, &suit{})
		ctx := context.TODO()
		as.judge(ctx)
		convey.So(flag, convey.ShouldEqual, 1)
	})
	convey.Convey("Test judge error", t, func() {
		flag := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&fakeFunctionAction{}), "GetWorkerInstances",
			func(fa *fakeFunctionAction) (map[string]v1alpha1.WorkerInstance, error) {
				flag = 1
				worker := make(map[string]v1alpha1.WorkerInstance)
				worker["sw"] = v1alpha1.WorkerInstance{}
				return worker, nil
			})
		patch.ApplyMethod(reflect.TypeOf(&suit{}), "JudgeScaleDown",
			func(s *suit, sc *ScaleStrategyContext) ([]v1alpha1.WorkerInstance, error) {
				return nil, nil
			})
		patch.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		defer patch.Reset()
		as := NewAutoScaler(&fakeFunctionAction{}, &suit{})
		ctx := context.TODO()
		as.judge(ctx)
		convey.So(flag, convey.ShouldEqual, 1)
	})
}

func TestGetFuncAllPendingRequest(t *testing.T) {
	convey.Convey("funcSvc does not exist", t, func() {
		requestNum := GetFuncAllPendingRequest("not-exist")
		convey.So(requestNum, convey.ShouldEqual, 0)
	})

	convey.Convey("aaaa funcSvc exist, there are already two elements in the PendingRequestCache will return 3",
		t, func() {
			currentTimeStamp := time.Now().UTC().Unix()
			metrics := []FuncRequestReportMetric{
				{
					PendingRequests: 1,
					ReportTimestamp: currentTimeStamp,
				},
				{
					PendingRequests: 2,
					ReportTimestamp: currentTimeStamp,
				},
			}
			PendingRequestCache.Store("aaaa", metrics)
			requestNum := GetFuncAllPendingRequest("aaaa")
			convey.So(requestNum, convey.ShouldEqual, 3)
			PendingRequestCache.Delete("aaaa")
		})
}

func Test_deleteNoReportMetricsFunc(t *testing.T) {
	convey.Convey("delete no report metric", t, func() {
		PendingRequestCache.Store("delete-funcSvc", []FuncRequestReportMetric{})
		deleteNoReportMetricsFunc()
		_, exist := PendingRequestCache.Load("delete-funcSvc")
		convey.So(exist, convey.ShouldEqual, false)
	})
}

func Test_confirmValidInstance(t *testing.T) {
	convey.Convey("delete invalid instances", t, func() {
		workers := make(map[string]v1alpha1.WorkerInstance, 0)
		workers[""] = v1alpha1.WorkerInstance{}
		workers["empty"] = v1alpha1.WorkerInstance{}
		workers["defaultaz-#-pool22-300-128-1"] = v1alpha1.WorkerInstance{
			InstanceID: "defaultaz-#-pool22-300-128-fusion-6fd755b8d4-qv4wk",
		}
		confirmValidInstance(workers)
		convey.So(len(workers), convey.ShouldEqual, 1)
	})
}
