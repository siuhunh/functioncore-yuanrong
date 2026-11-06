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

// Package requestdispatcher contains request lb, autoscaler and so on
package requestdispatcher

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/dataaggregation"
	commonQueue "functioncore/pkg/common/queue"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/proxyview"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/queue"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxy/workersview"
	"functioncore/pkg/functionbus/servicediscovery"
	mockUtils "functioncore/test/utils"
)

const (
	funcKeyWithResource = "7e186a/0@base@testresourcepython36768/latest/800-768"
)

type fakeAutoScaler struct {
	num int
}

func (f *fakeAutoScaler) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option queue.BackPressuredOption) {
}

func (f *fakeAutoScaler) CalculateScaleDown(currentRequests int, requestLatencyMs int) int {
	return 1
}

func (f *fakeAutoScaler) ScaleDown(workerID string) {}

func (f *fakeAutoScaler) SetFuncMeta(metadata servicediscovery.FunctionMetaInfo) {
}

func (f *fakeAutoScaler) UpdateUsedWorkerNum(num int) {
	f.num = num
}

func (f *fakeAutoScaler) Reset() {}

func (f *fakeAutoScaler) CheckUsedWorker() bool {
	return f.num > 0
}

func TestSchedule(t *testing.T) {
	autoscalerConfig := common.FConfig.AutoScalerConfig
	common.FConfig.AutoScalerConfig = &common.AutoScalerConfig{RequestLatencyAggregationThreshold: 100}
	stopCh := make(chan struct{})
	metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
		ConcurrentNum: 100,
		IdleMode:      false,
	}}
	scheduler := newDefaultScheduler("", &fakeAutoScaler{}, metadata, stopCh)
	if "" != scheduler.Schedule(&types.TCPRequest{}, "traceid") {
		t.Errorf("schedule no worker failed")
	}

	nodeIP := "127.0.0.1"
	metrics := &workerevaluator.WorkerMetrics{
		ID:        "abc",
		Enable:    1,
		Latencies: []int64{},
		NodeIP:    nodeIP,
	}
	workersview.GetWorkersView().EnableNode(nodeIP)
	scheduler.Update(metrics)
	if "abc" != scheduler.Schedule(&types.TCPRequest{}, "traceid") {
		t.Errorf("schedule one worker failed")
	}

	// requests finished, worker exited
	metrics.Latencies = append(metrics.Latencies, 100)
	atomic.StoreUint32(&metrics.Enable, 0)
	scheduler.Update(metrics)
	if "" != scheduler.Schedule(&types.TCPRequest{}, "traceid") {
		t.Errorf("worker exited failed")
	}
	close(stopCh)
	common.FConfig.AutoScalerConfig = autoscalerConfig
}

func TestSortedWorkers(t *testing.T) {
	stopCh := make(chan struct{})
	metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
		ConcurrentNum: 100,
		IdleMode:      false,
	}}
	scheduler := newDefaultScheduler("", &fakeAutoScaler{}, metadata, stopCh)
	if "" != scheduler.Schedule(&types.TCPRequest{}, "traceid") {
		t.Errorf("schedule no worker failed")
	}

	nodeIP := "127.0.0.2"
	metricsA := &workerevaluator.WorkerMetrics{
		ID:        "abc",
		Enable:    1,
		Latencies: []int64{},
		NodeIP:    nodeIP,
	}
	metricsB := &workerevaluator.WorkerMetrics{
		ID:        "bcd",
		Enable:    1,
		Latencies: []int64{},
		NodeIP:    nodeIP,
	}

	workersview.GetWorkersView().EnableNode(nodeIP)
	scheduler.Update(metricsA)
	scheduler.Update(metricsB)

	scheduleWorkerFirst := scheduler.Schedule(&types.TCPRequest{}, "traceid")
	if "" == scheduleWorkerFirst {
		t.Errorf("schedule one worker failed")
	}

	t.Logf("first result: %s", scheduleWorkerFirst)

	secondResult := scheduler.Schedule(&types.TCPRequest{}, "traceid")
	t.Logf("second result: %s", secondResult)

	if scheduleWorkerFirst != secondResult {
		t.Errorf("schedule sorted worker failed")
	}
	close(stopCh)
}

func TestDefaultScheduler_SetInstanceMetadata(t *testing.T) {
	convey.Convey("TestDefaultScheduler_SetInstanceMetadata", t, func() {
		stopCh := make(chan struct{})
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 100,
			IdleMode:      false,
		}}
		schedulerITF := newDefaultScheduler(funcKeyWithResource, &fakeAutoScaler{}, metadata, stopCh)
		scheduler, ok := schedulerITF.(*defaultScheduler)
		convey.So(ok, convey.ShouldBeTrue)

		concurrency := 200
		scheduler.sortedSlices = append(scheduler.sortedSlices, scheduleEntry{
			concurrency: 0,
		})
		metadata.InstanceMetaData.ConcurrentNum = concurrency
		scheduler.SetFunctionMetadata(metadata)
		convey.So(scheduler.lastConcurrency, convey.ShouldEqual, concurrency)
		scheduler.SetFunctionMetadata(metadata)
		convey.So(scheduler.lastConcurrency, convey.ShouldEqual, concurrency)
	})

	convey.Convey("TestDefaultScheduler_SetInstanceMetadata_WithMcpFunc", t, func() {
		stopCh := make(chan struct{})
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 100,
			IdleMode:      false,
		}}
		schedulerITF := newDefaultScheduler(funcKeyWithResource, &fakeAutoScaler{}, metadata, stopCh)
		scheduler, ok := schedulerITF.(*defaultScheduler)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduler.lastConcurrency, convey.ShouldEqual, 100)
		convey.So(scheduler.lastSessionConcurrency, convey.ShouldEqual, 10)

		concurrency := 200
		scheduler.sortedSlices = append(scheduler.sortedSlices, scheduleEntry{
			concurrency: 0,
		})
		metadata.InstanceMetaData.ConcurrentNum = concurrency
		metadata.ExtendedMetaData.IsMcpFunction = true
		scheduler.SetFunctionMetadata(metadata)
		convey.So(scheduler.lastConcurrency, convey.ShouldEqual, concurrency)
		convey.So(scheduler.lastSessionConcurrency, convey.ShouldEqual, 20)
		scheduler.SetFunctionMetadata(metadata)
		convey.So(scheduler.lastConcurrency, convey.ShouldEqual, concurrency)
		convey.So(scheduler.lastSessionConcurrency, convey.ShouldEqual, 20)
	})
}

func TestCalculateScheduleEntry(t *testing.T) {
	convey.Convey("TestCalculateScheduleEntry", t, func() {
		old := &scheduleEntry{currentRequests: 10}
		metrics := &workerevaluator.WorkerMetrics{
			Latencies: []int64{1, 2, 3},
		}

		stopCh := make(chan struct{})
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 100,
			IdleMode:      false,
		}}
		schedulerITF := newDefaultScheduler(funcKeyWithResource, &fakeAutoScaler{}, metadata, stopCh)
		scheduler, ok := schedulerITF.(*defaultScheduler)
		convey.So(ok, convey.ShouldBeTrue)

		scheduler.calculateScheduleEntry(old, metrics)
		convey.So(old.currentRequests, convey.ShouldEqual, 7)
	})
}

func TestGetNodeAvailable(t *testing.T) {
	convey.Convey("TestGetNodeAvailable", t, func() {
		hostIP := "127.0.0.2"
		internal.SelfNodeIP = hostIP
		available := getNodeAvailable(hostIP, "workerID")
		internal.SelfNodeIP = ""
		convey.So(available, convey.ShouldBeTrue)
		proxyview.UpdateProxyAvailableStatus(hostIP, false)
		available = getNodeAvailable(hostIP, "workerID")
		convey.So(available, convey.ShouldBeFalse)
		proxyview.UpdateProxyAvailableStatus(hostIP, true)
	})
}

func TestComputePriority(t *testing.T) {
	convey.Convey("TestComputePriority", t, func() {
		scheduleEntry := &scheduleEntry{}
		scheduleEntry.concurrency = 0
		scheduleEntry.computePriority(false)
		convey.So(scheduleEntry.priority, convey.ShouldEqual, 0)

		scheduleEntry.currentRequests = 0
		scheduleEntry.concurrency = 1000
		scheduleEntry.isMinInstance = true
		scheduleEntry.computePriority(true)
		convey.So(scheduleEntry.priority, convey.ShouldEqual, 2000)
		scheduleEntry.computePriority(false)
		convey.So(scheduleEntry.priority, convey.ShouldEqual, 3000)

		scheduleEntry.currentRequests = 100
		scheduleEntry.concurrency = 1000
		scheduleEntry.isMinInstance = true
		scheduleEntry.computePriority(true)
		convey.So(scheduleEntry.priority, convey.ShouldEqual, 2100)
		scheduleEntry.computePriority(false)
		convey.So(scheduleEntry.priority, convey.ShouldEqual, 2900)

		scheduleEntry.isMinInstance = false
		scheduleEntry.isLocalWorker = true
		scheduleEntry.computePriority(false)
		convey.So(scheduleEntry.priority, convey.ShouldEqual, 1100)
	})
}

func TestDeleteInSlices(t *testing.T) {
	convey.Convey("TestDeleteInSlices", t, func() {
		scheduler := &defaultScheduler{
			requestLatencyAggregation: dataaggregation.NewAtomicAverage(-1),
		}
		nodeIP := "127.0.0.2"
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyFunc((*dataaggregation.Count).AddSamples, func(_ *dataaggregation.Count, sample []int64, t time.Time) {
		})
		scheduler.sortedSlices = append(scheduler.sortedSlices, scheduleEntry{
			workerID:    "abc",
			concurrency: 0,
		})

		metrics := &workerevaluator.WorkerMetrics{
			ID:        "abc",
			Enable:    1,
			Latencies: []int64{1},
			NodeIP:    nodeIP,
		}
		scheduler.updateSortedWorkers(metrics)
		slices := scheduler.deleteInSlices("abc", scheduler.sortedSlices)
		convey.So(slices, convey.ShouldBeEmpty)
		scheduler.sortedSlices = append(scheduler.sortedSlices, scheduleEntry{
			workerID:        "def",
			currentRequests: -1,
		})
		count := scheduler.sumCurrentRequests()
		convey.So(count, convey.ShouldEqual, -1)
	})
}

var mockScheduler = &defaultScheduler{}

type mockScaler struct {
}

func (m *mockScaler) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option queue.BackPressuredOption) {
}

func (m *mockScaler) CalculateScaleDown(currentRequests int, requestLatencyMs int) int {
	scaleNum := len(mockScheduler.sortedSlices) - currentRequests
	if scaleNum < 0 {
		return 0
	}
	return scaleNum
}

func (m *mockScaler) ScaleDown(workerID string) {
}

func (m *mockScaler) SetFuncMeta(metadata servicediscovery.FunctionMetaInfo) {
}

func (m *mockScaler) UpdateUsedWorkerNum(num int) {}

func (m *mockScaler) Reset() {}

func (m *mockScaler) CheckUsedWorker() bool {
	return false
}

type mockMicroServiceScaler struct {
}

func (m *mockMicroServiceScaler) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option queue.BackPressuredOption) {
}

func (m *mockMicroServiceScaler) CalculateScaleDown(currentRequests int, requestLatencyMs int) int {
	return 0
}

func (m *mockMicroServiceScaler) ScaleDown(workerID string) {
}

func (m *mockMicroServiceScaler) SetFuncMeta(metadata servicediscovery.FunctionMetaInfo) {
}

func (m *mockMicroServiceScaler) UpdateUsedWorkerNum(num int) {}

func (m *mockMicroServiceScaler) Reset() {}

func (m *mockMicroServiceScaler) CheckUsedWorker() bool {
	return false
}

func TestCheckScaleDown(t *testing.T) {
	keepaliveTime := 10
	oldIdleTime := internal.IdleTimeForWorker
	internal.IdleTimeForWorker = time.Duration(keepaliveTime) * time.Second
	defer func() {
		internal.IdleTimeForWorker = oldIdleTime
	}()
	tests := []struct {
		name        string
		newIns      []int
		requests    []int
		expectedIns []int
	}{
		{
			name:        "case1: instance scaled up, but no request, wait a scale down window, then scale down",
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0},
		},
		{
			name:        "case2: more instance scaled up, but no request, wait a scale down window, then scale all down",
			newIns:      []int{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0},
		},
		{
			name:        "case3: normal case, one instance one request",
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0},
		},
		{
			name:        "case4: normal case, more instance, one request per instance",
			newIns:      []int{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0},
		},
		{
			name:        "case5: scale up 2 instance with one request, should scale them all down",
			newIns:      []int{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0},
		},
		{
			name:        "case6: scale up 2 instance with one request, should scale them all down",
			newIns:      []int{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0},
		},
		{
			name:        "case7: request stop when scale down, should not scale down immediately",
			newIns:      []int{10, 00, 00, 00, 00, 00, 00, 00, 00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{05, 05, 05, 05, 05, 05, 05, 05, 05, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{10, 10, 10, 10, 10, 10, 10, 10, 10, 5, 5, 5, 5, 5, 5, 5, 5, 5, 0},
		},
		{
			name:        "case7: request goes up, should scale down less",
			newIns:      []int{10, 00, 00, 00, 00, 00, 00, 00, 00, 0},
			requests:    []int{05, 05, 05, 05, 05, 05, 05, 05, 06, 6},
			expectedIns: []int{10, 10, 10, 10, 10, 10, 10, 10, 10, 6},
		},
		{
			name:        "case8: long run request",
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0},
		},
	}
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc((*defaultScheduler).getKeepAliveTimeInternal, func(_ *defaultScheduler) int {
			return keepaliveTime
		}),
	})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockScheduler = &defaultScheduler{
				requestLatencyAggregation: dataaggregation.NewAtomicAverage(-1),
				autoScaler:                &mockScaler{},
				needIns:                   commonQueue.NewMonotonicQueue(10),
				scaleNum:                  commonQueue.NewMonotonicQueue(10),
			}

			// assume that the scheduler has been running for a while
			for i := 0; i < 20; i++ {
				mockScheduler.scaleNum.Push(0)
				mockScheduler.needIns.Push(0)
			}

			for i := range tt.requests {
				for j := 0; j < tt.newIns[i]; j++ {
					mockScheduler.sortedSlices = append(mockScheduler.sortedSlices, scheduleEntry{})
				}
				mockScheduler.currentAllRequests = tt.requests[i]

				mockScheduler.checkScaleDown()

				assert.Equal(t, tt.expectedIns[i], len(mockScheduler.sortedSlices), i)
			}
		})
	}
	patches.ResetAll()
}

func TestMicroServiceCheckScaleDown(t *testing.T) {
	oldIdleTime := internal.IdleTimeForWorker
	internal.IdleTimeForWorker = 10 * time.Second
	defer func() {
		internal.IdleTimeForWorker = oldIdleTime
	}()
	tests := []struct {
		name        string
		newIns      []int
		requests    []int
		expectedIns []int
	}{
		{
			name:        "case1: instance scaled up, but no request, no scale down",
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name:        "case2: normal case, one instance one request",
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockScheduler = &defaultScheduler{
				requestLatencyAggregation: dataaggregation.NewAtomicAverage(-1),
				autoScaler:                &mockMicroServiceScaler{},
				needIns:                   commonQueue.NewMonotonicQueue(10),
				scaleNum:                  commonQueue.NewMonotonicQueue(10),
			}

			// assume that the scheduler has been running for a while
			for i := 0; i < 20; i++ {
				mockScheduler.scaleNum.Push(0)
				mockScheduler.needIns.Push(0)
			}

			for i := range tt.requests {
				for j := 0; j < tt.newIns[i]; j++ {
					mockScheduler.sortedSlices = append(mockScheduler.sortedSlices, scheduleEntry{})
				}
				mockScheduler.currentAllRequests = tt.requests[i]

				mockScheduler.checkScaleDown()

				assert.Equal(t, tt.expectedIns[i], len(mockScheduler.sortedSlices), i)
			}
		})
	}
}

func TestCalculateScheduleEntryWithInvalidInput(t *testing.T) {
	convey.Convey("TestCalculateScheduleEntry", t, func() {
		scheduler := &defaultScheduler{
			requestLatencyAggregation: dataaggregation.NewAtomicAverage(-1),
		}
		entry := &scheduleEntry{
			currentRequests: 1,
		}
		scheduler.currentAllRequests = 1
		metrics := &workerevaluator.WorkerMetrics{
			Latencies: []int64{1, 2},
		}
		scheduler.calculateScheduleEntry(entry, metrics)
		convey.So(entry.currentRequests, convey.ShouldEqual, 0)
		convey.So(scheduler.currentAllRequests, convey.ShouldEqual, 0)
	})
}

func Test_defaultScheduler_Update(t *testing.T) {
	ddp := &defaultScheduler{
		autoScaler:   &fakeAutoScaler{},
		sortedSlices: []scheduleEntry{{}, {}},
		Mutex:        sync.Mutex{},
	}
	ddp.Update(&workerevaluator.WorkerMetrics{})
	if !ddp.autoScaler.CheckUsedWorker() {
		t.Errorf("failed to check user worker after update")
	}
}
