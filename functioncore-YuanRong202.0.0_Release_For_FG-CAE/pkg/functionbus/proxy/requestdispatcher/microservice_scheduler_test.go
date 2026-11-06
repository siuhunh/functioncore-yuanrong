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

// Package requestdispatcher -
package requestdispatcher

import (
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	commonQueue "functioncore/pkg/common/queue"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/queue"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/servicediscovery"
	mockUtils "functioncore/test/utils"
)

func TestMircoServiceSchedule(t *testing.T) {
	convey.Convey("TestMircoServiceSchedule_RoundRobin", t, func() {
		patch := ApplyGlobalVar(&autoscaler.EnableMicroserviceAutoScaler, false)
		defer func() {
			time.Sleep(10 * time.Millisecond)
			patch.Reset()
		}()
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 10,
		}}
		scheduler := newMicronServiceScheduler("funcKeyWithResource", &fakeAutoScaler{},
			metadata, nil)
		convey.So(scheduler, convey.ShouldEqual, nil)
		stopCh := make(chan struct{})
		common.FConfig.MicroServiceSchedulingPolicy = common.RoundRobin
		scheduler = newMicronServiceScheduler("funcKeyWithResource", &fakeAutoScaler{},
			metadata, stopCh)
		workerId := scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, "")
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, "")
		metrics1 := workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker1",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 3,
		}
		scheduler.Update(&metrics1)
		metrics2 := workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker2",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 3,
		}
		scheduler.Update(&metrics2)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics2.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{UsedInstance: map[string]struct{}{metrics2.ID: {}}}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
	})

	convey.Convey("TestMircoServiceSchedule_LeastConnections", t, func() {
		patch := ApplyGlobalVar(&autoscaler.EnableMicroserviceAutoScaler, false)
		defer func() {
			time.Sleep(10 * time.Millisecond)
			patch.Reset()
		}()
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 10,
		}}
		scheduler := newMicronServiceScheduler("funcKeyWithResource2",
			&fakeAutoScaler{}, metadata, nil)
		convey.So(scheduler, convey.ShouldEqual, nil)
		stopCh := make(chan struct{})
		common.FConfig.MicroServiceSchedulingPolicy = common.LeastConnections
		scheduler = newMicronServiceScheduler("funcKeyWithResource2", &fakeAutoScaler{},
			metadata, stopCh)
		workerId := scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, "")
		metrics1 := workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker3",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 1,
		}
		scheduler.Update(&metrics1)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
		metrics2 := workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker4",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 1,
		}
		scheduler.Update(&metrics2)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics2.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics2.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{UsedInstance: map[string]struct{}{metrics2.ID: {}}}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
	})
}

func TestMircoServiceSetFunctionMetadata(t *testing.T) {
	convey.Convey("TestMircoServiceSetFunctionMetadata", t, func() {
		patch := ApplyGlobalVar(&autoscaler.EnableMicroserviceAutoScaler, false)
		defer func() {
			time.Sleep(10 * time.Millisecond)
			patch.Reset()
		}()
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 0,
		}}
		stopCh := make(chan struct{})
		common.FConfig.MicroServiceSchedulingPolicy = common.RoundRobin
		scheduler := newMicronServiceScheduler("funcKeyWithResource", &fakeAutoScaler{},
			metadata, stopCh)
		functionMetaInfo := servicediscovery.FunctionMetaInfo{
			InstanceMetaData: servicediscovery.InstanceMetaData{
				ConcurrentNum: 1,
			},
		}
		metrics1 := workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker3",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 0,
		}
		scheduler.Update(&metrics1)
		scheduler.SetFunctionMetadata(functionMetaInfo)
		workerId := scheduler.Schedule(&types.TCPRequest{}, "traceid")
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, "")
		functionMetaInfo = servicediscovery.FunctionMetaInfo{
			InstanceMetaData: servicediscovery.InstanceMetaData{
				ConcurrentNum: 10,
			},
		}
		scheduler.SetFunctionMetadata(functionMetaInfo)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics1.ID)
	})
}

func TestMircoServiceUpdate(t *testing.T) {
	convey.Convey("TestMircoServiceUpdate", t, func() {
		patch := ApplyGlobalVar(&autoscaler.EnableMicroserviceAutoScaler, false)
		defer func() {
			time.Sleep(10 * time.Millisecond)
			patch.Reset()
		}()
		// delete
		metrics := workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker3",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 0,
		}
		metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
			ConcurrentNum: 0,
		}}
		stopCh := make(chan struct{})
		common.FConfig.MicroServiceSchedulingPolicy = common.RoundRobin
		scheduler := newMicronServiceScheduler("funcKeyWithResource", &fakeAutoScaler{},
			metadata, stopCh)
		scheduler.Update(&metrics)
		workerId := scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics.ID)

		metricsDelete := workerevaluator.WorkerMetrics{
			Enable:              0,
			ID:                  "worker3",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 3,
		}
		scheduler.Update(&metricsDelete)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, "")
		// update
		scheduler.Update(&metrics)
		// set concurrency 1
		functionMetaInfo := servicediscovery.FunctionMetaInfo{
			InstanceMetaData: servicediscovery.InstanceMetaData{
				ConcurrentNum: 1,
			},
		}
		scheduler.SetFunctionMetadata(functionMetaInfo)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics.ID)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, "")
		metrics = workerevaluator.WorkerMetrics{
			Enable:              1,
			ID:                  "worker3",
			IsLocalWorker:       true,
			NodeIP:              "127.0.0.1",
			RecoveredRequestNum: 0,
			Latencies:           []int64{1},
		}
		scheduler.Update(&metrics)
		workerId = scheduler.Schedule(&types.TCPRequest{}, "traceid")
		convey.So(workerId, convey.ShouldEqual, metrics.ID)
	})
}

func TestGetSchedulerType(t *testing.T) {
	patch := ApplyGlobalVar(&autoscaler.EnableMicroserviceAutoScaler, false)
	defer func() {
		time.Sleep(10 * time.Millisecond)
		patch.Reset()
	}()
	metadata := servicediscovery.FunctionMetaInfo{InstanceMetaData: servicediscovery.InstanceMetaData{
		ConcurrentNum: 0,
	}}
	stopCh := make(chan struct{})
	common.FConfig.MicroServiceSchedulingPolicy = common.RoundRobin
	scheduler := newMicronServiceScheduler("funcKeyWithResource", &fakeAutoScaler{},
		metadata, stopCh)
	assert.Equal(t, constants.BusinessTypeCAE, scheduler.GetSchedulerType())
}

func TestPreSchedule(t *testing.T) {
	entry1 := scheduleEntry{workerID: "defaultaz-#-pod1"}
	entry2 := scheduleEntry{workerID: "defaultaz-#-pod2"}
	mss := &microserviceScheduler{
		sortedSlices: []scheduleEntry{entry1, entry2},
	}

	mss.PreSchedule("nosuchentry")
	assert.Equal(t, 0, mss.sortedSlices[0].currentRequests)
	assert.Equal(t, 0, mss.sortedSlices[1].currentRequests)
	assert.Equal(t, 0, mss.currentAllRequests)

	mss.PreSchedule("defaultaz-#-pod2")
	assert.Equal(t, 0, mss.sortedSlices[0].currentRequests)
	assert.Equal(t, 1, mss.sortedSlices[1].currentRequests)
	assert.Equal(t, 1, mss.currentAllRequests)

	mss.PreSchedule("defaultaz-#-pod1")
	assert.Equal(t, 1, mss.sortedSlices[0].currentRequests)
	assert.Equal(t, 1, mss.sortedSlices[1].currentRequests)
	assert.Equal(t, 2, mss.currentAllRequests)
}

var mockMicroserviceScheduler = &microserviceScheduler{}

type mockMicroserviceScaler struct {
}

func (m *mockMicroserviceScaler) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option queue.BackPressuredOption) {
}

func (m *mockMicroserviceScaler) CalculateScaleDown(currentRequests int, requestLatencyMs int) int {
	scaleNum := len(mockMicroserviceScheduler.sortedSlices) - currentRequests
	if scaleNum < 0 {
		return 0
	}
	return scaleNum
}

func (m *mockMicroserviceScaler) ScaleDown(workerID string) {
}

func (m *mockMicroserviceScaler) SetFuncMeta(metadata servicediscovery.FunctionMetaInfo) {
}

func (m *mockMicroserviceScaler) UpdateUsedWorkerNum(num int) {}

func (m *mockMicroserviceScaler) Reset() {}

func (m *mockMicroserviceScaler) CheckUsedWorker() bool {
	return false
}

func TestMicroserviceSchedulerCheckScaleDown(t *testing.T) {
	keepaliveTime := 10
	oldIdleTime := internal.IdleTimeForCaeWorker
	internal.IdleTimeForCaeWorker = time.Duration(keepaliveTime) * time.Second
	defer func() {
		internal.IdleTimeForCaeWorker = oldIdleTime
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
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0},
		},
		{
			name:        "case4: normal case, more instance, one request per instance",
			newIns:      []int{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0},
		},
		{
			name:        "case5: scale up 2 instance with one request, should scale them all down",
			newIns:      []int{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 0},
		},
		{
			name:        "case6: scale up 2 instance with one request, should scale them all down",
			newIns:      []int{1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0},
		},
		{
			name:        "case7: request stop when scale down, should not scale down immediately",
			newIns:      []int{10, 00, 00, 00, 00, 00, 00, 00, 00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{05, 05, 05, 05, 05, 05, 05, 05, 05, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{10, 10, 10, 10, 10, 10, 10, 10, 10, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 0},
		},
		{
			name:        "case8: request goes up, should scale down less",
			newIns:      []int{10, 00, 00, 00, 00, 00, 00, 00, 00, 0},
			requests:    []int{05, 05, 05, 05, 05, 05, 05, 05, 06, 6},
			expectedIns: []int{10, 10, 10, 10, 10, 10, 10, 10, 10, 6},
		},
		{
			name:        "case9: long run request",
			newIns:      []int{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			requests:    []int{1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			expectedIns: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0},
		},
	}
	patches := mockUtils.InitPatchSlice()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockMicroserviceScheduler = &microserviceScheduler{
				autoScaler: &mockMicroserviceScaler{},
				needIns:    commonQueue.NewMonotonicQueue(10),
				scaleNum:   commonQueue.NewMonotonicQueue(10),
			}

			// assume that the scheduler has been running for a while
			for i := 0; i < 20; i++ {
				mockMicroserviceScheduler.scaleNum.Push(0)
				mockMicroserviceScheduler.needIns.Push(0)
			}

			for i := range tt.requests {
				for j := 0; j < tt.newIns[i]; j++ {
					mockMicroserviceScheduler.sortedSlices = append(mockMicroserviceScheduler.sortedSlices, scheduleEntry{})
				}
				mockMicroserviceScheduler.currentAllRequests = tt.requests[i]
				if mockMicroserviceScheduler.currentAllRequests > mockMicroserviceScheduler.scaleWindow.maxRequests {
					mockMicroserviceScheduler.scaleWindow.maxRequests = mockMicroserviceScheduler.currentAllRequests
				}
				mockMicroserviceScheduler.checkScaleDown()

				assert.Equal(t, tt.expectedIns[i], len(mockMicroserviceScheduler.sortedSlices), i)

			}
		})
	}
	patches.ResetAll()
}
