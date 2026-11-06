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

// Package queue imply multiple queues
package queue

import (
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/dataaggregation"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
)

func mkReq(id string) *types.DispatcherRequest {
	return &types.DispatcherRequest{Request: &types.TCPRequest{}, TraceID: id}
}

type mockLengthObserver struct{}

func (m *mockLengthObserver) QueueLengthChanged(length, rps int, info common.TraceInfo,
	queuedTime time.Duration, option BackPressuredOption) {
}

func (m *mockLengthObserver) Reset() {}

func (f *mockLengthObserver) CheckUsedWorker() bool {
	return false
}

func unuse(request *types.DispatcherRequest) error {
	return errors.New("")
}

func use(request *types.DispatcherRequest) error {
	return nil
}

func TestDefaultQueue_Clear(t *testing.T) {
	stopCh := make(<-chan struct{})
	f := NewRequestQueue("fifo", time.Second, stopCh)
	f.RegisterHandler(use)
	f.Push(mkReq("1"))
	f.Push(mkReq("2"))
	time.Sleep(100 * time.Millisecond)
	if requests := f.Pop(1); len(requests) != 0 {
		t.Fatalf("failed to handle queue")
	}
	f.queue.Push(123)
	if requests := f.Pop(200001); len(requests) != 0 {
		t.Fatalf("failed to handle queue:%d", len(requests))
	}
}

func TestDefaultQueue_ScaleErr(t *testing.T) {
	stopCh := make(<-chan struct{})
	f := NewRequestQueue("fifo", time.Nanosecond, stopCh)
	f.RegisterHandler(unuse)
	f.ObserveLength(&mockLengthObserver{})
	if f.scaleError != nil {
		t.Fatalf("failed to assert burst false after initialized")
	}
	f.scaleError = &types.TCPResponse{ErrorCode: 150500}
	req := &types.DispatcherRequest{Request: &types.TCPRequest{}, Response: make(chan *types.TCPResponse, 1)}
	f.Push(req)
	resp := <-req.Response
	// Temporary solution: The system does not distinguish whether the cluster is overloaded or the pod
	// obtaining is slow. it determines return overload error based on the request queuing timeout.
	if resp.ErrorCode != snerror.ClusterOverloadCode {
		t.Errorf("failed to test request queued timeout and scale failed")
	}

	// ReachMaxInstances error should be passed to user
	f.scaleError = &types.TCPResponse{ErrorCode: snerror.ReachMaxInstancesCode}
	req = &types.DispatcherRequest{Request: &types.TCPRequest{}, Response: make(chan *types.TCPResponse, 1)}
	f.Push(req)
	resp = <-req.Response
	if resp.ErrorCode != snerror.ReachMaxInstancesCode {
		t.Errorf("failed to test request queued timeout and ReachMaxInstances error")
	}
}

func TestPopN(t *testing.T) {
	convey.Convey("TestPopN", t, func() {
		f := NewRequestQueue(Priority, time.Nanosecond, nil)
		f.ScheduleWorkerChanged(true)
		f.ScheduleWorkerChanged(false)
		requests := f.PopN(1, 200001)
		convey.So(requests, convey.ShouldBeEmpty)

		request := &types.DispatcherRequest{
			Request: &types.TCPRequest{
				CanBackpressure: true,
				Priority:        1,
			},
		}
		f.Push(request)
		request.Request.CanBackpressure = false
		f.Push(request)
		requests = f.PopN(-1, 5)
		convey.So(len(requests), convey.ShouldEqual, 2)

		f.queue.Push(1223)
		requests = f.PopN(1, 5)
		convey.So(requests, convey.ShouldBeEmpty)

		f = NewRequestQueue(FIFO, time.Nanosecond, nil)
		f.queue.Push(1223)
		requests = f.PopN(1, 5)
		convey.So(requests, convey.ShouldBeEmpty)
	})
}

func TestQueueWithBackPressure_PopBackPressureReq(t *testing.T) {
	mkFullReq := func() *types.DispatcherRequest {
		return &types.DispatcherRequest{
			Request: &types.TCPRequest{
				CanBackpressure: true,
			},
			Response:    make(chan *types.TCPResponse, 1),
			Position:    types.Position{},
			FunctionKey: "",
			TraceID:     "",
			NodeLabel:   "",
			FutureID:    "",
			Canceled:    false,
			WorkerID:    "",
		}
	}
	stopCh := make(<-chan struct{})
	f := NewRequestQueue("fifo", time.Second, stopCh)
	f.RegisterHandler(unuse)
	req1 := mkFullReq()
	req2 := mkFullReq()
	f.Push(req1)
	f.Push(req2)
	resp := <-req1.Response
	if resp.ErrorCode != constants.BackpressureCode {
		t.Errorf("failed to test back pressure by scheduled failed")
	}
	resp = <-req2.Response
	if resp.ErrorCode != constants.BackpressureCode {
		t.Errorf("failed to test back pressure by scheduled failed")
	}
}

func TestHandleFunctionDisable(t *testing.T) {
	convey.Convey("TestHandleFunctionDisable", t, func() {
		stopCh := make(<-chan struct{})
		f := NewRequestQueue("fifo", time.Nanosecond, stopCh)
		f.RegisterHandler(unuse)
		f.ObserveLength(&mockLengthObserver{})
		f.scaleError = &types.TCPResponse{ErrorCode: snerror.FunctionIsDisabled}
		req := &types.DispatcherRequest{Request: &types.TCPRequest{}, Response: make(chan *types.TCPResponse, 1)}

		f.Push(req)
		resp := <-req.Response
		convey.So(resp.ErrorCode, convey.ShouldEqual, snerror.FunctionIsDisabled)
	})
}

func Test_checkBackPressured(t *testing.T) {
	tests := []struct {
		name     string
		request  types.DispatcherRequest
		expected bool
	}{
		{
			name:     "normal request from frontend",
			request:  types.DispatcherRequest{Request: &types.TCPRequest{}},
			expected: false,
		},
		{
			name:     "backpressure request from frontend",
			request:  types.DispatcherRequest{Request: &types.TCPRequest{BackpressureNodeID: "task-xxx"}},
			expected: true,
		},
		{
			name:     "request from other proxy",
			request:  types.DispatcherRequest{Request: &types.TCPRequest{}, Position: types.Position{InstanceID: common.ProxyReqPrefix + "xxx"}},
			expected: false,
		},
		{
			name:     "backpressure request from other proxy",
			request:  types.DispatcherRequest{Request: &types.TCPRequest{}, Position: types.Position{InstanceID: common.ProxyBackPressurePrefix + "xxx"}},
			expected: true,
		},
		{
			name:     "backpressure request from same node",
			request:  types.DispatcherRequest{Request: &types.TCPRequest{}, Position: types.Position{InstanceID: localProxy}},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, checkBackPressured(&tt.request))
		})
	}
}

func TestRequestQueue_handleFunctionQueuedTimeout(t *testing.T) {
	scaleUserErr := &types.TCPResponse{
		ErrorCode:    4201,
		ErrorMessage: "mock user error",
	}
	scaleSystemErr := &types.TCPResponse{
		ErrorCode:    150500,
		ErrorMessage: "mock user error",
	}
	type fields struct {
		handler          func(*types.DispatcherRequest) error
		queue            *queueWithBackPressure
		scaleError       *types.TCPResponse
		signal           chan struct{}
		stop             <-chan struct{}
		rpsCounter       dataaggregation.DataAggregation
		observer         LengthObserver
		queueTimeout     time.Duration
		RWMutex          sync.RWMutex
		AutoScalerConfig *common.AutoScalerConfig
	}
	type args struct {
		entry      Entry
		queuedTime time.Duration
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		expectCode uint32
	}{
		{
			name: "test scale error is user error return user error",
			fields: fields{
				scaleError: scaleUserErr,
				queue: &queueWithBackPressure{
					queue:                NewQueue("mock queue"),
					canBackPressureQueue: NewQueue("mock queue"),
				},
			},
			args: args{
				entry: Entry{
					request: &types.DispatcherRequest{
						Response: make(chan *types.TCPResponse, 1),
					},
				},
				queuedTime: 1 * time.Second,
			},
			expectCode: scaleUserErr.ErrorCode,
		},
		{
			name: "test scale error is user error return user error",
			fields: fields{
				scaleError: scaleSystemErr,
				queue: &queueWithBackPressure{
					queue:                NewQueue("mock queue"),
					canBackPressureQueue: NewQueue("mock queue"),
				},
			},
			args: args{
				entry: Entry{
					request: &types.DispatcherRequest{
						Response: make(chan *types.TCPResponse, 1),
					},
				},
				queuedTime: 1 * time.Second,
			},
			expectCode: snerror.ClusterOverloadCode,
		},
		{
			name: "test session request timeout",
			fields: fields{
				scaleError: nil,
				queue: &queueWithBackPressure{
					queue:                NewQueue("mock queue"),
					canBackPressureQueue: NewQueue("mock queue"),
				},
			},
			args: args{
				entry: Entry{
					request: &types.DispatcherRequest{
						Request:  &types.TCPRequest{SessionPod: "pool22-300-128-fusion-6755d66cb9-ncwmv"},
						Response: make(chan *types.TCPResponse, 1),
					},
				},
				queuedTime: 1 * time.Second,
			},
			expectCode: snerror.ClusterOverloadCode,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &RequestQueue{
				handler:          tt.fields.handler,
				queue:            tt.fields.queue,
				scaleError:       tt.fields.scaleError,
				signal:           tt.fields.signal,
				stop:             tt.fields.stop,
				rpsCounter:       tt.fields.rpsCounter,
				observer:         tt.fields.observer,
				queueTimeout:     tt.fields.queueTimeout,
				RWMutex:          tt.fields.RWMutex,
				AutoScalerConfig: tt.fields.AutoScalerConfig,
			}
			b.handleFunctionQueuedTimeout(tt.args.entry, tt.args.queuedTime)
			response := <-tt.args.entry.request.Response
			assert.NotNil(t, response)
			assert.Equal(t, tt.expectCode, response.ErrorCode)
		})
	}
}

func TestSchedule_SessionRequest(t *testing.T) {
	convey.Convey("TestSchedule_SessionRequest_success", t, func() {
		stopCh := make(<-chan struct{})
		queue := &RequestQueue{
			signal:       make(chan struct{}, 1),
			rpsCounter:   dataaggregation.NewDataAggregation(dataaggregation.COUNT, time.Second, 1, 0),
			queueTimeout: 500 * time.Millisecond,
			handler:      mockHandler,
			observer:     nil,
			queue:        newQueueWithBackPressure("fifo"),
			scaleError:   nil,
			stop:         stopCh,
		}
		req1 := &types.DispatcherRequest{
			Request:  &types.TCPRequest{SessionPod: "pool22-300-128-fusion-6755d66cb9-ncwmv"},
			Response: make(chan *types.TCPResponse, 1),
		}
		queue.Push(req1)
		req2 := &types.DispatcherRequest{
			Request:  &types.TCPRequest{SessionPod: "pool22-300-128-fusion-6755d66cb9-ncwmv2"},
			Response: make(chan *types.TCPResponse, 1),
		}
		queue.Push(req2)
		queue.schedule()

		response := <-req2.Response
		assert.NotNil(t, response)
		assert.Equal(t, snerror.MCPSessionInstanceInvalid, int(response.ErrorCode))

		time.Sleep(500 * time.Millisecond)
		queue.schedule()
		response = <-req1.Response
		assert.Equal(t, snerror.ClusterOverloadCode, int(response.ErrorCode))
	})

	convey.Convey("TestDispatch_noSessionInstanceEnabledWorkerID", t, func() {
		stopCh := make(<-chan struct{})
		queue := &RequestQueue{
			signal:       make(chan struct{}, 1),
			rpsCounter:   dataaggregation.NewDataAggregation(dataaggregation.COUNT, time.Second, 1, 0),
			queueTimeout: 500 * time.Millisecond,
			handler:      mockHandler,
			observer:     nil,
			queue:        newQueueWithBackPressure("fifo"),
			scaleError:   nil,
			stop:         stopCh,
		}
		patch := gomonkey.ApplyMethod(reflect.TypeOf(queue.queue),
			"Pop", func(mq *queueWithBackPressure) (interface{}, error) {
				return "mocked data", errors.New("pop error")
			})

		req1 := &types.DispatcherRequest{
			Request:  &types.TCPRequest{SessionPod: "pool22-300-128-fusion-6755d66cb9-ncwmv"},
			Response: make(chan *types.TCPResponse, 1),
		}
		queue.Push(req1)
		req2 := &types.DispatcherRequest{
			Request:  &types.TCPRequest{SessionPod: "pool22-300-128-fusion-6755d66cb9-ncwmv2"},
			Response: make(chan *types.TCPResponse, 1),
		}
		queue.Push(req2)
		queue.schedule()

		patch.Reset()

		time.Sleep(500 * time.Millisecond)
		queue.schedule()
		response := <-req2.Response
		assert.NotNil(t, response)
		assert.Equal(t, snerror.MCPSessionInstanceInvalid, int(response.ErrorCode))

		response = <-req1.Response
		assert.Equal(t, snerror.ClusterOverloadCode, int(response.ErrorCode))
	})

}

func mockHandler(req *types.DispatcherRequest) error {
	if req != nil && req.Request != nil && req.Request.SessionPod == "pool22-300-128-fusion-6755d66cb9-ncwmv2" {
		req.Response <- &types.TCPResponse{
			ErrorCode:    snerror.MCPSessionInstanceInvalid,
			ErrorMessage: snerror.ErrText(snerror.MCPSessionInstanceInvalid),
		}
		return nil
	}
	return errors.New("test error")
}
