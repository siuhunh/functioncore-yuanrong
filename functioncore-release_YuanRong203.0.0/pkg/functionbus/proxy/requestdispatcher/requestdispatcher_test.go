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
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/queue"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxy/workersview"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	testutils "functioncore/test/utils"
)

const (
	instance                = "worker01"
	stateId                 = "state01"
	funcKey                 = "TenantID01/function01/latest"
	cpu                     = "300"
	memory                  = "128"
	functionKeyWithResource = "TenantID01/function01/latest/300-128"
	localIP                 = "127.0.0.1"
	remoteIP                = "127.0.0.2"
	retryTimes              = 10
)

var (
	patches           testutils.PatchSlice
	backupSendCallReq func(nodeID, traceID string, callReq *types.TCPRequest, dst *common.FunctionProperty) *types.TCPResponse
)

type fakeScheduler struct {
}

func (f *fakeScheduler) Update(metrics *workerevaluator.WorkerMetrics) {
}

func (f *fakeScheduler) Schedule(_ *types.TCPRequest, traceID string) string {
	return ""
}

func (f *fakeScheduler) SetFunctionMetadata(metadata servicediscovery.FunctionMetaInfo) {
}

func (f *fakeScheduler) UpdateWorkerWillDeleteStatus(info *willDeleteInfo) {
}

func (f *fakeScheduler) SetKeepaliveTime(kaTime int) {
}

func (f *fakeScheduler) GetSchedulerType() string {
	return constants.BusinessTypeFaaS
}

func (f *fakeScheduler) PreSchedule(workerID string) {
	return
}

func (f *fakeScheduler) ScheduleAll() []string {
	return []string{}
}

// SetDispatcher -
func (f *fakeScheduler) SetDispatcher(dispatcher *RequestDispatcher) {

}

func TestRequestLocalAndRemoteFailed(t *testing.T) {
	os.Setenv("QueueType", "fifo")
	meta := servicediscovery.FunctionMetaInfo{}
	meta.InstanceMetaData = servicediscovery.InstanceMetaData{
		ConcurrentNum: 100,
		IdleMode:      false,
	}
	dispatcher := CreateRequestDispatcher("", meta, false)
	dispatcher.scaler = &fakeAutoScaler{}
	dispatcher.scheduler = &fakeScheduler{}
	dispatcher.Start()

	request := &types.DispatcherRequest{
		Response: make(chan *types.TCPResponse),
		Request:  &types.TCPRequest{},
	}
	// schedule local failed and fakeAutoscaler return ok
	dispatcher.Request(request)
	if dispatcher.requestQueue.Len() != 1 {
		t.Error("schedule failed and queue length 1")
	}

	// sleep some times for requestQueue to call "handleFront"
	time.Sleep(3 * time.Second)

	dispatcher.requestQueue.Pop(1)
	if dispatcher.requestQueue.Len() != 0 {
		t.Error("after pop from queue, queue length 0")
	}

	dispatcher.Stop()
	t.Log("Stop dispatcher success")
}

func TestMain(m *testing.M) {
	data := "{\"httpsConfig\":{\"httpsEnable\":true},\"localAuth\":{\"localAuthEnable\":true}}"
	err := json.Unmarshal([]byte(data), common.FConfig)
	if err != nil {
		return
	}

	stopChan := make(chan struct{}, 1)
	workerevaluator.CreateInstance(stopChan)
	ec := m.Run()
	close(stopChan)
	os.Exit(ec)
}

func buildTestRequest() *types.DispatcherRequest {
	return &types.DispatcherRequest{
		FunctionKey: funcKey,
		Request: &types.TCPRequest{
			CPU:    cpu,
			Memory: memory,
		},
		Response: make(chan *types.TCPResponse, 1),
	}
}

func setup() {
	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(workersview.ChooseRemoteNode, func(str string, _ map[string]struct{}) interface{} {
			if str == "abc/500-500" {
				return 123
			}
			return "127.0.0.2"
		}),
	})

	backupSendCallReq = common.SendCallReq
	common.SendCallReq = mockSendCallReq
}

func tearDown() {
	patches.ResetAll()
	common.SendCallReq = backupSendCallReq
}

func mockSendCallReq(_, _ string, _ *types.TCPRequest, _ *common.FunctionProperty) *types.TCPResponse {
	return &types.TCPResponse{
		RequestID:    1,
		ErrorCode:    proxyerrors.StateOKCode,
		ErrorMessage: "",
	}
}

func TestScheduleWithForward(t *testing.T) {
	convey.Convey("TestScheduleWithForward", t, func() {
		setup()
		defer func() {
			tearDown()
		}()
		meta := servicediscovery.FunctionMetaInfo{}
		meta.InstanceMetaData = servicediscovery.InstanceMetaData{
			ConcurrentNum: 100,
			IdleMode:      false,
		}
		dispatcher := CreateRequestDispatcher(functionKeyWithResource, meta, false)
		req := buildTestRequest()
		convey.So(dispatcher.schedule(req), convey.ShouldBeNil)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
	})
}

func TestOnObserved(t *testing.T) {
	convey.Convey("TestOnObserved", t, func() {
		setup()
		defer tearDown()
		requestQueue := &queue.RequestQueue{}
		rd := &RequestDispatcher{
			scheduler:    &fakeScheduler{},
			requestQueue: requestQueue,
		}
		rd.OnObserved("err", false)
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(requestQueue), "ScheduleWorkerChanged",
				func(_ *queue.RequestQueue, remote bool) {
					return
				}),
		})
		metrics := &workerevaluator.WorkerMetrics{}
		rd.OnObserved(metrics, false)

	})
}

func TestDispatch_CAEScheduleRequestErrCode(t *testing.T) {
	convey.Convey("TestDispatch_CAEScheduleRequestErrCode", t, func() {
		setup()
		defer tearDown()
		requestQueue := &queue.RequestQueue{}
		rd := &RequestDispatcher{
			scheduler:    &microserviceScheduler{},
			requestQueue: requestQueue,
		}
		response := make(chan *types.TCPResponse, 1)
		request := &types.DispatcherRequest{
			FunctionKey: "abc",
			Request: &types.TCPRequest{
				CPU:           "500",
				Memory:        "500",
				DirectRequest: true,
			},
			Response: response,
		}
		rd.dispatch(request)
		rsp := <-request.Response
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.CAEScheduleRequestErrCode)
		convey.So(rsp.ErrorMessage, convey.ShouldEqual, proxyerrors.CAEScheduleRequestErrMsg)
	})
	convey.Convey("TestDispatch_SpecificInstanceID", t, func() {
		setup()
		defer tearDown()
		requestQueue := &queue.RequestQueue{}
		rd := &RequestDispatcher{
			scheduler:    &microserviceScheduler{},
			requestQueue: requestQueue,
		}
		response := make(chan *types.TCPResponse, 1)
		request := &types.DispatcherRequest{
			FunctionKey: "abc",
			Request: &types.TCPRequest{
				CPU:           "500",
				Memory:        "500",
				DirectRequest: true,
				InstanceID:    "instanceID",
			},
			Response: response,
		}
		err := rd.dispatch(request)
		convey.So(strings.Contains(err.Error(), "schedule current node worker"), convey.ShouldBeTrue)
	})
}

func TestSetCon(t *testing.T) {
	convey.Convey("TestSetCon", t, func() {
		rd := &RequestDispatcher{
			scheduler: &fakeScheduler{},
			scaler:    &fakeAutoScaler{},
		}
		rd.SetInstanceMetadata(servicediscovery.FunctionMetaInfo{
			InstanceMetaData: servicediscovery.InstanceMetaData{ConcurrentNum: -1},
		})
		convey.So(rd.concurrency, convey.ShouldEqual, defaultConcurrency)
	})
}

func TestDispatcherSchedule(t *testing.T) {
	convey.Convey("TestDispatcherSchedule", t, func() {
		convey.Convey("test faas", func() {
			setup()
			defer tearDown()
			scheduler := &fakeScheduler{}
			rd := &RequestDispatcher{
				scheduler: scheduler,
			}
			request := &types.DispatcherRequest{
				Canceled: true,
				Response: make(chan *types.TCPResponse, 1),
			}
			err := rd.schedule(request)
			response, _ := <-request.Response
			convey.So(response.ErrorCode, convey.ShouldEqual, proxyerrors.RequestCanceledCode)
			convey.So(err, convey.ShouldBeNil)

			manager := workerbroker.GetInstance()
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "Schedule", func(_ *fakeScheduler, _ *types.TCPRequest) string {
					return "id"
				}),
			})
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(manager), "Process", func(_ *workerbroker.Manager, workerID string, request *types.DispatcherRequest) {
					return
				}),
			})
			request.Canceled = false
			request.Request = &types.TCPRequest{
				DirectRequest:   true,
				CanBackpressure: true,
			}
			rd.concurrency = 0
			err = rd.schedule(request)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("test CAE", func() {
			setup()
			defer tearDown()
			scheduler := &microserviceScheduler{}
			remoteInstances := map[string]string{"worker1": "192.168.1.1", "worker2": "192.168.1.2"}

			rd := &RequestDispatcher{
				scheduler:           scheduler,
				funcKeyWithResource: "test-func-key",
				concurrency:         uint32(2),
			}
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyFunc(atomic.LoadUint32, func(addr *uint32) uint32 {
					return *addr
				}),
				gomonkey.ApplyFunc(workersview.GetAllRemoteInstances, func(funcKey string) map[string]string {
					return remoteInstances
				}),
				gomonkey.ApplyFunc(common.SendCallReq, func(nodeID, traceID string, callReq *types.TCPRequest,
					dst *common.FunctionProperty) *types.TCPResponse {
					return &types.TCPResponse{}
				}),
			})

			request := &types.DispatcherRequest{
				Canceled: false,
				Request: &types.TCPRequest{
					IsInvokeAll: true,
				},
				Response:    make(chan *types.TCPResponse, 1),
				FutureID:    "123",
				TraceID:     "trace-123",
				FunctionKey: "test-func-key",
				Position:    types.Position{},
			}

			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&workerbroker.Manager{}), "Process",
					func(_ *workerbroker.Manager, workerID string, request *types.DispatcherRequest) {
						return
					}),
			})

			rd.schedule(request)
			time.Sleep(50 * time.Millisecond)

			select {
			case resp := <-request.Response:
				assert.NotNil(t, resp)
			default:
				t.Fatal("Expected a response in the response channel")
			}
		})
	})
}

func TestForward(t *testing.T) {
	convey.Convey("TestForward", t, func() {
		setup()
		defer tearDown()
		rd := &RequestDispatcher{
			scheduler: &fakeScheduler{},
		}
		request := &types.DispatcherRequest{
			FunctionKey: "abc",
			Request: &types.TCPRequest{
				CPU:    "500",
				Memory: "500",
			},
			Response: make(chan *types.TCPResponse, 1),
		}
		err := rd.forward(request)
		convey.So(err, convey.ShouldBeError)
	})
}

func TestRequestDispatcher_StopWhenTerminate(t *testing.T) {
	convey.Convey("StopWhenTerminate_ReturnError", t, func() {
		meta := servicediscovery.FunctionMetaInfo{}
		meta.InstanceMetaData = servicediscovery.InstanceMetaData{
			ConcurrentNum: 10,
			IdleMode:      false,
		}
		//newQueue := queue.NewRequestQueue("", internal.RequestQueueTimeout, stopCh1)
		request := &types.DispatcherRequest{
			Response: make(chan *types.TCPResponse, 1),
			Request:  &types.TCPRequest{},
		}
		dispatcher := CreateRequestDispatcher(functionKey, meta, false)
		dispatcher.Start()
		dispatcher.Request(request)
		dispatcher.StopWhenTerminate()
		res := <-request.Response
		convey.So(res.ErrorCode, convey.ShouldEqual, proxyerrors.IsUpgradingCode)
	})
}

func TestRequestDispatcher_GetRequestQueueLen(t *testing.T) {
	convey.Convey("GetRequestQueueLen will return 0", t, func() {
		stopCh := make(chan struct{})
		requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
		rd := &RequestDispatcher{
			scheduler:    &fakeScheduler{},
			requestQueue: requestQueue,
		}
		number := rd.GetRequestQueueLen()
		convey.So(number, convey.ShouldEqual, number)
	})
}

func TestRequestDispatcher_UpdateWorkerWillDeleteStatus(t *testing.T) {
	convey.Convey("scheduler is not nil", t, func() {
		stopCh := make(chan struct{})
		requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
		rd := &RequestDispatcher{
			scheduler:    &fakeScheduler{},
			requestQueue: requestQueue,
		}
		rd.UpdateWorkerWillDeleteStatus(&willDeleteInfo{
			"aaaa",
			"172.28.0.80",
			true,
		})
	})

	convey.Convey("scheduler is nil", t, func() {
		stopCh := make(chan struct{})
		requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
		rd := &RequestDispatcher{
			requestQueue: requestQueue,
		}
		rd.UpdateWorkerWillDeleteStatus(&willDeleteInfo{
			"aaaa",
			"172.28.0.80",
			true,
		})
	})
}

func TestRequestDispatcher_forwardCAE(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(workersview.ChooseRemoteInstanceSkipUsed, func(_ string, used map[string]struct{}) (string, string) {
			if used == nil {
				return "pod1", "node1"
			}
			return "", ""
		}),
	}

	oldCall := common.SendCallReq
	common.SendCallReq = func(nodeID, traceID string, callReq *types.TCPRequest, dst *common.FunctionProperty) *types.TCPResponse {
		return &types.TCPResponse{}
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
		common.SendCallReq = oldCall
	}()

	tests := []struct {
		name         string
		usedIns      map[string]struct{}
		expectedCode uint32
	}{
		{
			name:         "forward instance success",
			usedIns:      nil,
			expectedCode: 0,
		},
		{
			name:         "no instance to forward",
			usedIns:      make(map[string]struct{}),
			expectedCode: proxyerrors.CAEScheduleRequestErrCode,
		},
	}

	dispatcher := &RequestDispatcher{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &types.DispatcherRequest{
				Request:  &types.TCPRequest{UsedInstance: tt.usedIns},
				Response: make(chan *types.TCPResponse, 1),
			}
			assert.Nil(t, dispatcher.forwardCAE(req))
			resp := <-req.Response
			assert.Equal(t, tt.expectedCode, resp.ErrorCode)
		})
	}
	autoscaler.EnableMicroserviceAutoScaler = true
	req := &types.DispatcherRequest{
		Request:  &types.TCPRequest{UsedInstance: make(map[string]struct{})},
		Response: make(chan *types.TCPResponse, 1),
	}
	err := dispatcher.forwardCAE(req)
	assert.NotNil(t, err)
	assert.Equal(t, errScheduleFailed, err)
}

// TestDispatchAll tests the dispatchAll method of the RequestDispatcher.
func TestDispatchAll(t *testing.T) {
	setup()
	defer tearDown()
	count := 0
	scheduler := &microserviceScheduler{}
	rd := &RequestDispatcher{
		scheduler:           scheduler,
		funcKeyWithResource: "test-func-key",
		concurrency:         uint32(2),
	}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(atomic.LoadUint32, func(addr *uint32) uint32 {
			return *addr
		}),
		gomonkey.ApplyFunc((*microserviceScheduler).ScheduleAll, func(_ *microserviceScheduler) []string {
			return []string{"worker1", "worker2"}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&workerbroker.Manager{}), "Process",
			func(_ *workerbroker.Manager, workerID string, request *types.DispatcherRequest) {
				count++
				return
			}),
	})

	request := &types.DispatcherRequest{
		Canceled: false,
		Request: &types.TCPRequest{
			IsInvokeAll: true,
		},
		Response:    make(chan *types.TCPResponse, 1),
		FutureID:    "123",
		TraceID:     "trace-123",
		FunctionKey: "test-func-key",
		Position:    types.Position{},
	}

	rd.dispatchAll(request)
	time.Sleep(30 * time.Millisecond)

	assert.Equal(t, len(scheduler.ScheduleAll()), count)
}

func TestRequestDispatcher_Stop(t *testing.T) {
	var wg sync.WaitGroup
	meta := servicediscovery.FunctionMetaInfo{}
	meta.InstanceMetaData = servicediscovery.InstanceMetaData{
		ConcurrentNum: 100,
		IdleMode:      false,
	}
	dispatcher := CreateRequestDispatcher("", meta, false)
	if dispatcher.requestQueue.Len() != 0 {
		t.Error("before pop from queue, queue length 0")
	}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			dispatcher.Stop()
		}(i)
	}
	wg.Wait()

	if dispatcher.requestQueue.Len() != 0 {
		t.Error("after pop from queue, queue length 0")
	}
}
