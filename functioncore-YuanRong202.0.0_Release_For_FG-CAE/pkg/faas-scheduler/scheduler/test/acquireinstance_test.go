/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/scheduler/funcmetamanager"
	"functioncore/pkg/faas-scheduler/scheduler/insscheduler"
	"functioncore/pkg/faas-scheduler/scheduler/schedulermanager"
	"functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/faas-scheduler/scheduler/test/testsuit"
	"functioncore/pkg/faas-scheduler/types"
	"functioncore/pkg/faas-scheduler/utils"
	mockUtils "functioncore/test/utils"
)

// condition:function concurrency 1, instance num 1
// 1.acquire 1 processor, expect success
// 2.acquire 1 processor, expect NoIdleInstanceErrorCode
func TestAcquireInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)

		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)
	})
}

// condition:function concurrency 10, instance num 10
// 1.Concurrent acquire 100 processor, expect success, and processor is unique
func TestConcurrentAcquireInstanceWith1v10(t *testing.T) {
	convey.Convey("TestConcurrentAcquireInstanceWith1v10", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		funcMeta := *testsuit.TestFuncMeta
		funcMeta.InstanceMetaData.ConcurrentNum = 10
		service.UpdateFuncMeta(testsuit.TestFuncKey, &funcMeta)

		suit.ConcurrentAddInstance(service, testsuit.TestFuncKeyWithRes, 10)

		res := suit.ConcurrentAcquireInstance(service, testsuit.TestAcquireInstanceReq, 100)
		acquireCnt := suit.CountUniqueProcessor(res)
		convey.So(acquireCnt, convey.ShouldEqual, 100)
	})
}

// condition:no function metadata and function instance
// 1.acquire 1 instance, expect CreateSchedulerErrorCode
// 2.release 1 instance, expect CreateSchedulerErrorCode
// 3.retain 1 instance, expect CreateSchedulerErrorCode
func TestAcquireInstanceWithNoFuncMeta(t *testing.T) {
	convey.Convey("TestAcquireInstanceWithNoFuncMeta", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(suit.GetFuncMetaDatabase().FetchFuncMeta)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.CreateSchedulerErrorCode)

		err = service.ReleaseInstance(testsuit.TestReleaseInstanceReq)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.CreateSchedulerErrorCode)

		retainRsp, err := service.RetainInstance(testsuit.TestRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(retainRsp.Error.ErrorCode, convey.ShouldEqual, snerror.CreateSchedulerErrorCode)
	})
}

// condition:function concurrency 1
// 1.add normal instance A,
// 2.add reserved instance B
// 3.acquire 1 processor, expect reserved instance B
// 4.acquire 1 processor, expect reserved instance A
func TestAcquireInstanceWithPriority(t *testing.T) {
	convey.Convey("TestAcquireInstanceWithNoFuncMeta", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)

		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestNormalWorkerInstance)
		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireNormalInstanceRsp), convey.ShouldBeTrue)
	})
}

func TestAcquireIdleWebSocketInstance(t *testing.T) {
	mockFunc := func(podIP string, podPort string,
		traceID string, reqBody []byte, scalingOutTimeout time.Duration) error {
		return nil
	}
	suit := testsuit.CreateTestSuit()
	suit.Setup()
	mockService, _ := service.CreateService(nil)
	mockService.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestWebSocketFuncMeta)
	type fields struct {
		req           *schedulerapi.AcquireInstanceReq
		addedInstance *types.WorkerInstance
	}
	type expectedType struct {
		err     error
		errCode uint32
	}
	tests := []struct {
		name        string
		fields      fields
		expected    expectedType
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case 1 acquire normal websocket instance stop existing instance suiciding",
			fields{
				req:           testsuit.TestWebSocketAcquireInstanceReq,
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
					gomonkey.ApplyFunc(utils.SendBuildWebSocketConRequestToWorker, mockFunc),
				})
				return patches
			},
		}, {"case 2 acquire reserved websocket instance",
			fields{
				req:           testsuit.TestWebSocketAcquireInstanceReq,
				addedInstance: testsuit.TestReservedWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
					gomonkey.ApplyFunc(utils.SendBuildWebSocketConRequestToWorker, mockFunc),
				})
				return patches
			},
		}, {"case 3 no scheduler can be found",
			fields{
				req:           testsuit.TestWebSocketAcquireInstanceReq,
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: snerror.CreateSchedulerErrorCode},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*schedulermanager.Manager).GetScheduler, func(
						_ *schedulermanager.Manager, funcKeyWithRes string) *insscheduler.Scheduler {
						return nil
					}),
					gomonkey.ApplyFunc((*funcmetamanager.Manager).GetFuncMeta, func(
						_ *funcmetamanager.Manager, funcKeyWithRes string) (*types.FunctionMetaInfo, error) {
						return nil, errors.New("funcMetaFetcher is nil")
					}),
				})
				return patches
			},
		}, {"case 4 with bad timeout format then use default timeout setting",
			fields{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        testsuit.TraceID,
					FuncKeyWithRes: testsuit.TestFuncKeyWithRes,
					ReqType:        schedulerapi.WebSocketRequest,
					Timeout:        "sss",
				},
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
					gomonkey.ApplyFunc(utils.SendBuildWebSocketConRequestToWorker, mockFunc),
				})
				return patches
			},
		}, {"case 5 when target instance property existing, should send build ws connection request to worker",
			fields{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        testsuit.TraceID,
					FuncKeyWithRes: testsuit.TestFuncKeyWithRes,
					ReqType:        schedulerapi.WebSocketRequest,
					Timeout:        "sss",
					WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{
						TargetIP:   "testIP",
						TargetPort: "testPort",
					},
				},
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.PrepareBuildWebSocketConRequestBody, func(
						req *schedulerapi.AcquireInstanceReq) ([]byte, error) {
						return []byte("mock body"), nil
					}),
					gomonkey.ApplyFunc(utils.SendBuildWebSocketConRequestToWorker, func(podIP string, podPort string,
						traceID string, reqBody []byte, scalingOutTimeout time.Duration) error {
						return nil
					}),
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
					gomonkey.ApplyFunc(utils.SendBuildWebSocketConRequestToWorker, mockFunc),
				})
				return patches
			},
		}, {"case 6 failed to send build ws connection request to worker when failed to prepare request body",
			fields{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        testsuit.TraceID,
					FuncKeyWithRes: testsuit.TestFuncKeyWithRes,
					ReqType:        schedulerapi.WebSocketRequest,
					Timeout:        "sss",
					WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{
						TargetIP:   "testIP",
						TargetPort: "testPort",
					},
				},
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: 170009},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.PrepareBuildWebSocketConRequestBody, func(
						req *schedulerapi.AcquireInstanceReq) ([]byte, error) {
						return nil, errors.New("mock err")
					}),
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
				})
				return patches
			},
		}, {"case 7 fail to send build ws connection request to worker when scheduler failed to release processor",
			fields{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        testsuit.TraceID,
					FuncKeyWithRes: testsuit.TestFuncKeyWithRes,
					ReqType:        schedulerapi.WebSocketRequest,
					Timeout:        "60s",
					WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{
						TargetIP:   "testIP",
						TargetPort: "testPort",
					},
				},
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: nil, errCode: 170009},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.PrepareBuildWebSocketConRequestBody, func(
						req *schedulerapi.AcquireInstanceReq) ([]byte, error) {
						return []byte("mock body"), nil
					}),
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
					gomonkey.ApplyFunc(utils.SendBuildWebSocketConRequestToWorker, func(podIP string, podPort string,
						traceID string, reqBody []byte, scalingOutTimeout time.Duration) error {
						return errors.New("mock error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			mockService.AddInstance(testsuit.TestFuncKeyWithRes, tt.fields.addedInstance)
			rsp, err := mockService.AcquireInstance(tt.fields.req)
			if tt.expected.errCode != rsp.Error.ErrorCode {
				t.Errorf("case name: %s actual %d, want %d", tt.name, rsp.Error.ErrorCode, tt.expected.errCode)
			}
			if err != tt.expected.err && tt.expected.err.Error() != err.Error() {
				t.Errorf("case name: %s actual %s, want %s", tt.name, err.Error(), tt.expected.err.Error())
			}
			mockService.DeleteInstance(testsuit.TestFuncKeyWithRes, tt.fields.addedInstance)
			time.Sleep(500 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestAcquireNoIdleWebSocketInstance(t *testing.T) {
	suit := testsuit.CreateTestSuit()
	suit.Setup()
	mockService, _ := service.CreateService(nil)
	mockService.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestWebSocketFuncMeta)
	scaleOutMockFunc := func(req *schedulerapi.AcquireInstanceReq) error {
		// simulating scaling out latency
		time.Sleep(time.Millisecond * 500)
		mockService.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestNormalWebSocketWorkerInstance)
		return nil
	}
	type fields struct {
		req *schedulerapi.AcquireInstanceReq
	}
	type expectedType struct {
		err     error
		errCode uint32
	}
	tests := []struct {
		name        string
		fields      fields
		expected    expectedType
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case 1 acquire websocket instance when no idle instance to use",
			fields{req: testsuit.TestWebSocketAcquireInstanceReq},
			expectedType{err: nil, errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.ScaleOutInstance, scaleOutMockFunc),
				})
				return patches
			},
		}, {"case 2 acquire websocket instance when no idle instance to use and scale out timeout",
			fields{req: testsuit.TestWebSocketAcquireInstanceReq},
			expectedType{err: nil, errCode: snerror.ScalingOutInstanceError},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.ScaleOutInstance, func(req *schedulerapi.AcquireInstanceReq) error {
						return nil
					}),
					gomonkey.ApplyFunc(time.After, func(d time.Duration) <-chan time.Time {
						return time.NewTimer(0).C
					}),
				})
				return patches
			},
		}, {"case 3 acquire websocket instance when internal strategy returned err",
			fields{req: testsuit.TestWebSocketAcquireInstanceReq},
			expectedType{err: nil, errCode: snerror.SchedulerInternalErrorCode},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.ScaleOutInstance, scaleOutMockFunc),
					gomonkey.ApplyFunc((*insscheduler.Scheduler).AcquireInstance, func(_ *insscheduler.Scheduler,
						req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			rsp, err := mockService.AcquireInstance(tt.fields.req)
			if tt.expected.errCode != rsp.Error.ErrorCode {
				t.Errorf("case name: %s actual %d, want %d", tt.name, rsp.Error.ErrorCode, tt.expected.errCode)
			}
			if err != tt.expected.err && tt.expected.err.Error() != err.Error() {
				t.Errorf("case name: %s actual %s, want %s", tt.name, err.Error(), tt.expected.err.Error())
			}
			patches.ResetAll()
		})
	}
}

func TestAcquireNoIdleWebSocketInstanceWithConcurrencyMoreThanOne(t *testing.T) {
	suit := testsuit.CreateTestSuit()
	suit.Setup()
	mockService, _ := service.CreateService(nil)
	mockService.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestWebSocketFuncMetaWithConcurrencyMoreThanOne)
	scaleOutMockFunc := func(req *schedulerapi.AcquireInstanceReq) error {
		// simulating scaling out latency
		time.Sleep(time.Millisecond * 500)
		mockService.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestNormalWebSocketWorkerInstance)
		return nil
	}
	type fields struct {
		req *schedulerapi.AcquireInstanceReq
	}
	type expectedType struct {
		err     error
		errCode uint32
	}
	tests := []struct {
		name        string
		fields      fields
		expected    expectedType
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case 1 acquire websocket instance when no idle instance to use",
			fields{req: testsuit.TestWebSocketAcquireInstanceReq},
			expectedType{err: nil, errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.ScaleOutInstance, scaleOutMockFunc),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			rsp, err := mockService.AcquireInstance(tt.fields.req)
			if tt.expected.errCode != rsp.Error.ErrorCode {
				t.Errorf("case name: %s actual %d, want %d", tt.name, rsp.Error.ErrorCode, tt.expected.errCode)
			}
			if tt.expected.err != err && tt.expected.err.Error() != err.Error() {
				t.Errorf("case name: %s actual %s, want %s", tt.name, err.Error(), tt.expected.err.Error())
			}
			patches.ResetAll()
		})
	}
}
