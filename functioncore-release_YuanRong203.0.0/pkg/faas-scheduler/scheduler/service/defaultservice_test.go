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

// Package service contains faas scheduler domain service
package service

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/etcdadapter/etcd"
	"functioncore/pkg/faas-scheduler/scheduler/funcinsmanager"
	"functioncore/pkg/faas-scheduler/scheduler/funcmetamanager"
	"functioncore/pkg/faas-scheduler/scheduler/insscheduler"
	"functioncore/pkg/faas-scheduler/scheduler/schedulermanager"
	"functioncore/pkg/faas-scheduler/types"
	"functioncore/pkg/faas-scheduler/utils"
	testutils "functioncore/test/utils"
)

func Test_defaultService_processScalingOutTimeoutInternal(t *testing.T) {
	service := &defaultService{}
	type args struct {
		req *schedulerapi.AcquireInstanceReq
		rsp *schedulerapi.AcquireInstanceRsp
	}
	tests := []struct {
		name        string
		args        args
		expected    *schedulerapi.Error
		patchesFunc testutils.PatchesFunc
	}{
		{
			name: "case 1 when get scheduler return nil should return CreateSchedulerErrorCode",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					FuncKeyWithRes: "mockKey",
				},
				rsp: &schedulerapi.AcquireInstanceRsp{},
			},
			expected: &schedulerapi.Error{
				ErrorCode: snerror.CreateSchedulerErrorCode,
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*defaultService).getOrCreateScheduler, func(
						_ *defaultService, funcKeyWithRes string) (*insscheduler.Scheduler, error) {
						return nil, errors.New("no scheduler")
					}),
				})
				return patches
			},
		}, {
			name: "case 2 when get scheduler return nil should return CreateSchedulerErrorCode",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					FuncKeyWithRes: "mockKey",
				},
				rsp: &schedulerapi.AcquireInstanceRsp{},
			},
			expected: &schedulerapi.Error{
				ErrorCode: snerror.ScalingOutInstanceError,
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*defaultService).getOrCreateScheduler, func(
						_ *defaultService, funcKeyWithRes string) (*insscheduler.Scheduler, error) {
						return &insscheduler.Scheduler{}, nil
					}),
					gomonkey.ApplyFunc((*insscheduler.Scheduler).ReleasePlaceHolderInstanceProcessor, func(
						_ *insscheduler.Scheduler, insKey string, funcKeyWithRes string, processorID uint32) {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			service.processScalingOutTimeoutInternal(tt.args.req, tt.args.rsp)
			if tt.args.rsp.Error.ErrorCode != tt.expected.ErrorCode {
				t.Errorf("case name: %s actual %d, want %d",
					tt.name, tt.args.rsp.Error.ErrorCode, tt.expected.ErrorCode)
			}
			patches.ResetAll()
		})
	}
}

func Test_defaultService_handleSendWebSocketBuildingRequestToWorkerError(t *testing.T) {
	type args struct {
		req *schedulerapi.AcquireInstanceReq
		rsp *schedulerapi.AcquireInstanceRsp
		err error
	}
	type expected struct {
		code    uint32
		message string
	}
	tests := []struct {
		name        string
		args        args
		expected    expected
		patchesFunc testutils.PatchesFunc
	}{
		{
			name: "handle internal error",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        "mock trace id",
					FuncKeyWithRes: "mock func key with res",
				},
				rsp: &schedulerapi.AcquireInstanceRsp{
					InstanceKey: "mock instance key",
					ProcessorID: 1,
					Error:       schedulerapi.Error{},
				},
				err: errors.New("mock internal error"),
			},
			expected: expected{
				code:    170009,
				message: "failed to build websocket connection between worker and control plane",
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*schedulermanager.Manager).GetScheduler, func(
						_ *schedulermanager.Manager, funcKeyWithRes string) *insscheduler.Scheduler {
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "handle sn error",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        "mock trace id",
					FuncKeyWithRes: "mock func key with res",
				},
				rsp: &schedulerapi.AcquireInstanceRsp{
					InstanceKey: "mock instance key",
					ProcessorID: 1,
					Error:       schedulerapi.Error{},
				},
				err: snerror.New(100, "mock error message"),
			},
			expected: expected{
				code:    100,
				message: "mock error message",
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*schedulermanager.Manager).GetScheduler, func(
						_ *schedulermanager.Manager, funcKeyWithRes string) *insscheduler.Scheduler {
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "handle sn error and release function instance failed",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        "mock trace id",
					FuncKeyWithRes: "mock func key with res",
				},
				rsp: &schedulerapi.AcquireInstanceRsp{
					InstanceKey: "mock instance key",
					ProcessorID: 1,
					Error:       schedulerapi.Error{},
				},
				err: snerror.New(100, "mock error message"),
			},
			expected: expected{
				code:    100,
				message: "mock error message",
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*schedulermanager.Manager).GetScheduler, func(
						_ *schedulermanager.Manager, funcKeyWithRes string) *insscheduler.Scheduler {
						return &insscheduler.Scheduler{}
					}),
					gomonkey.ApplyFunc((*insscheduler.Scheduler).ReleaseInstance, func(_ *insscheduler.Scheduler, req *schedulerapi.ReleaseInstanceReq) error {
						return errors.New("release failed")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			service := &defaultService{}
			service.handleSendWebSocketBuildingRequestToWorkerError(tt.args.req, tt.args.rsp, tt.args.err)
			if tt.args.rsp.Error.ErrorCode != tt.expected.code {
				t.Errorf("failed to test handleSendWebSocketBuildingRequestToWorkerError, expected code: %d, "+
					"actual code: %d", tt.expected.code, tt.args.rsp.Error.ErrorCode)
			}
			if tt.args.rsp.Error.ErrorMessage != tt.expected.message {
				t.Errorf("failed to test handleSendWebSocketBuildingRequestToWorkerError, expected message: %s, "+
					"actual message: %s", tt.expected.message, tt.args.rsp.Error.ErrorMessage)
			}
			patches.ResetAll()
		})
	}
}

func Test_defaultService_acquireWebSocketInstance(t *testing.T) {
	type args struct {
		req               *schedulerapi.AcquireInstanceReq
		rsp               *schedulerapi.AcquireInstanceRsp
		scalingOutTimeout time.Duration
		wsInstanceInfo    *schedulerapi.WebsocketInsInfo
	}
	tests := []struct {
		name                 string
		args                 args
		expectedInstanceName string
		expectedErrorCode    uint32
		patchesFunc          testutils.PatchesFunc
	}{
		{
			name: "return normal websocket function instance",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{},
				rsp: &schedulerapi.AcquireInstanceRsp{
					PlaceHolderChan: make(chan *schedulerapi.WebsocketInsInfo, 10),
					Error:           schedulerapi.Error{ErrorCode: snerror.GetPlaceHolderInstanceError},
				},
				scalingOutTimeout: time.Minute,
				wsInstanceInfo: &schedulerapi.WebsocketInsInfo{
					PodName: "mock name",
					Error:   schedulerapi.Error{},
				},
			},
			expectedInstanceName: "mock name",
			expectedErrorCode:    0,
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				return patches
			},
		}, {
			name: "return error because worker manager failed",
			args: args{
				req: &schedulerapi.AcquireInstanceReq{},
				rsp: &schedulerapi.AcquireInstanceRsp{
					PlaceHolderChan: make(chan *schedulerapi.WebsocketInsInfo, 10),
					Error:           schedulerapi.Error{ErrorCode: snerror.GetPlaceHolderInstanceError},
				},
				scalingOutTimeout: time.Minute,
				wsInstanceInfo: &schedulerapi.WebsocketInsInfo{
					Error: snerror.ScalingOutFunctionInstanceError,
				},
			},
			expectedInstanceName: "",
			expectedErrorCode:    snerror.ScalingOutInstanceError,
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			service := &defaultService{}
			go func() {
				if tt.args.wsInstanceInfo != nil {
					tt.args.rsp.PlaceHolderChan <- tt.args.wsInstanceInfo
				}
			}()
			service.acquireWebSocketInstance(tt.args.req, tt.args.rsp, tt.args.scalingOutTimeout)
			if tt.args.rsp.InstanceKey != tt.expectedInstanceName {
				t.Errorf("failed to test acquireWebSocketInstance, expected instance name: %s, "+
					"actual instance name: %s", tt.expectedInstanceName, tt.args.rsp.InstanceKey)
			}
			if tt.args.rsp.Error.ErrorCode != tt.expectedErrorCode {
				t.Errorf("failed to test acquireWebSocketInstance, expected error code: %d, "+
					"actual error code: %d", tt.expectedErrorCode, tt.args.rsp.Error.ErrorCode)
			}
			patches.ResetAll()
		})
	}
}

func Test_defaultService_AcquireInstance(t *testing.T) {
	type fields struct {
		funcMetaManager  *funcmetamanager.Manager
		funcInsManager   *funcinsmanager.Manager
		schedulerManager *schedulermanager.Manager
	}
	type args struct {
		req *schedulerapi.AcquireInstanceReq
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc testutils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "test remote debug function",
			fields: fields{},
			args: args{
				req: &schedulerapi.AcquireInstanceReq{
					TraceID:        "123456",
					FuncKeyWithRes: "7e186a/0@base@testresourcepython36768/latest/300-128",
					ReqType:        schedulerapi.WebsocketHTTPRequest,
				},
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				gomonkey.ApplyFunc((*schedulermanager.Manager).GetScheduler,
					func(_ *schedulermanager.Manager, funcKeyWithRes string) *insscheduler.Scheduler {
						return &insscheduler.Scheduler{}
					})
				gomonkey.ApplyFunc((*insscheduler.Scheduler).AcquireInstance,
					func(_ *insscheduler.Scheduler, req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return &schedulerapi.AcquireInstanceRsp{
							Error: schedulerapi.Error{
								ErrorCode: snerror.RemoteDebugInsNotReadyErrorCode,
							},
						}, nil
					})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := &defaultService{
				funcMetaManager:  tt.fields.funcMetaManager,
				funcInsManager:   tt.fields.funcInsManager,
				schedulerManager: tt.fields.schedulerManager,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, err := service.AcquireInstance(tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("AcquireInstance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestCheckInstance(t *testing.T) {
	convey.Convey("TestCheckInstance", t, func() {
		convey.Convey("GetScheduler failed", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*defaultService).getOrCreateScheduler, func(
					s *defaultService, funcKeyWithRes string) (*insscheduler.Scheduler, error) {
					return nil, errors.New("get scheduler error")
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			processesStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
			processesStatusMap[0] = &schedulerapi.ProcessStatus{
				ProcessorID:  0,
				LeaseVersion: 1,
				RestThread:   9,
			}
			instancesStatus := make(map[string]*schedulerapi.InstancesStatus)
			instancesStatus["instanceA"] = &schedulerapi.InstancesStatus{
				ProcessesStatusMap: processesStatusMap,
			}
			req := &schedulerapi.CheckInstanceReq{
				FuncKeyWithRes:     "funcA/300-128",
				InstancesStatusMap: instancesStatus,
			}
			service := &defaultService{schedulerManager: schedulermanager.CreateManager()}
			err := service.CheckInstance(req)
			assert.NotNil(t, err)
		})
		convey.Convey("instance not exist", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*defaultService).getOrCreateScheduler, func(
					s *defaultService, funcKeyWithRes string) (*insscheduler.Scheduler, error) {
					return createScheduler(), nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			processesStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
			processesStatusMap[0] = &schedulerapi.ProcessStatus{
				ProcessorID:  0,
				LeaseVersion: 1,
				RestThread:   9,
			}
			instancesStatus := make(map[string]*schedulerapi.InstancesStatus)
			instancesStatus["instanceA"] = &schedulerapi.InstancesStatus{
				ProcessesStatusMap: processesStatusMap,
			}
			req := &schedulerapi.CheckInstanceReq{
				FuncKeyWithRes:     "funcA/300-128",
				InstancesStatusMap: instancesStatus,
			}
			service := &defaultService{schedulerManager: schedulermanager.CreateManager()}
			err := service.CheckInstance(req)
			assert.Nil(t, err)
		})
	})
}

func createScheduler() *insscheduler.Scheduler {
	funcMeta := &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 10,
		},
		FuncMetaData: types.FuncMetaData{
			BusinessType: "FAAS",
		},
	}
	scheduler := insscheduler.CreateScheduler("funcA/300-128", funcMeta)
	return scheduler
}

func TestRecoverInstance(t *testing.T) {
	convey.Convey("test RecoverInstance", t, func() {
		convey.Convey("case1: success", func() {
			count := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*defaultService).getOrCreateScheduler, func(
					s *defaultService, funcKeyWithRes string) (*insscheduler.Scheduler, error) {
					count++
					return createScheduler(), nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			service := defaultService{}
			service.RecoverInstance("funcKey", &schedulerapi.CheckInstanceReq{})
			convey.So(count, convey.ShouldEqual, 1)
		})
		convey.Convey("case2: error", func() {
			count := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*defaultService).getOrCreateScheduler, func(
					s *defaultService, funcKeyWithRes string) (*insscheduler.Scheduler, error) {
					count++
					return nil, errors.New("error")
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			service := defaultService{}
			service.RecoverInstance("funcKey", &schedulerapi.CheckInstanceReq{})
			convey.So(count, convey.ShouldEqual, 4)
		})
	})
}

func Test_acquireWebSocketInstance(t *testing.T) {
	convey.Convey("Test_acquireWebSocketInstance websocket cae", t, func() {
		config.FConfig.InstanceLeaseConfig = &config.InstanceLeaseConfig{LeaseCheckInterval: 10}
		funcMetaManager := funcmetamanager.CreateManager(etcd.FetchFuncMeta)
		funcMetaManager.UpdateFuncMeta("00580943/0@base@aprilyolies/latest", &types.FunctionMetaInfo{
			FuncMetaData: types.FuncMetaData{
				BusinessType: "CAE",
			},
			ExtendedMetaData: types.ExtendedMetaData{
				EnableWebsocket: true,
			},
		})
		schedulerManager := schedulermanager.CreateManager()
		funcInsManager := funcinsmanager.CreateManager(schedulerManager)
		service := &defaultService{
			funcMetaManager:  funcMetaManager,
			funcInsManager:   funcInsManager,
			schedulerManager: schedulerManager,
		}
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker,
				func(podIP, podPort, traceID string) (uint32, uint32, error) {
					return 10, 20, nil
				}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(service), "sendWebSocketBuildingRequestToWorker",
				func() (service *defaultService, req *schedulerapi.AcquireInstanceReq,
					rsp *schedulerapi.AcquireInstanceRsp, scalingOutTimeout time.Duration) {
					count++
					return
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		service.acquireWebSocketInstance(&schedulerapi.AcquireInstanceReq{
			FuncKeyWithRes: "00580943/0@base@aprilyolies/latest/1000-1024",
			ReqType:        schedulerapi.WebSocketRequest,
		}, &schedulerapi.AcquireInstanceRsp{}, time.Second)
		convey.So(count, convey.ShouldEqual, 1)
	})
	convey.Convey("Test_acquireWebSocketInstance websocket cae with getOrCreateScheduler err", t, func() {
		config.FConfig.InstanceLeaseConfig = &config.InstanceLeaseConfig{LeaseCheckInterval: 10}
		funcMetaManager := funcmetamanager.CreateManager(etcd.FetchFuncMeta)
		funcMetaManager.UpdateFuncMeta("00580943/0@base@aprilyolies/latest", &types.FunctionMetaInfo{
			FuncMetaData: types.FuncMetaData{
				BusinessType: "CAE",
			},
			ExtendedMetaData: types.ExtendedMetaData{
				EnableWebsocket: true,
			},
		})
		schedulerManager := schedulermanager.CreateManager()
		funcInsManager := funcinsmanager.CreateManager(schedulerManager)
		service := &defaultService{
			funcMetaManager: funcMetaManager,
			funcInsManager:  funcInsManager,
		}
		count := 0
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker,
				func(podIP, podPort, traceID string) (uint32, uint32, error) {
					return 10, 20, nil
				}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(service), "sendWebSocketBuildingRequestToWorker",
				func() (service *defaultService, req *schedulerapi.AcquireInstanceReq,
					rsp *schedulerapi.AcquireInstanceRsp, scalingOutTimeout time.Duration) {
					count++
					return
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		service.acquireWebSocketInstance(&schedulerapi.AcquireInstanceReq{
			FuncKeyWithRes: "00580943/0@base@aprilyolies/latest/1000-1024",
			ReqType:        schedulerapi.WebSocketRequest,
		}, &schedulerapi.AcquireInstanceRsp{}, time.Second)
		convey.So(count, convey.ShouldEqual, 1)
	})
}
