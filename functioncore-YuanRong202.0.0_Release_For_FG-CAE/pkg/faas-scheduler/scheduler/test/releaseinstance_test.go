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
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/urnutils"
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
// 2.release invalid processorID processor, expect fail
// 3.release valid processorID processor, expect success
// 4.acquire 1 processor, expect success
func TestReleaseInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		invalidReleaseReq := *testsuit.TestReleaseInstanceReq
		invalidReleaseReq.ProcessorID = 10
		err = service.ReleaseInstance(&invalidReleaseReq)
		convey.So(err, convey.ShouldNotBeNil)

		err = service.ReleaseInstance(testsuit.TestReleaseInstanceReq)
		convey.So(err, convey.ShouldBeNil)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		expectRsp := *testsuit.TestAcquireInstanceRsp
		expectRsp.LeaseVersion = 2
		convey.So(reflect.DeepEqual(rsp, &expectRsp), convey.ShouldBeTrue)
	})
}

func TestReleaseWebSocketInstance(t *testing.T) {
	suit := testsuit.CreateTestSuit()
	suit.Setup()
	mockService, _ := service.CreateService(nil)
	mockService.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestWebSocketFuncMeta)
	suit.Setup()
	type fields struct {
		req           *schedulerapi.ReleaseInstanceReq
		addedInstance *types.WorkerInstance
	}
	type expectedType struct {
		err     error
		errCode uint32
	}
	type testStruct struct {
		name        string
		fields      fields
		expected    expectedType
		patchesFunc mockUtils.PatchesFunc
	}
	tests := []testStruct{
		{"case 1 release normal websocket instance",
			fields{
				req:           testsuit.TestWebSocketReleaseInstanceReq,
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
				})
				return patches
			},
		}, {"case 2 release no exiting websocket instance",
			fields{
				req: &schedulerapi.ReleaseInstanceReq{
					TraceID:        testsuit.TraceID,
					FuncKeyWithRes: testsuit.TestFuncKeyWithRes,
					IsWebSocketReq: true,
					InstanceKey:    "noExistingInstanceKey",
				},
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: fmt.Errorf("failed to ReleaseInstance funcKeyWithRes %s traceID %s processorKey %s err %s",
				urnutils.AnonymizeTenantKey(testsuit.TestFuncKeyWithRes), testsuit.TraceID,
				urnutils.GetProcessorKey("noExistingInstanceKey", 0, 0), "instance not exist"), errCode: 0},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.SendCountWebSocketConRequestToWorker, func(
						podIP, podPort, traceID string) (uint32, uint32, error) {
						return 0, 0, nil
					}),
				})
				return patches
			},
		}, {"case 3 no scheduler can be found",
			fields{
				req:           testsuit.TestWebSocketReleaseInstanceReq,
				addedInstance: testsuit.TestNormalWebSocketWorkerInstance,
			},
			expectedType{err: fmt.Errorf("failed to ReleaseInstance funcKeyWithRes %s"+
				" traceID %s processorKey %s no scheduler",
				urnutils.AnonymizeTenantKey(testsuit.TestWebSocketReleaseInstanceReq.FuncKeyWithRes), testsuit.TestWebSocketReleaseInstanceReq.TraceID,
				urnutils.GetProcessorKey(testsuit.TestWebSocketReleaseInstanceReq.InstanceKey, 0, 0)), errCode: 0},
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			mockService.AddInstance(testsuit.TestFuncKeyWithRes, tt.fields.addedInstance)
			_, err := mockService.AcquireInstance(testsuit.TestWebSocketAcquireInstanceReq)
			err = mockService.ReleaseInstance(tt.fields.req)
			if tt.expected.err != err && tt.expected.err.Error() != err.Error() {
				t.Errorf("case name: %s, actual: %s, want, %s", tt.name, err.Error(), tt.expected.err.Error())
			}
			mockService.DeleteInstance(testsuit.TestFuncKeyWithRes, tt.fields.addedInstance)
			patches.ResetAll()
		})
	}
}
