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
	"reflect"
	"testing"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/faas-scheduler/scheduler/test/testsuit"
)

// condition:function meta in database, no instance
// 1.acquire 1 instance, expect NoIdleInstanceErrorCode
func TestAcquireInstanceWithFuncMetaInDatabase(t *testing.T) {
	convey.Convey("TestAcquireInstanceWithFuncMetaInDatabase", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(suit.GetFuncMetaDatabase().FetchFuncMeta)

		suit.GetFuncMetaDatabase().AddFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)
	})
}

// condition:ffunction concurrency 1, instance num 1
// 1.acquire 1 instance, expect success
// 2.delete function
// 3.acquire 1 instance, expect CreateSchedulerErrorCode
func TestDeleteFuncMeta(t *testing.T) {
	convey.Convey("TestDeleteFuncMeta", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		service.DeleteFuncMeta(testsuit.TestFuncKeyWithRes, 0)
		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)

		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)
		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		service.DeleteFuncMeta(testsuit.TestFuncKey, 0)
		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.CreateSchedulerErrorCode)
	})
}

// condition:function concurrency 1, instance num 1
// 1.acquire 1 instance, expect success
// 2.update function ConcurrentNum
// 3.retain 1 instance, expect InstanceInternalError
// 4.acquire 1 instance, expect success
// 5.acquire 1 instance, expect success
func TestUpdateFuncMetaConcurrency(t *testing.T) {
	convey.Convey("TestUpdateFuncMetaConcurrency", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		newFuncMeta := *testsuit.TestFuncMeta
		newFuncMeta.InstanceMetaData.ConcurrentNum = 11
		service.UpdateFuncMeta(testsuit.TestFuncKey, &newFuncMeta)

		retainRsp, err := service.RetainInstance(testsuit.TestRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(retainRsp.Error.ErrorCode, convey.ShouldEqual, snerror.InstanceInternalError)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, 0)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, 0)
	})
}
