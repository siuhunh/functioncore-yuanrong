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

// condition:function concurrency 1, instance num 1
// 1.acquire 1 instance, expect success
// 2.delete instance
// 3.retain instance, expect CreateSchedulerErrorCode
// 4.release instance, expect CreateSchedulerErrorCode
// 5.acquire 1 instance, expect NoIdleInstanceErrorCode
func TestFuncInsDelete(t *testing.T) {
	convey.Convey("TestFuncInsDelete", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)

		service.DeleteInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		service.DeleteInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		retainRsp, err := service.RetainInstance(testsuit.TestRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(retainRsp.Error.ErrorCode, convey.ShouldEqual, snerror.CreateSchedulerErrorCode)

		err = service.ReleaseInstance(testsuit.TestReleaseInstanceReq)
		convey.So(err, convey.ShouldNotBeNil)

		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)
	})
}

// condition:function concurrency 1
// 1.concurrent add 10 instance
// 2.acquire 1 instance, expect success
// 3.concurrent delete 10 instance
// 4.acquire 1 instance, expect NoIdleInstanceErrorCode
func TestFuncInsConcurrentAdd(t *testing.T) {
	convey.Convey("TestFuncInsDelete", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		service, _ := service.CreateService(nil)
		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)

		suit.ConcurrentAddInstance(service, testsuit.TestFuncKeyWithRes, 10)
		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, 0)

		suit.ConcurrentDeleteInstance(service, testsuit.TestFuncKeyWithRes, 10)
		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)
	})
}

// condition:function concurrency 1, instance num 0
// 1.acquire 1 instance, expect NoIdleInstanceErrorCode
// 2.add 1 instance
// 3.acquire 1 instance, expect success
func TestFuncInsAdd(t *testing.T) {
	convey.Convey("TestFuncInsDelete", t, func() {
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
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, 0)
	})
}
