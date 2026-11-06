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
	"time"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/faas-scheduler/scheduler/test/testsuit"
)

// condition:function concurrency 1, instance num 1
// 1.acquire 1 processor, expect success
// 2.sleep LeaseSpan + 100 ms
// 3.acquire 1 processor, expect success
func TestLeaseInstanceTimeout(t *testing.T) {
	convey.Convey("TestLeaseInstance", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		suit.EnableLeaseCheck()
		service, _ := service.CreateService(nil)
		leaseSpan := config.FConfig.InstanceLeaseConfig.LeaseSpan

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		time.Sleep(time.Duration(leaseSpan+1000) * time.Millisecond)

		expectRsp := *testsuit.TestAcquireInstanceRsp
		expectRsp.LeaseVersion = 2
		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, &expectRsp), convey.ShouldBeTrue)

	})
}

// condition:function concurrency 1, instance num 1
// 1.acquire 1 processor, expect success
// 2.sleep LeaseSpan/2 ms
// 2.retain processor, expect success
// 3.acquire 1 processor, expect NoIdleInstanceErrorCode
func TestLeaseInstance(t *testing.T) {
	convey.Convey("TestLeaseInstance", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		suit.EnableLeaseCheck()
		service, _ := service.CreateService(nil)
		leaseSpan := config.FConfig.InstanceLeaseConfig.LeaseSpan

		service.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		service.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)

		rsp, err := service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(reflect.DeepEqual(rsp, testsuit.TestAcquireInstanceRsp), convey.ShouldBeTrue)

		time.Sleep(time.Duration(leaseSpan/2) * time.Millisecond)

		retainRsp, err := service.RetainInstance(testsuit.TestRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(retainRsp.Error.ErrorCode, convey.ShouldEqual, 0)

		time.Sleep(time.Duration(leaseSpan/2+100) * time.Millisecond)
		rsp, err = service.AcquireInstance(testsuit.TestAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)
	})
}
