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

package insscheduler

import (
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	testFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	testInstanceKey    = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	traceID            = "1234"
)

var (
	testFuncMeta = &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
	}

	testRemoteDebugFuncMeta = &types.FunctionMetaInfo{
		FuncMetaData: types.FuncMetaData{
			BusinessType:      constants.BusinessTypeCAE,
			EnableRemoteDebug: true,
		},
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
	}

	testWorkerInstance = &common.Instance{
		InstanceKey: testInstanceKey,
		Applier:     "worker-manager",
		PodIP:       "127.0.0.1",
		PodPort:     "8766",
	}

	testAcquireInstanceReq = &schedulerapi.AcquireInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: testFuncKeyWithRes,
	}

	testRetainInstanceReq = &schedulerapi.RetainInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: testFuncKeyWithRes,
		InstanceKey:    testInstanceKey,
		ProcessorID:    0,
		LeaseVersion:   1,
	}

	testReleaseInstanceReq = &schedulerapi.ReleaseInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: testFuncKeyWithRes,
		InstanceKey:    testInstanceKey,
		ProcessorID:    0,
		LeaseVersion:   1,
	}
)

func TestAcquireInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		scheduler := CreateScheduler(testFuncKeyWithRes, testFuncMeta)
		scheduler.OnObserved(testWorkerInstance, common.AddInstance)
		rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)

		scheduler.OnObserved(testWorkerInstance, common.DeleteInstance)
		rsp, err = scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, snerror.NoIdleInstanceErrorCode)
	})
}

func TestReleaseInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
		scheduler := CreateScheduler(testFuncKeyWithRes, testFuncMeta)
		scheduler.OnObserved(testWorkerInstance, common.AddInstance)
		rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)

		err = scheduler.ReleaseInstance(testReleaseInstanceReq)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestRetainInstance(t *testing.T) {
	convey.Convey("TestRetainInstance", t, func() {
		scheduler := CreateScheduler(testFuncKeyWithRes, testFuncMeta)
		scheduler.OnObserved(testWorkerInstance, common.AddInstance)
		rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)

		retainRsp, err := scheduler.RetainInstance(testRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(retainRsp.Error.ErrorCode, convey.ShouldEqual, 0)
	})
	convey.Convey("TestCentralizedStrategyRetainInstance", t, func() {
		scheduler := CreateScheduler(testFuncKeyWithRes, testRemoteDebugFuncMeta)
		scheduler.OnObserved(testWorkerInstance, common.AddInstance)
		rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)

		retainRsp, err := scheduler.RetainInstance(testRetainInstanceReq)
		convey.So(err, convey.ShouldNotBeNil)
		convey.So(retainRsp, convey.ShouldBeNil)
	})
}

func TestUpdateFuncMeta(t *testing.T) {
	convey.Convey("TestUpdateFuncMeta", t, func() {
		scheduler := CreateScheduler(testFuncKeyWithRes, testFuncMeta)
		scheduler.OnObserved(testWorkerInstance, common.AddInstance)

		newFuncMeta := *testFuncMeta
		newFuncMeta.InstanceMetaData.ConcurrentNum = 10
		scheduler.UpdateFuncMeta(&newFuncMeta)

		for i := 0; i < 10; i++ {
			rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
			convey.So(err, convey.ShouldBeNil)
			convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)
		}
	})
}

func TestCheckLease(t *testing.T) {
	convey.Convey("TestUpdateFuncMeta", t, func() {
		config.InitDefaultConfig()
		config.FConfig.InstanceLeaseConfig.LeaseCheckInterval = 100
		config.FConfig.InstanceLeaseConfig.LeaseSpan = 200

		scheduler := CreateScheduler(testFuncKeyWithRes, testFuncMeta)
		scheduler.Start()
		scheduler.OnObserved(testWorkerInstance, common.AddInstance)

		rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)

		time.Sleep(250 * time.Millisecond)
		rsp, err = scheduler.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)
	})
}

func TestAcquireRemoteDebugInstance(t *testing.T) {
	scheduler := CreateScheduler(testFuncKeyWithRes, testRemoteDebugFuncMeta)
	rsp, err := scheduler.AcquireInstance(testAcquireInstanceReq)
	assert.Nil(t, err, "acquire should return no error when no instance")
	assert.Equal(t, rsp.Error.ErrorCode, uint32(snerror.RemoteDebugInsNotReadyErrorCode), "should return no idle instance code when no instance")

	scheduler.OnObserved(testWorkerInstance, common.AddInstance)
	rsp, err = scheduler.AcquireInstance(testAcquireInstanceReq)
	assert.Nil(t, err, "acquire should return no error")
	assert.Equal(t, testInstanceKey, rsp.InstanceKey)

	rsp, err = scheduler.AcquireInstance(testAcquireInstanceReq)
	assert.Nil(t, err, "acquire should return no error")
	assert.Equal(t, testInstanceKey, rsp.InstanceKey)

	scheduler.OnObserved(&common.Instance{
		InstanceKey: testInstanceKey + "2",
		Applier:     "worker-manager",
		PodIP:       "127.0.0.1",
	}, common.AddInstance)
	rsp, err = scheduler.AcquireInstance(testAcquireInstanceReq)
	assert.Nil(t, err, "acquire should return no error")
	assert.Equal(t, uint32(snerror.MoreThanOneInstanceErrorCode), rsp.Error.ErrorCode)

	scheduler.OnObserved(&common.Instance{
		InstanceKey: testInstanceKey + "2",
		Applier:     "worker-manager",
		PodIP:       "127.0.0.1",
	}, common.DeleteInstance)
	rsp, err = scheduler.AcquireInstance(testAcquireInstanceReq)
	assert.Nil(t, err, "acquire should return no error")
	assert.Equal(t, testInstanceKey, rsp.InstanceKey)
}
