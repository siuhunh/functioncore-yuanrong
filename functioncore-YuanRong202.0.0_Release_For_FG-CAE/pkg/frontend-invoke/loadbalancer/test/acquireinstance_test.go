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
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/loadbalancer/service"
	"functioncore/pkg/frontend-invoke/loadbalancer/test/testsuit"
	"functioncore/pkg/frontend-invoke/metadata"
)

// condition:function concurrency 100, instance num 1
// 1.schedule req 1, expect from TotalPool
func TestAcquireInstanceFromTotalPool(t *testing.T) {
	convey.Convey("TestAcquireInstanceFromTotalPool", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "Faas",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		suit := testsuit.CreateTestSuit()
		suit.Setup()

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 100)
		lb.Add(1, 0)
		lb.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok := scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)

		lb.Done(testsuit.TestInstanceKey, 0)
		lb.Done(1, 0)

		lb.Remove(testsuit.TestInstanceKey)
		lb.Remove(1)

		scheduleResultIf = lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok = scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, "")
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)

	})
}

// condition:function concurrency 1, instance num 1
// 1.schedule req 1, expect from RemoteScheduler
// 2.schedule req 2, expect from RemoteScheduler
func TestAcquireInstanceFromRemoteScheduler(t *testing.T) {
	convey.Convey("TestAcquireInstanceFromRemoteScheduler", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "CAE",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		suit := testsuit.CreateTestSuit()
		suit.Setup()

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 1)
		lb.Add(1, 0)
		lb.Add(testsuit.TestInstanceKey, 0)

		suit.MockFaaSScheduler.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		defer testsuit.StopFunction(suit.MockFaaSScheduler, testsuit.TestFuncKey)
		suit.MockFaaSScheduler.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)
		remotescheduler.GetInstance().AddScheduler(testsuit.TestSchedulerKey, testsuit.TestSchedulerInfo)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok := scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf = lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok = scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)

	})
}

// condition:function concurrency 10, instance num 1
// 1.concurrency schedule 10 req, expect all from RemoteScheduler
func TestConcurrentAcquireInstanceWith1v10(t *testing.T) {
	convey.Convey("TestConcurrentAcquireInstanceWith1v10", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "CAE",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		funcMeta := *testsuit.TestFuncMeta
		funcMeta.InstanceMetaData.ConcurrentNum = 10
		suit.MockFaaSScheduler.UpdateFuncMeta(testsuit.TestFuncKey, &funcMeta)
		defer testsuit.StopFunction(suit.MockFaaSScheduler, testsuit.TestFuncKey)
		suit.MockFaaSScheduler.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)
		remotescheduler.GetInstance().AddScheduler(testsuit.TestSchedulerKey, testsuit.TestSchedulerInfo)

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 10)
		lb.Start()
		defer lb.Stop()

		lb.Add(testsuit.TestInstanceKey, 0)

		resList := suit.ConcurrentAcquireInstance(lb, 10)
		for _, scheduleResult := range resList {
			convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
			convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)
		}
		for i := 0; i < 10; i++ {
			lb.Done(testsuit.TestInstanceKey, 0)
		}
		time.Sleep(100 * time.Millisecond)
	})
}

// condition:function concurrency 1, instance num 1
// 1.schedule req 1, expect from RemoteScheduler
// 2.req 1 done
// 3.schedule req 2, expect from RemoteScheduler
func TestReleaseInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()

		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "CAE",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		suit.MockFaaSScheduler.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		defer testsuit.StopFunction(suit.MockFaaSScheduler, testsuit.TestFuncKey)
		suit.MockFaaSScheduler.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)
		remotescheduler.GetInstance().AddScheduler(testsuit.TestSchedulerKey, testsuit.TestSchedulerInfo)

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 1)
		lb.Start()
		defer lb.Stop()

		lb.Add(1, 0)
		lb.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok := scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)

		lb.Done(testsuit.TestInstanceKey, 0)
		time.Sleep(100 * time.Millisecond)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf = lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok = scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)
	})
}

// condition:function concurrency 1, instance num 1
// 1.frontend schedule req 1, expect from RemoteScheduler
// 2.set MaxInstanceThreshold 0
// 2.frontend schedule req 2, expect from AcquirePool
func TestAcquireInstanceWithMaxInstanceThreshold(t *testing.T) {
	convey.Convey("TestAcquireInstanceWithMaxInstanceThreshold", t, func() {
		suit := testsuit.CreateTestSuit()
		suit.Setup()
		suit.EnableLeaseCheck()
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "CAE",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		suit.MockFaaSScheduler.UpdateFuncMeta(testsuit.TestFuncKey, testsuit.TestFuncMeta)
		defer testsuit.StopFunction(suit.MockFaaSScheduler, testsuit.TestFuncKey)

		suit.MockFaaSScheduler.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)
		remotescheduler.GetInstance().AddScheduler(testsuit.TestSchedulerKey, testsuit.TestSchedulerInfo)

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 1)
		lb.Start()
		defer lb.Stop()

		lb.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, _ := scheduleResultIf.(service.ScheduleResult)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)

		constant.FConfig.LoadBalanceConfig.MaxInstanceThreshold = 0

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf = lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, _ = scheduleResultIf.(service.ScheduleResult)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)
	})
}
