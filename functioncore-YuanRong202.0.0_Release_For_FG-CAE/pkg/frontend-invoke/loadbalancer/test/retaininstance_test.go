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

// condition:function concurrency 1, instance num 1
// 1.frontend A schedule req 1, expect from RemoteScheduler
// 2.frontend A sleep constant.FConfig.LoadBalanceConfig.LeaseSpan+500 ms
// 3.frontend B schedule req 2, expect from TotalPool
func TestRetainInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
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

		lbA := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 1)
		lbA.Start()
		defer lbA.Stop()

		lbA.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lbA.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, _ := scheduleResultIf.(service.ScheduleResult)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)

		time.Sleep(time.Duration(constant.FConfig.LoadBalanceConfig.LeaseSpan+500) * time.Millisecond)

		lbB := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 1)
		lbB.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf = lbB.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, _ = scheduleResultIf.(service.ScheduleResult)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)
	})
}

// condition:function concurrency 20, instance num 1, InjectRetainInstanceError
// 1.frontend schedule req 1, expect from RemoteScheduler
// 2.frontend A sleep constant.FConfig.LoadBalanceConfig.LeaseSpan+500 ms
// 3.frontend schedule req 2, expect from RemoteScheduler
func TestRetainInstanceWith1v20RetainFail(t *testing.T) {
	convey.Convey("TestRetainInstanceWith1v1", t, func() {
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
		suit.InjectRetainInstanceError()
		suit.EnableLeaseCheck()

		constant.FConfig.LoadBalanceConfig.ConcurrencyThreshold = 20

		funcMeta := *testsuit.TestFuncMeta
		funcMeta.InstanceMetaData.ConcurrentNum = 20
		suit.MockFaaSScheduler.UpdateFuncMeta(testsuit.TestFuncKey, &funcMeta)
		defer testsuit.StopFunction(suit.MockFaaSScheduler, testsuit.TestFuncKey)
		suit.MockFaaSScheduler.AddInstance(testsuit.TestFuncKeyWithRes, testsuit.TestWorkerInstance)
		remotescheduler.GetInstance().AddScheduler(testsuit.TestSchedulerKey, testsuit.TestSchedulerInfo)

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 20)
		lb.Start()
		defer lb.Stop()

		lb.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, _ := scheduleResultIf.(service.ScheduleResult)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)

		time.Sleep(time.Duration(constant.FConfig.LoadBalanceConfig.LeaseSpan+500) * time.Millisecond)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf = lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult1, _ := scheduleResultIf.(service.ScheduleResult)
		convey.So(scheduleResult1.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult1.InstanceSource, convey.ShouldEqual, service.TotalPool)
	})
}
