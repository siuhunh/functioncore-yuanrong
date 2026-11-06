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

	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/loadbalancer/service"
	"functioncore/pkg/frontend-invoke/loadbalancer/test/testsuit"
	"functioncore/pkg/frontend-invoke/metadata"
)

// condition:function concurrency 1, instance num 1, no faas-scheduler
// 1.frontend schedule req 1, expect from totalPool
func TestLoadBalancerWithNoScheduler(t *testing.T) {
	convey.Convey("TestLoadBalancerWithNoScheduler", t, func() {
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

		lb := suit.CreateLoadBalancer(testsuit.TestFuncKeyWithRes, 1)
		lb.Add(testsuit.TestInstanceKey, 0)

		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok := scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)
	})
}

// condition:function concurrency 1, instance num 1
// 1.frontend schedule req 1, expect from RemoteScheduler
// 2.delete faas-scheduler
// 3.req 1 done
// 4.schedule req 2, expect from RemoteScheduler
func TestDeleteRemoteScheduler(t *testing.T) {
	convey.Convey("TestLoadBalancerWithNoScheduler", t, func() {
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
		lb.Add(testsuit.TestInstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf := lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok := scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.RemoteScheduler)
		faaSInfo := &types.FaaSServiceInfo{}
		remotescheduler.GetInstance().DeleteScheduler(testsuit.TestSchedulerKey, faaSInfo)

		lb.Done(scheduleResult.InstanceKey, 0)

		testsuit.TestNextReq.Timestamp = time.Now()
		scheduleResultIf = lb.NextWithRequest(testsuit.TestNextReq, false)
		scheduleResult, ok = scheduleResultIf.(service.ScheduleResult)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scheduleResult.InstanceKey, convey.ShouldEqual, testsuit.TestInstanceKey)
		convey.So(scheduleResult.InstanceSource, convey.ShouldEqual, service.TotalPool)
	})
}
