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

package service

import (
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/localscheduler"
)

func Test_DefaultLoadBalancerSchedule(t *testing.T) {
	convey.Convey("Test_DefaultLoadBalancerSchedule whitelist is empty", t, func() {
		scheduler := &localscheduler.Manager{}
		dftBalancer := &defaultLoadBalancer{
			localScheduler: scheduler,
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(dftBalancer), "reachThreshold",
				func(balancer *defaultLoadBalancer, requestNum uint64) bool {
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "NextFromTotalPool",
				func(manager *localscheduler.Manager, targetIns string) string {
					return "instance01"
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		instance, _, _ := dftBalancer.schedule(&loadbalance.Request{}, "")
		convey.So(instance, convey.ShouldEqual, "instance01")
	})
	convey.Convey("Test_DefaultLoadBalancerSchedule AcquireInstance err", t, func() {
		scheduler := &localscheduler.Manager{}
		dftBalancer := &defaultLoadBalancer{
			localScheduler: scheduler,
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(dftBalancer), "reachThreshold",
				func(balancer *defaultLoadBalancer, requestNum uint64) bool {
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "AcquireInstance",
				func(manager *localscheduler.Manager, traceID string) (*common.Processor, string, error) {
					return nil, "", errors.New("AcquireInstance err")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "NextFromTotalPool",
				func(manager *localscheduler.Manager, targetIns string) string {
					return "instance01"
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		instance, _, _ := dftBalancer.schedule(&loadbalance.Request{}, "123")
		convey.So(instance, convey.ShouldEqual, "instance01")
	})
	convey.Convey("Test_DefaultLoadBalancerSchedule AddProcessorAndTryScheduler empty", t, func() {
		scheduler := &localscheduler.Manager{}
		dftBalancer := &defaultLoadBalancer{
			localScheduler: scheduler,
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(dftBalancer), "reachThreshold",
				func(balancer *defaultLoadBalancer, requestNum uint64) bool {
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "AcquireInstance",
				func(manager *localscheduler.Manager, traceID string) (*common.Processor, string, error) {
					return &common.Processor{ServerKey: ""}, "", nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "NextFromTotalPool",
				func(manager *localscheduler.Manager, targetIns string) string {
					return "instance01"
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(scheduler), "AddProcessorAndTryScheduler",
				func(manager *localscheduler.Manager, processor *common.Processor, instanceKey, traceID string) string {
					return "instance01"
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		instance, _, _ := dftBalancer.schedule(&loadbalance.Request{}, "123")
		convey.So(instance, convey.ShouldEqual, "instance01")
	})
}

func Test_createDefaultLoadBalancer(t *testing.T) {
	constant.FConfig = &constant.Config{
		LoadBalanceConfig: &constant.LoadBalanceConfig{
			ConcurrencyThreshold: 10,
		},
	}

	got := createDefaultLoadBalancer("funcKeyWithRes", 2, true, true)
	res := got.NextWithRequest(&loadbalance.Request{}, false)
	balance, ok := res.(ScheduleResult)
	if !ok {
		t.Errorf("failed to Test_createDefaultLoadBalancer")
	}
	if balance.InstanceSource != TotalPool {
		t.Errorf("failed to Test_createDefaultLoadBalancer")
	}

}
