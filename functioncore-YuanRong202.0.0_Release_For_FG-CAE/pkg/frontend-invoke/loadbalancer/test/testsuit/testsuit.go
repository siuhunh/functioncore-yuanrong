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

// Package testsuit contains test tool for loadbalancer api test
package testsuit

import (
	"errors"
	"sync"
	"time"

	"functioncore/pkg/faas-scheduler/config"
	schedulerservice "functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/loadbalancer/service"
)

// TestSuit -
type TestSuit struct {
	MockFaaSScheduler schedulerservice.SchedulerService
	retainError       error
}

// CreateTestSuit -
func CreateTestSuit() *TestSuit {
	return &TestSuit{}
}

// Setup -
func (suit *TestSuit) Setup() {
	constant.InitDefaultConfig()
	constant.FConfig.LoadBalanceConfig.ConcurrencyThreshold = 10
	remotescheduler.CreateInstance()
	common.CreatePoolInstance()

	config.InitDefaultConfig()
	suit.DisableLeaseCheck()

	suit.MockFaaSScheduler, _ = schedulerservice.CreateService(nil)
	common.SetRPCClientCreator(suit.createMockRPCClient)
}

// DisableLeaseCheck -
func (suit *TestSuit) DisableLeaseCheck() {
	config.FConfig.InstanceLeaseConfig.LeaseCheckInterval = 300000
	config.FConfig.InstanceLeaseConfig.LeaseSpan = 300000

	constant.FConfig.LoadBalanceConfig.LeaseCheckInterval = 300000
	constant.FConfig.LoadBalanceConfig.LeaseSpan = 300000
}

func (suit *TestSuit) EnableLeaseCheck() {
	config.FConfig.InstanceLeaseConfig.LeaseCheckInterval = 1000
	config.FConfig.InstanceLeaseConfig.LeaseSpan = 2000

	constant.FConfig.LoadBalanceConfig.LeaseCheckInterval = 1000
	constant.FConfig.LoadBalanceConfig.LeaseSpan = 2000
}

// CreateLoadBalancer -
func (suit *TestSuit) CreateLoadBalancer(
	funcKeyWithRes string, concurrency int) service.LoadBalancer {
	return service.CreateLoadBalancer(funcKeyWithRes, concurrency, false, false)
}

func (suit *TestSuit) createMockRPCClient(
	config common.RPCClientConfig) (common.RPCClient, error) {
	return createMockRPCClient(suit.MockFaaSScheduler, config, suit.retainError)
}

func (suit *TestSuit) ConcurrentAcquireInstance(
	lb service.LoadBalancer, invokeTime int) []service.ScheduleResult {
	resList := make([]service.ScheduleResult, 0)
	wg := sync.WaitGroup{}
	lock := sync.Mutex{}
	for i := 0; i < invokeTime; i++ {
		wg.Add(1)
		go func() {
			TestNextReq.Timestamp = time.Now()
			scheduleResultIf := lb.NextWithRequest(TestNextReq, false)
			scheduleResult, _ := scheduleResultIf.(service.ScheduleResult)
			lock.Lock()
			resList = append(resList, scheduleResult)
			lock.Unlock()

			wg.Done()
		}()
	}
	wg.Wait()
	return resList
}

func (suit *TestSuit) InjectRetainInstanceError() {
	suit.retainError = errors.New("retain failed")
}

func StopFunction(scheduler schedulerservice.SchedulerService, funcKey string) {
	scheduler.DeleteFuncMeta(funcKey, 0)
}
