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

// Package service define load-balance service
package service

import (
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/dataaggregation"
	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/localscheduler"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	msPerSec = 1000
)

type defaultLoadBalancer struct {
	localScheduler *localscheduler.Manager
	funcKeyWithRes string // tenantID1/0@serviceID@funcName/funcVersion/300-128
	funcKey        string // tenantID1/0@serviceID@funcName/funcVersion
	tenantID       string
	concurrency    int
	isLongRun      bool
	schedulerLock  sync.Mutex
	stopCh         chan struct{}
	once           sync.Once
	avgLatency     dataaggregation.AtomicAverage
	isCaeFunc      bool
}

func createDefaultLoadBalancer(funcKeyWithRes string, concurrency int, isLongRun, isCaeFunc bool) LoadBalancer {
	log.GetLogger().Infof("CreateLoadBalancer funcKeyWithRes %s", funcKeyWithRes)
	stopCh := make(chan struct{}, 1)
	tenantID, funcKey := util.ParseFuncKeyWithRes(funcKeyWithRes)
	return &defaultLoadBalancer{
		isLongRun:      isLongRun,
		localScheduler: localscheduler.CreateManager(funcKeyWithRes, stopCh),
		funcKeyWithRes: funcKeyWithRes,
		concurrency:    concurrency,
		schedulerLock:  sync.Mutex{},
		stopCh:         stopCh,
		once:           sync.Once{},
		avgLatency:     dataaggregation.NewAtomicAverage(-1),
		funcKey:        funcKey,
		tenantID:       tenantID,
		isCaeFunc:      isCaeFunc,
	}
}

// Start defaultLoadBalancer need some goroutine, start method start all goroutine
func (balancer *defaultLoadBalancer) Start() {
	balancer.localScheduler.Start()
}

// Stop - stop all goroutine
func (balancer *defaultLoadBalancer) Stop() {
	balancer.once.Do(func() {
		close(balancer.stopCh)
	})
}

// NextWithRequest -
func (balancer *defaultLoadBalancer) NextWithRequest(
	req *loadbalance.Request, move bool) interface{} {
	threshold := constant.FConfig.LoadBalanceConfig.ConcurrencyThreshold
	disable := atomic.LoadUint32(&constant.FConfig.MultiConcurrencySchedule.DisableFaaSSchedule) != 0
	whiteList := remotescheduler.GetWhitelist().Query(balancer.funcKey, balancer.tenantID)
	if (whiteList == "" && disable && balancer.concurrency > threshold) || balancer.isLongRun || balancer.isCaeFunc {
		balancer.schedulerLock.Lock()
		instance := balancer.localScheduler.NextFromTotalPool(req.TargetInstance)
		balancer.schedulerLock.Unlock()

		return ScheduleResult{
			InstanceKey:    instance,
			InstanceSource: TotalPool,
		}
	}

	start := time.Now()
	instance, faaSScheduler, source := balancer.schedule(req, whiteList)
	log.GetLogger().Infof("nextWithReq | %s | %s | %s | source %d | totalDelay %s | %s | schedulerDelay %s",
		req.TraceID, urnutils.FuncKeyWithResToShort(balancer.funcKeyWithRes), urnutils.ExtractPodName(instance), source,
		time.Since(req.Timestamp).String(), faaSScheduler, time.Since(start).String())
	return ScheduleResult{
		InstanceKey:    instance,
		InstanceSource: source,
	}
}

// SetConcurrency -
func (balancer *defaultLoadBalancer) SetConcurrency(concurrency int) {
	balancer.concurrency = concurrency
}

func (balancer *defaultLoadBalancer) SetIsLongRun(isLongRun bool) {
	balancer.isLongRun = isLongRun
}

// Next -
func (balancer *defaultLoadBalancer) Next(name string, move bool) interface{} {
	return nil
}

// Add -
func (balancer *defaultLoadBalancer) Add(node interface{}, _ int) {
	instanceKey, ok := node.(string)
	if !ok {
		log.GetLogger().Errorf("failed to add node,"+
			" node type is invalid, funcKeyWithRes %s",
			urnutils.AnonymizeTenantKey(balancer.funcKeyWithRes))
		return
	}
	balancer.schedulerLock.Lock()
	balancer.localScheduler.AddInstance(instanceKey)
	balancer.schedulerLock.Unlock()
}

// Remove -
func (balancer *defaultLoadBalancer) Remove(node interface{}) {
	instanceKey, ok := node.(string)
	if !ok {
		log.GetLogger().Errorf("failed to remove node,"+
			" node type is invalid, funcKeyWithRes %s",
			urnutils.AnonymizeTenantKey(balancer.funcKeyWithRes))
		return
	}

	balancer.schedulerLock.Lock()
	balancer.localScheduler.DeleteInstance(instanceKey)
	balancer.schedulerLock.Unlock()
}

// RemoveAll -
func (balancer *defaultLoadBalancer) RemoveAll() {}

// Reset -
func (balancer *defaultLoadBalancer) Reset() {}

// Done -
func (balancer *defaultLoadBalancer) Done(node interface{}, cost time.Duration) {
	instanceKey, ok := node.(string)
	if !ok {
		log.GetLogger().Errorf("failed to done request,"+
			" node type is invalid, funcKeyWithRes %s",
			urnutils.AnonymizeTenantKey(balancer.funcKeyWithRes))
		return
	}
	balancer.avgLatency.AddSample(int64(cost / time.Millisecond))

	balancer.schedulerLock.Lock()
	proc := balancer.localScheduler.Done(instanceKey)
	balancer.schedulerLock.Unlock()
	balancer.localScheduler.Release(instanceKey, proc, cost)
}

// NoLock -
func (balancer *defaultLoadBalancer) NoLock() bool {
	return true
}

func (balancer *defaultLoadBalancer) reachThreshold(requestNum uint64) bool {
	reqNumThreshold := atomic.LoadInt64(&constant.FConfig.LoadBalanceConfig.MaxRequestNumThreshold)
	if reqNumThreshold <= 0 {
		return balancer.localScheduler.GetInstanceNum() > constant.FConfig.LoadBalanceConfig.MaxInstanceThreshold
	}
	avgLatency := balancer.avgLatency.GetValue()
	if avgLatency == 0 {
		return requestNum > uint64(reqNumThreshold)
	}
	return int64(requestNum)*msPerSec/avgLatency > atomic.LoadInt64(&constant.FConfig.LoadBalanceConfig.QPSThreshold)
}

func (balancer *defaultLoadBalancer) schedule(req *loadbalance.Request,
	whiteList string) (string, string, InstanceSource) {
	// if instance num over threshold, for performance, NextFromTotalPool
	balancer.schedulerLock.Lock()
	if whiteList == "" && balancer.reachThreshold(req.RequestNum) {
		instanceKey := balancer.localScheduler.NextFromTotalPool(req.TargetInstance)
		balancer.schedulerLock.Unlock()
		return instanceKey, "", TotalPool
	}
	balancer.schedulerLock.Unlock()

	// concurrent acquire instance from remote scheduler
	acquiredProcessor, acquiredInstanceKey, err :=
		balancer.localScheduler.AcquireInstance(req.TraceID)

	balancer.schedulerLock.Lock()
	defer balancer.schedulerLock.Unlock()
	if err != nil {
		log.GetLogger().Debugf("failed to AcquireInstance"+
			" funcKeyWithRes %s, traceID %s, err %s",
			urnutils.AnonymizeTenantKey(balancer.funcKeyWithRes),
			req.TraceID, err.Error())
		return balancer.localScheduler.NextFromTotalPool(req.TargetInstance), "", TotalPool
	}

	// if acquired pool has no idle instance, try to acquire instance from remote scheduler
	instanceKey := balancer.localScheduler.AddProcessorAndTryScheduler(
		acquiredProcessor, acquiredInstanceKey, req.TraceID)
	if instanceKey != "" {
		return instanceKey, acquiredProcessor.ServerKey, RemoteScheduler
	}

	return balancer.localScheduler.NextFromTotalPool(req.TargetInstance), acquiredProcessor.ServerKey, TotalPool
}
