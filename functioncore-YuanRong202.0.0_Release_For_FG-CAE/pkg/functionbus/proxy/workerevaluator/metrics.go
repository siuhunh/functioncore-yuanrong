/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

// Package workerevaluator contains worker metrics and supports observers
package workerevaluator

import (
	"sync/atomic"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

// WorkerMetrics - data for worker load balance
type WorkerMetrics struct {
	twView              *timeWindowView
	ID                  string
	FuncKeyWithResource string
	Latencies           []int64
	SessionFinishCount  int
	Enable              uint32
	metricsMsgChan      chan metricsMsg
	IsLocalWorker       bool
	IsMinInstance       bool
	NodeIP              string
	RecoveredRequestNum int
	StateID             string
	IsBrokenConnection  bool
}

type metricsMsg struct {
	chanType      int
	value         int64 // for latenciesType
	sessionFinish bool
}

const (
	latenciesType = 2
	enableType    = 3

	defaultChanCapacity = 1000
)

func (wm *WorkerMetrics) processMetricsMsg(msg metricsMsg) bool {
	switch msg.chanType {
	case latenciesType:
		latency := msg.value
		wm.Latencies = append(wm.Latencies, latency)
		if msg.sessionFinish {
			wm.SessionFinishCount++
		}
	case enableType:
		log.GetLogger().Debugf("receive metrics enable msg, FuncKeyWithResource:%s, enable: %d, before: %d",
			urnutils.AnonymizeTenantKey(wm.FuncKeyWithResource), msg.value, atomic.LoadUint32(&wm.Enable))
		if msg.value == 0 {
			atomic.StoreUint32(&wm.Enable, 0)
			return true
		}
		atomic.StoreUint32(&wm.Enable, 1)
	default:
		// abnormal
		log.GetLogger().Errorf("receive unknown metrics msg, FuncKeyWithResource: %s",
			urnutils.AnonymizeTenantKey(wm.FuncKeyWithResource))
	}
	return false
}

func (wm *WorkerMetrics) sendLatenciesChan(latency int64, IsSessionCreateReq bool) {
	wm.metricsMsgChan <- metricsMsg{
		chanType:      latenciesType,
		value:         latency,
		sessionFinish: IsSessionCreateReq,
	}
}

func (wm *WorkerMetrics) sendEnableChan(enable bool) {
	if enable {
		wm.metricsMsgChan <- metricsMsg{
			chanType: enableType,
			value:    1,
		}
	} else {
		wm.metricsMsgChan <- metricsMsg{
			chanType: enableType,
			value:    0,
		}
	}
}

func (we *WorkerEvaluator) record(workerID string) {
	metrics := we.getWorkerMetrics(workerID)
	if metrics == nil {
		log.GetLogger().Errorf("workerEvaluator metrics(%s) Record routine start failed", workerID)
		return
	}

	for {
		select {
		case msg, ok := <-metrics.metricsMsgChan:
			if !ok {
				log.GetLogger().Errorf("workerEvaluator metrics(%s) Record routine exit abnormal", workerID)
				return
			}
			needExit := we.processMetricsMsg(workerID, msg, metrics)

			if needExit {
				we.metrics.Delete(workerID)
				we.publish(metrics.FuncKeyWithResource, metrics)
				log.GetLogger().Warnf("workerEvaluator metrics(%s) Record routine exit", workerID)
				return
			}
		}
	}
}

func (we *WorkerEvaluator) processMetricsMsg(workerID string, msg metricsMsg, metrics *WorkerMetrics) bool {
	needExit := metrics.processMetricsMsg(msg)
	if needExit {
		return needExit
	}

	for i := 0; i < len(metrics.metricsMsgChan); i++ {
		needExit := metrics.processMetricsMsg(<-metrics.metricsMsgChan)
		if needExit {
			return needExit
		}
	}
	we.publish(metrics.FuncKeyWithResource, metrics)
	// clear reported metrics
	metrics.Latencies = metrics.Latencies[:0:0]
	metrics.SessionFinishCount = 0
	return needExit
}

func (we *WorkerEvaluator) getOrAddWorkerMetrics(workerID string, opts ...WorkerMetricsOpt) *WorkerMetrics {
	if obj, ok := we.metrics.Load(workerID); ok {
		if metrics, ok := obj.(*WorkerMetrics); ok {
			return metrics
		}
	}
	metrics := &WorkerMetrics{
		twView:         &timeWindowView{},
		ID:             workerID,
		Latencies:      []int64{},
		Enable:         1, // worker is ready for requests
		metricsMsgChan: make(chan metricsMsg, defaultChanCapacity),
	}
	for _, opt := range opts {
		opt(metrics)
	}
	obj, loaded := we.metrics.LoadOrStore(workerID, metrics)
	if loaded {
		if metrics, ok := obj.(*WorkerMetrics); ok {
			return metrics
		}
	}

	// publishing new worker is synchronized in scaling
	we.publish(metrics.FuncKeyWithResource, metrics)

	go we.record(workerID)
	return metrics
}

// DeleteWorkerMetrics - delete worker metrics
func (we *WorkerEvaluator) DeleteWorkerMetrics(workerID, funcKeyWithResource string) {
	log.GetLogger().Infof("delete worker(%s) metrics, funcKeyWithResource: %s", workerID,
		urnutils.AnonymizeTenantKey(funcKeyWithResource))
	metrics := we.getWorkerMetrics(workerID)
	if metrics == nil {
		log.GetLogger().Warnf("no metrics (%s) in WorkerEvaluator", workerID)
		we.publish(funcKeyWithResource, &WorkerMetrics{
			ID:                  workerID,
			FuncKeyWithResource: funcKeyWithResource,
			Latencies:           []int64{},
			Enable:              0,
		})
		return
	}
	metrics.sendEnableChan(false)
}

// RecordRequestStart -
func (we *WorkerEvaluator) RecordRequestStart(workerID string) {
	metrics := we.getWorkerMetrics(workerID)
	if metrics == nil {
		log.GetLogger().Warnf("no metrics(%s) in WorkerEvaluator", workerID)
		return
	}
	metrics.twView.requestStarted()
}

// RecordRequestEnd - add request latency
func (we *WorkerEvaluator) RecordRequestEnd(workerID string, costTime time.Duration, IsSessionCreateReq bool) {
	latency := costTime.Milliseconds()
	metrics := we.getWorkerMetrics(workerID)
	if metrics == nil {
		log.GetLogger().Warnf("no metrics(%s) in WorkerEvaluator", workerID)
		return
	}
	metrics.twView.requestEnded()
	metrics.sendLatenciesChan(latency, IsSessionCreateReq)
}

// IsExist -
func (we *WorkerEvaluator) IsExist(workerID string) bool {
	return we.getWorkerMetrics(workerID) != nil
}

func (we *WorkerEvaluator) getWorkerMetrics(workerID string) *WorkerMetrics {
	if obj, ok := we.metrics.Load(workerID); ok {
		if metrics, ok := obj.(*WorkerMetrics); ok {
			return metrics
		}
	}
	return nil
}
