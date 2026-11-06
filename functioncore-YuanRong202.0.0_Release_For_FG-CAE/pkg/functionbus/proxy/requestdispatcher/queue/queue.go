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

// Package queue imply multiple queues
package queue

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/dataaggregation"
	"functioncore/pkg/common/logger/log"
	commonQueue "functioncore/pkg/common/queue"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
)

const (
	// FIFO -
	FIFO = "fifo"
	// Priority -
	Priority           = "priority"
	timeTickerInterval = time.Second
)

var (
	priorityFunc = func(obj interface{}) (int, error) {
		entry, ok := obj.(Entry)
		if ok {
			if entry.request.Request.CanBackpressure {
				return entry.request.Request.Priority + 1, nil
			}
			return entry.request.Request.Priority, nil
		}
		return -1, fmt.Errorf("failed to assert request queue entry")
	}

	autoScalerConfigOnce sync.Once
)

// LengthObserver observer interface for queue length
type LengthObserver interface {
	QueueLengthChanged(length, rps int, info common.TraceInfo, queuedTime time.Duration, option BackPressuredOption)
	CheckUsedWorker() bool
	Reset()
}

// BackPressuredOption backPressured option
type BackPressuredOption struct {
	IsBackPressured bool
	ScaleErrorCode  uint32
}

// Entry request queue entry
type Entry struct {
	request   *types.DispatcherRequest
	timestamp time.Time
}

// RequestQueue request queue for a function
type RequestQueue struct {
	handler func(*types.DispatcherRequest) error
	queue   *queueWithBackPressure
	// cluster is overloaded, request cannot be handled at once
	scaleError   *types.TCPResponse
	signal       chan struct{}
	stop         <-chan struct{}
	rpsCounter   dataaggregation.DataAggregation
	observer     LengthObserver
	queueTimeout time.Duration
	sync.RWMutex
	*common.AutoScalerConfig
}

// NewRequestQueue create request queue.
func NewRequestQueue(t string, queueTimeout time.Duration, stopCh <-chan struct{}) *RequestQueue {
	autoScalerConfigOnce.Do(common.InitDefaultAutoScalerConfig)
	rq := &RequestQueue{
		signal:           make(chan struct{}, 1),
		rpsCounter:       dataaggregation.NewDataAggregation(dataaggregation.COUNT, time.Second, 1, 0),
		queueTimeout:     queueTimeout,
		handler:          nil,
		observer:         nil,
		queue:            newQueueWithBackPressure(t),
		scaleError:       nil,
		stop:             stopCh,
		AutoScalerConfig: common.FConfig.AutoScalerConfig,
	}
	if stopCh == nil {
		log.GetLogger().Warnf("request queue stopCh is nil")
		return rq
	}
	go func() {
		timeTicker := time.NewTicker(timeTickerInterval)
		defer timeTicker.Stop()
		for {
			select {
			case <-timeTicker.C:
				rq.triggerSchedule()
			case <-stopCh:
				return
			}
		}
	}()

	return rq
}

// NewQueue create queue, default queue is fifo queue.
func NewQueue(t string) commonQueue.Queue {
	switch t {
	case FIFO:
		return commonQueue.NewFifoQueue()
	case Priority:
		return commonQueue.NewPriorityQueue(priorityFunc)
	default:
		return commonQueue.NewFifoQueue()
	}
}

// RegisterHandler register request handle in queue
func (b *RequestQueue) RegisterHandler(function func(*types.DispatcherRequest) error) {
	b.handler = function
	go b.handleFront()
}

// ObserveLength observe request queue length
func (b *RequestQueue) ObserveLength(obs LengthObserver) {
	b.observer = obs
}

// SetError set scale error
func (b *RequestQueue) SetError(errResponse *types.TCPResponse) {
	b.Lock()
	b.scaleError = errResponse
	b.Unlock()
}

// ScheduleWorkerChanged trigger handle
func (b *RequestQueue) ScheduleWorkerChanged(remote bool) {
	// don't trigger schedule when remote workers update
	if remote {
		return
	}
	b.triggerSchedule()
}

func (b *RequestQueue) triggerSchedule() {
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *RequestQueue) handleFront() {
	for {
		select {
		case _, ok := <-b.signal:
			if !ok {
				return
			}
			b.schedule()
		case <-b.stop:
			return
		}
	}
}

var localProxy = common.ProxyBackPressurePrefix + internal.SelfNodeIP

func checkBackPressured(request *types.DispatcherRequest) bool {
	if request.Request.SessionPod != "" {
		return false
	}
	// check if request is backpressured from other proxy by frontend
	if request.Request.BackpressureNodeID != "" {
		return true
	}
	if request.Position.InstanceID == "" {
		return false
	}
	// check if request is backpressured from other proxy directly
	if strings.HasPrefix(request.Position.InstanceID, common.ProxyBackPressurePrefix) &&
		request.Position.InstanceID != localProxy {
		return true
	}
	return false
}

func (b *RequestQueue) schedule() {
	b.Lock()
	currentTimestamp := time.Now()
	var requestTimeStamp time.Time
	var isBackPressured bool
	traceInfo := common.TraceInfo{}
	var sessionReqCount int
	for b.queue.Len() != 0 {
		obj, err := b.queue.Front()
		if err != nil {
			break
		}
		entry, ok := obj.(Entry)
		if !ok {
			break
		}

		if err = b.handler(entry.request); err == nil {
			// request got scheduled
			_, e := b.queue.Pop()
			if e != nil {
				log.GetLogger().Warnf("failed to pop, err: %s", e.Error())
			}
			continue
		}

		if b.scaleError != nil && b.scaleError.ErrorCode == snerror.FunctionIsDisabled {
			b.handleFunctionDisable(entry)
			continue
		}
		check, queuedTime := b.requestQueuedTimeoutAndError(entry, currentTimestamp)
		if !check {
			// wait for next schedule cycle
			traceInfo.TraceID = entry.request.TraceID
			traceInfo.TraceParent = entry.request.Request.Extra[constants.HeaderTraceParent]
			requestTimeStamp = entry.timestamp
			isBackPressured = checkBackPressured(entry.request)
			if entry.request.Request.SessionPod != "" {
				sessionReqCount++
				object, e := b.queue.Pop()
				if e != nil {
					log.GetLogger().Warnf("failed to pop "+
						"entry request %s, err: %s", entry.request.TraceID, e.Error())
					if b.queue.Len() <= sessionReqCount {
						break
					}
					continue
				}
				if pushErr := b.queue.Push(object); pushErr != nil {
					log.GetLogger().Warnf("failed to push "+
						"entry request %s, err: %s", entry.request.TraceID, pushErr.Error())
					entry.request.Response <- &types.TCPResponse{
						ErrorCode:    snerror.ClusterOverloadCode,
						ErrorMessage: snerror.ErrText(snerror.ClusterOverloadCode),
					}
					sessionReqCount--
				}
				if b.queue.Len() <= sessionReqCount {
					break
				}
				continue
			}
			break
		}

		b.handleFunctionQueuedTimeout(entry, queuedTime)
	}

	// pop can-backpressure requests if those cannot get scheduled at once
	b.queue.PopBackPressureReq()

	length := b.queue.Len() - sessionReqCount
	if length < 0 {
		length = 0
	}
	rps := b.getRPS(currentTimestamp)
	if length == 0 {
		// clear old scale error
		b.scaleError = nil
	}
	b.Unlock()

	// publish queue event
	if b.observer != nil {
		option := BackPressuredOption{IsBackPressured: isBackPressured}
		if b.scaleError != nil {
			option.ScaleErrorCode = b.scaleError.ErrorCode
		}
		b.observer.QueueLengthChanged(length, rps, traceInfo, currentTimestamp.Sub(requestTimeStamp), option)
	}
}

func (b *RequestQueue) requestQueuedTimeoutAndError(entry Entry, currentTimestamp time.Time) (bool, time.Duration) {
	queuedTime := currentTimestamp.Sub(entry.timestamp)
	if entry.request.Request.SessionPod != "" {
		return queuedTime >= b.queueTimeout, queuedTime
	}
	if b.scaleError == nil {
		return false, 0
	}
	if (b.scaleError.ErrorCode == snerror.ReachMaxInstancesCode ||
		b.scaleError.ErrorCode == snerror.ReachMaxOnDemandInstancesPerTenant) &&
		queuedTime >= time.Duration(b.MaxInstanceQueueTimeout)*time.Second {
		return true, queuedTime
	}
	if b.observer == nil {
		return queuedTime >= b.queueTimeout, queuedTime
	}
	if b.observer.CheckUsedWorker() {
		return queuedTime >= time.Duration(b.WithInstanceQueueTimeout)*time.Second, queuedTime
	}
	return queuedTime >= b.queueTimeout, queuedTime
}

func (b *RequestQueue) getRPS(t time.Time) int {
	return int(b.rpsCounter.GetValue(t))
}

// Push an item into queue.
func (b *RequestQueue) Push(request *types.DispatcherRequest) error {
	b.Lock()
	currentTimestamp := time.Now()
	if err := b.queue.Push(Entry{
		timestamp: currentTimestamp,
		request:   request,
	}); err != nil {
		b.Unlock()
		return err
	}

	b.rpsCounter.AddSample(0, currentTimestamp)

	b.Unlock()

	b.triggerSchedule()
	return nil
}

// PopN pop 1/N requests
func (b *RequestQueue) PopN(n int, threshold int) []*types.DispatcherRequest {
	b.Lock()
	defer b.Unlock()
	count := int(math.Ceil(float64(b.queue.Len()) / float64(n)))
	if count < threshold {
		count = threshold
	}
	if count < 0 || count > int(internal.RequestsLimit) {
		count = 0
	}
	requests := make([]*types.DispatcherRequest, 0, count)
	for i := 0; i < count; i++ {
		if obj, err := b.queue.Pop(); err == nil {
			entry, ok := obj.(Entry)
			if !ok {
				log.GetLogger().Errorf("failed to convert obj to queue entry")
				return nil
			}
			requests = append(requests, entry.request)
		} else {
			break
		}
	}
	return requests
}

// Pop pop request from queue
func (b *RequestQueue) Pop(count int) []*types.DispatcherRequest {
	if count > int(internal.RequestsLimit) {
		count = int(internal.RequestsLimit)
	}
	requests := make([]*types.DispatcherRequest, 0, count)
	b.Lock()
	defer b.Unlock()
	for i := 0; i < count; i++ {
		if obj, err := b.queue.Pop(); err == nil {
			entry, ok := obj.(Entry)
			if !ok {
				log.GetLogger().Errorf("failed to convert obj to queue entry")
				return nil
			}
			requests = append(requests, entry.request)
		} else {
			break
		}
	}
	return requests
}

// Len return the length of queue.
func (b *RequestQueue) Len() int {
	b.RLock()
	length := b.queue.Len()
	b.RUnlock()
	return length
}

func (b *RequestQueue) handleFunctionDisable(entry Entry) {
	log.GetLogger().Warnf("function %s is disabled, abort request %s immediately",
		urnutils.AnonymizeTenantKey(entry.request.FunctionKey), entry.request.TraceID)
	entry.request.Response <- &types.TCPResponse{
		ErrorCode:    snerror.FunctionIsDisabled,
		ErrorMessage: "function is disabled",
	}
	_, err := b.queue.Pop()
	if err != nil {
		log.GetLogger().Errorf("function %s failed to pop request %s from queue",
			urnutils.AnonymizeTenantKey(entry.request.FunctionKey), entry.request.TraceID)
	}
	length := b.queue.Len()
	if length == 0 {
		// clear old scale error
		b.scaleError = nil
	}
}

func (b *RequestQueue) handleFunctionQueuedTimeout(entry Entry, queuedTime time.Duration) {
	if entry.request.Request != nil && entry.request.Request.SessionPod != "" {
		entry.request.Response <- &types.TCPResponse{
			ErrorCode:    snerror.ClusterOverloadCode,
			ErrorMessage: snerror.ErrText(snerror.ClusterOverloadCode),
		}
		// request queued timeout and cluster overloaded must be popped from queue
		_, err := b.queue.Pop()
		if err != nil {
			log.GetLogger().Errorf("function %s failed to pop request %s from queue",
				urnutils.AnonymizeTenantKey(entry.request.FunctionKey), entry.request.TraceID)
		}
		return
	}
	log.GetLogger().Warnf("request %s queued timeout after %v because of scaling error: %s ",
		entry.request.TraceID, queuedTime, b.scaleError.ErrorMessage)
	scaleError := snerror.New(int(b.scaleError.ErrorCode), b.scaleError.ErrorMessage)
	if b.scaleError.ErrorCode == snerror.ReachMaxInstancesCode ||
		b.scaleError.ErrorCode == snerror.ReachMaxOnDemandInstancesPerTenant || snerror.IsUserError(scaleError) {
		entry.request.Response <- b.scaleError
	} else {
		// Temporary solution: The system does not distinguish whether the cluster is overloaded or the pod
		// obtaining is slow. it determines return overload error based on the request queuing timeout.
		entry.request.Response <- &types.TCPResponse{
			ErrorCode:    snerror.ClusterOverloadCode,
			ErrorMessage: snerror.ErrText(snerror.ClusterOverloadCode),
		}
	}
	// request queued timeout and cluster overloaded must be popped from queue
	_, err := b.queue.Pop()
	if err != nil {
		log.GetLogger().Errorf("function %s failed to pop request %s from queue",
			urnutils.AnonymizeTenantKey(entry.request.FunctionKey), entry.request.TraceID)
	}
}

type queueWithBackPressure struct {
	queue                commonQueue.Queue
	canBackPressureQueue commonQueue.Queue
}

// Push an item into queue.
func (mq *queueWithBackPressure) Push(obj interface{}) error {
	entry, ok := obj.(Entry)
	if !ok {
		log.GetLogger().Errorf("failed to convert obj to queue entry")
		return fmt.Errorf("failed to convert obj to queue entry")
	}
	if entry.request.Request.CanBackpressure {
		return mq.canBackPressureQueue.Push(obj)
	}

	return mq.queue.Push(obj)
}

// Front return front item of queue
func (mq *queueWithBackPressure) Front() (interface{}, error) {
	if mq.queue.Len() == 0 {
		return mq.canBackPressureQueue.Front()
	}
	return mq.queue.Front()
}

// Pop pop an item from queue.
func (mq *queueWithBackPressure) Pop() (interface{}, error) {
	if mq.queue.Len() == 0 {
		return mq.canBackPressureQueue.Pop()
	}
	return mq.queue.Pop()
}

// PopBackPressureReq -
func (mq *queueWithBackPressure) PopBackPressureReq() {
	if internal.DisableBackPressure {
		return
	}
	for {
		obj, err := mq.canBackPressureQueue.Pop()
		if err != nil {
			// queue is empty
			break
		}
		entry, ok := obj.(Entry)
		if !ok {
			continue
		}
		log.GetLogger().Infof("backPressureReq | noSchedule | %s | %s",
			urnutils.FuncKeyToShort(entry.request.FunctionKey), entry.request.TraceID)
		entry.request.Response <- &types.TCPResponse{
			ErrorCode: constants.BackpressureCode,
		}
	}
}

// Len return the length of queue.
func (mq *queueWithBackPressure) Len() int {
	return mq.queue.Len() + mq.canBackPressureQueue.Len()
}

// newQueueWithBackPressure queue decorator contains normal queue and can-backpressure queue
func newQueueWithBackPressure(t string) *queueWithBackPressure {
	return &queueWithBackPressure{
		queue:                NewQueue(t),
		canBackPressureQueue: NewQueue(t),
	}
}
