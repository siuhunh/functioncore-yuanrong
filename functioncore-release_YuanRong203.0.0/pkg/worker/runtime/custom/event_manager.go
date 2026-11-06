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

package custom

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/stream"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
)

const (
	// Graceful exit time of the worker is 300 seconds,
	// the writeTimeout is supposed to be longer than graceful exit time,
	// but it's too long to make sense
	waitRequestTimeout = 600 * time.Second
)

// EventManager manager event
type EventManager struct {
	requestChan chan *runtime.InvokeRequest

	waitMap struct {
		sync.RWMutex
		waiters       map[string]chan *runtime.InvokeResponse
		timers        map[string]*time.Timer
		streamOptions map[string]*stream.ClientOption
	}
}

// NewEventManager create new EventManager
func NewEventManager() *EventManager {
	manager := &EventManager{}

	const chanCapacity = 8
	manager.requestChan = make(chan *runtime.InvokeRequest, chanCapacity)
	const waitMapCapacity = 100
	manager.waitMap.waiters = make(map[string]chan *runtime.InvokeResponse, waitMapCapacity)
	const timersMapCapacity = 100
	manager.waitMap.timers = make(map[string]*time.Timer, timersMapCapacity)
	const optionMapCapacity = 100
	manager.waitMap.streamOptions = make(map[string]*stream.ClientOption, optionMapCapacity)

	return manager
}

// NextRequest get next request
func (e *EventManager) NextRequest() *runtime.InvokeRequest {
	timer := time.NewTimer(waitRequestTimeout)
	select {
	case <-timer.C:
		return nil
	case request, ok := <-e.requestChan:
		if !ok {
			log.GetLogger().Errorf("Request channel closed unexpectedly")
			return nil
		}
		return request
	}
}

// PutRequest put request into EventManager, channel will close automatically,
// but method "Release" should be called manually after getting response from channel
func (e *EventManager) PutRequest(ic invokectx.InvokeCtx, request *runtime.InvokeRequest,
	rspChan chan *runtime.InvokeResponse) error {
	if request == nil {
		return errors.New("request is nil")
	}
	e.waitMap.Lock()
	e.waitMap.waiters[request.TraceID] = rspChan
	e.waitMap.Unlock()
	e.requestChan <- request
	invokectx.GetTimeTraceContext(ic).Start(worker.TimeTraceRuntime, time.Now())

	return nil
}

// PutResponse put response into EventManager
func (e *EventManager) PutResponse(traceID string, response *runtime.InvokeResponse) error {
	if response == nil {
		return errors.New("response is nil")
	}
	e.waitMap.Lock()
	waiter, ok := e.waitMap.waiters[traceID]
	e.waitMap.Unlock()
	if !ok || waiter == nil {
		log.GetLogger().Errorf("traceID %s isn't exist", traceID)
		return fmt.Errorf("traceID %s isn't exist", traceID)
	}

	select {
	case waiter <- response:
	default:
		log.GetLogger().Warnf("failed to put response for traceID: %s", traceID)
	}

	return nil
}

// GetResponse get response from EventManager by traceID
func (e *EventManager) GetResponse(traceID string) *runtime.InvokeResponse {
	e.waitMap.Lock()
	waiter, ok := e.waitMap.waiters[traceID]
	e.waitMap.Unlock()
	if !ok || waiter == nil {
		log.GetLogger().Errorf("traceID %s isn't exist", traceID)
		return nil
	}

	ret, ok := <-waiter
	if !ok {
		log.GetLogger().Errorf("response channel closed unexpectedly")
	}
	e.Release(traceID)
	return ret
}

// Release release resource in EventManager of traceID
func (e *EventManager) Release(traceID string) {
	e.waitMap.Lock()
	// Remove waiter from waiter map
	delete(e.waitMap.waiters, traceID)
	delete(e.waitMap.timers, traceID)
	delete(e.waitMap.streamOptions, traceID)
	e.waitMap.Unlock()
}

func (e *EventManager) isAvailable(traceID string) bool {
	e.waitMap.Lock()
	defer e.waitMap.Unlock()

	if _, ok := e.waitMap.waiters[traceID]; ok {
		return true
	}
	return false
}
