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

// Package calls contains interface of bus and basic codes
package calls

import (
	"fmt"
	"sync"
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
)

const defaultMapSize = 2

var (
	callCollectorInstance *CallCollector
	once                  sync.Once
)

// CallCollector contains all call requests
type CallCollector struct {
	pendingCalls   map[uint64]func(*types.TCPResponse)
	pengindsMu     sync.RWMutex
	seq            uint64
	maxConcurrency int
	stopChan       chan bool
}

// InitCollectorOnce inits the call collector once
func InitCollectorOnce(maxConcurrency int) *CallCollector {
	once.Do(func() {
		callCollectorInstance = InitCollector(maxConcurrency)
	})
	return callCollectorInstance
}

// InitCollector inits the call collector
func InitCollector(maxConcurrency int) *CallCollector {
	return &CallCollector{
		pendingCalls:   make(map[uint64]func(*types.TCPResponse), defaultMapSize),
		maxConcurrency: maxConcurrency,
		stopChan:       nil,
	}
}

// AddRequest adds call request
func (c *CallCollector) AddRequest() (uint64, chan *types.TCPResponse, error) {
	ch := make(chan *types.TCPResponse, 1)
	requestID, err := c.AddAsyncRequest(func(resp *types.TCPResponse) {
		ch <- resp
	})
	return requestID, ch, err
}

// AddAsyncRequest -
func (c *CallCollector) AddAsyncRequest(w func(*types.TCPResponse)) (uint64, error) {
	requestID := c.getSequenceNumber()
	c.pengindsMu.Lock()
	defer c.pengindsMu.Unlock()
	if len(c.pendingCalls) >= c.maxConcurrency {
		log.GetLogger().Errorf("pendingCalls reaches limit")
		return requestID, fmt.Errorf("pendingCalls reaches limit")
	}
	c.pendingCalls[requestID] = w
	log.GetLogger().Debugf("addRequest requestID: %d", requestID)
	return requestID, nil
}

// AddResponse adds call response
func (c *CallCollector) AddResponse(response *types.TCPResponse) bool {
	c.pengindsMu.Lock()
	w, exist := c.pendingCalls[response.RequestID]
	if exist {
		delete(c.pendingCalls, response.RequestID)
	} else {
		log.GetLogger().Warnf("callResponse doesn't match any requestID: %d", response.RequestID)
		c.pengindsMu.Unlock()
		return exist
	}

	c.pengindsMu.Unlock()

	w(response)

	c.pengindsMu.Lock()
	if c.stopChan != nil && len(c.pendingCalls) == 0 {
		select {
		case c.stopChan <- true:
		default:
		}
	}
	c.pengindsMu.Unlock()
	return exist
}

// Clear waiting for pendingCalls empty
func (c *CallCollector) Clear() {
	c.pengindsMu.Lock()
	if len(c.pendingCalls) == 0 {
		c.pengindsMu.Unlock()
		return
	}
	c.stopChan = make(chan bool)
	c.pengindsMu.Unlock()
	<-c.stopChan
}

func (c *CallCollector) getSequenceNumber() uint64 {
	return atomic.AddUint64(&c.seq, 1)
}
