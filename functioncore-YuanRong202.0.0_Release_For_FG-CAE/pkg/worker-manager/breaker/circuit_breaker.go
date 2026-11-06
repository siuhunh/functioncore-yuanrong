/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package breaker for specialization failure
package breaker

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/utils"
)

const (
	maxRandomNumRange = 100
)

// Breaker for specialization failure
type Breaker interface {
	Fail()
	Succeed()
	IsAllowed() bool

	Reset()

	GetState() State
}

// CircuitBreaker circuit breaker for specialization failure
type CircuitBreaker struct {
	rule         *Rule
	statistic    Statistic
	state        State
	maxRandomNum int
	openStateTTL time.Time
	sync.RWMutex
}

// NewCircuitBreaker new circuit breaker with rule
func NewCircuitBreaker(rule *Rule) Breaker {
	listStatistic := NewListStatistic(rule)

	return &CircuitBreaker{
		rule:         rule,
		statistic:    listStatistic,
		state:        Closed,
		maxRandomNum: int(rule.HalfOpenPassRate * maxRandomNumRange),
	}
}

func (cb *CircuitBreaker) isAllowedWhenHalfOpen() bool {
	// check if it is not allowed by random number
	randomNum := utils.GetRandomIndex(maxRandomNumRange) // [0, 100)
	// for example, 20 >= 0.2 * 100, it will not be allowed, 19 is allowed
	if randomNum >= cb.maxRandomNum {
		log.GetLogger().Debugf("it is not allowed with random number %d, half open pass rate %f when half open",
			randomNum, cb.rule.HalfOpenPassRate)
		return false
	}
	return true
}

// GetState get breaker state
func (cb *CircuitBreaker) GetState() State {
	cb.RLock()
	state := cb.state
	cb.RUnlock()
	return state
}

// Reset reset breaker
func (cb *CircuitBreaker) Reset() {
	cb.Lock()
	cb.statistic.Reset()
	cb.state = Closed
	cb.Unlock()
}

// Succeed call succeed when request is succeed
func (cb *CircuitBreaker) Succeed() {
	cb.Lock()
	cb.statistic.Reset()
	cb.state = Closed
	cb.Unlock()
}

// Fail call fail when request is fail
func (cb *CircuitBreaker) Fail() {
	cb.Lock()

	cb.statistic.Fail()
	switch cb.state {
	case Open: // do noting
	case HalfOpen: // become to Open
		cb.openStateTTL = time.Now().Add(time.Duration(cb.rule.CoolingTimeout) * time.Second)
		cb.state = Open
	case Closed: // become to Open
		if cb.statistic.ConsecutiveFails() >= cb.rule.MaxConsecutiveFails {
			cb.openStateTTL = time.Now().Add(time.Duration(cb.rule.CoolingTimeout) * time.Second)
			cb.state = Open
		}
	default:
		log.GetLogger().Warnf("unknown state %s", cb.state.String())
	}

	cb.Unlock()
}

// IsAllowed check if request is allowed
func (cb *CircuitBreaker) IsAllowed() bool {
	cb.Lock()
	defer cb.Unlock()

	switch cb.state {
	case Closed:
		return true
	case HalfOpen:
		return cb.isAllowedWhenHalfOpen()
	case Open:
		// now > openStateTTL, it means exceed cooling timeout
		if time.Now().After(cb.openStateTTL) {
			// cooling timeout, then become HalfOpen
			cb.state = HalfOpen
			return cb.isAllowedWhenHalfOpen()
		}
		return false
	default:
		log.GetLogger().Errorf("unknown state %s", cb.state.String())
		return false
	}
}
