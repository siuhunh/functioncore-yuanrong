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
	"fmt"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
)

const (
	closedStr   = "Closed"
	halfOpenStr = "HalfOpen"
	openStr     = "Open"
)

func TestCircuitBreakerHalfOpen2Open(t *testing.T) {
	rule := &Rule{
		TimeWindow:          10,
		MaxConsecutiveFails: 3,
		MaxAllowedFails:     10,
		CoolingTimeout:      5,
		HalfOpenPassRate:    0.5,
	}
	breaker := NewCircuitBreaker(rule)

	// Closed to Open
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()

	state := breaker.GetState()
	if state != Open || state.String() != openStr {
		t.Errorf("expect Open but found %v", state.String())
		return
	}
	isAllowed := breaker.IsAllowed()
	if isAllowed != false {
		t.Errorf("expect false but found %v", isAllowed)
		return
	}

	coolDownTime := time.Now().Add(time.Duration(rule.CoolingTimeout+1) * time.Second)
	patch := gomonkey.ApplyFunc(time.Now, func() time.Time { return coolDownTime })
	// Open to HalfOpen
	fmt.Printf("begin to sleep %d seconds, wait state Open become HalfOpen\n", rule.CoolingTimeout)
	breaker.IsAllowed()
	patch.Reset()
	// state become HalfOpen
	state = breaker.GetState() // state become HalfOpen
	if state != HalfOpen || state.String() != halfOpenStr {
		t.Errorf("expect HalfOpen but found %s", state.String())
		return
	}

	// HalfOpen to Open
	breaker.Fail()
	isAllowed = breaker.IsAllowed()
	if isAllowed != false {
		t.Errorf("expect false but found %v", isAllowed)
		return
	}
	state = breaker.GetState() // state become HalfOpen
	if state != Open || state.String() != openStr {
		t.Errorf("expect HalfOpen but found %s", state.String())
		return
	}
}

func TestCircuitBreakerHalfOpen2Closed(t *testing.T) {
	rule := &Rule{
		TimeWindow:          10,
		MaxConsecutiveFails: 3,
		MaxAllowedFails:     10,
		CoolingTimeout:      5,
		HalfOpenPassRate:    0.5,
	}
	breaker := NewCircuitBreaker(rule)

	// Closed to Open
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()

	state := breaker.GetState()
	if state != Open || state.String() != openStr {
		t.Errorf("expect Open but found %v", state.String())
		return
	}
	isAllowed := breaker.IsAllowed()
	if isAllowed != false {
		t.Errorf("expect false but found %v", isAllowed)
		return
	}

	coolDownTime := time.Now().Add(time.Duration(rule.CoolingTimeout+1) * time.Second)
	patch := gomonkey.ApplyFunc(time.Now, func() time.Time { return coolDownTime })
	// Open to HalfOpen
	fmt.Printf("begin to sleep %d seconds, wait state Open become HalfOpen\n", rule.CoolingTimeout)
	breaker.IsAllowed()
	patch.Reset()
	// state become HalfOpen
	state = breaker.GetState() // state become HalfOpen
	if state != HalfOpen || state.String() != halfOpenStr {
		t.Errorf("expect HalfOpen but found %s", state.String())
		return
	}

	// HalfOpen to Closed
	breaker.Succeed()
	isAllowed = breaker.IsAllowed()
	if isAllowed != true {
		t.Errorf("expect true but found %v", isAllowed)
		return
	}
	state = breaker.GetState() // state become HalfOpen
	if state != Closed || state.String() != closedStr {
		t.Errorf("expect Closed but found %s", state.String())
		return
	}
}

func TestCircuitBreakerWithExpiredFail(t *testing.T) {
	rule := &Rule{
		TimeWindow:          6,
		MaxConsecutiveFails: 3,
		MaxAllowedFails:     10,
		CoolingTimeout:      5,
		HalfOpenPassRate:    0.5,
	}
	breaker := NewCircuitBreaker(rule)

	breaker.Fail()
	coolDownTime := time.Now().Add(time.Duration(rule.TimeWindow+1) * time.Second)
	patch := gomonkey.ApplyFunc(time.Now, func() time.Time { return coolDownTime })
	defer patch.Reset()
	fmt.Printf("begin to sleep %d seconds, wait this fail expired\n", rule.TimeWindow)

	// state is still Closed
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()

	state := breaker.GetState()
	if state != Closed || state.String() != closedStr {
		t.Errorf("expect Closed but found %v", state.String())
		return
	}
	isAllowed := breaker.IsAllowed()
	if isAllowed != true {
		t.Errorf("expect true but found %v", isAllowed)
		return
	}

	// Closed to Open
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()

	state = breaker.GetState()
	if state != Open || state.String() != openStr {
		t.Errorf("expect Open but found %v", state.String())
		return
	}
	isAllowed = breaker.IsAllowed()
	if isAllowed != false {
		t.Errorf("expect false but found %v", isAllowed)
		return
	}
}

func TestCircuitBreakerWithReset(t *testing.T) {
	rule := &Rule{
		TimeWindow:          10,
		MaxConsecutiveFails: 3,
		MaxAllowedFails:     10,
		CoolingTimeout:      5,
		HalfOpenPassRate:    0.5,
	}
	breaker := NewCircuitBreaker(rule)

	// Closed to Open
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()
	time.Sleep(10 * time.Millisecond)
	breaker.Fail()

	state := breaker.GetState()
	if state != Open || state.String() != openStr {
		t.Errorf("expect Open but found %v", state.String())
		return
	}
	isAllowed := breaker.IsAllowed()
	if isAllowed != false {
		t.Errorf("expect false but found %v", isAllowed)
		return
	}

	breaker.Reset()

	state = breaker.GetState() // state become HalfOpen
	if state != Closed || state.String() != closedStr {
		t.Errorf("expect Closed but found %s", state.String())
		return
	}
}
