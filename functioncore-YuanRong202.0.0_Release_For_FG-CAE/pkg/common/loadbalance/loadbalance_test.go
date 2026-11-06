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

// Package loadbalance provides consistent hash algorithm
package loadbalance

import (
	"strconv"
	"sync"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LBTestSuite struct {
	suite.Suite
	LoadBalance
	lbType    LBType
	m         sync.RWMutex
	emptyNode interface{}
}

func (lbs *LBTestSuite) SetupSuite() {
	switch lbs.lbType {
	case RoundRobinNginx, RoundRobinLVS:
		lbs.emptyNode = nil
	case ConsistentHashGeneric:
		lbs.emptyNode = ""
	default:
		lbs.emptyNode = ""
	}
}

func (lbs *LBTestSuite) SetupTest() {
	lbs.m = sync.RWMutex{}
	lbs.LoadBalance = LBFactory(lbs.lbType)
}

func (lbs *LBTestSuite) TearDownTest() {
	lbs.LoadBalance = nil
}

func (lbs *LBTestSuite) AddToLB(workerInstance interface{}, weight int) {
	switch lbs.lbType {
	case RoundRobinNginx, RoundRobinLVS:
		lbs.m.Lock()
		lbs.Add(workerInstance, weight)
		lbs.Reset()
		lbs.m.Unlock()
	case ConsistentHashGeneric:
		lbs.Add(workerInstance, 0)
	default:
	}
}

func (lbs *LBTestSuite) DelFromLB(workerInstance interface{}) {
	switch lbs.lbType {
	case RoundRobinNginx, RoundRobinLVS:
		lbs.m.Lock()
		lbs.Remove(workerInstance)
		lbs.Reset()
		defer lbs.m.Unlock()
	case ConsistentHashGeneric:
		lbs.Remove(workerInstance)
	default:
	}
}

func (lbs *LBTestSuite) TestAdd() {
	lbs.AddToLB("new-node-01", 0)
	lbs.AddToLB("new-node-01", 1) // test duplicate
	lbs.AddToLB("new-node-02", 2)
	lbs.AddToLB("new-node-03", 5)
	lbs.AddToLB("", 6)
	lbs.AddToLB(nil, 4)
	next := lbs.Next("fn-urn-01", true)
	assert.NotEqual(lbs.T(), lbs.emptyNode, next)
	lbs.Reset()
	next = lbs.Next("fn-urn-01", true)
	assert.NotEqual(lbs.T(), lbs.emptyNode, next)
}

func (lbs *LBTestSuite) TestNext() {
	var wg sync.WaitGroup
	next := lbs.Next("fn-urn-01", false)
	assert.Equal(lbs.T(), lbs.emptyNode, next)

	lbs.AddToLB("new-node-01", 5)
	next = lbs.Next("fn-urn-01", true)
	assert.Equal(lbs.T(), "new-node-01", next)

	for i := 2; i < 5; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			lbs.AddToLB("new-node-0"+strconv.Itoa(i), 5)
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	next = lbs.Next("fn-urn-01", true)
	assert.NotEqual(lbs.T(), lbs.emptyNode, next)
}

func (lbs *LBTestSuite) TestRemove() {
	var wg sync.WaitGroup
	for i := 1; i < 5; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			lbs.AddToLB("new-node-0"+strconv.Itoa(i), 5)
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	for i := 1; i < 4; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			lbs.DelFromLB("new-node-0" + strconv.Itoa(i))
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	next := lbs.Next("fn-urn-01", true)
	assert.Equal(lbs.T(), "new-node-04", next)
}

func (lbs *LBTestSuite) TestRemoveAll() {
	var wg sync.WaitGroup
	for i := 1; i < 5; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			lbs.Add("new-node-0"+strconv.Itoa(i), 5)
			wg.Done()
		}(i, &wg)
	}
	wg.Wait()
	lbs.RemoveAll()
	next := lbs.Next("fn-urn-01", true)
	assert.Equal(lbs.T(), lbs.emptyNode, next)
}

func TestLBTestSuite(t *testing.T) {
	suite.Run(t, &LBTestSuite{lbType: ConsistentHashGeneric})
	suite.Run(t, &LBTestSuite{lbType: RoundRobinLVS})
	suite.Run(t, &LBTestSuite{lbType: RoundRobinNginx})
}

func TestConcurrentCHGeneric(t *testing.T) {
	convey.Convey("Test Concurren CH Generic", t, func() {
		generic := NewConcurrentCHGeneric(1)
		mode := generic.Next("name", true)
		generic.RemoveAll()
		generic.Add("node", 2)
		generic.Remove("node")
		convey.So(mode, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Concurren CH Generic 2", t, func() {
		generic := NewConcurrentCHGeneric(1)
		generic.counter["name"] = &concurrentCounter{count: 3}
		mode := generic.Next("name", true)
		convey.So(mode, convey.ShouldNotBeNil)
	})
}
