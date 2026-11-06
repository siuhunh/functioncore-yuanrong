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
	"container/list"
	"time"

	"functioncore/pkg/common/logger/log"
)

type element struct {
	ttl time.Time
}

func (e *element) isExpired(now time.Time) bool {
	return now.After(e.ttl)
}

// Statistic for breaker
type Statistic interface {
	Fail()
	Succeed()
	ConsecutiveFails() int

	Reset()
}

// ListStatistic realized with list, it is not concurrency-safe and depends on circuit breaker lock.
type ListStatistic struct {
	l          list.List
	timeWindow int
	maxEleNums int
}

// NewListStatistic new ListStatistic
func NewListStatistic(rule *Rule) Statistic {
	return &ListStatistic{
		l:          list.List{},
		timeWindow: rule.TimeWindow,
		maxEleNums: rule.MaxAllowedFails,
	}
}

func (ls *ListStatistic) addEle() {
	// check if failed nums exceed max limit, it will discard this element
	if ls.l.Len() >= ls.maxEleNums {
		return
	}
	now := time.Now()
	ttl := now.Add(time.Duration(ls.timeWindow) * time.Second)
	ls.l.PushBack(&element{
		ttl: ttl,
	})
	log.GetLogger().Infof("add element with ttl %v and now %v", ttl, now)
}

func (ls *ListStatistic) removeExpiredEle() {
	now := time.Now()
	// remove all expired element
	var next *list.Element
	for e := ls.l.Front(); e != nil; e = next {
		next = e.Next()
		ele, ok := e.Value.(*element)
		if !ok {
			continue
		}
		log.GetLogger().Warnf("ttl is: %v, now %v", ele.ttl, now)
		if ele.isExpired(now) {
			ls.l.Remove(e)
		} else {
			// the element is pushed back to list, if an element is not expired, subsequent elements are not expired
			break
		}
	}
}

// Fail -
func (ls *ListStatistic) Fail() {
	ls.removeExpiredEle()
	ls.addEle()
}

// Succeed -
func (ls *ListStatistic) Succeed() {
	ls.l.Init()
}

// ConsecutiveFails it should call Fail method when calling ConsecutiveFails method
func (ls *ListStatistic) ConsecutiveFails() int {
	return ls.l.Len()
}

// Reset -
func (ls *ListStatistic) Reset() {
	ls.l.Init()
}
