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

package checkpoint

import (
	"fmt"
	"sync"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
)

// ConcurrencyLimiter limit the concurrent operation number
type ConcurrencyLimiter struct {
	runningNum  int
	limit       int
	blockingNum int
	isAlarmed   bool

	// snapshot queue is too long then report alarm
	isAlarmedQueueLongTime bool

	cond *sync.Cond
	mu   *sync.Mutex
}

// CreateConcurrencyLimiter create limiter with limit number
func CreateConcurrencyLimiter(limit int) ConcurrencyLimiter {
	l := new(sync.Mutex)
	return ConcurrencyLimiter{
		limit: limit,
		cond:  sync.NewCond(l),
		mu:    l,
	}
}

// Reset concurrent limiter
func (c *ConcurrencyLimiter) Reset(limit int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tmp := c.limit
	c.limit = limit
	blockingNum := c.blockingNum
	if limit-tmp > 0 && blockingNum > 0 {
		for i := 0; i < limit-tmp && blockingNum > 0; i++ {
			c.cond.Signal()
			blockingNum--
		}
	}
}

// Apply concurrent operation
func (c *ConcurrencyLimiter) Apply() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.runningNum < c.limit {
		c.runningNum++
		return
	}
	c.blockingNum++
	log.GetLogger().Warnf("checkpoint concurrent num is %d, blocking num is %d, exceed limit is %d, isAlarmed is %t",
		c.runningNum, c.blockingNum, c.limit, c.isAlarmed)
	doneCh := make(chan struct{}, 1)
	if !c.isAlarmedQueueLongTime {
		go c.checkWaitingQueueTime(doneCh)
	}
	if !c.isAlarmed && c.blockingNum > 0 {
		c.isAlarmed = true
		alarm.GetClient().ReportAlarm(alarm.CheckpointConcurrentNumExceedLimit,
			fmt.Sprintf("checkpoint concurrent num is %d exceed limit %d with blocking num %d",
				c.runningNum, c.limit, c.blockingNum))
	}
	for !(c.runningNum < c.limit) {
		c.cond.Wait()
	}
	c.runningNum++
	c.blockingNum--
	doneCh <- struct{}{}
	log.GetLogger().Infof("release blocking checkpoint concurrent num is %d, blocking num is %d, "+
		"exceed limit is %d, isAlarmed is %t", c.runningNum, c.blockingNum, c.limit, c.isAlarmed)
	if c.isAlarmed && c.blockingNum <= 0 {
		c.isAlarmed = false
		alarm.GetClient().ClearAlarm(alarm.CheckpointConcurrentNumExceedLimit)
	}
	if c.isAlarmedQueueLongTime && c.blockingNum <= 0 {
		c.isAlarmedQueueLongTime = false
		alarm.GetClient().ClearAlarm(alarm.CheckpointQueueLongTime)
	}
}

// Release concurrent operation
func (c *ConcurrencyLimiter) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.blockingNum > 0 {
		c.runningNum--
		c.cond.Signal()
		return
	}

	c.runningNum--
}

func (c *ConcurrencyLimiter) checkWaitingQueueTime(done <-chan struct{}) {
	timer := time.NewTimer(snapshotQueueAlarmTime)
	defer timer.Stop()

	select {
	case <-done:
		return
	case <-timer.C:
		c.mu.Lock()
		if c.isAlarmedQueueLongTime {
			c.mu.Unlock()
			log.GetLogger().Info("snapshot queue waiting time exceeds threshold, already report alarm")
			return
		}
		c.isAlarmedQueueLongTime = true
		limit := c.limit
		blockingNum := c.blockingNum
		c.mu.Unlock()

		alarm.GetClient().ReportAlarm(alarm.CheckpointQueueLongTime,
			fmt.Sprintf("snapshot queue waiting time exceeds threshold: %3f (s) with limit %d and blocking num %d",
				snapshotQueueAlarmTime.Seconds(), limit, blockingNum))
		log.GetLogger().Warnf("snapshot queue waiting time exceeds threshold: %3f (s), report alarm",
			snapshotQueueAlarmTime.Seconds())
		return
	}
}
