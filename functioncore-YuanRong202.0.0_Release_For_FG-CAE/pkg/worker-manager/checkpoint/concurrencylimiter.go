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

import "sync"

// ConcurrencyLimiter limit the concurrent operation number
type ConcurrencyLimiter struct {
	runningNum  int
	limit       int
	blockingNum int
	cond        *sync.Cond
	mu          *sync.Mutex
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

// Reset reset concurrent limiter
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

// Apply apply concurrent operation
func (c *ConcurrencyLimiter) Apply() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.runningNum < c.limit {
		c.runningNum++
		return
	}
	c.blockingNum++
	for !(c.runningNum < c.limit) {
		c.cond.Wait()
	}
	c.runningNum++
	c.blockingNum--
}

// Release release concurrent operation
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
