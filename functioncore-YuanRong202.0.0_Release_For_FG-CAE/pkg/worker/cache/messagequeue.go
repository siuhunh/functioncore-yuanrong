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

// Package config contains messagequeue of worker
package cache

import (
	"sync"
)

// MessageQueue is used to cache the messages that the worker receives and needs
// to be sent to the runtime.
type MessageQueue struct {
	blockCond   *sync.Cond
	backlogCond *sync.Cond // notify when nWaiting becomes > 0
	flushCond   *sync.Cond // notify when nWaiting becomes 0
	mux         sync.Mutex
	nWaiting    int
	block       bool
	err         error
	request     RequestInfo
	noBlockType bool
}

// RequestInfo is used to records the charging information about initialization failures.
type RequestInfo struct {
	concurrency   uint32
	directRequest bool
}

var queue = newMessageQueue()

func newMessageQueue() *MessageQueue {
	mq := &MessageQueue{}
	mq.blockCond = sync.NewCond(&mq.mux)
	mq.backlogCond = sync.NewCond(&mq.mux)
	mq.flushCond = sync.NewCond(&mq.mux)
	mq.noBlockType = false
	return mq
}

// SetNoBlockType -
func (q *MessageQueue) SetNoBlockType() {
	q.mux.Lock()
	q.noBlockType = true
	q.mux.Unlock()
}

// GetMessageQueue -
func GetMessageQueue() *MessageQueue {
	return queue
}

// WaitUnblock -
func (q *MessageQueue) WaitUnblock(concurrency uint32, directRequest bool) error {
	q.mux.Lock()
	q.nWaiting++
	if q.nWaiting == 1 {
		q.request.concurrency = concurrency
		q.request.directRequest = directRequest
	}
	if q.block {
		q.backlogCond.Broadcast()
		if q.noBlockType && q.err != nil {
			q.nWaiting--
			if q.nWaiting == 0 {
				q.flushCond.Broadcast()
				q.block = false
				q.mux.Unlock()
				blockErr := q.err
				return blockErr
			}
		}
		q.blockCond.Wait()
	}
	q.nWaiting--
	if q.nWaiting == 0 {
		q.flushCond.Broadcast()
	}
	err := q.err
	q.mux.Unlock()
	return err
}

// WaitForBacklog -
func (q *MessageQueue) WaitForBacklog() (uint32, bool) {
	q.mux.Lock()
	for q.block && q.nWaiting == 0 {
		q.backlogCond.Wait()
	}
	concurrency := q.request.concurrency
	directRequest := q.request.directRequest
	q.mux.Unlock()
	return concurrency, directRequest
}

// Block blocks the queue. Use "UnblockAndFlush" to unblock.
func (q *MessageQueue) Block() {
	q.mux.Lock()
	q.block = true
	q.mux.Unlock()
}

// UnblockAndFlush unblocks the queue. Use "Block" to block.
func (q *MessageQueue) UnblockAndFlush(err error) {
	q.mux.Lock()
	q.err = err
	q.block = false
	q.blockCond.Broadcast()
	for q.nWaiting != 0 {
		q.flushCond.Wait()
	}
	q.mux.Unlock()
}

// Error -
func (q *MessageQueue) Error(err error) {
	q.mux.Lock()
	q.err = err
	q.mux.Unlock()
}
