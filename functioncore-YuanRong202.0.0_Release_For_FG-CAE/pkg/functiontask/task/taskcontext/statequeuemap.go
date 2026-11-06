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

package taskcontext

import (
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/statequeue"
)

// StateQueueMapContext represents context for state queue.
type StateQueueMapContext interface {
	StateQueueMap() *statequeue.Map
	Invoker() common.Invoker
	StateClient() stateclient.StateClient
	SetStateQueue(queue *statequeue.Map)
}

// StateQueueMapImpl implements context for state queue.
type StateQueueMapImpl struct {
	stateQueueMap *statequeue.Map
}

// StateQueueMap returns context's stateQueueMap.
func (c *StateQueueMapImpl) StateQueueMap() *statequeue.Map {
	return c.stateQueueMap
}

// Invoker returns context's Invoker.
func (c *StateQueueMapImpl) Invoker() common.Invoker {
	return c.stateQueueMap.Invoker
}

// StateClient returns context's StateClient.
func (c *StateQueueMapImpl) StateClient() stateclient.StateClient {
	return c.stateQueueMap.StateClient
}

// SetStateQueue sets futureStore for context.
func (c *StateQueueMapImpl) SetStateQueue(queue *statequeue.Map) {
	c.stateQueueMap = queue
}
