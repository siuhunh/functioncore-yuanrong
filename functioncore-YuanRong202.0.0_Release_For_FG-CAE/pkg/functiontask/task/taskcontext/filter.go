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
	"sync"

	"go.uber.org/zap"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

/*
Filter is a guarantee for request's idempotence, as the retry request should not be executed.
for example:
   1. a2, a3, a4 is the retry request for a1.
   2. task receives a2 and a3 when a1 is running, so a2 and a3 do not need to be executed.
   3. task receives a4 when a1 is finished, so a4 needs to be executed,
   but whether a4 needs to be retried is determined by the future store.
*/

// Filter returns a new filter for retry request.
type Filter struct {
	runningRequests map[snfuture.ID]chan Serializable
	mutex           sync.Mutex
}

// NewFilter returns a new filter for retry request.
func NewFilter() *Filter {
	return &Filter{
		runningRequests: make(map[snfuture.ID]chan Serializable),
	}
}

// Start returns nil if it is not a retry request.
// futureID uniquely determines the request, it may be replaced by request id.
func (f *Filter) Start(futureID snfuture.ID) chan Serializable {
	f.mutex.Lock()
	c := f.runningRequests[futureID]
	if c == nil {
		f.runningRequests[futureID] = make(chan Serializable, 1)
		f.mutex.Unlock()
		return nil
	}
	f.mutex.Unlock()
	return c
}

// Get returns response if it is not a retry request.
func (f *Filter) Get(c chan Serializable) Serializable {
	if c == nil {
		return nil
	}
	// there might be more then one retry request
	resp := <-c
	c <- resp
	return resp
}

// Set sets the response if it is a retry request.
func (f *Filter) Set(futureID snfuture.ID, resp Serializable) {
	f.mutex.Lock()
	c := f.runningRequests[futureID]
	// the request is done, so that the future id could be removed from the running request list.
	delete(f.runningRequests, futureID)
	f.mutex.Unlock()
	if c != nil {
		c <- resp
	} else {
		log.GetLogger().Errorf("Filter sets the response, futureID; %s", zap.String("futureID", futureID))
	}
}
