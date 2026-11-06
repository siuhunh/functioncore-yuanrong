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

/*
Package statequeue receives a request for a stateful function, puts it into a FIFO queue, and executes them in order.
When detection of deadlocks may occur, scheduling may lead to the execution of deadlocked requests,
at which time multiple requests are executed together.
*/
package statequeue

import (
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	metadata2 "functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/statequeue/reqnode"
	"functioncore/pkg/functiontask/task/util/short"
)

const (
	maxCallStackDepth            = 16
	defaultDeadlockResolversSize = 10
	defaultStateTTL              = 7 * 24 * 3600
)

type deadlockResolver struct {
	mutex             *sync.RWMutex
	reqIDLists        []common.ReqIDList
	runningReqIDLists []common.ReqIDList
}

func (resolver *deadlockResolver) end(req *common.Request) {
	req.GetResponse()
	resolver.mutex.Lock()
	defer resolver.mutex.Unlock()
	runningReqIDListsLen := len(resolver.runningReqIDLists)
	if runningReqIDListsLen > maxCallStackDepth {
		log.GetLogger().Errorf("depth of call stack exceeds maximum limit, funcName: %s, reqIDList: %s, "+
			"resolver.reqIDLists: %s, resolver.runningReqIDLists: %s", req.FuncName, short.List(req.ReqIDList),
			short.Lists(resolver.reqIDLists), short.Lists(resolver.runningReqIDLists))
		req.SetResponse(common.SystematicError(common.ECallStackDepthExceedsLimit,
			"depth of call stack exceeds maximum limit"))
		return
	}
	if runningReqIDListsLen == 0 {
		log.GetLogger().Errorf("invalid length of running requestID list, funcName: %s", req.FuncName)
		req.SetResponse(common.SystematicError(common.EInvalidParam, "invalid length of running requestID list"))
		return
	}
	runningReqIDLists := make([]common.ReqIDList, runningReqIDListsLen-1)
	find := false
	for _, reqIDList := range resolver.runningReqIDLists {
		if reqnode.Equals(req.ReqIDList, reqIDList) {
			find = true
			break
		}
		runningReqIDLists = append(runningReqIDLists, reqIDList)
	}
	if find == false {
		log.GetLogger().Errorf("error in deadlock resolver, funcName: %s, reqIDList: %s, resolver.reqIDLists: %s, "+
			"resolver.runningReqIDLists: %s", req.FuncName, short.List(req.ReqIDList),
			short.Lists(resolver.reqIDLists), short.Lists(resolver.runningReqIDLists))
	}
	resolver.runningReqIDLists = runningReqIDLists
}

// StateQueue represents a state queue that schedules invoke requests.
type StateQueue struct {
	g *Map
	// since there may be multiple running request when we check and resolve deadlock, we need to lock when read or write fields in queue.
	mutex             *sync.RWMutex
	root              *reqnode.Node
	head              *reqnode.Node
	curReq            *reqnode.Node
	metadata          *metadata2.Metadata
	deadlockResolvers map[string]*deadlockResolver
	// get state by state client when creating the queue.
	state     string
	terminate bool
	ttl       int
}

func newStateQueue(g *Map, metadata *metadata2.Metadata, state string) *StateQueue {
	q := &StateQueue{
		g:                 g,
		metadata:          metadata,
		state:             state,
		mutex:             new(sync.RWMutex),
		head:              reqnode.NewHead(),
		deadlockResolvers: make(map[string]*deadlockResolver, defaultDeadlockResolversSize),
		terminate:         false,
		ttl:               defaultStateTTL,
	}
	if config.Conf.ResolveDeadlock {
		q.root = new(reqnode.Node)
	}
	return q
}

// GetMetaData return queue's metadata
func (q *StateQueue) GetMetaData() *metadata2.Metadata {
	return q.metadata
}

// GetTTL gets the TTL of the state.
func (q *StateQueue) GetTTL() int {
	return q.ttl
}

// SetState the state.
func (q *StateQueue) SetState(state string) {
	q.state = state
}

func (q *StateQueue) getNodeOrItsDescendants(list common.ReqIDList) *reqnode.Node {
	if q.curReq == nil {
		return nil
	}
	return q.curReq.GetNodeOrItsDescendants(list)
}

// addRequest to the queue.
func (q *StateQueue) addRequest(r *common.Request) (*reqnode.Node, common.TaskError) {
	node := reqnode.New(r)
	err := node.Add(q.root, q.head)
	if err != nil {
		log.GetLogger().Errorf("invalid request, err: %s", err.Error())
		return nil, common.NewTaskError(common.EFunctionTask, err.Error())
	}
	return node, nil
}

func (q *StateQueue) newReqNode(r *common.Request) *reqnode.Node {
	node := reqnode.New(r)
	return node
}

func (q *StateQueue) isEmpty() bool {
	return q.head.Next == q.head
}
