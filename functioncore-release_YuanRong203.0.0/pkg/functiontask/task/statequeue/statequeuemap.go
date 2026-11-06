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
	"fmt"
	"strings"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/uuid"
	core "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/statequeue/reqnode"
	"functioncore/pkg/functiontask/task/util"
	"functioncore/pkg/functiontask/task/util/short"
)

// QueueConfig represents the configurations of the state queue.
type QueueConfig struct {
}

// DefaultConfig return the default configuration of the state queue.
func DefaultConfig() *QueueConfig {
	return &QueueConfig{}
}

// Map represents a map of the state queue.
type Map struct {
	mutex         *sync.RWMutex
	config        *QueueConfig
	stateQueueMap map[string]*StateQueue
	Invoker       common.Invoker
	StateClient   stateclient.StateClient
}

// NewStateQueueMap returns a new state queue map.
func NewStateQueueMap(invoker common.Invoker, stateClient stateclient.StateClient, config *QueueConfig) *Map {
	return &Map{
		stateQueueMap: make(map[string]*StateQueue),
		Invoker:       invoker,
		StateClient:   stateClient,
		config:        config,
		mutex:         new(sync.RWMutex),
	}
}

func (g *Map) requestTerminate(q *StateQueue, node *reqnode.Node, concurrency bool) {
	log.GetLogger().Debugf("terminate, reqIDList: %s, serviceID: %s, stateID: %s",
		short.List(node.Req.ReqIDList), q.metadata.ServiceID, q.metadata.StateID)
	r := node.Req
	// if it fails, it will be deleted when expired.
	err := g.deleteState(q)

	if err != nil {
		log.GetLogger().Errorf("terminate error, reqIDList: %s, serviceID: %s, stateID: %s, state: %s",
			short.List(node.Req.ReqIDList), q.metadata.ServiceID, q.metadata.StateID, q.state)
		r.SetResponse(common.SystematicError(err.Code(), err.Error()))
	}

	q.mutex.Lock()
	if q.head.Next != node && !concurrency {
		log.GetLogger().Warnf("the first request in queue should be the terminate request, stateID: %s",
			q.metadata.StateID)
	}
	// skip the terminate request.
	p := q.head.Next
	// response the remaining request.
	for {
		p = p.Next
		if p == q.head {
			break
		}
		log.GetLogger().Errorf("state queue has been terminated, reqIDList: %s, stateID: %s, state: %s",
			short.List(q.curReq.Req.ReqIDList), q.metadata.StateID, q.state)
		msg := "state queue has been terminated"
		p.Req.SetResponse(common.SystematicError(common.EStateIDNotFound, msg))
	}
	q.mutex.Unlock()
	g.mutex.Lock()
	delete(g.stateQueueMap, q.metadata.Key())
	g.mutex.Unlock()

	if err == nil {
		r.SetResponse(&common.Response{
			Code:   common.EOK,
			Result: `{"result":"DELETE SUCCESSFULLY"}`,
		})
	}
}

func (g *Map) deleteState(q *StateQueue) common.TaskError {
	err := g.StateClient.DeleteStateValueAndStateMapping(q.metadata.Key())
	if err != nil {
		return err
	}

	snErr := util.DeleteStateInRedis(q.metadata.TenantID, q.metadata.ServiceID,
		q.metadata.FunctionName, q.metadata.StateID)

	if snErr != nil {
		log.GetLogger().Warnf("failed to delete state: %s funcName: %s in redis errCode: %s, error: %s",
			q.metadata.StateID, q.metadata.FunctionName, snErr.Code(), snErr.Error())
	}
	return nil
}

func (g *Map) requestSetTimeout(q *StateQueue, node *reqnode.Node, concurrency bool) {
	log.GetLogger().Infof("setTimeout, reqIDList: %s, serviceID: %s, stateID: %s, state; %s",
		short.List(node.Req.ReqIDList), q.metadata.ServiceID, q.metadata.StateID, q.state)
	if !config.Conf.PersistentState {
		node.Req.SetResponse(&common.Response{
			Code:   common.EUnsupported,
			Result: "Unsupported method: setTimeout",
		})
	} else {
		err := g.StateClient.SetTTL(q.metadata.Key(), q.ttl)
		if err != nil {
			log.GetLogger().Errorf("failed to set ttl by resetexpiredttl api, reqIDList: %s, stateID: %s, "+
				"ttl: %d, err: %s", short.List(node.Req.ReqIDList), q.metadata.StateID, q.ttl, err.Error())
			node.Req.SetResponse(common.SystematicError(
				err.Code(), err.Error()))
		} else {
			log.GetLogger().Debugf("succeed to set ttl by resetexpiredttl api, stateID: %s, ttl: %d",
				q.metadata.StateID, q.ttl)
			node.Req.SetResponse(&common.Response{
				Code:   common.EOK,
				Result: `{"result":"SET TTL SUCCESS"}`,
			})
		}
	}
	// SetTimeout Request always belong to root children.
	if !concurrency {
		node.Remove()
		if q.head.Next != q.head {
			g.schedule(q, q.head.Next)
			return
		}
		q.curReq = nil
	}
}

func (g *Map) scheduleStatefulFuncWithConcurrency(q *StateQueue, node *reqnode.Node) {
	r := node.Req
	go func() {
		if r.RequestType == common.TerminateRequest {
			g.requestTerminate(q, node, true)
			return
		}
		if r.RequestType == common.SetTimeoutRequest {
			g.requestSetTimeout(q, node, true)
			return
		}
		log.GetLogger().Debugf("schedule, funcName: %s, stateID: %s, state: %s, reqIDList: %s", r.FuncName,
			q.metadata.StateID, q.state, short.List(r.ReqIDList))
		resp := g.Invoker.Invoke(&common.InvokeArgs{
			InvokeConfig: r.Config,
			FuncName:     r.FuncName,
			Params:       r.Params,
			StateID:      q.metadata.StateID,
			State:        q.state,
			ReqIDList:    r.ReqIDList,
		})
		oldState := q.state
		log.GetLogger().Debugf("done, reqIDList: %s, funcName: %s, stateID: %s, code: %d, message: %s, result: %s, "+
			"oldState: %s, state: %s", short.List(r.ReqIDList), r.FuncName, q.metadata.StateID, resp.Code,
			resp.Message, resp.Result, oldState, resp.State)
		if resp.Code == common.InitStateErr {
			g.mutex.Lock()
			delete(g.stateQueueMap, q.metadata.Key())
			g.mutex.Unlock()
		}
		r.SetResponse(resp)
	}()
}

func (g *Map) schedule(q *StateQueue, node *reqnode.Node) {
	q.curReq = node
	go func() {
		r := node.Req
		if r.RequestType == common.TerminateRequest {
			g.requestTerminate(q, node, false)
			return
		}
		if r.RequestType == common.SetTimeoutRequest {
			g.requestSetTimeout(q, node, false)
			return
		}
		log.GetLogger().Infof("schedule, funcName: %s, stateID: %s, state: %s, reqIDList: %s", r.FuncName,
			q.metadata.StateID, q.state, short.List(r.ReqIDList))
		resp := g.Invoker.Invoke(&common.InvokeArgs{
			InvokeConfig: r.Config,
			FuncName:     r.FuncName,
			Params:       r.Params,
			StateID:      q.metadata.StateID,
			State:        q.state,
			ReqIDList:    r.ReqIDList,
		})
		log.GetLogger().Debugf("schedule done, reqIDList: %s, funcName: %s, stateID: %s, code: %d, message: %s, result: %s",
			short.List(r.ReqIDList), r.FuncName, q.metadata.StateID, resp.Code, resp.Message, resp.Result)
		q.mutex.Lock()
		g.done(q, node, resp)
		q.mutex.Unlock()
	}()
}

// The caller guarantees that q is not null.
func (g *Map) syncStateToExternalStorage(q *StateQueue, req *common.Request, resp *common.Response) common.TaskError {
	if req != nil && resp != nil && config.Conf.PersistentFuture {
		err := g.StateClient.SetStateAndResult(q.metadata, q.state, q.ttl, req, resp)
		if err != nil {
			log.GetLogger().Errorf("failed to SetStateAndResult, err: %s, state: %s, stateID: %s",
				err.Error(), q.state, q.metadata.StateID)
			return err
		}
		return nil
	}
	err := g.StateClient.Set(q.metadata.Key(), q.state, q.ttl)
	if err != nil {
		log.GetLogger().Errorf("failed to set state by state client, err: %s, state: %s, stateID: %s",
			err.Error(), q.state, q.metadata.StateID)
		return err
	}
	return nil
}

func (g *Map) done(q *StateQueue, node *reqnode.Node, resp *common.Response) {
	r := node.Req
	oldState := q.state
	log.GetLogger().Infof("done, reqIDList: %s, funcName: %s, stateID: %s, code: %d, message: %s, result: %s, "+
		"oldState: %s, state: %s, curFuncName: %s, curReqIDList: %s", short.List(r.ReqIDList), r.FuncName,
		q.metadata.StateID, resp.Code, resp.Message, resp.Result, oldState, resp.State, q.curReq.Req.FuncName,
		short.List(q.curReq.Req.ReqIDList))

	if resp.Code == common.InitStateErr {
		g.mutex.Lock()
		delete(g.stateQueueMap, q.metadata.Key())
		g.mutex.Unlock()
	}
	r.SetResponse(resp)
	g.next(q, node)
}

func (g *Map) next(q *StateQueue, node *reqnode.Node) {
	parent := node.Parent
	node.Remove()

	if q.curReq != node {
		log.GetLogger().Errorf("dataRace, cur: %s, done: %s", q.curReq.Req.FuncName, node.Req.FuncName)
		q.curReq.Req.SetResponse(common.SystematicError(common.EUnacceptableUncertainty,
			"data race between "+q.curReq.Req.FuncName+" and "+node.Req.FuncName))
		return
	}
	// schedule next request and update current request.
	if config.Conf.ResolveDeadlock {
		log.GetLogger().Debugf("q.root.Children, len: %d", len(q.root.Children))
		if parent != q.root {
			q.curReq = parent
			return
		}
	}
	if q.head.Next != q.head {
		g.schedule(q, q.head.Next)
		return
	}
	q.curReq = nil
}

// StartCheckAndResolveDeadlock check whether there is a deadlock, and if so,
// find the request causing the deadlock and schedule the execution.
func (g *Map) StartCheckAndResolveDeadlock(metadata *metadata.Metadata,
	state string, futureReqIDLists []common.ReqIDList) (string, common.TaskError) {
	q := g.GetQueue(metadata)
	if q == nil {
		return "", common.NewTaskError(common.EStateIDNotFound,
			fmt.Sprintf("no such state queue with id: %s", metadata.StateID))
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.curReq == nil {
		if q.head.Next != q.head {
			log.GetLogger().Warnf("fatal error, concurrency problem occurs, stateID: %s", q.metadata.StateID)
		}
		return "", nil
	}

	log.GetLogger().Debugf("startCheckAndResolveDeadlock, stateID: %s, state: %s, oldState: %s, "+
		"futureReqIDLists: %s, curReqIDList: %s", q.metadata.StateID, state, q.state, short.Lists(futureReqIDLists),
		short.List(q.curReq.Req.ReqIDList))

	needSchedule := false
	for _, futureReqIDList := range futureReqIDLists {
		if reqnode.IsParentOf(q.curReq.Req.ReqIDList, futureReqIDList, false) {
			needSchedule = true
		}
	}
	if !needSchedule {
		return "", nil
	}
	return g.scheduleDeadlock(q, state, futureReqIDLists)
}

func (g *Map) scheduleDeadlock(q *StateQueue, state string,
	futureReqIDLists []common.ReqIDList) (string, common.TaskError) {
	deadlockID := uuid.New().String()
	log.GetLogger().Debugf("resolveDeadlock register callback, stateID: %s, deadlockID: %s",
		q.metadata.StateID, deadlockID)
	q.state = state
	resolver := &deadlockResolver{
		mutex:             new(sync.RWMutex),
		reqIDLists:        futureReqIDLists,
		runningReqIDLists: []common.ReqIDList{},
	}
	q.deadlockResolvers[deadlockID] = resolver
	// find the request whose reqIDList is futureReqIDList or the descendant of futureReqIDList.
	for _, futureReqIDList := range futureReqIDLists {
		node := q.getNodeOrItsDescendants(futureReqIDList)
		if node != nil {
			log.GetLogger().Debugf("resolveDeadlock, schedule: %s, cur: %s, scheduleReqIDList: %s, "+
				"curReqIDList: %s, stateID: %s, state: %s, futureReqIDList: %s", node.Req.FuncName,
				q.curReq.Req.FuncName, short.List(node.Req.ReqIDList), short.List(q.curReq.Req.ReqIDList),
				q.metadata.StateID, q.state, strings.Join(futureReqIDList, ","))
			g.schedule(q, node)
			resolver.mutex.Lock()
			resolver.runningReqIDLists = append(resolver.runningReqIDLists, node.Req.ReqIDList)
			resolver.mutex.Unlock()
			// multiply deadlocks
			break
		}
	}
	return deadlockID, nil
}

// EndCheckAndResolveDeadlock and StartCheckAndResolveDeadlock are used in pairs.
func (g *Map) EndCheckAndResolveDeadlock(metadata *metadata.Metadata, deadlockID string) common.TaskError {
	log.GetLogger().Debugf("endCheckAndResolveDeadlock, stateID: %s, deadlockID: %s", metadata.StateID, deadlockID)
	if deadlockID == "" {
		return nil
	}
	q := g.GetQueue(metadata)
	if q == nil {
		return common.NewTaskError(common.EStateIDNotFound, "no such state queue with id: "+metadata.StateID)
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.deadlockResolvers[deadlockID] == nil {
		return common.NewTaskError(common.EFunctionTask, "invalid deadlock id: "+deadlockID)
	}
	delete(q.deadlockResolvers, deadlockID)
	return nil
}

// CreateStateQueue creates a state queue by state id.
func (g *Map) CreateStateQueue(metadata *metadata.Metadata) common.TaskError {
	g.mutex.Lock()
	_, err := g.checkStateQueue(metadata, true)
	g.mutex.Unlock()
	return err
}

// CheckAndClearOldStateQueue you need to clear the old one when create a new one
func (g *Map) CheckAndClearOldStateQueue(meta *metadata.Metadata) {
	g.mutex.Lock()
	q, exist := g.stateQueueMap[meta.Key()]
	if !exist {
		g.mutex.Unlock()
		return
	}
	// skip the terminate request.
	p := q.head.Next
	// response the remaining request.
	for {
		p = p.Next
		if p == q.head {
			break
		}
		log.GetLogger().Errorf("state queue has been terminated, reqIDList: %s, stateID: %s, state: %s",
			short.List(q.curReq.Req.ReqIDList), q.metadata.StateID, q.state)
		msg := "state queue has been terminated"
		p.Req.SetResponse(common.SystematicError(common.EStateIDNotFound, msg))
	}
	delete(g.stateQueueMap, meta.Key())
	g.mutex.Unlock()
}

func (g *Map) checkStateQueue(meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
	log.GetLogger().Debugf("checkStateQueue start, stateID: %s, stateKey: %s, isCreate: %t",
		meta.Key(), meta.StateHashKey(), create)
	key := meta.Key()
	if queue := g.stateQueueMap[key]; queue != nil {
		return queue, nil
	}
	if !config.Conf.PersistentState {
		return g.ifNotPersistentState(meta, create)
	}
	recoverStateQueue := false
	state, err := g.StateClient.Get(key)
	if err == nil {
		recoverStateQueue = true
	} else {
		log.GetLogger().Warnf("getting client's state failed, stateID: %s, stateKey: %s, err: %s",
			meta.Key(), meta.StateHashKey(), err.Error())
		// if it not unexpected error expect not exist, continue
		if !g.StateClient.IsNotExist(err) {
			log.GetLogger().Warnf("get stateID: %s, stateKey: %s from redis failed, err: %s",
				meta.StateID, meta.Key(), err.Error())
		}
	}
	if recoverStateQueue {
		log.GetLogger().Debugf("recoverStateQueue, stateID: %s, stateKey: %s, state: %s",
			meta.Key(), meta.StateHashKey(), state)
		g.stateQueueMap[key] = newStateQueue(g, meta, state)
		return g.stateQueueMap[key], nil
	}
	if create {
		log.GetLogger().Debugf("Need to create state queue, stateID: %s", meta.StateID)
		return g.createStateQueue(meta, state)
	}
	// if the state hash exists, we need to create a state queue.
	stateHashKey := metadata.GeneStateHashKey(meta.TenantID, meta.ServiceID, meta.FunctionName)
	exist, _ := core.HExists(stateHashKey, meta.StateID)
	if exist {
		log.GetLogger().Infof("stateHashKey exists in the redis, need to create state queue, stateID: %s",
			meta.StateID)
		return g.createStateQueue(meta, state)
	}
	log.GetLogger().Infof("stateHashKey: %s, stateID: %s is not exist", stateHashKey, meta.StateID)

	// if not found in redis, last check in meta etcd stateworkers cache
	existInETCD := servicediscovery.GetRegistry().IsStateInsExist(meta.StateID)
	if existInETCD {
		log.GetLogger().Infof("stateID: %s exists in meta etcd, need to create state queue",
			meta.StateID)
		return g.createStateQueue(meta, state)
	}
	log.GetLogger().Infof("stateID: %s is not exist in meta etcd", meta.StateID)
	return g.stateQueueMap[key], nil
}

func (g *Map) ifNotPersistentState(meta *metadata.Metadata, create bool) (*StateQueue, common.TaskError) {
	var queue *StateQueue
	key := meta.Key()
	if create {
		if err := util.HasExceedNumsError(g.StateClient, meta.TenantID, meta.ServiceID); err != nil {
			log.GetLogger().Warnf("get nums of stateID by serviceID, stateID: %s, stateKey: %s, err: %s",
				meta.Key(), meta.StateHashKey(), err.Error())
			return nil, common.NewTaskError(common.ETooManyStateInService, err.Error())
		}
		queue = newStateQueue(g, meta, "")
		g.stateQueueMap[key] = queue
	}
	return queue, nil
}

func (g *Map) createStateQueue(meta *metadata.Metadata, state string) (*StateQueue, common.TaskError) {
	key := meta.Key()
	if err := util.HasExceedNumsError(g.StateClient, meta.TenantID, meta.ServiceID); err != nil {
		log.GetLogger().Warnf("get nums of stateID by serviceID %s, stateID: %s, err: %s",
			meta.ServiceID, meta.StateID, err.Error())
		return nil, common.NewTaskError(common.ETooManyStateInService, err.Error())
	}
	log.GetLogger().Debugf("createStateQueue start, functionName: %s, stateID: %s , stateKey: %s",
		meta.FunctionName, meta.StateID, meta.StateKey)
	g.stateQueueMap[key] = newStateQueue(g, meta, state)
	log.GetLogger().Debugf("createStateQueue finish, functionName: %s, stateID: %s , stateKey: %s",
		meta.FunctionName, meta.StateID, meta.StateKey)
	return g.stateQueueMap[key], nil
}

// Request receives a request with state id, and will schedule it when appropriate.
func (g *Map) Request(metadata *metadata.Metadata, r *common.Request, create bool) common.TaskError {
	if configReq, ok := r.Config.(*core.MsgConfig); ok {
		if configReq.FuncVersion != "" && configReq.FuncVersion != "latest" {
			metadata.FunctionName = metadata.FunctionName + ":" + configReq.FuncVersion
		}
	}
	concurrentNum, error := core.FunctionCheck.GetStatefulFunctionConcurrentNum(metadata.TenantID,
		metadata.FunctionName, metadata.ServiceID)
	if error != nil {
		log.GetLogger().Errorf("failed to get concurrentNum, funcName: %s, stateID: %s, err: %s",
			metadata.FunctionName, metadata.StateID, error.Error())
	}
	log.GetLogger().Debugf("request [%s] with state id, funcName: %s, stateID: %s, reqIDList: %s, concurrentNum %d",
		r.RequestType, metadata.FunctionName, metadata.StateID, short.List(r.ReqIDList), concurrentNum)

	g.mutex.Lock()
	defer g.mutex.Unlock()
	q, err := g.checkStateQueue(metadata, create)
	if err != nil {
		log.GetLogger().Errorf("failed to get state queue when request with state id,"+
			"funcName: %s, stateID: %s, reqIDList: %s, error: %s",
			metadata.FunctionName, metadata.StateID, short.List(r.ReqIDList), err.Error())
		r.SetResponse(common.SystematicError(err.Code(), err.Error()))
		return err
	}
	if q == nil {
		log.GetLogger().Errorf("request with state id, err: %s, reqIDList: %s",
			"failed to get state queue by state id: "+metadata.StateID, short.List(r.ReqIDList))
		r.SetResponse(common.SystematicError(common.EStateIDNotFound,
			"failed to get state queue by state id: "+metadata.StateID))
		return common.NewTaskError(common.EStateIDNotFound,
			fmt.Sprintf("failed to get state queue by state id: %s", metadata.StateID))
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.terminate {
		log.GetLogger().Errorf("request with state id, err: %s, reqIDList: %s",
			"terminate state queue, state id: "+metadata.StateID, short.List(r.ReqIDList))
		r.SetResponse(common.SystematicError(common.EStateIDNotFound,
			"terminate state queue, state id: "+metadata.StateID))
		return common.NewTaskError(common.EStateIDNotFound,
			fmt.Sprintf("failed to get state queue by state id: %s", metadata.StateID))
	}
	if r.RequestType == common.TerminateRequest {
		q.terminate = true
	}
	if r.RequestType == common.SetTimeoutRequest {
		q.ttl = r.TTL
	}
	if concurrentNum > 1 {
		g.scheduleStatefulFuncWithConcurrency(q, q.newReqNode(r))
		return nil
	}
	newReq, err := q.addRequest(r)
	if err != nil {
		return err
	}
	if q.curReq == nil {
		g.schedule(q, newReq)
		return nil
	}
	if config.Conf.ResolveDeadlock {
		g.resolveDeadlock(q, newReq)
	}
	return nil
}

func (g *Map) resolveDeadlock(q *StateQueue, newReq *reqnode.Node) {
	for id, resolver := range q.deadlockResolvers {
		log.GetLogger().Debugf("resolveDeadlock check, deadlockID: %s, reqIDList: %s, deadlockReqIDLists: %s", id,
			short.List(newReq.Req.ReqIDList), short.Lists(resolver.reqIDLists))
		needSchedule := true
		for _, runningReqIDList := range resolver.runningReqIDLists {
			if reqnode.IsParentOf(runningReqIDList, newReq.Req.ReqIDList, true) {
				needSchedule = false
				log.GetLogger().Debugf("resolveDeadlock check, funcName: %s, stateID: %s, reqIDList: %s, "+
					"runningReqIDList: %s", newReq.Req.FuncName, q.metadata.StateID, short.List(newReq.Req.ReqIDList),
					short.List(runningReqIDList))
			}
		}
		if !needSchedule {
			continue
		}
		for _, futureReqIDList := range resolver.reqIDLists {
			if reqnode.IsParentOf(futureReqIDList, newReq.Req.ReqIDList, true) {
				log.GetLogger().Debugf("resolveDeadlock callback, funcName: %s, stateID: %s, reqIDList: %s, "+
					"futureReqIDList: %s", newReq.Req.FuncName, q.metadata.StateID, short.List(newReq.Req.ReqIDList),
					short.List(futureReqIDList))
				resolver.mutex.Lock()
				resolver.runningReqIDLists = append(resolver.runningReqIDLists, newReq.Req.ReqIDList)
				resolver.mutex.Unlock()
				g.schedule(q, newReq)
				go resolver.end(newReq.Req)
			}
		}
	}
}

// GetQueueWithRecover returns the queue by stateID from cache. If no state queue exits, recovers from the redis.
func (g *Map) GetQueueWithRecover(metadata *metadata.Metadata) *StateQueue {
	g.mutex.Lock()
	q, err := g.checkStateQueue(metadata, false)
	if err != nil {
		g.mutex.Unlock()
		log.GetLogger().Errorf("failed to get state queue when recover, funcName: %s, stateID: %s, error: %s",
			metadata.FunctionName, metadata.StateID, err.Error())
		return nil
	}
	g.mutex.Unlock()
	return q
}

// GetQueue returns the queue by stateID.
func (g *Map) GetQueue(metadata *metadata.Metadata) *StateQueue {
	g.mutex.RLock()
	q := g.stateQueueMap[metadata.Key()]
	g.mutex.RUnlock()
	return q
}

// GetState returns the state by stateID in state queue.
func (g *Map) GetState(metadata *metadata.Metadata) (string, common.TaskError) {
	log.GetLogger().Debugf("get State, stateID: %s", metadata.StateID)
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	if q := g.stateQueueMap[metadata.Key()]; q != nil {
		log.GetLogger().Infof("getStateLockQueue, stateID: %s, funcName: %s, state: %s",
			metadata.StateID, metadata.FunctionName, q.state)
		q.mutex.RLock()
		defer q.mutex.RUnlock()
		return q.state, nil
	}
	return "", common.NewTaskError(common.EStateIDNotFound,
		fmt.Sprintf("no such queue of the state id %s", metadata.StateID))
}

// UpdateState update state
func (g *Map) UpdateState(metadata *metadata.Metadata, state string) {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	if q := g.stateQueueMap[metadata.Key()]; q != nil {
		q.mutex.Lock()
		defer q.mutex.Unlock()
		q.SetState(state)
	}
}

// GetStateCount get functionTask state count.
func (g *Map) GetStateCount() int {
	g.mutex.Lock()
	l := len(g.stateQueueMap)
	g.mutex.Unlock()
	return l
}

// GetStateSize get functionTask state size.
func (g *Map) GetStateSize() int {
	g.mutex.Lock()
	size := 0
	for _, queue := range g.stateQueueMap {
		queue.mutex.RLock()
		size += len(queue.state)
		queue.mutex.RUnlock()
	}
	g.mutex.Unlock()
	return size
}

// DelStateIfIdle delete the state by stateID if there is no request in state queue.
func (g *Map) DelStateIfIdle(stateID string) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if q := g.stateQueueMap[stateID]; q != nil {
		q.mutex.RLock()
		defer q.mutex.RUnlock()
		if !q.isEmpty() {
			// recover state in external storage.
			err := g.syncStateToExternalStorage(q, nil, nil)
			if err != nil {
				log.GetLogger().Errorf("delete state if no request, err: %s", err.Error())
			}
			return false
		}
		delete(g.stateQueueMap, stateID)
		return true
	}
	return false
}

// DelState delete the state by stateID
func (g *Map) DelState(stateID string) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if q := g.stateQueueMap[stateID]; q != nil {
		delete(g.stateQueueMap, stateID)
		return true
	}
	return false
}
