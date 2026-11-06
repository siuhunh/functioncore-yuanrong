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

// Package testcontext implements unit test for api.
package testcontext

import (
	"time"

	uuid "github.com/satori/go.uuid"

	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/functiontask/task/api/samenode"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/efs"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// Context defines task context used for testing
type Context struct {
	taskcontext.LoggerImpl
	taskcontext.ReqIDListImpl
	taskcontext.StateQueueMapImpl
	taskcontext.FutureStoreImpl
	taskcontext.ExtFutureStoreImpl
	taskcontext.ServiceIDImpl

	timeout time.Duration
	// DisableCreateState true will stop adding state to stateMapping.
	// so we can simulate situation that  state node lost forever.
	DisableCreateState bool
}

func (c *Context) IsNewStateKeyScenario() bool {
	return false
}

func (c *Context) GetFuncStateKey(funcName string, clientInstance bgtypes.ClientInstance) (string, common.TaskError) {
	return "", nil
}

func (c *Context) Release(funcName string, clientInstance bgtypes.ClientInstance) (bool, common.TaskError) {
	return false, nil
}

// New returns a new Context for test.
func New(method int32) *Context {
	c := &Context{}
	c.LoggerImpl.Init()
	c.SetExtFutureStore(new(efs.EmptyExtFutureStore))
	c.SetStateQueue(statequeue.NewStateQueueMap(
		newTestInvoker(),
		stateclient.NewInMemoryStateStore(),
		statequeue.DefaultConfig(),
	))
	c.SetFutureStore(futurestore.New(), futurestore.New())
	c.SetSource(common.SourceRuntimeToCentralFunctionTask)
	c.SetReqIDList(common.ReqIDList{uuid.NewV4().String()})
	c.SetMethod(method)
	return c
}

// TenantID returns tenant id.
func (c *Context) TenantID() string {
	return ""
}

// InvokeConfig describes config for invoke.
func (c *Context) InvokeConfig(version string, callKernel bool, futureID string, requestAttributes map[string]int32) interface{} {
	return nil
}

// CallTask calls another function task.
// req should be a pointer to a struct, and the struct must have a field named StateID with type string,
// resp should be a pointer to a struct, and the struct must have
// a field named Code with type int and a field named Message with type string.
func (c *Context) CallTask(method int32, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {

	// Copy constructor
	if _, ok := stateMapping[req.GetStateID()]; !ok {
		resp.SetCode(common.ScheduleInsErr)
		resp.SetMessage("failed to send call request")
		return
	}
	cc := *c
	samenode.CallTask(&cc, method, req, resp)
}

var stateMapping = make(map[string]bool)

// CreateState creates a new state mapping.
func (c *Context) CreateState(funcName string, stateID string) (string, string, common.TaskError) {
	if stateID == taskcontext.NewState {
		stateID = uuid.NewV4().String()
	}
	if c.DisableCreateState {
		return stateID, "", nil
	}
	stateMapping[stateID] = true
	return stateID, "", nil
}

// SetResult sets the result by funcCallID.
func (c *Context) SetResult(resultStruct taskcontext.Serializable) {
	log.GetLogger().Errorf("unsupported SetResult")
}

// SetResultWithLog sets the result with log by funcCallID.
func (c *Context) SetResultWithLog(resultStruct taskcontext.Serializable, logResult string) {
	log.GetLogger().Errorf("unsupported SetResultWithLog")
}

// SetResultWithCode sets the result with code by funcCallID.
func (c *Context) SetResultWithCode(code int32, resultStruct interface{}, logResult string) {
	log.GetLogger().Errorf("unsupported SetResultWithCode")
}

// IsSameNode returns whether the src and dest are the same node
func (c *Context) IsSameNode(stateID string) bool {
	return true
}

// Clone returns a duplicate context.
func (c *Context) Clone() taskcontext.Context {
	clone := *c
	return &clone
}

// SetIsGetInstanceScenario --
func (c *Context) SetIsGetInstanceScenario(flag bool) {
	return
}

// IsGetInstanceScenario --
func (c *Context) IsGetInstanceScenario() bool {
	return false
}
