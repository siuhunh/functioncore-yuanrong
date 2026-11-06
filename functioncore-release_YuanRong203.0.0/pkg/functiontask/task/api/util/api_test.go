/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package util implements util api.
package util

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

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

func (c *Context) TraceID() string {
	return ""
}

func (c *Context) IsNewStateKeyScenario() bool {
	return false
}

func (c *Context) GetFuncStateKey(funcName string, clientInstance bridgetypes.ClientInstance) (string, common.TaskError) {
	return "", nil
}

func (c *Context) Release(funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError) {
	var err common.TaskError
	return false, err
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
	return
}

var stateMapping = make(map[string]bool)

// CreateState creates a new state mapping.
func (c *Context) CreateState(funcName string, stateID string) (string, string, common.TaskError) {
	if stateID == taskcontext.NewState {
		stateID = uuid.New().String()
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

type MockRequestWithStateID struct {
}

func (m *MockRequestWithStateID) GetStateID() string {
	return ""
}

func (m *MockRequestWithStateID) GetFuncName() string {
	return ""
}
func (m *MockRequestWithStateID) GetCreateState() bool {
	return true
}
func (m *MockRequestWithStateID) SetStateID(key string) {
	return
}

func Test_CreateStateIfNeeded(t *testing.T) {
	convey.Convey("Test CreateStateIfNeeded CreateState error", t, func() {
		var testCtx taskcontext.Context
		tc := &Context{}
		testCtx = tc
		patch := gomonkey.ApplyFunc(CreateState, func(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
			err := common.NewTaskError(500, "error")
			return "", err
		})
		defer patch.Reset()
		var req taskcontext.CreateStateRequest
		request := &MockRequestWithStateID{}
		req = request
		err := CreateStateIfNeeded(testCtx, req)
		if err == nil {
			t.Errorf("this is error")
		}
	})
}

func Test_GetFuncStateKey(t *testing.T) {
	convey.Convey("Test taskCtx.GetFuncStateKey error", t, func() {
		clientInstance := bridgetypes.ClientInstance{}
		var testCtx taskcontext.Context
		tc := &Context{}
		testCtx = tc
		patch := gomonkey.ApplyFunc((*Context).GetFuncStateKey, func(_ *Context, uncName string, clientInstance bridgetypes.ClientInstance) (string,
			common.TaskError) {
			err := common.NewTaskError(500, "error")
			return "", err
		})
		defer patch.Reset()
		res, err := GetFuncStateKey(testCtx, "funcName", clientInstance)
		if res != "" && err == nil {
			t.Errorf("this is error")
		}
	})

	convey.Convey("Test taskCtx.GetFuncStateKey succeed", t, func() {
		clientInstance := bridgetypes.ClientInstance{}
		var testCtx taskcontext.Context
		tc := &Context{}
		testCtx = tc
		patch := gomonkey.ApplyFunc((*Context).GetFuncStateKey, func(_ *Context, uncName string, clientInstance bridgetypes.ClientInstance) (string,
			common.TaskError) {
			return "test", nil
		})
		defer patch.Reset()
		res, err := GetFuncStateKey(testCtx, "funcName", clientInstance)
		if res == "" && err != nil {
			t.Errorf("this is error")
		}
	})
}

func Test_Release(t *testing.T) {
	convey.Convey("Test taskCtx.GetFuncStateKey error", t, func() {
		clientInstance := bridgetypes.ClientInstance{}
		var testCtx taskcontext.Context
		tc := &Context{}
		testCtx = tc
		patch := gomonkey.ApplyFunc((*Context).Release, func(_ *Context, funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError) {
			err := common.NewTaskError(500, "error")
			return false, err
		})
		defer patch.Reset()
		res, err := Release(testCtx, "funcName", clientInstance)
		if res {
			t.Errorf("this should be false")
		}
		if err == nil {
			t.Errorf("this is error")
		}
	})

	convey.Convey("Test taskCtx.GetFuncStateKey succeed", t, func() {
		clientInstance := bridgetypes.ClientInstance{}
		var testCtx taskcontext.Context
		tc := &Context{}
		testCtx = tc
		patch := gomonkey.ApplyFunc((*Context).Release, func(_ *Context, funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError) {
			return true, nil
		})
		defer patch.Reset()
		res, err := Release(testCtx, "funcName", clientInstance)
		if !res {
			t.Errorf("this should be true")
		}
		if err != nil {
			t.Errorf("this is error")
		}
	})
}

func (c *Context) SetIsGetInstanceScenario(flag bool) {
	return
}

func (c *Context) IsGetInstanceScenario() bool {
	return false
}
