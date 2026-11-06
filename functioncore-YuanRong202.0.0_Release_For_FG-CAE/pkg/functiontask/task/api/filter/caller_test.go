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
package filter

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

func TestCaller(t *testing.T) {
	Convey("TestCaller", t, func() {
		req := &RequestPb{}
		taskCtx := &Context{}
		caller := req.Caller(taskCtx)
		assert.NotNil(t, caller)
	})
}

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

func (c *Context) TenantID() string {
	return ""
}

func (c *Context) InvokeConfig(version string, callKernel bool, futureID string, requestAttributes map[string]int32) interface{} {
	return nil
}

func (c *Context) CallTask(method int32, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
}

func (c *Context) CreateState(funcName string, stateID string) (string, string, common.TaskError) {
	return stateID, "", nil
}

func (c *Context) SetResult(resultStruct taskcontext.Serializable) {
}

func (c *Context) SetResultWithLog(resultStruct taskcontext.Serializable, logResult string) {
}

func (c *Context) SetResultWithCode(code int32, resultStruct interface{}, logResult string) {
}

func (c *Context) IsSameNode(stateID string) bool {
	return true
}

func (c *Context) Clone() taskcontext.Context {
	return c
}

func (c *Context) SetIsGetInstanceScenario(flag bool) {
	return
}

func (c *Context) IsGetInstanceScenario() bool {
	return false
}
