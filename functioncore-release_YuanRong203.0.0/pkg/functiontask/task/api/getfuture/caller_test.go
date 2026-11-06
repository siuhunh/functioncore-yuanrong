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

package getfuture

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/utils"
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

func (c *Context) TraceID() string {
	return ""
}

func (c *Context) GetFuncStateKey(funcName string, clientInstance bridgetypes.ClientInstance) (string, common.TaskError) {
	return "", nil
}

func (c *Context) Release(funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError) {
	return false, nil
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
func (c *Context) CallTask(_ int32, _ taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
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

func (c *Context) IsNewStateKeyScenario() bool {
	return false
}

// Clone returns a duplicate context.
func (c *Context) Clone() taskcontext.Context {
	clone := *c
	return &clone
}

func TestRequest_getResolveDeadLock(t *testing.T) {
	type args struct {
		taskCtx   taskcontext.Context
		sncFuture *snfuture.Future
		checker   deadlock.Checker
	}
	tests := []struct {
		name        string
		req         Request
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test startResp.Code != 0
		{
			"case1",
			Request{},
			args{
				taskCtx:   &Context{},
				sncFuture: &snfuture.Future{},
				checker:   &deadlock.EmptyChecker{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&snfuture.Future{}), "PeekResponse",
						func(_ *snfuture.Future) *common.Response {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&deadlock.EmptyChecker{}), "Start",
						func(_ *deadlock.EmptyChecker, _ *deadlock.StartRequest) *deadlock.StartResponse {
							return &deadlock.StartResponse{
								Code: 100,
							}
						}),
				})

				return patches
			},
			true,
		},
		// test EndResp code != 0
		{
			"case2",
			Request{},
			args{
				taskCtx:   &Context{},
				sncFuture: &snfuture.Future{},
				checker:   &deadlock.EmptyChecker{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&snfuture.Future{}), "PeekResponse",
						func(_ *snfuture.Future) *common.Response {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&deadlock.EmptyChecker{}), "Start",
						func(_ *deadlock.EmptyChecker, _ *deadlock.StartRequest) *deadlock.StartResponse {
							return &deadlock.StartResponse{
								Code:       0,
								DeadlockID: "test",
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&deadlock.EmptyChecker{}), "End",
						func(_ *deadlock.EmptyChecker, _ *deadlock.EndRequest) *deadlock.EndResponse {
							return &deadlock.EndResponse{
								Code: 100,
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&snfuture.Future{}), "GetResponse",
						func(_ *snfuture.Future) *common.Response {
							return &common.Response{}
						}),
				})

				return patches
			},
			true,
		},
		// test all
		{
			"case3",
			Request{},
			args{
				taskCtx:   &Context{},
				sncFuture: &snfuture.Future{},
				checker:   &deadlock.EmptyChecker{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&snfuture.Future{}), "PeekResponse",
						func(_ *snfuture.Future) *common.Response {
							return nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&deadlock.EmptyChecker{}), "Start",
						func(_ *deadlock.EmptyChecker, _ *deadlock.StartRequest) *deadlock.StartResponse {
							return &deadlock.StartResponse{
								Code:       0,
								DeadlockID: "test",
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&deadlock.EmptyChecker{}), "End",
						func(_ *deadlock.EmptyChecker, _ *deadlock.EndRequest) *deadlock.EndResponse {
							return &deadlock.EndResponse{
								Code: 0,
							}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&snfuture.Future{}), "GetResponse",
						func(_ *snfuture.Future) *common.Response {
							return &common.Response{}
						}),
				})

				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, _ := tt.req.getResolveDeadLock(tt.args.taskCtx, tt.args.sncFuture, tt.args.checker)
			if (got != nil) != tt.want {
				t.Errorf("getResolveDeadLock() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func (c *Context) SetIsGetInstanceScenario(flag bool) {
	return
}

func (c *Context) IsGetInstanceScenario() bool {
	return false
}

func Test_Callee(t *testing.T) {
	req := &TaskRequest{}
	var taskCtx taskcontext.Context
	tc := &Context{FutureStoreImpl: taskcontext.FutureStoreImpl{}}
	taskCtx = tc
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc((*taskcontext.FutureStoreImpl).FutureStore, func(_ *taskcontext.FutureStoreImpl) *futurestore.FutureStore {
			return nil
		}),
		gomonkey.ApplyFunc((*futurestore.FutureStore).Get, func(_ *futurestore.FutureStore, id snfuture.ID) *snfuture.Future {
			return nil
		}),
		gomonkey.ApplyFunc((*futurestore.FutureStore).Delete, func(_ *futurestore.FutureStore, id string) {
		}),
	})
	defer patches.ResetAll()
	rep := req.Callee(taskCtx)
	if rep.Code == common.EInvalidParam {
		t.Errorf("this is error")
	}
	if rep.Code != common.EOK {
		t.Errorf("this is error")
	}
	if rep.Message != common.TaskGetFutureForwardMsg {
		t.Errorf("this is error")
	}
}
