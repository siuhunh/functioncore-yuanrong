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

package service

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"gotest.tools/assert"

	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/api/terminate"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/taskimpl/corecontext"
	"functioncore/test/functiontask/test/testcontext"
)

func Test_clearOneWithRetry(t *testing.T) {
	patch := gomonkey.ApplyFunc(clearOne, func(taskCtx taskcontext.Context, id string) common.TaskError { return nil })
	defer patch.Reset()
	type args struct {
		taskCtx taskcontext.Context
		id      string
	}
	hookArgs := &functioncore.HookArgs{}
	taskCtx := corecontext.New(1, hookArgs)
	var a args
	a.taskCtx = taskCtx
	a.id = "testid"
	var result common.TaskError
	tests := []struct {
		name string
		args args
		want common.TaskError
	}{
		{"success clear", a, result},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clearOneWithRetry(tt.args.taskCtx, tt.args.id); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("clearOneWithRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_clearOneWithRetry_2(t *testing.T) {
	patch := gomonkey.ApplyFunc(clearOne, func(taskCtx taskcontext.Context, id string) common.TaskError {
		return common.NewTaskError(common.EUnknownFutureID, "mock error")
	})
	defer patch.Reset()
	type args struct {
		taskCtx taskcontext.Context
		id      string
	}
	hookArgs := &functioncore.HookArgs{}
	taskCtx := corecontext.New(1, hookArgs)
	var a args
	a.taskCtx = taskCtx
	a.id = "testid"
	result := common.NewTaskError(common.EUnknownFutureID, "mock error")
	tests := []struct {
		name string
		args args
		want common.TaskError
	}{
		{"failed clear", a, result},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clearOneWithRetry(tt.args.taskCtx, tt.args.id); !reflect.DeepEqual(got.Error(), tt.want.Error()) {
				t.Errorf("clearOneWithRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

// Test_clearOne tests no result condition
func Test_clearOne(t *testing.T) {
	patchs := gomonkey.ApplyMethod(reflect.TypeOf(&terminate.Request{}), "Caller",
		func(_ *terminate.Request, taskCtx taskcontext.Context) taskcontext.Serializable {
			resp := &common.ErrorResponse{
				Code:    common.EFiniteStateMachine,
				Message: "no result",
			}
			return resp
		})
	defer patchs.Reset()
	type args struct {
		taskCtx taskcontext.Context
		id      string
	}
	var a args
	hookArgs := &functioncore.HookArgs{}
	taskCtx := corecontext.New(1, hookArgs)
	a.taskCtx = taskCtx
	a.id = "testid"
	result := common.NewTaskError(common.EFiniteStateMachine, "no result")
	tests := []struct {
		name string
		args args
		want common.TaskError
	}{
		{"case1", a, result},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := clearOne(tt.args.taskCtx, tt.args.id); !reflect.DeepEqual(got.Error(), tt.want.Error()) {
				t.Errorf("clearOne() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetAllStateID(t *testing.T) {
	context := testcontext.New(common.MethodGetFuture)
	_, err := GetAllStateID(context)
	assert.Equal(t, nil, err)
}

func TestClearAllState(t *testing.T) {
	context := testcontext.New(common.MethodGetFuture)
	err := ClearAllState(context)
	assert.Equal(t, nil, err)
}

func TestCheckErr(t *testing.T) {
	context := testcontext.New(common.MethodGetFuture)
	id := "1"
	code := common.ScheduleInsErr
	message := "test"
	err := checkErr(context, id, int32(code), message)
	assert.Equal(t, nil, err)

	code = common.EStateIDNotFound
	message = "test"
	err = checkErr(context, id, int32(code), message)
	assert.Equal(t, nil, err)

	code = common.ECallStackDepthExceedsLimit
	message = "test"
	err = checkErr(context, id, int32(code), message)
	assert.Equal(t, "test", err.Error())
}

func TestPostError(t *testing.T) {
	ch := make(chan common.TaskError, 10)
	ch <- common.NewTaskError(int32(1), "")
	postError(ch, common.NewTaskError(int32(2), "test"))

	ch = nil
	postError(ch, common.NewTaskError(int32(2), "test"))
}

func TestAssigner(t *testing.T) {
	context := testcontext.New(common.EFunctionTask)
	idMap := make(map[string]string)
	idMap["test1"] = "test1"
	wkr := func(taskcontext.Context, string) common.TaskError {
		return nil
	}
	err := Assigner(context, idMap, wkr)
	assert.Equal(t, nil, err)
}
