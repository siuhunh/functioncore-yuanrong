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

// Package fsm implements finite state machine.
package fsm

import (
	"strconv"

	"functioncore/pkg/functiontask/task/api/fsm/statetype"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// State represents an state in fsm.
type State interface {
	Type() int
	Result() taskcontext.Serializable
	Future() *snfuture.Future
}

type baseState struct {
	typ int
}

// Type represents state type.
func (s *baseState) Type() int {
	return s.typ
}

// Result is not nil if the fsm is in the final state.
func (s *baseState) Result() taskcontext.Serializable {
	return &common.ErrorResponse{
		Code:    common.EFiniteStateMachine,
		Message: "no result",
	}
}

// Future is not nil if the future need to update by recovered future.
func (s *baseState) Future() *snfuture.Future {
	return nil
}

func newState(next int) State {
	return &baseState{
		typ: next,
	}
}

// State for fsm.
var (
	StateRecoverFromMemoryFutureStore = newState(statetype.RecoverFromMemoryFutureStore)
	StateRecoverFromExtFutureStore    = newState(statetype.RecoverFromExtFutureStore)
	StateSaveRequest                  = newState(statetype.SaveRequest)
	StateBeforeStateful               = newState(statetype.BeforeStateful)
	StateStateful                     = newState(statetype.Stateful)
)

type resultState struct {
	baseState
	result taskcontext.Serializable
}

// Result is not nil if the fsm is in the final state.
func (s *resultState) Result() taskcontext.Serializable {
	return s.result
}

// StateError for fsm.
func StateError(code int32, message string) State {
	return &resultState{
		baseState: baseState{typ: statetype.Error},
		result: &common.ErrorResponse{
			Code:    code,
			Message: message,
		},
	}
}

// StateSuccess for fsm.
func StateSuccess(result taskcontext.Serializable) State {
	return &resultState{
		baseState: baseState{typ: statetype.Success},
		result:    result,
	}
}

type futureState struct {
	sncFuture *snfuture.Future
	baseState
}

// Future is not nil if the future need to update by recovered future.
func (s *futureState) Future() *snfuture.Future {
	return s.sncFuture
}

// StateNotRecovered for fsm.
func StateNotRecovered(sncFuture *snfuture.Future) State {
	return &futureState{
		baseState: baseState{typ: statetype.NotRecovered},
		sncFuture: sncFuture,
	}
}

// StateRecovered for fsm.
func StateRecovered(sncFuture *snfuture.Future) State {
	return &futureState{
		baseState: baseState{typ: statetype.Recovered},
		sncFuture: sncFuture,
	}
}

// Handler will be executed in every state.
type Handler func(taskCtx taskcontext.Context, sncFuture *snfuture.Future) State

// Run runs the fsm and get the result.
func Run(handlers map[int]Handler, stateType int,
	taskCtx taskcontext.Context, sncFuture *snfuture.Future) taskcontext.Serializable {
	var state State
	for {
		handler := handlers[stateType]
		if handler == nil {
			state = StateError(common.EFunctionTask, "invalid state: "+strconv.Itoa(stateType))
			break
		}
		state = handler(taskCtx, sncFuture)
		if state.Future() != nil {
			sncFuture = state.Future()
		}
		stateType = state.Type()
		if stateType == statetype.Success || stateType == statetype.Error {
			break
		}
	}
	return state.Result()
}
