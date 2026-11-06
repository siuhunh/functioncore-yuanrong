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

// Package adaptor defines structs and interfaces used for adaptor.
package adaptor

import (
	"reflect"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/filter"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/getinstance"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/readstate"
	"functioncore/pkg/functiontask/task/api/savestate"
	"functioncore/pkg/functiontask/task/api/settimeout"
	"functioncore/pkg/functiontask/task/api/terminate"
	"functioncore/pkg/functiontask/task/api/waitfuture"
)

// CallRequestData represents data is CallRequest.
type CallRequestData interface {
	isCallRequestPb_Data
}

// FuncArgs for requests.
type FuncArgs = CallRequestPb

type convertFunc func(interface{}) CallRequestData

var convertMap map[reflect.Type]convertFunc

func init() {
	convertMap = map[reflect.Type]convertFunc{
		reflect.TypeOf((*invoke.TaskRequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_InvokeTaskReq{InvokeTaskReq: inter.(*invoke.TaskRequestPb)}
		},
		reflect.TypeOf((*invoke.TaskCrateStateQueueRequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_InvokeTaskCreateStateReq{
				InvokeTaskCreateStateReq: inter.(*invoke.TaskCrateStateQueueRequestPb),
			}
		},
		reflect.TypeOf((*getfuture.TaskRequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_GetTaskReq{GetTaskReq: inter.(*getfuture.TaskRequestPb)}
		},
		reflect.TypeOf((*readstate.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_ReadReq{ReadReq: inter.(*readstate.RequestPb)}
		},
		reflect.TypeOf((*savestate.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_SaveReq{SaveReq: inter.(*savestate.RequestPb)}
		},
		reflect.TypeOf((*settimeout.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_SetTimeoutReq{SetTimeoutReq: inter.(*settimeout.RequestPb)}
		},
		reflect.TypeOf((*terminate.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_TerminateReq{TerminateReq: inter.(*terminate.RequestPb)}
		},
		reflect.TypeOf((*waitfuture.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_WaitReq{WaitReq: inter.(*waitfuture.RequestPb)}
		},
		reflect.TypeOf((*deadlock.StartRequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_DeadlockStartReq{DeadlockStartReq: inter.(*deadlock.StartRequestPb)}
		},
		reflect.TypeOf((*deadlock.EndRequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_DeadlockEndReq{DeadlockEndReq: inter.(*deadlock.EndRequestPb)}
		},
		reflect.TypeOf((*getinstance.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_GetInstanceReq{GetInstanceReq: inter.(*getinstance.RequestPb)}
		},
		reflect.TypeOf((*filter.RequestPb)(nil)): func(inter interface{}) CallRequestData {
			return &CallRequestPb_FilterReq{FilterReq: inter.(*filter.RequestPb)}
		},
	}
}

// ConvertInterToData converts `inter` to a CallRequest data or nil.
func ConvertInterToData(inter interface{}) CallRequestData {
	if convFunc, ok := convertMap[reflect.TypeOf(inter)]; ok {
		return convFunc(inter)
	}
	return nil
}
