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

// Package samenode calls function task at the same node
package samenode

import (
	"fmt"
	"reflect"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/filter"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/readstate"
	"functioncore/pkg/functiontask/task/api/savestate"
	"functioncore/pkg/functiontask/task/api/settimeout"
	"functioncore/pkg/functiontask/task/api/terminate"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

const defaultMapSize = 10

type handle func(taskcontext.Context, taskcontext.RequestWithStateID, taskcontext.Serializable)

var specialReqMap = make(map[reflect.Type]handle, defaultMapSize)
var commonReqMap = make(map[reflect.Type]string, defaultMapSize)

func init() {
	initCommonReqMap()
	initSpecialReqMap()
}

func initSpecialReqMap() {
	specialReqMap[reflect.TypeOf((*deadlock.StartRequest)(nil))] =
		func(c taskcontext.Context, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
			checker := &deadlock.LocalChecker{
				StateQueueMap: c.StateQueueMap(),
			}
			if _, ok := req.(*deadlock.StartRequest); !ok {
				return
			}
			result := checker.Start(req.(*deadlock.StartRequest))
			reflect.ValueOf(resp).Elem().Set(reflect.ValueOf(result).Elem())
		}
	specialReqMap[reflect.TypeOf((*deadlock.EndRequest)(nil))] =
		func(c taskcontext.Context, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
			checker := &deadlock.LocalChecker{
				StateQueueMap: c.StateQueueMap(),
			}
			if _, ok := req.(*deadlock.EndRequest); !ok {
				return
			}
			result := checker.End(req.(*deadlock.EndRequest))
			reflect.ValueOf(resp).Elem().Set(reflect.ValueOf(result).Elem())
		}
	specialReqMap[reflect.TypeOf((*invoke.TaskRequest)(nil))] =
		func(c taskcontext.Context, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
			if _, ok := req.(*invoke.TaskRequest); !ok {
				return
			}
			result := req.(*invoke.TaskRequest).Callee(c)
			if errResp, ok := result.(*common.ErrorResponsePb); ok {
				reflect.ValueOf(resp).Elem().FieldByName("Code").Set(reflect.ValueOf(errResp.Code))
				reflect.ValueOf(resp).Elem().FieldByName("Message").Set(reflect.ValueOf(errResp.Message))
				return
			}
			reflect.ValueOf(resp).Elem().Set(reflect.ValueOf(result).Elem())
		}

}

func initCommonReqMap() {
	commonReqMap[reflect.TypeOf((*invoke.TaskRequest)(nil))] = "Callee"
	commonReqMap[reflect.TypeOf((*getfuture.TaskRequest)(nil))] = "Callee"
	commonReqMap[reflect.TypeOf((*settimeout.Request)(nil))] = "SetTimeout"
	commonReqMap[reflect.TypeOf((*terminate.Request)(nil))] = "Terminate"
	commonReqMap[reflect.TypeOf((*readstate.Request)(nil))] = "ReadState"
	commonReqMap[reflect.TypeOf((*savestate.Request)(nil))] = "SaveState"
	commonReqMap[reflect.TypeOf((*savestate.Request)(nil))] = "SaveState"
	commonReqMap[reflect.TypeOf((*invoke.TaskCrateStateQueueRequest)(nil))] = "CreateStateQueue"
	commonReqMap[reflect.TypeOf((*filter.Request)(nil))] = "Filter"
}

func doCommonRequest(c taskcontext.Context, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
	serializableType := reflect.TypeOf((*taskcontext.Serializable)(nil)).Elem()
	methodName, ok := commonReqMap[reflect.TypeOf(req)]
	if !ok {
		fmt.Println("unsupported task request method", reflect.TypeOf(req), req)
		resp.SetCode(common.EFunctionTask)
		resp.SetMessage("unsupported task request")
		return
	}
	method, exists := reflect.TypeOf(req).MethodByName(methodName)
	if !exists {
		fmt.Println("unsupported task request", reflect.TypeOf(req), req)
		resp.SetCode(common.EFunctionTask)
		resp.SetMessage("unsupported task request")
		return
	}
	result := method.Func.Call([]reflect.Value{reflect.ValueOf(req), reflect.ValueOf(c)})
	if len(result) != 1 || !reflect.TypeOf(result[0].Interface()).Implements(serializableType) {
		fmt.Println("unsupported task request method", reflect.TypeOf(req), req)
		resp.SetCode(common.EFunctionTask)
		resp.SetMessage("unsupported task request")
		return
	}
	reflect.ValueOf(resp).Elem().Set(result[0].Elem())
}

// CallTask calls function task at the same node.
func CallTask(c taskcontext.Context, method int32,
	req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
	c.SetSource(common.SourceFunctionTaskToFunctionTask)
	c.SetMethod(method)
	log.GetLogger().Debugf("callTaskSameNode, method: %s, stateID: %s, traceID: %s", common.TypeNames[method],
		req.GetStateID(), c.TraceID())

	// deal deadLock request.
	if handle, ok := specialReqMap[reflect.TypeOf(req)]; ok {
		handle(c, req, resp)
		return
	}
	doCommonRequest(c, req, resp)
}
