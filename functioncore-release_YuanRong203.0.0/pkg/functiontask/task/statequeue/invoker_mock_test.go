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

package statequeue

import (
	"fmt"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
)

const (
	invokeTimeout = 5 * time.Second
)

type simpleInvoker struct {
	common.Invoker
}

func handlePanic(ctx *context, val interface{}) *common.Response {
	if e, ok := val.(error); ok {
		log.GetLogger().Warnf("there is an exception in user code, err: %s", e.Error())
		return common.UserCodeError(e.Error(), marshalState(ctx.state))
	}
	if resp, ok := val.(*common.Response); ok {
		return resp
	}
	log.GetLogger().Warnf("unknown error, err: %s", fmt.Sprintf("%#v", val))
	return common.UserCodeError("unknown error", marshalState(ctx.state))
}

func execFunc(funcName string, params common.Params, reqIDList common.ReqIDList,
	stateID string, rawState string) (resp *common.Response) {
	if functions[funcName] != nil {
		ctx := newContext(funcName, reqIDList, stateID, rawState)
		defer func() {
			if val := recover(); val != nil {
				resp = handlePanic(ctx, val)
			}
		}()
		resp = functions[funcName](params, ctx)
		return resp
	}
	resp = common.SystematicError(common.ENoSuchFunction, "no such function " + funcName)
	return resp
}

// Invoke execute the function.
func (g *simpleInvoker) Invoke(args *common.InvokeArgs) *common.Response {
	funcName := args.FuncName
	params := args.Params
	stateID := args.StateID
	rawState := args.State
	reqIDList := args.ReqIDList
	ch := make(chan *common.Response, 1)
	go func() {
		ch <- execFunc(funcName, params, reqIDList, stateID, rawState)
		close(ch)
	}()
	var resp *common.Response
	select {
	case resp = <-ch:
		return resp
	case <-time.After(invokeTimeout):
		return common.SystematicError(common.ETimeout, "timeout")
	}
}
