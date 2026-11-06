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
	"strconv"
	"time"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/stateclient"
)

var testStateQueue *Map

var functions map[string]func(params common.Params, ctx *context) *common.Response

func init() {
	testStateQueue = NewStateQueueMap(new(simpleInvoker), stateclient.NewInMemoryStateStore(), DefaultConfig())

	functions = make(map[string]func(params common.Params, ctx *context) *common.Response)

	functions["hello"] = hello
	functions["panicHandler"] = panicHandler

	functions["addOne"] = addOne
	functions["addTwoDeadlock"] = addTwoDeadlock
	functions["addTwo"] = addTwo
	functions["addTwoAddOneSlowly"] = addTwoAddOneSlowly

	functions["asyncAddOne"] = asyncAddOne
	functions["addTwoAsyncAddOne1"] = addTwoAsyncAddOne1
	functions["asyncAddOneWithTimeout"] = asyncAddOneWithTimeout
	functions["addTwoAsyncAddOne2"] = addTwoAsyncAddOne2

	functions["addOneBlock"] = addOneBlock
	functions["asyncAddOneBlock"] = asyncAddOneBlock
	functions["addTwoAsyncAddOne3"] = addTwoAsyncAddOne3

	functions["addTwoSync"] = addTwoSync
	functions["largeTree"] = largeTree
	functions["initState"] = initState
}

func hello(params common.Params, ctx *context) *common.Response {
	return ctx.callback("hello, " + params)
}

func panicHandler(params common.Params, ctx *context) *common.Response {
	var err error
	ctx.state.Count, err = strconv.Atoi(params)
	if err != nil {
		panic(err)
	}
	panic(fmt.Errorf("panic test"))
}

func addOne(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	return ctx.callback(strconv.Itoa(ctx.state.Count))
}

func addTwoDeadlock(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	req := ctx.invoke("addOne", "", ctx.stateID)
	resp := ctx.blockGet(req, "")
	if resp.Code == common.EPanic || resp.Code == common.EUnacceptableUncertainty {
		return resp
	}
	return ctx.callback(strconv.Itoa(ctx.state.Count))
}

func addTwo(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	req := ctx.invoke("addOne", "", ctx.stateID)
	// get.
	deadlockID, err := ctx.startCheckAndResolveDeadlock(req)
	if err != nil {
		return common.SystematicError(common.EUnknown, err.Error())
	}
	resp := ctx.blockGet(req, deadlockID)
	if resp.Code == common.EPanic || resp.Code == common.EUnacceptableUncertainty {
		return resp
	}

	return ctx.callback(strconv.Itoa(ctx.state.Count))
}

func addTwoAddOneSlowly(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	req := ctx.invokeWithTimeout("addOne", "", ctx.stateID, 1*time.Second)
	deadlockID, err := ctx.startCheckAndResolveDeadlock(req)
	if err != nil {
		return common.SystematicError(common.EUnknown, err.Error())
	}
	resp := ctx.blockGet(req, deadlockID)
	if resp.Code == common.EPanic || resp.Code == common.EUnacceptableUncertainty {
		return resp
	}

	return ctx.callback(strconv.Itoa(ctx.state.Count))
}

func initState(params common.Params, ctx *context) *common.Response {
	return common.SystematicError(common.InitStateErr, "init state fail")
}
