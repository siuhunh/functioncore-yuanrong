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
	"strconv"
	"time"

	"functioncore/pkg/functiontask/task/common"
)

const (
	time500ms = 500 * time.Millisecond
)

func asyncAddOne(params common.Params, ctx *context) *common.Response {
	ctx.invoke("addOne", "", params)
	return ctx.callback("")
}

func asyncAddOneWithTimeout(params common.Params, ctx *context) *common.Response {
	ctx.invokeWithTimeout("addOne", "", params, time500ms)
	return ctx.callback("")
}

// case 1.
func addTwoAsyncAddOne1(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	req := ctx.invoke("asyncAddOne", ctx.stateID, "")
	deadlockID, err := ctx.startCheckAndResolveDeadlock(req)
	if err != nil {
		return common.SystematicError(common.EUnknown, err.Error())
	}
	// sleep one second to make sure addOne is completed.
	time.Sleep(time.Second)
	resp := ctx.blockGet(req, deadlockID)
	if resp.Code == common.EPanic || resp.Code == common.EUnacceptableUncertainty {
		return resp
	}
	return ctx.callback(strconv.Itoa(ctx.state.Count))
}

// case 2.
func addTwoAsyncAddOne2(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	req := ctx.invoke("asyncAddOneWithTimeout", ctx.stateID, "")
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

func addOneBlock(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	time.Sleep(time500ms)
	return ctx.callback("")
}

func asyncAddOneBlock(params common.Params, ctx *context) *common.Response {
	ctx.invoke("addOneBlock", "", params)
	return ctx.callback("")
}

// case 3.
func addTwoAsyncAddOne3(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	req := ctx.invoke("asyncAddOneBlock", ctx.stateID, "")
	deadlockID, err := ctx.startCheckAndResolveDeadlock(req)
	if err != nil {
		return common.SystematicError(common.EUnknown, err.Error())
	}
	resp := ctx.blockGet(req, deadlockID)
	// sleep one second to make sure not exit.
	time.Sleep(time.Second)
	if resp.Code == common.EPanic || resp.Code == common.EUnacceptableUncertainty {
		return resp
	}
	return ctx.callback(strconv.Itoa(ctx.state.Count))
}
