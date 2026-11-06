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

func addTwoSync(params common.Params, ctx *context) *common.Response {
	ctx.state.Count++
	// make sure the request of `req2 := ctx.invoke("addOne", nil, stateID)` is received in state queue.
	time.Sleep(time500ms)
	ctx.invoke("addOneBlock", "", ctx.stateID)
	// make sure the request of `ctx.invoke("addOne", nil, ctx.stateID)` is received in state queue.
	time.Sleep(time500ms)
	return ctx.callback(strconv.Itoa(ctx.state.Count))
}

func largeTree(params common.Params, ctx *context) *common.Response {
	stateID := params
	req1 := ctx.invoke("addTwoSync", "", stateID)
	req2 := ctx.invoke("addTwo", "", stateID)

	deadlockID1, err := ctx.startCheckAndResolveDeadlock(req1)
	if err != nil {
		return common.SystematicError(common.EUnknown, err.Error())
	}
	resp1 := ctx.blockGet(req1, deadlockID1)
	if resp1.Code == common.EPanic || resp1.Code == common.EUnacceptableUncertainty {
		return resp1
	}

	deadlockID2, err := ctx.startCheckAndResolveDeadlock(req2)
	if err != nil {
		return common.SystematicError(common.EUnknown, err.Error())
	}
	resp2 := ctx.blockGet(req2, deadlockID2)
	if resp2.Code == common.EPanic || resp2.Code == common.EUnacceptableUncertainty {
		return resp2
	}

	return ctx.callback(resp1.Result + "," + resp2.Result)
}
