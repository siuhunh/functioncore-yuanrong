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
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
)

type context struct {
	state     *state
	funcName  string
	reqIDList common.ReqIDList
	stateID   string
}

func newContext(funcName string, reqIDList common.ReqIDList, stateID string, rawState string) *context {
	return &context{funcName: funcName, reqIDList: reqIDList, stateID: stateID, state: unmarshalState(rawState)}
}

func (c *context) callback(result common.Result) *common.Response {
	return common.Success(result, c.stateID, marshalState(c.state))
}

func (c *context) blockGet(req *common.Request, deadlockID string) *common.Response {
	resp := req.GetResponse()
	if c.stateID == "" {
		return resp
	}
	rawState, err := testStateQueue.GetState(&metadata.Metadata{StateID: c.stateID})
	if err != nil {
		log.GetLogger().Errorf("failed to get state from state queue, err: %s", err.Error())
		panic(common.SystematicError(common.EGetState, err.Error()))
	}
	if err2 := testStateQueue.EndCheckAndResolveDeadlock(&metadata.Metadata{StateID: c.stateID}, deadlockID); err2 != nil {
		panic(common.SystematicError(common.EUnknown, err2.Error()))
	}
	c.state = unmarshalState(string(rawState))
	return resp
}

func (c *context) startCheckAndResolveDeadlock(req *common.Request) (string, error) {
	if c.stateID == "" {
		return "", nil
	}
	return testStateQueue.StartCheckAndResolveDeadlock(&metadata.Metadata{StateID: c.stateID},
		marshalState(c.state),
		[]common.ReqIDList{req.ReqIDList})
}

func (c *context) invoke(funcName string, params common.Params, stateID string) *common.Request {
	reqIDList := append(c.reqIDList, common.NewReqID(nil, "", "", 0))
	req := common.NewRequest(common.RequestOptions{
		FuncName:  funcName,
		ReqIDList: reqIDList,
		Params:    params,
		Config:    c.funcName,
	})
	if stateID == "" {
		go func() {
			req.SetResponse(execFunc(funcName, params, reqIDList, stateID, marshalState(new(state))))
		}()
		return req
	}
	testStateQueue.Request(&metadata.Metadata{StateID: stateID}, req, true)
	return req
}

func (c *context) invokeWithTimeout(FuncName string, Params common.Params, stateID string,
	d time.Duration) *common.Request {
	reqIDList := append(c.reqIDList, common.NewReqID(nil, "", "", 0))
	req := common.NewRequest(common.RequestOptions{
		FuncName:  FuncName,
		ReqIDList: reqIDList,
		Params:    Params,
		Config:    c.funcName,
	})
	go func() {
		<-time.After(d)
		testStateQueue.Request(&metadata.Metadata{StateID: stateID}, req, false)
	}()
	return req
}
