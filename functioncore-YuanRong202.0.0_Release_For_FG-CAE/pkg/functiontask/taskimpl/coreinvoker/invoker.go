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

// Package coreinvoker implements invoker interface.
package coreinvoker

import (
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/util/short"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

type property struct {
	ReqIDList []string `json:"reqIDList"`
}

// Invoker implements invoker interface.
type Invoker struct {
	common.Invoker
}

const maxRetryTimes = 3
const intervalRetryTimes = time.Second

var retryableErrCodes = []uint32{211400, 211406, 211408, 211503,
	snerror.WorkerExitErrCode, snerror.UserFuncIsUpdatedCode, snerror.RefreshSilentFunc}

// Invoke execute the function.
func (g *Invoker) Invoke(args *common.InvokeArgs) *common.Response {
	rawProperty, err := json.Marshal(&property{
		ReqIDList: args.ReqIDList,
	})
	invokeLogger := log.GetLogger().With(zap.String("requestId", short.List(args.ReqIDList)),
		zap.String("stateID", args.StateID),
		zap.String("function", args.FuncName))
	if err != nil {
		invokeLogger.Errorf("marshalProperty, err: %s", err.Error())
		return common.SystematicError(common.EFieldDefinition, "MarshalProperty")
	}
	raw, err := proto.Marshal(&common.CallRequest{
		Event: []byte(args.Params),
		State: &common.State{
			Id:      args.StateID,
			Content: args.State,
		},
		Property: string(rawProperty),
	})
	if err != nil {
		invokeLogger.Errorf("marshalCallMessage, err; %s", err.Error())
		return common.SystematicError(common.EFieldDefinition, "MarshalCallMessage")
	}
	var msgConfig *functioncore.MsgConfig
	var ok bool
	if msgConfig, ok = args.InvokeConfig.(*functioncore.MsgConfig); !ok {
		invokeLogger.Errorf(fmt.Sprintf("InvalidInvokeArgsConfig: %#v", args.InvokeConfig))
		return common.SystematicError(common.EFieldDefinition, "TypeAssertionMsgConfig")
	}
	return call(args, raw, msgConfig)
}

func isRetryableErrCode(errCode uint32) bool {
	for _, code := range retryableErrCodes {
		if errCode == code {
			return true
		}
	}
	return false
}

func call(args *common.InvokeArgs, raw []byte, msgConfig *functioncore.MsgConfig) *common.Response {
	var result functioncore.Result

	log.GetLogger().Debugf("call data, funcName: %s, stateID: %s, traceID: %s, funcCallID: %d, "+
		"reqIDList: %s", args.FuncName, args.StateID, msgConfig.TraceID, msgConfig.FuncCallID,
		short.List(args.ReqIDList))
	if args.StateID == "" {
		result = functioncore.CallOnDemand(args.FuncName, "", "", raw, *msgConfig)
	} else {
		result = functioncore.CallOnDemand(args.FuncName, coreconfig.NodeID, args.StateID, raw, *msgConfig)
	}
	log.GetLogger().Debugf("callReply, code: %d, errMsg: %s, traceID: %s, reqIDList: %s",
		result.ErrCode, result.ErrMsg, msgConfig.TraceID, short.List(args.ReqIDList))
	resp := parseCoreResult(result)
	if resp.Code != common.EOK {
		log.GetLogger().Errorf("callResult, code: %d, errMsg: %s, funcCallID: %d, reqIDList: %s, traceID: %s",
			result.ErrCode, result.ErrMsg, msgConfig.FuncCallID, short.List(args.ReqIDList), msgConfig.TraceID)
		return resp
	}
	log.GetLogger().Debugf("callResult, result: %s, stateID: %s, state: %s, funcCallID: %d, reqIDList: %s"+
		", traceID: %s", resp.Result, resp.StateID, resp.State, msgConfig.FuncCallID, short.List(args.ReqIDList),
		msgConfig.TraceID)
	return resp
}

func parseCoreResult(result functioncore.Result) *common.Response {
	if result.ErrCode != 0 {
		return &common.Response{
			Code:        int32(result.ErrCode),
			Message:     result.ErrMsg,
			Log:         result.Log,
			DcCallStack: result.DcCallStack,
		}
	}
	var reply []byte
	var ok bool
	if reply, ok = result.Reply.([]byte); !ok {
		// should not fail
		log.GetLogger().Errorf("invalidReply, result.Reply: %s", fmt.Sprintf("%+v", result.Reply))
		return &common.Response{
			Code:    common.EFieldDefinition,
			Message: "invalidReply",
		}
	}
	resp := new(common.CallResult)
	err := proto.Unmarshal(reply, resp)
	if err != nil {
		response := new(common.Response)
		response.Code = common.EOK
		response.Result = string(reply)
		response.Log = result.Log
		return response
	}
	response := new(common.Response)
	response.Code = common.EOK
	response.Result = string(resp.Result)
	response.Log = result.Log
	if resp.State != nil {
		response.State = resp.State.Content
		response.StateID = resp.State.Id
	}
	return response
}
