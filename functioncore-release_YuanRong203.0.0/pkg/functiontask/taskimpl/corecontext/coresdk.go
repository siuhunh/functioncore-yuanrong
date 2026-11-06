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

package corecontext

import (
	"encoding/json"
	"reflect"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/core-sdk/sdk/bridgefunc"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functiontask/task/adaptor"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/samenode"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/task/util"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

// InvokeConfig describes config for invoke.
func (c *Context) InvokeConfig(version string, isCallKernel bool, futureID string,
	requestAttributes map[string]int32) interface{} {
	return &functioncore.MsgConfig{
		Timeout:           time.Duration(config.Conf.CallTimeout),
		FuncCallID:        c.funcCallID,
		DestFuncTenant:    c.tenantID,
		FuncVersion:       version,
		LogType:           c.logType,
		SNServiceID:       c.ServiceID(),
		TraceID:           c.traceID,
		FutureID:          futureID,
		RequestAttributes: requestAttributes,
		SrcFuncUrn:        c.srcFuncUrn,
		SrcPodName:        c.srcPodName,
	}
}

// createFuncArgs creates FuncArgs.
// The caller guarantees that the inter can be converted into json.
// This function guarantees that the return value is not null.
func (c *Context) createFuncArgs(method int32, inter interface{}) []byte {
	data := adaptor.ConvertInterToData(inter)
	if data == nil {
		log.GetLogger().Errorf("marshalFuncArgsData, error: invalid type for method: %s:%s",
			common.TypeNames[method], reflect.TypeOf(inter).String())
		return nil
	}
	funcArgs, err := proto.Marshal(&adaptor.FuncArgs{
		Method:    method,
		Source:    common.SourceFunctionTaskToFunctionTask,
		Data:      data,
		ReqIDList: c.ReqIDList(),
	})
	if err != nil || funcArgs == nil {
		log.GetLogger().Errorf("marshalFuncArgs, err: %s", err.Error())
		return nil
	}
	return funcArgs
}

func setError(resp taskcontext.Serializable, code int32, message string) {
	resp.SetCode(code)
	resp.SetMessage(message)
}

func (c *Context) msgConfigForTask() *functioncore.MsgConfig {
	return &functioncore.MsgConfig{
		Timeout:        time.Duration(config.Conf.CallTimeout),
		FuncCallID:     c.funcCallID,
		SrcFuncTenant:  c.tenantID,
		DestFuncTenant: coreconfig.TenantID,
		FuncVersion:    "$latest",
		LogType:        c.logType,
		SNServiceID:    c.ServiceID(),
		TraceID:        c.traceID,
		SrcFuncUrn:     c.srcFuncUrn,
		SrcPodName:     c.srcPodName,
	}
}

// MsgConfigFields return fields expect DestFuncTenant.
func MsgConfigFields(msgConfig *functioncore.MsgConfig) map[string]interface{} {
	return map[string]interface{}{
		"serviceID":   msgConfig.SNServiceID,
		"sn":          msgConfig.SN,
		"logType":     msgConfig.LogType,
		"timeout":     msgConfig.Timeout,
		"nodeID":      msgConfig.NodeID,
		"funcVersion": msgConfig.FuncVersion,
		"traceID":     msgConfig.TraceID,
	}
}

// CallTask calls another function task.
// req should be a pointer to a struct, and the struct must have a field named StateID with type string,
// resp should be a pointer to a struct, and the struct must have
// a field named Code with type int and a field named Message with type string.
func (c *Context) CallTask(method int32, req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
	if c.IsSameNode(req.GetStateID()) {
		// Copy constructor
		cc := *c
		samenode.CallTask(&cc, method, req, resp)
		return
	}
	raw := c.createFuncArgs(method, req)
	callerMsgConfig := c.msgConfigForTask()
	if r, ok := req.(*invoke.TaskRequest); ok {
		callerMsgConfig.FutureID = r.FutureID
	}
	stateID := req.GetStateID()
	log.GetLogger().Debugf("callTask, traceID: %s, method: %s, stateID: %s", c.TraceID(), common.TypeNames[method],
		stateID)
	result := functioncore.Call(coreconfig.NameOfFunctionTask, stateID, raw, *callerMsgConfig, false)
	if result.ErrCode != 0 {
		log.GetLogger().Errorf("callTask.result, traceID: %s, stateID: %s, code: %d, err: %s", c.TraceID(), stateID,
			result.ErrCode, result.ErrMsg)
		setError(resp, int32(result.ErrCode), result.ErrMsg)
		return
	}
	log.GetLogger().Debugf("callTask.result, traceID: %s, stateID: %s, code: %d, errMsg: %s", c.TraceID(), stateID,
		result.ErrCode, result.ErrMsg)
	reply, ok := result.Reply.([]byte)
	if !ok {
		setError(resp, common.EFieldDefinition, "assert err")
		return
	}
	err := resp.Unmarshal(reply)
	if err != nil {
		log.GetLogger().Errorf("unmarshalCallTaskResult, traceID: %s, stateID: %s, err: %s", c.TraceID(),
			stateID, err.Error())
		setError(resp, common.EFieldDefinition, "UnmarshalCallTaskResult")
		return
	}
}

// CreateState creates a new state mapping.
func (c *Context) CreateState(funcName string, stateID string) (string, string, common.TaskError) {
	if stateID == taskcontext.NewState {
		stateID = "0"
	}
	functionName, functionVersion := util.SplitFuncNameAndVersion(funcName)
	log.GetLogger().Debugf("funcName: %s has been split into name: %s, version: %s, stateID: %s",
		funcName, functionName, functionVersion, stateID)
	businessID := util.GetBusinessID(c.TenantID(), c.ServiceID()+"@"+functionName, functionVersion)
	stateConfig := types.CreateStateConfig{
		TenantID:      c.TenantID(),
		SnServiceID:   c.ServiceID(),
		ProxyFunction: coreconfig.NameOfFunctionTask,
		TraceID:       c.traceID,
		Creator:       c.GetCreator(),
		BusinessID:    businessID,
		FuncName:      functionName,
		Version:       functionVersion,
		CreateOptions: c.GetStateCreateOptions(),
	}

	createdStateID, nodeID, err := functioncore.CreateState(funcName, stateID, stateConfig)
	if nodeID == coreconfig.NodeID {
		c.sameNode = true
	}
	if err != nil {
		log.GetLogger().Errorf("createState, traceID: %s, funcName: %s, stateID: %s, code: %d, err: %s", c.TraceID(),
			funcName, stateID, err.Code(), err.Error())
		return createdStateID, nodeID, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return createdStateID, nodeID, nil
}

// SetResult sets the result by funcCallID.
func (c *Context) SetResult(resultStruct taskcontext.Serializable) {
	if resultStruct.GetCode() != common.EOK {
		log.GetLogger().Debugf("setResult, traceID: %s, code: %d, err: %s", c.TraceID(), resultStruct.GetCode(),
			resultStruct.GetMessage())
	}
	rawResp, err := resultStruct.Marshal()
	if err != nil {
		log.GetLogger().Errorf("marshal, traceID: %s, err: %s", c.TraceID(), err.Error())
	}
	_, err = functioncore.SetResult(c.funcCallID, 0, rawResp, "")
	if err != nil {
		log.GetLogger().Errorf("setResult, traceID: %s, err: %s", c.TraceID(), err.Error())
	}
}

// SetResultWithLog sets the result with log by funcCallID.
func (c *Context) SetResultWithLog(resultStruct taskcontext.Serializable, logResult string) {
	if resultStruct.GetCode() != common.EOK {
		log.GetLogger().Infof("setResultWithLog, traceID: %s, code: %d, err: %s", c.TraceID(), resultStruct.GetCode(),
			resultStruct.GetMessage())
	}
	rawResp, err := resultStruct.Marshal()
	if err != nil {
		log.GetLogger().Warnf("marshal, err: %s", err.Error())
	}
	_, err = functioncore.SetResult(c.funcCallID, 0, rawResp, logResult)
	if err != nil {
		log.GetLogger().Errorf("setResult, traceID: %s, err: %s", c.TraceID(), err.Error())
	}
}

// SetResultWithCode sets the result with code by funcCallID.
func (c *Context) SetResultWithCode(code int32, resultStruct interface{}, logResult string) {
	log.GetLogger().Infof("setResultWithCode, traceID: %s, code: %d", c.TraceID(), code)
	var resp []byte
	var err error
	if respStr, ok := resultStruct.(string); ok {
		resp = []byte(respStr)
	} else if respBytes, ok := resultStruct.([]byte); ok {
		resp = respBytes
	} else {
		resp, err = json.Marshal(resultStruct)
		if err != nil {
			log.GetLogger().Errorf("marshal, err: %s", err.Error())
		}
	}
	_, err = functioncore.SetResult(c.funcCallID, code, resp, logResult)
	if err != nil {
		log.GetLogger().Errorf("setResultWithCode, traceID: %s, err: %s", c.TraceID(), err.Error())
	}
}

// GetFuncStateKey get func stateKey.
func (c *Context) GetFuncStateKey(funcName string, clientInstance bridgetypes.ClientInstance) (string,
	common.TaskError) {
	stateKey, isNewStateKeyScenario, err := bridgefunc.ApplyStateKey(clientInstance, c.traceID)
	c.isNewStateKeyScenario = isNewStateKeyScenario
	if err != nil {
		log.GetLogger().Errorf("GetFuncStateKey, traceID: %s, funcName: %s, isNewStateKeyScenario: %t, code: %d,"+
			"err: %s", c.TraceID(), funcName, c.IsNewStateKeyScenario(), err.Code(), err.Error())
		return "", common.NewTaskError(int32(err.Code()), err.Error())
	}
	return stateKey, nil
}

// Release -
func (c *Context) Release(funcName string, clientInstance bridgetypes.ClientInstance) (bool, common.TaskError) {
	isTerminate, err := bridgefunc.Release(clientInstance, c.traceID)
	if err != nil {
		log.GetLogger().Errorf("release, traceID: %s, funcName: %s, code: %d, err: %s", c.TraceID(), funcName,
			err.Code(), err.Error())
		return false, common.NewTaskError(int32(err.Code()), err.Error())
	}

	return isTerminate, nil
}

// SetIsGetInstanceScenario --
func (c *Context) SetIsGetInstanceScenario(flag bool) {
	c.isGetInstanceScenario = flag
}

// IsGetInstanceScenario --
func (c *Context) IsGetInstanceScenario() bool {
	return c.isGetInstanceScenario
}
