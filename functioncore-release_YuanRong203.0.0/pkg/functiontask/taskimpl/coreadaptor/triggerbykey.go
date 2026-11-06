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

package coreadaptor

import (
	"encoding/json"

	"functioncore/pkg/common/logger/log"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/triggerbykey"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/taskcontext"
)

// sourceType external source type
type sourceType string

// cmdType cmd type
type cmdType string

const (
	// sourceCLI external source is cli
	sourceCLI sourceType = "cli"
	// getServiceState cli
	getServiceState cmdType = "getServiceState"
	// deleteServiceState cli
	deleteServiceState cmdType = "deleteServiceState"
)

// StateKeyRequest stateKey for user
type StateKeyRequest struct {
	Source   sourceType `json:"source"`
	CMD      cmdType    `json:"cmd"`
	StateKey string     `json:"statekey"`
}

type externalRequest struct {
	Body string `json:"body"`
}

func (a *CoreAdaptor) handleExternalRequest(taskCtx taskcontext.Context,
	funcName string, hookArgs *functioncore.HookArgs) {
	log.GetLogger().Debugf("handleExternalRequest, funcName: %s, traceID: %s, stateKey: %s, stateID: %s",
		funcName, hookArgs.TraceID, hookArgs.StateKey, hookArgs.StateID)
	req := new(externalRequest)
	err := json.Unmarshal(hookArgs.FuncArgs, req)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal, err: %s", err.Error())
		taskCtx.SetResult(&getfuture.TaskResponse{
			Code:    common.EFunctionTask,
			Message: "unmarshal",
		})
		return
	}
	// handle request of triggerByKey
	a.externalRequestProcess(taskCtx, funcName, hookArgs)
}

func (a *CoreAdaptor) externalRequestProcess(taskCtx taskcontext.Context, funcName string,
	hookArgs *functioncore.HookArgs) {
	payload := string(hookArgs.FuncArgs)
	go func() {
		if hookArgs.TraceID == "" {
			hookArgs.TraceID = common.NewTraceID()
			log.GetLogger().Warnf("traceID is empty, new traceID: %s", hookArgs.TraceID)
		}

		code, data, _ := triggerbykey.ByStateID(taskCtx, &triggerbykey.Request{
			FuncName: funcName,
			Version:  hookArgs.FunctionVersion,
			Key:      "",
			StateID:  hookArgs.StateID,
			Payload:  payload,
		})
		log.GetLogger().Infof("trigger result, code: %d, traceID: %s, funcName: %s, stateID: %s",
			code, taskCtx.TraceID(), funcName, hookArgs.StateID)
		if code == common.EOK {
			taskCtx.SetResultWithCode(code, data, "")
		} else {
			taskCtx.SetResultWithCode(code, internalServerError(data), "")
		}
	}()
}

type httpResponse struct {
	Headers     map[string]string `json:"headers"`
	Body        string            `json:"body"`
	ContentType string            `json:"contentType"`
	StatusCode  string            `json:"statusCode"`
}

func internalServerError(body string) []byte {
	bytes, err := json.Marshal(&httpResponse{
		Headers: map[string]string{
			"faas-content-type": "json",
		},
		Body:        body,
		ContentType: "text/plain",
		StatusCode:  "500",
	})
	if err != nil {
		log.GetLogger().Errorf("marshalHttpResponse, err: %s", err.Error())
		return nil
	}
	log.GetLogger().Debugf("response, context: %d", bytes)
	return bytes
}
