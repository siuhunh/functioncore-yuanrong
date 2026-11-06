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

package function

import (
	"encoding/json"
	"strconv"
	"strings"

	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/http/common"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/common/localauth"
)

// Invoke invoke function
func Invoke(c client.Client, info InvokeInfo) (*fhttp.Response, error) {
	functionName := strings.Join([]string{"0", info.ServiceName, info.FunctionName}, "@")
	funcUrn := strings.Join([]string{"sn:cn:yrk",
		info.TenantID, "function", functionName, info.Qualifier}, ":")
	path := "serverless/v1/functions" + "/" + funcUrn + "/invocations"
	headers := make(map[string]string, 3)
	headers[common.HeaderTraceID] = utils.UniqueID()
	if info.InstanceMemory != "" {
		headers[common.HeaderMemorySize] = info.InstanceMemory
		memory, err := strconv.Atoi(info.InstanceMemory)
		if err != nil {
			return nil, err
		}
		headers[common.HeaderCPUSize] = strconv.Itoa(memory/128*100 + 200)
	}
	authorization, timestamp := localauth.CreateAuthorization(config.FConfig.CLIConfig.Auth.AK,
		config.FConfig.CLIConfig.Auth.SK, "", "cli", nil)
	headers[common.HeaderAuthTimestamp] = timestamp
	headers[common.HeaderAuthorization] = authorization
	if info.TailLog {
		headers[common.HeaderLog] = "Tail"
	}
	if len(info.Headers) > 0 {
		for k, v := range info.Headers {
			headers[k] = v
		}
	}
	if err := setStateKey(info.StateKey, headers); err != nil {
		return nil, err
	}
	resp, err := c.Post(path, info.Event, headers)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// InvokeInfo -
type InvokeInfo struct {
	RequestID      string
	ServiceName    string
	FunctionName   string
	TenantID       string
	Qualifier      string
	Event          []byte
	StateKey       string
	InstanceMemory string
	Headers        map[string]string
	TailLog        bool
}

func setStateKey(stateKey string, headers map[string]string) error {
	if stateKey == "" || headers == nil {
		return nil
	}
	body, err := getToFunctionTaskBody(stateKey)
	if err != nil {
		return err
	}
	headers[common.HeaderStateKey] = string(body)
	return nil
}

func getToFunctionTaskBody(stateKey string) ([]byte, error) {
	var toFunctionTaskReq = ToFunctionTaskReq{
		CMD:      "invoke",
		Source:   "cli",
		StateKey: stateKey,
	}
	body, err := json.Marshal(toFunctionTaskReq)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// ToFunctionTaskReq 调用日志
type ToFunctionTaskReq struct {
	CMD      string `json:"cmd"`
	Source   string `json:"source"`
	StateKey string `json:"statekey"`
}
