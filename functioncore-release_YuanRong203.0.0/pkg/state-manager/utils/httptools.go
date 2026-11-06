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

package utils

import "functioncore/pkg/common/types"

// Request receive http request body json
type Request struct {
	FunctionName   string                    `json:"functionName,omitempty" valid:"stringlength(1|128),optional"`
	StateID        string                    `json:"stateID,omitempty" valid:"stringlength(1|36),optional"`
	TenantID       string                    `json:"tenantID,omitempty" valid:"optional"`
	ProxyFunction  string                    `json:"proxyFunction,omitempty" valid:"optional"`
	TraceID        string                    `json:"traceID,omitempty" valid:"optional"`
	ServiceID      string                    `json:"serviceID,omitempty"`
	FunctionDelete bool                      `json:"functionDelete,omitempty"`
	BusinessID     string                    `json:"businessID,omitempty" valid:"optional"`
	Version        string                    `json:"version,omitempty" valid:"optional"`
	Creator        string                    `json:"creator,omitempty" valid:"optional"`
	FuncName       string                    `json:"funcName,omitempty" valid:"optional"`
	CreateOptions  *types.StateCreateOptions `json:"createOptions,omitempty" valid:"optional"`
}

// Response is http response json
type Response struct {
	Code    int    `json:"code,omitempty"`
	StateID string `json:"stateID,omitempty"`
	NodeID  string `json:"nodeID,omitempty"`
	Error   string `json:"error,omitempty"`
}
