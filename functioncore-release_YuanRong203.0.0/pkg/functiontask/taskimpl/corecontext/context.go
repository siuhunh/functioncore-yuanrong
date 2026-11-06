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

// Package corecontext implements api without function core.
package corecontext

import (
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

// Context contains some common fields used in APIs.
type Context struct {
	taskcontext.LoggerImpl
	taskcontext.ReqIDListImpl
	taskcontext.StateQueueMapImpl
	taskcontext.FutureStoreImpl
	taskcontext.ExtFutureStoreImpl
	taskcontext.ServiceIDImpl

	funcCallID            uint64
	tenantID              string
	logType               string
	traceID               string
	sameNode              bool
	isNewStateKeyScenario bool
	isGetInstanceScenario bool
	srcFuncUrn            string
	srcPodName            string
}

// TraceID returns trace id.
func (c *Context) TraceID() string {
	return c.traceID
}

// TenantID returns tenant id.
func (c *Context) TenantID() string {
	return c.tenantID
}

// IsNewStateKeyScenario returns isNewStateKeyScenario value.
func (c *Context) IsNewStateKeyScenario() bool {
	return c.isNewStateKeyScenario
}

// New creates task context by function core.
func New(funcCallID uint64, hookArgs *functioncore.HookArgs) taskcontext.Context {
	c := new(Context)
	c.tenantID = hookArgs.SrcTenant
	c.logType = hookArgs.LogType
	c.traceID = hookArgs.TraceID
	c.SetServiceID(hookArgs.SNServiceID)
	c.funcCallID = funcCallID
	c.LoggerImpl.Init()
	c.srcFuncUrn = hookArgs.SrcFuncUrn
	c.srcPodName = hookArgs.SrcPodName
	return c
}

// IsSameNode returns whether the src and dest are the same node
func (c *Context) IsSameNode(stateID string) bool {
	stateNodeID := functioncore.FunctionCheck.GetNodeIDByStateID(stateID)
	if stateNodeID != "" {
		c.sameNode = stateNodeID == coreconfig.NodeID
	}
	if !c.sameNode {
		c.StateQueueMap().CheckAndClearOldStateQueue(&metadata.Metadata{
			TenantID:  c.tenantID,
			ServiceID: c.ServiceID(),
			StateID:   stateID,
		})
	}
	return c.sameNode
}

// Clone returns a duplicate context.
func (c *Context) Clone() taskcontext.Context {
	clone := *c
	clone.LoggerImpl.Init()
	return &clone
}
