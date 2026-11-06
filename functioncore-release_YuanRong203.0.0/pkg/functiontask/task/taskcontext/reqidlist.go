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

package taskcontext

import (
	"functioncore/pkg/common/types"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/util/short"
)

// ReqIDListContext represents context for request id list.
type ReqIDListContext interface {
	SetReqIDList(reqIDList common.ReqIDList)
	ReqIDList() common.ReqIDList
	ShortReqIDList() string
	ReqID() string
	SetCreator(creator string)
	GetCreator() string
	SetStateKey(key string)
	GetStateKey() string
	SetStateCreateOptions(createOptions *types.StateCreateOptions)
	GetStateCreateOptions() *types.StateCreateOptions
}

// ReqIDListImpl implements context for request id list.
type ReqIDListImpl struct {
	reqIDList     common.ReqIDList
	creator       string
	stateKey      string
	createOptions *types.StateCreateOptions
}

// SetReqIDList sets reqIDList for context.
func (c *ReqIDListImpl) SetReqIDList(reqIDList common.ReqIDList) {
	c.reqIDList = reqIDList
}

// ReqIDList returns context's reqIDList.
func (c *ReqIDListImpl) ReqIDList() common.ReqIDList {
	return c.reqIDList
}

// ShortReqIDList returns context's reqIDList.
func (c *ReqIDListImpl) ShortReqIDList() string {
	return short.List(c.reqIDList)
}

// ReqID returns context's reqID.
func (c *ReqIDListImpl) ReqID() string {
	return c.reqIDList[len(c.reqIDList)-1]
}

// SetCreator sets creator for context.
func (c *ReqIDListImpl) SetCreator(creator string) {
	c.creator = creator
}

// GetCreator returns context's creator.
func (c *ReqIDListImpl) GetCreator() string {
	return c.creator
}

// SetStateKey sets creator for context.
func (c *ReqIDListImpl) SetStateKey(key string) {
	c.stateKey = key
}

// GetStateKey returns context's stateKey.
func (c *ReqIDListImpl) GetStateKey() string {
	return c.stateKey
}

// SetStateCreateOptions sets stateful createOptions for context.
func (c *ReqIDListImpl) SetStateCreateOptions(createOptions *types.StateCreateOptions) {
	c.createOptions = createOptions
}

// GetStateCreateOptions returns context's stateful createOptions.
func (c *ReqIDListImpl) GetStateCreateOptions() *types.StateCreateOptions {
	return c.createOptions
}
