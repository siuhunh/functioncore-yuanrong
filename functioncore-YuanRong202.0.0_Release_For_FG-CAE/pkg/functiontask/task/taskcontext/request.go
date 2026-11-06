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

import "go.uber.org/zap"

// RequestWithStateID defines request with stateID.
type RequestWithStateID interface {
	GetStateID() string
}

// Serializable defines type which can serialize.
type Serializable interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	GetCode() int32
	GetMessage() string
	SetCode(int32)
	SetMessage(string)
	Fields() []zap.Field
}

// CreateStateRequest defines request which can set stateID.
type CreateStateRequest interface {
	RequestWithStateID
	GetFuncName() string
	GetCreateState() bool
	SetStateID(string)
}
