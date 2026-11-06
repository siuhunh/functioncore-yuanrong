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

// Package efs describes the interface for external future store.
package efs

import (
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

// ExtFutureStore represents an external future store.
type ExtFutureStore interface {
	ClearResult(tenantID string, serviceID string,
		sncFuture *snfuture.Future) common.TaskError
	SetResult(tenantID string, serviceID string,
		sncFuture *snfuture.Future, result *common.Response) common.TaskError
	SetRequest(tenantID string, serviceID string,
		sncFuture *snfuture.Future, reqFutureID string) common.TaskError
	GetRequest(tenantID string, serviceID string,
		id snfuture.ID, reqIDList common.ReqIDList) *snfuture.Future
}
