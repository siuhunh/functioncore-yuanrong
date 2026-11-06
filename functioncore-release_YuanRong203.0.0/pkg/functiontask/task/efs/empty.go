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

package efs

import (
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

// EmptyExtFutureStore returns an empty external future store.
type EmptyExtFutureStore struct {
}

// SetResult sets result to external future store.
func (store *EmptyExtFutureStore) SetResult(tenantID string, serviceID string,
	sncFuture *snfuture.Future, result *common.Response) common.TaskError {
	return nil
}

// SetRequest sets request to external future store.
func (store *EmptyExtFutureStore) SetRequest(tenantID string, serviceID string,
	sncFuture *snfuture.Future, reqFutureID string) common.TaskError {
	return nil
}

// GetRequest recovers request from external future store.
func (store *EmptyExtFutureStore) GetRequest(tenantID string, serviceID string,
	id snfuture.ID, reqIDList common.ReqIDList) *snfuture.Future {
	return nil
}

// ClearResult clears result in external future store.
func (store *EmptyExtFutureStore) ClearResult(tenantID string, serviceID string,
	sncFuture *snfuture.Future) common.TaskError {
	return nil
}
