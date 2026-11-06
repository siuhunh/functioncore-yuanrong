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
	"time"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

// MockExtFutureStore represents an mock external future store.
type MockExtFutureStore struct {
	s     *futurestore.FutureStore
	delay time.Duration
}

// Mock returns an mock external future store.
func Mock(delay time.Duration) *MockExtFutureStore {
	return &MockExtFutureStore{s: futurestore.New(), delay: delay}
}

// SetResult sets result to external future store.
func (store *MockExtFutureStore) SetResult(tenantID string, serviceID string,
	sncFuture *snfuture.Future, result *common.Response) common.TaskError {
	time.Sleep(store.delay)
	if f := store.s.Get(sncFuture.ID); f != nil {
		f.SetResponse(result)
		return nil
	}
	return common.NewTaskError(common.ESetValue, "no such future: "+sncFuture.ID)
}

// SetRequest sets request to external future store.
func (store *MockExtFutureStore) SetRequest(tenantID string, serviceID string,
	sncFuture *snfuture.Future, reqFutureID string) common.TaskError {
	time.Sleep(store.delay)
	store.s.Add(sncFuture)
	return nil
}

// GetRequest recovers request from external future store.
func (store *MockExtFutureStore) GetRequest(tenantID string, serviceID string,
	id snfuture.ID, reqIDList common.ReqIDList) *snfuture.Future {
	time.Sleep(store.delay)
	return store.s.Get(id)
}

// ClearResult clears result in external future store.
func (store *MockExtFutureStore) ClearResult(tenantID string, serviceID string,
	sncFuture *snfuture.Future) common.TaskError {
	time.Sleep(store.delay)
	if f := store.s.Get(sncFuture.ID); f != nil {
		f.ClearResponse()
	}
	return nil
}
