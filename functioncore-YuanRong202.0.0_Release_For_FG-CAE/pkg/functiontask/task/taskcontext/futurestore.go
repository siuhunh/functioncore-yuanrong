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
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore"
)

// FutureStoreContext represents context for future store.
type FutureStoreContext interface {
	SetFutureStore(caller *futurestore.FutureStore, callee *futurestore.FutureStore)
	FutureStore() *futurestore.FutureStore
	SetFilter(caller *Filter, callee *Filter)
	Filter() *Filter
	SetSource(source int32)
	SetMethod(method int32)
	Source() int32
	Category() string
}

// FutureStoreImpl implements context for future store.
type FutureStoreImpl struct {
	callerFutureStore *futurestore.FutureStore
	futureStore       *futurestore.FutureStore
	filter            *Filter
	callerFilter      *Filter
	source            int32
	method            int32
}

// SetSource sets source for context.
func (c *FutureStoreImpl) SetSource(source int32) {
	c.source = source
}

// SetMethod sets method for context.
func (c *FutureStoreImpl) SetMethod(method int32) {
	c.method = method
}

// Category returns category.
func (c *FutureStoreImpl) Category() string {
	return genCategory(c.source, c.method)
}

// FutureStore returns context's futureStore.
func (c *FutureStoreImpl) FutureStore() *futurestore.FutureStore {
	if c.source == common.SourceRuntimeToLocalFunctionTask {
		return c.callerFutureStore
	}
	return c.futureStore
}

// Source returns context's source.
func (c *FutureStoreImpl) Source() int32 {
	return c.source
}

// SetFutureStore sets futureStore for context.
func (c *FutureStoreImpl) SetFutureStore(caller *futurestore.FutureStore, callee *futurestore.FutureStore) {
	c.callerFutureStore = caller
	c.futureStore = callee
}

// Filter returns the retry filter.
func (c *FutureStoreImpl) Filter() *Filter {
	if c.source == common.SourceRuntimeToLocalFunctionTask {
		return c.callerFilter
	}
	return c.filter
}

// SetFilter sets retry filter for context.
func (c *FutureStoreImpl) SetFilter(caller *Filter, callee *Filter) {
	c.callerFilter = caller
	c.filter = callee
}
