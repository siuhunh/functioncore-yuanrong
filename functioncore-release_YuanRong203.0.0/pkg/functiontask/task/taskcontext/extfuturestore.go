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
	"functioncore/pkg/functiontask/task/efs"
)

// ExtFutureStoreContext represents context for external future store.
type ExtFutureStoreContext interface {
	SetExtFutureStore(extFutureStore efs.ExtFutureStore)
	EFS() efs.ExtFutureStore
}

// ExtFutureStoreImpl implements context for external future store.
type ExtFutureStoreImpl struct {
	extFutureStore efs.ExtFutureStore
}

// EFS return external future store.
func (c *ExtFutureStoreImpl) EFS() efs.ExtFutureStore {
	return c.extFutureStore
}

// SetExtFutureStore sets extFutureStore for context.
func (c *ExtFutureStoreImpl) SetExtFutureStore(extFutureStore efs.ExtFutureStore) {
	c.extFutureStore = extFutureStore
}
