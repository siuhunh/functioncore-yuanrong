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

package deadlock

import (
	"functioncore/pkg/functiontask/task/common"
)

// EmptyChecker do not check deadlock.
type EmptyChecker struct{}

// Start always returns OK.
func (e *EmptyChecker) Start(_ *StartRequest) *StartResponse {
	return &StartResponse{
		Code: common.EOK,
	}
}

// End always returns OK.
func (e *EmptyChecker) End(_ *EndRequest) *EndResponse {
	return &EndResponse{
		Code: common.EOK,
	}
}
