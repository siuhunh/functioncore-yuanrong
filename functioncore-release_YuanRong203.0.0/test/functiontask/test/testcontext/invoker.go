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

package testcontext

import (
	"functioncore/pkg/functiontask/task/common"
)

type testInvoker struct {
}

func newTestInvoker() *testInvoker {
	return &testInvoker{}
}

// Invoke execute the function.
func (g *testInvoker) Invoke(args *common.InvokeArgs) *common.Response {
	if args.FuncName == "hello" {
		return &common.Response{
			Code:   common.EOK,
			Result: "hello",
		}
	}
	return common.SystematicError(common.ETimeout, "timeout")
}
