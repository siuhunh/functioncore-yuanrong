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
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/common"
)

func TestEmpty(t *testing.T) {
	Convey("Test empty Start", t, func() {
		req := &deadlock.StartRequest{
			StateID:          "testStateID",
			State:            "testState",
			FutureReqIDLists: nil,
			TenantID:         "testTenantID",
			ServiceID:        "testServiceID",
		}
		checker := new(deadlock.EmptyChecker)
		resp := checker.Start(req)
		So(resp.Code, ShouldEqual, common.EOK)
	})
	Convey("Test empty End", t, func() {
		req := &deadlock.EndRequest{
			StateID:    "testStateID",
			DeadlockID: "xxx",
			TenantID:   "testTenantID",
			ServiceID:  "testServiceID",
		}
		checker := new(deadlock.EmptyChecker)
		resp := checker.End(req)
		So(resp.Code, ShouldEqual, common.EOK)
	})

}
