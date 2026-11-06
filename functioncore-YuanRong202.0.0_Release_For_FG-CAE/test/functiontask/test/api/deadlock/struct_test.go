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

func TestStruct(t *testing.T) {
	Convey("Test ConvertLists2Struct", t, func() {
		reqList := []common.ReqIDList{
			{
				"testdata1",
			},
			{
				"testdata2",
			},
		}
		resp := deadlock.ConvertLists2Struct(reqList)
		So(len(resp), ShouldEqual, 2)
	})
	Convey("Test ConvertLists2Array", t, func() {
		reqList1 := []*deadlock.ReqIDList{
			{
				ReqIDs: []string{"123", "456"},
			},
		}
		resp := deadlock.ConvertLists2Array(reqList1)
		So(resp[0][0], ShouldEqual, "123")
		So(resp[0][1], ShouldEqual, "456")
	})
	Convey("Test Deadlock StartResponse Fields", t, func() {
		deadlockReq := &deadlock.StartResponse{
			DeadlockID: "deadlockID",
		}
		resp := deadlockReq.Fields()
		So(resp[0].String, ShouldEqual, "deadlockID")

	})
	Convey("Test Deadlock EndResponse Fields", t, func() {
		deadlockReq := &deadlock.EndResponse{
			State: "testSta",
		}
		resp := deadlockReq.Fields()
		So(resp[0].String, ShouldEqual, "testSta")
	})
}
