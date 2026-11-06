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

package getfuture

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/getfuture"
)

func TestGetFutureRespFields(t *testing.T) {
	Convey("Test GetFuture Response Fields", t, func() {
		invokeReq := &getfuture.Response{
			Code:        0,
			Message:     "testMsg",
			Result:      "testRes",
			State:       "testSta",
			UpdateState: false,
		}
		resp := invokeReq.Fields()
		So(resp[0].String, ShouldEqual, "testRes")
		So(resp[1].String, ShouldEqual, "testSta")
		So(resp[2].Integer, ShouldEqual, 0)
	})
	Convey("Test GetFuture TaskResponse Fields", t, func() {
		invokeReq := &getfuture.TaskResponse{
			Result:  "testRes",
			Log:     "testLog",
		}
		resp := invokeReq.Fields()
		So(resp[0].String, ShouldEqual, "testRes")
		So(resp[1].String, ShouldEqual, "testLog")
	})
}
