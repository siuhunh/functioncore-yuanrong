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

package invoke

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/invoke"
)

func TestInvokeStruct(t *testing.T) {
	Convey("Test invoke struct Request GetPayload", t, func() {
		invokeReq := &invoke.Request{
			RequestPb: invoke.RequestPb{
				FuncName:        "",
				Version:         "",
				Payload:         "testPayload",
				ByKey:           false,
				Key:             "",
				StateID:         "",
				CountOfFuncName: 0,
			},
		}
		resp := invokeReq.GetPayload()
		So(resp, ShouldEqual, "testPayload")
	})
	Convey("Test invoke struct Fields", t, func() {
		invokeReq := &invoke.TaskResponse{
			Code:    0,
			Message: "test",
		}
		resp := invokeReq.Fields()
		So(resp, ShouldBeNil)
	})
	Convey("Test invoke struct OnCompleteRequest GetPayload", t, func() {
		invokeReq := &invoke.OnCompleteRequest{
			OnCompleteRequestPb: invoke.OnCompleteRequestPb{
				FuncName:        "",
				Version:         "",
				StateID:         "",
				FutureID:        "",
				CountOfFuncName: 0,
			},
		}
		resp := invokeReq.GetPayload()
		So(resp, ShouldBeEmpty)
	})
}
