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

package corecontext

import (
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/statequeue"
)

func TestNew(t *testing.T) {
	Convey("TestNew", t, func() {
		patch := ApplyFunc((*statequeue.Map).CheckAndClearOldStateQueue, func(
			_ *statequeue.Map, metadata *metadata.Metadata) {
		})
		functioncore.FunctionCheck.GetNodeIDByStateID = func(stateID string) string {
			return "testNodeXXX"
		}
		defer func() {
			patch.Reset()
			functioncore.FunctionCheck.GetNodeIDByStateID = nil
		}()
		ctx := New(1, &functioncore.HookArgs{
			SrcTenant:   "xx",
			LogType:     "x",
			TraceID:     "xxxx",
			SNServiceID: "xxx",
		})
		ctx.SetStateQueue(
			&statequeue.Map{})
		So(ctx, ShouldNotBeNil)
		So(ctx.TenantID(), ShouldEqual, "xx")
		ctx.Clone()
		same := ctx.IsSameNode("xxx")
		So(same, ShouldBeFalse)
	})
}
