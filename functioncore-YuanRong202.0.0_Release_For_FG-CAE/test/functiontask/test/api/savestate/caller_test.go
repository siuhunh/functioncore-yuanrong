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

package savestate

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/savestate"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/test/functiontask/test/testcontext"
)

func TestSaveStateCaller(t *testing.T) {
	Convey("Test SaveState Caller", t, func() {
		taskCtx := testcontext.New(common.MethodSaveState)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		var req = &savestate.Request{
			StateID: md.StateID,
		}
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := req.Caller(taskCtx)
		So(resp.Code, ShouldEqual, common.EOK)
	})
}
