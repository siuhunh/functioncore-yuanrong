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
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/test/functiontask/test/testcontext"
)

func TestLocalStart(t *testing.T) {
	Convey("Test Local Start", t, func() {
		taskCtx := testcontext.New(common.MethodTaskStartCheckAndResolveDeadlock)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		var req = &deadlock.StartRequest{
			StateID:          md.StateID,
			State:            "testState",
			FutureReqIDLists: nil,
			TenantID:         "",
			ServiceID:        md.ServiceID,
		}
		checker := deadlock.LocalChecker{
			StateQueueMap: taskCtx.StateQueueMap(),
		}
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := checker.Start(req)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.Message, ShouldEqual, "")
	})
	Convey("Test Local Start no such state queue", t, func() {
		taskCtx := testcontext.New(common.MethodTaskStartCheckAndResolveDeadlock)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey1",
		}
		var req = &deadlock.StartRequest{
			StateID:          md.StateID,
			State:            "testState",
			FutureReqIDLists: nil,
			TenantID:         "",
			ServiceID:        md.ServiceID,
		}
		checker := deadlock.LocalChecker{
			StateQueueMap: taskCtx.StateQueueMap(),
		}
		resp := checker.Start(req)
		So(resp.Code, ShouldEqual, common.EStateIDNotFound)
		So(resp.Message, ShouldEqual, "no such state queue with id: testKey1")
	})
}

func TestLocalEnd(t *testing.T) {
	Convey("Test Local End", t, func() {
		taskCtx := testcontext.New(common.MethodTaskEndCheckAndResolveDeadlock)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey2",
		}
		var req = &deadlock.EndRequest{
			StateID:    md.StateID,
			DeadlockID: "",
			TenantID:   "",
			ServiceID:  md.ServiceID,
		}
		checker := deadlock.LocalChecker{
			StateQueueMap: taskCtx.StateQueueMap(),
		}
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := checker.End(req)
		So(resp.Code, ShouldEqual, common.EOK)
		So(resp.Message, ShouldEqual, "")
	})
	Convey("Test Local End invalid deadlock id", t, func() {
		taskCtx := testcontext.New(common.MethodTaskEndCheckAndResolveDeadlock)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey3",
		}
		var req = &deadlock.EndRequest{
			StateID:    md.StateID,
			DeadlockID: "xxx",
			TenantID:   "",
			ServiceID:  md.ServiceID,
		}
		checker := deadlock.LocalChecker{
			StateQueueMap: taskCtx.StateQueueMap(),
		}
		_, _, _ = taskCtx.CreateState("", md.StateID)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		resp := checker.End(req)
		So(resp.Code, ShouldEqual, common.EFunctionTask)
		So(resp.Message, ShouldEqual, "invalid deadlock id: xxx")
	})
	Convey("Test Local End no such queue", t, func() {
		taskCtx := testcontext.New(common.MethodTaskEndCheckAndResolveDeadlock)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey4",
		}
		var req = &deadlock.EndRequest{
			StateID:    md.StateID,
			DeadlockID: "",
			TenantID:   "",
			ServiceID:  md.ServiceID,
		}
		checker := deadlock.LocalChecker{
			StateQueueMap: taskCtx.StateQueueMap(),
		}
		resp := checker.End(req)
		So(resp.Code, ShouldEqual, common.EStateIDNotFound)
		So(resp.Message, ShouldEqual, "no such queue of the state id testKey4")
	})
}


