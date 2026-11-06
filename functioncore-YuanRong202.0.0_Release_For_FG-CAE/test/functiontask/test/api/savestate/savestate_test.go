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
	"functioncore/pkg/functiontask/task/config"
	"functioncore/test/functiontask/test/testcontext"
)

func TestSaveState(t *testing.T) {
	Convey("TestSaveState", t, func() {
		Convey("SaveLocalState", func() {
			config.Conf.PersistentState = false
			taskCtx := testcontext.New(common.MethodSaveState)
			var md = &metadata.Metadata{
				TenantID:  "",
				ServiceID: "",
				StateID:   "testkey",
			}
			req := &savestate.Request{
				StateID: md.StateID,
				State:   "online",
			}
			_ = taskCtx.StateQueueMap().CreateStateQueue(md)
			resp := req.SaveState(taskCtx)
			So(resp.Code, ShouldEqual, common.EOK)
		})
		Convey("SavePersistentState", func() {
			config.Conf.PersistentState = true
			taskCtx := testcontext.New(common.MethodSaveState)
			var md = &metadata.Metadata{
				TenantID:  "",
				ServiceID: "",
				StateID:   "testkey1",
			}
			_ = taskCtx.StateQueueMap().CreateStateQueue(md)
			req := &savestate.Request{
				StateID: md.StateID,
				State: "online1",
			}
			resp := req.SaveState(taskCtx)
			So(resp.Code, ShouldEqual, common.EOK)
		})
		Convey("SavePersistentStateButNoState", func() {
			config.Conf.PersistentState = true
			taskCtx := testcontext.New(common.MethodSaveState)
			var md = &metadata.Metadata{
				TenantID:  "",
				ServiceID: "",
				StateID:   "testkey2",
			}
			req := &savestate.Request{
				StateID: md.StateID,
				State: "online2",
			}
			resp := req.SaveState(taskCtx)
			So(resp.Code, ShouldEqual, common.EStateIDNotFound)
			So(resp.Message, ShouldEqual, "state queue without such ID: testkey2")
		})
	})
}