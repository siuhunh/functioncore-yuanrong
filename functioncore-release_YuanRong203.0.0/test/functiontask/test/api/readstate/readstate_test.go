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

package readstate

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/readstate"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/test/functiontask/test/testcontext"
)

func TestReadState(t *testing.T) {
	Convey("TestReadState", t, func() {
		Convey("ReadLocalState", func() {
			config.Conf.PersistentState = false
			taskCtx := testcontext.New(common.MethodReadState)
			var md = &metadata.Metadata{
				TenantID:  "",
				ServiceID: "",
				StateID:   "testKey",
			}
			req := &readstate.Request{
				ByKey:   false,
				Key:     "",
				StateID: md.StateID,
			}
			_ = taskCtx.StateQueueMap().CreateStateQueue(md)
			queue := taskCtx.StateQueueMap().GetQueue(md)
			queue.SetState("testState")
			resp := req.ReadState(taskCtx)
			So(resp.Code, ShouldEqual, common.EOK)
			So(resp.State, ShouldEqual, "testState")
		})
		Convey("ReadLocalStateErr", func() {
			config.Conf.PersistentState = false
			taskCtx := testcontext.New(common.MethodReadState)
			var md = &metadata.Metadata{
				TenantID:  "",
				ServiceID: "",
				StateID:   "testKey1",
			}
			req := &readstate.Request{
				ByKey:   false,
				Key:     "",
				StateID: md.StateID,
			}
			resp := req.ReadState(taskCtx)
			So(resp.Code, ShouldEqual, common.EStateIDNotFound)
			So(resp.Message, ShouldEqual, "no such queue of the state id testKey1")
		})
		Convey("ReadPersistentState", func() {
			config.Conf.PersistentState = true
			taskCtx := testcontext.New(common.MethodReadState)
			var md = &metadata.Metadata{
				TenantID:  "",
				ServiceID: "",
				StateID:   "testKey2",
			}
			_ = taskCtx.StateQueueMap().StateClient.Set(md.Key(), "testState1", 0)
			_ = taskCtx.StateQueueMap().CreateStateQueue(md)
			req := &readstate.Request{
				ByKey:   false,
				Key:     "",
				StateID: md.StateID,
			}
			resp := req.ReadState(taskCtx)
			So(resp.Code, ShouldEqual, common.EOK)
			So(resp.State, ShouldEqual, "testState1")
		})
	})
}