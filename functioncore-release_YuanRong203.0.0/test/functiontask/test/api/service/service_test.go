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

package service

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/getfuture"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/service"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

const (
	times      = 10
	notExistID = "not_exist"
)

func init() {
	config.Conf.PersistentState = true
	config.Conf.InitStateClient = true
}

func addState(c C, taskCtx taskcontext.Context) map[string]string {
	ids := make(map[string]string, times)
	for i := 0; i < times; i++ {
		taskCtx = taskCtx.Clone()
		taskCtx.SetReqIDList(common.ReqIDList{common.NewTraceID()})
		req := &invoke.Request{RequestPb: invoke.RequestPb{
			FuncName: "hello",
			Payload:  "",
			StateID:  taskcontext.NewState,
		}}
		rawResp := req.Caller(taskCtx)
		c.So(rawResp.GetCode(), ShouldEqual, common.EOK)
		resp, ok := rawResp.(*invoke.Response)
		c.So(ok, ShouldBeTrue)
		ids[resp.StateID] = ""
		getReq := &getfuture.Request{FutureID: resp.FutureID}
		getResp, _ := getReq.Caller(taskCtx, new(deadlock.EmptyChecker))
		c.So(getResp.GetCode(), ShouldEqual, common.EOK)
		c.So(getResp.Result, ShouldEqual, "hello")
	}
	return ids
}

// TestGetAllStateID checks whether the api works correctly.
func TestGetAllStateID(t *testing.T) {
	Convey("Get all state id", t, func() {
		taskCtx := testcontext.New(-1)
		taskCtx.SetSource(common.SourceRuntimeToLocalFunctionTask)
		taskCtx.SetServiceID("TestGetAllStateID")
		rIDs, err := service.GetAllStateID(taskCtx)
		So(err, ShouldEqual, nil)
		So(len(rIDs), ShouldEqual, 0)

		var ids map[string]string
		Convey("add state", func(c C) {
			ids = addState(c, taskCtx)
		})
		rIDs, err = service.GetAllStateID(taskCtx)
		So(err, ShouldEqual, nil)
		So(len(rIDs), ShouldEqual, 0)
		for id := range rIDs {
			So(ids, ShouldContainKey, id)
		}

		taskCtx.SetServiceID(notExistID)
		rIDs, err = service.GetAllStateID(taskCtx)
		So(err, ShouldEqual, nil)
		So(len(rIDs), ShouldEqual, 0)
	})
}

// TestClearAllState checks whether the api works correctly.
func TestClearAllState(t *testing.T) {
	Convey("Clear all state id", t, func() {
		taskCtx := testcontext.New(-1)
		taskCtx.SetSource(common.SourceRuntimeToLocalFunctionTask)
		taskCtx.SetServiceID("TestClearAllState")
		err := service.ClearAllState(taskCtx)
		So(err, ShouldEqual, nil)

		var ids map[string]string
		Convey("add state", func(c C) {
			ids = addState(c, taskCtx)
		})
		err = service.ClearAllState(taskCtx)
		So(err, ShouldEqual, nil)

		rIDs, err := service.GetAllStateID(taskCtx)
		So(err, ShouldEqual, nil)
		So(len(rIDs), ShouldEqual, 0)
		for id := range rIDs {
			So(id, ShouldBeIn, ids)
		}

		taskCtx.SetServiceID(notExistID)
		err = service.ClearAllState(taskCtx)
		So(err, ShouldEqual, nil)
	})
}
