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

package triggerbykey

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/triggerbykey"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/test/functiontask/test/testcontext"
)

func TestByKey(t *testing.T) {
	Convey("Test ByKey", t, func() {
		config.Conf.InitStateClient = true
		taskCtx := testcontext.New(common.MethodTriggerByKey)
		var md = &metadata.Metadata{
			TenantID:  "",
			ServiceID: "",
			StateID:   "testKey",
		}
		req := &triggerbykey.Request{
			FuncName: "testFunc",
			Version:  "",
			Key:      "testKey",
			Payload:  "",
		}
		sncFuture := snfuture.NewInvoke(snfuture.Metadata{
			ServiceID: taskCtx.ServiceID(),
			FuncName:  req.FuncName,
			Version:   req.Version,
			ReqIDList: taskCtx.ReqIDList(),
		})
		sncFuture.SetResponse(&common.Response{
			Code:    0,
			Message: "hello",
		})
		taskCtx.FutureStore().Add(sncFuture)
		_ = taskCtx.StateQueueMap().CreateStateQueue(md)
		code, data, _ := triggerbykey.ByKey(taskCtx, req)
		So(code, ShouldEqual, 0)
		So(data, ShouldEqual, "")
	})
}
