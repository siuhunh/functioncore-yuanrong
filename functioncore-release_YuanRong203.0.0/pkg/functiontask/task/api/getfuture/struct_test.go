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
	"gotest.tools/assert"
)

func TestTaskResponse(t *testing.T) {
	Convey("TestTaskResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 3
		data := []byte{}
		var res Response
		var taskResponse TaskResponse
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		_ = taskResponse.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 3)
		res.SetMessage("message getfuture")
		So(res.GetMessage(), ShouldEqual, "message getfuture")
	})

	Convey("TaskResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 3
		data := []byte{}
		var taskResponse TaskResponse
		_, _ = taskResponse.Marshal()
		_ = taskResponse.Unmarshal(data)
		_ = taskResponse.Unmarshal(data)
		taskResponse.SetCode(a.code)
		So(taskResponse.GetCode(), ShouldEqual, 3)
		taskResponse.SetMessage("message getfuture")
		So(taskResponse.GetMessage(), ShouldEqual, "message getfuture")
	})
}

func TestFields(t *testing.T) {
	resp := &Response{}
	result := resp.Fields()
	assert.Equal(t, 3, len(result))

	taskResp := &TaskResponse{}
	result = taskResp.Fields()
	assert.Equal(t, 2, len(result))
}
