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
	"gotest.tools/assert"
)

func TestResponse_SetCode(t *testing.T) {
	Convey("TestResponse_SetCode", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 0
		data := []byte{}
		var res Response
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 0)
		res.SetMessage("TestResponse_SetCode")
		So(res.GetMessage(), ShouldEqual, "TestResponse_SetCode")
	})

	Convey("TaskResponse_SetCode", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 0
		data := []byte{}
		var res TaskResponse
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 0)
		res.SetMessage("TestResponse_SetCode")
		So(res.GetMessage(), ShouldEqual, "TestResponse_SetCode")
	})
}

func TestGetReqFutureID(t *testing.T) {
	completeReq := &OnCompleteRequest{}
	result := completeReq.GetReqFutureID()
	assert.Equal(t, "", result)

	req := &Request{}
	result = req.GetReqFutureID()
	assert.Equal(t, "", result)
}

func TestSetStateID(t *testing.T) {
	completeReq := &OnCompleteRequest{}
	completeReq.SetStateID("test")
	assert.Equal(t, "test", completeReq.StateID)

	req := &Request{}
	req.SetStateID("Test")
	assert.Equal(t, "Test", req.StateID)
}

func TestGetPayload(t *testing.T) {
	completeReq := &OnCompleteRequest{}
	result := completeReq.GetPayload()
	assert.Equal(t, "", result)

	req := &Request{}
	req.SetStateID("Test")
	result = req.GetPayload()
	assert.Equal(t, "", result)
}

func TestFields(t *testing.T) {
	resp := &TaskResponse{}
	result := resp.Fields()
	assert.Equal(t, 0, len(result))

	resp1 := &Response{}
	result = resp1.Fields()
	assert.Equal(t, 2, len(result))
}

func TestGetCreateState(t *testing.T) {
	req := &Request{create: true}
	result := req.GetCreateState()
	assert.Equal(t, true, result)

	completeReq := &OnCompleteRequest{}
	result = completeReq.GetCreateState()
	assert.Equal(t, false, result)
}
