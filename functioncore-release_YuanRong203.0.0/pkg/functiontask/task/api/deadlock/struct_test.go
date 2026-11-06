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
	"gotest.tools/assert"

	"functioncore/pkg/functiontask/task/common"
)

func TestStartResponse(t *testing.T) {
	Convey("TestStartResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 0
		data := []byte{}
		var res StartResponse
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 0)
		res.SetMessage("message deadlock")
		So(res.GetMessage(), ShouldEqual, "message deadlock")
	})
}

func TestEndResponse(t *testing.T) {
	Convey("TestEndResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 1
		data := []byte{}
		var res EndResponse
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 1)
		res.SetMessage("message deadlock")
		So(res.GetMessage(), ShouldEqual, "message deadlock")
	})
}

func TestFields(t *testing.T) {
	resp := &StartResponse{}
	result := resp.Fields()
	assert.Equal(t, 1, len(result))

	endResp := &EndResponse{}
	result = endResp.Fields()
	assert.Equal(t, 1, len(result))
}

func TestConvertLists2Array(t *testing.T) {
	lists := []*ReqIDList{}
	result := ConvertLists2Array(lists)
	assert.Equal(t, 0, len(result))
}

func TestConvertLists2Struct(t *testing.T) {
	lists := []common.ReqIDList{}
	result := ConvertLists2Struct(lists)
	assert.Equal(t, 0, len(result))
}
