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
package terminate

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"gotest.tools/assert"
)

func TestResponse(t *testing.T) {
	Convey("TestResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 7
		data := []byte{}
		var res Response
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 7)
		res.SetMessage("message terminate")
		So(res.GetMessage(), ShouldEqual, "message terminate")
	})
}

func TestFields(t *testing.T) {
	resp := &Response{}
	result := resp.Fields()
	assert.Equal(t, 1, len(result))
}
