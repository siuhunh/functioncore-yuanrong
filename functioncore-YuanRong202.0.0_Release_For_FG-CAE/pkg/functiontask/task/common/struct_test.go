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
package common

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestErrorResponse(t *testing.T) {
	Convey("TestStartResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 0
		data := []byte{}
		var res ErrorResponse
		res.Fields()
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 0)
		res.SetMessage("TestStartResponse")
		So(res.GetMessage(), ShouldEqual, "TestStartResponse")
	})
}
