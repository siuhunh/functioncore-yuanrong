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
package filter

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestResponse(t *testing.T) {
	Convey("TestResponse", t, func() {
		type args struct {
			code int32
		}
		var a args
		a.code = 2
		data := []byte{}
		var res Response
		_, _ = res.Marshal()
		_ = res.Unmarshal(data)
		res.Fields()
		res.SetCode(a.code)
		So(res.GetCode(), ShouldEqual, 2)
		res.SetMessage("message filter")
		So(res.GetMessage(), ShouldEqual, "message filter")
	})
}
