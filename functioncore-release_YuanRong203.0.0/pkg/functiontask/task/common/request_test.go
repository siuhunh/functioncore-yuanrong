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

// TestResponseTimeout checks filling the future with timeout.
func TestGetResponseConcurrently(t *testing.T) {
	funcName := "test-func"
	reqIDList := []ReqID{NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func(c C) {
		req := NewRequest(RequestOptions{
			FuncName:   funcName,
			ReqIDList:  reqIDList,
		})
		resp := &Response{Code: 100}
		req.SetResponse(resp)
		ch := make(chan bool, 100)
		for i := 0; i < 100; i++ {
			go func(c C) {
				c.So(req.GetResponse(), ShouldEqual, resp)
				ch <- true
			}(c)
		}
		for i := 0; i < 100; i++ {
			<-ch
		}
		So(req.response, ShouldNotBeNil)
	})
}
