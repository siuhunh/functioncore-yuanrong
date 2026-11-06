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
package input

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestMatchMultipleStrings(t *testing.T) {
	test := []struct {
		strArr  []string
		regular string

		result int
	}{
		{
			strArr:  []string{"aaa", "aab"},
			regular: "aa*",

			result: 2,
		},
		{
			strArr:  []string{},
			regular: "aa*",

			result: 0,
		},
	}
	convey.Convey("TestIsGBK", t, func() {
		for _, tt := range test {
			res := MatchMultipleStrings(tt.strArr, tt.regular)
			convey.So(len(res), convey.ShouldEqual, tt.result)
		}
	})
}
