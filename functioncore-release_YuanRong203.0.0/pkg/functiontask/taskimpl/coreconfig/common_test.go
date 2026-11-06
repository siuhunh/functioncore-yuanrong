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

package coreconfig

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGetRedisPassword(t *testing.T) {
	Convey("TestGetRedisPassword", t, func() {
		Convey("test: workKey is empty", func() {
			pass := GetRedisPassword(&Config{})
			So(pass, ShouldEqual, "")
		})
		Convey("test: decrypt fails", func() {
			pass := GetRedisPassword(&Config{})
			So(pass, ShouldEqual, "")
		})
		Convey("test: decrypt success", func() {
			pass := GetRedisPassword(&Config{})
			So(pass, ShouldEqual, "")
		})
	})
}
