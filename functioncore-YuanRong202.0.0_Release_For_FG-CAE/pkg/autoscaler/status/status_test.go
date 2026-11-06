/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package status

import (
	"fmt"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
)

func fakeSet(s Status) bool {
	Set(FlowOut)
	return true
}

func Test_fakeSet(t *testing.T) {
	convey.Convey("fakeSet", t, func() {
		convey.Convey("test true", func() {
			res := fakeSet(FlowOut)
			convey.So(res, convey.ShouldEqual, true)
		})
	})
}

func TestGet(t *testing.T) {
	convey.Convey("Get", t, func() {
		patches := gomonkey.ApplyGlobalVar(&(autoScalerStatus.status), FlowIn)
		defer patches.Reset()

		convey.Convey("test 1", func() {
			res := Get()
			convey.So(res, convey.ShouldEqual, FlowIn)
		})
	})
}

func TestStatus_String(t *testing.T) {
	convey.Convey("Get", t, func() {
		convey.Convey("test flowOut", func() {
			status := FlowOut
			res := status.String()
			convey.So(res, convey.ShouldEqual, "flowOut")
		})
		convey.Convey("test initializing", func() {
			status := FlowIn
			res := status.String()
			convey.So(res, convey.ShouldEqual, "flowIn")
		})
		convey.Convey("test ready", func() {
			status := Ready
			res := status.String()
			convey.So(res, convey.ShouldEqual, "ready")
		})
		convey.Convey("test 1", func() {
			var sta Status = 100
			res := sta.String()
			convey.So(res, convey.ShouldEqual, fmt.Sprintf("Unknown status - %d", sta))
		})
	})
}
