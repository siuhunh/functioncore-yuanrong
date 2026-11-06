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

package ratelimit

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestUpdateRateLimitConfig(t *testing.T) {
	convey.Convey("TestUpdateRateLimitConfig params nok", t, func() {
		UpdateRateLimitConfig(-1, 0.00000001)
		convey.So(GetTraceIDBucketSize(), convey.ShouldEqual, 1000)
		convey.So(GetTraceIDFrequency(), convey.ShouldEqual, 1.0)
	})
	convey.Convey("TestUpdateRateLimitConfig params float ok", t, func() {
		UpdateRateLimitConfig(500, 2.1)
		convey.So(GetTraceIDBucketSize(), convey.ShouldEqual, 500)
		convey.So(GetTraceIDFrequency(), convey.ShouldEqual, 2.1)
	})
	convey.Convey("TestUpdateRateLimitConfig params int ok", t, func() {
		UpdateRateLimitConfig(500, 2)
		convey.So(GetTraceIDBucketSize(), convey.ShouldEqual, 500)
		convey.So(GetTraceIDFrequency(), convey.ShouldEqual, 2)
	})
}
