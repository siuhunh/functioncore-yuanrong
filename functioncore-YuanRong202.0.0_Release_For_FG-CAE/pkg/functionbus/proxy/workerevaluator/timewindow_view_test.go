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

package workerevaluator

import (
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
)

func TestTimeWindowView_basic(t *testing.T) {
	twView := &timeWindowView{}
	twView.requestStarted()
	twView.requestEnded()

	if twView.maxConcurrentNum != 1 || twView.concurrentNum != 0 {
		t.Error("calculate concurrent number error")
	}
	twView.collectSingleTimeWindow(time.Now().Unix())
	if len(twView.getTimeWindows()) != 1 {
		t.Error("failed to get worker view time windows")
	}
}

func TestDivideCeil(t *testing.T) {
	convey.Convey("TestDivideCeil", t, func() {
		convey.So(0, convey.ShouldEqual, divideCeil(0, 0))
		convey.So(0, convey.ShouldEqual, divideCeil(0, 1))
		convey.So(0, convey.ShouldEqual, divideCeil(1, 0))
		convey.So(1, convey.ShouldEqual, divideCeil(1, 2))
		convey.So(3, convey.ShouldEqual, divideCeil(2, 1))
		convey.So(2, convey.ShouldEqual, divideCeil(1, 1))
	})
}
