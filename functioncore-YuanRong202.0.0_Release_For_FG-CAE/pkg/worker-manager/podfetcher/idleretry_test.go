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

// Package podfetcher -
package podfetcher

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/alarm"
)

func Test_initIdleRetryList(t *testing.T) {
	convey.Convey("IdleRetryList init updateRetryNum and updateCtxAndRetryNum", t, func() {
		gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
			func(a *alarm.Client, almType alarm.Type) {
				return
			})
		retryList, err := initIdleRetryList()
		convey.So(err, convey.ShouldBeNil)
		_, num := retryList.updateCtxAndRetryNum("test")
		convey.So(num, convey.ShouldEqual, 0)
		retryList.updateRetryNum("test", 2)
		_, num = retryList.updateCtxAndRetryNum("test")
		convey.So(num, convey.ShouldEqual, 2)
		retryList.updateRetryNum("test", 3)
		_, num = retryList.updateCtxAndRetryNum("test")
		convey.So(num, convey.ShouldEqual, 3)
	})
}
