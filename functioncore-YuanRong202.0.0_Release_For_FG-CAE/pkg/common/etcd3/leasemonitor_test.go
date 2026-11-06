/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package etcd3

import (
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
)

func TestLeaseMonitor(t *testing.T) {
	convey.Convey("TestLeaseMonitor", t, func() {
		m := CreateLeaseMonitor()
		m.Update()
		time.Sleep(500 * time.Millisecond)
		isTimeout, _ := m.CheckLeaseTimeout(500 * time.Millisecond)
		convey.So(isTimeout, convey.ShouldBeTrue)
	})
}
