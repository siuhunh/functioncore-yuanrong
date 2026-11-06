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

// Package scheduler schedules the tasks.
package scheduler

import (
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

// TestSubmit checks Submit api.
func TestSubmit(t *testing.T) {
	Convey("test submit", t, func() {
		all := 0
		Submit(100*time.Millisecond, func() {
			all ++
		})
		time.Sleep(150 * time.Millisecond)
		So(all, ShouldEqual, 1)
	})
}

// TestSubmitConcurrent checks Submit api in concurrent scenario.
func TestSubmitConcurrent(t *testing.T) {
	Convey("test submit concurrently", t, func() {
		var all int32 = 0
		times := 100
		for i := 0; i < times; i++ {
			go Submit(100*time.Millisecond, func() {
				atomic.AddInt32(&all, 1)
			})
		}
		time.Sleep(150 * time.Millisecond)
		So(all, ShouldEqual, times)
	})
}
