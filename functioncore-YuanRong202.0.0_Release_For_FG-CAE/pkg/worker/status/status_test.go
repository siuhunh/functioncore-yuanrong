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

package status

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
)

func TestCompareAndSet(t *testing.T) {
	convey.Convey("CompareAndSet", t, func() {
		patches := gomonkey.ApplyGlobalVar(&(workerStatus.status), Initializing)
		defer patches.Reset()

		convey.Convey("test true", func() {
			res := CompareAndSet(Initializing, Initializing)
			convey.So(res, convey.ShouldEqual, true)
		})
		convey.Convey("test false", func() {
			res := CompareAndSet(Waiting, Initializing)
			convey.So(res, convey.ShouldEqual, false)
		})
	})
}

func fakeSet(s Status) bool {
	Set(Waiting)
	return true
}

func Test_fakeSet(t *testing.T) {
	convey.Convey("fakeSet", t, func() {
		convey.Convey("test true", func() {
			res := fakeSet(Initializing)
			convey.So(res, convey.ShouldEqual, true)
		})
	})
}

func TestGet(t *testing.T) {
	convey.Convey("Get", t, func() {
		patches := gomonkey.ApplyGlobalVar(&(workerStatus.status), Initializing)
		defer patches.Reset()

		convey.Convey("test 1", func() {
			res := Get()
			convey.So(res, convey.ShouldEqual, Initializing)
		})
	})
}

func TestStatus_String(t *testing.T) {
	convey.Convey("Get", t, func() {
		convey.Convey("test waiting", func() {
			status := Waiting
			res := status.String()
			convey.So(res, convey.ShouldEqual, "waiting")
		})
		convey.Convey("test initializing", func() {
			status := Initializing
			res := status.String()
			convey.So(res, convey.ShouldEqual, "initializing")
		})
		convey.Convey("test ready", func() {
			status := Ready
			res := status.String()
			convey.So(res, convey.ShouldEqual, "ready")
		})
		convey.Convey("test checkpointing", func() {
			status := Checkpointing
			res := status.String()
			convey.So(res, convey.ShouldEqual, "checkpointing")
		})
		convey.Convey("test checkpointready", func() {
			status := CheckpointReady
			res := status.String()
			convey.So(res, convey.ShouldEqual, "checkpointready")
		})
		convey.Convey("test checkpointfailed", func() {
			status := CheckpointFailed
			res := status.String()
			convey.So(res, convey.ShouldEqual, "checkpointfailed")
		})
		convey.Convey("test 1", func() {
			var sta Status = 100
			res := sta.String()
			convey.So(res, convey.ShouldEqual, fmt.Sprintf("Unknown status - %d", sta))
		})
	})
}

func TestSetAndGetRuntimeInvalidStatus(t *testing.T) {
	convey.Convey("SetAndGetRuntimeInvalidStatus", t, func() {
		atomic.StoreInt32(&runtimeDirInvalid, 2)
		convey.Convey("get runtime dir invalid status ", func() {
			res := GetRuntimeDirInvalidStatus()
			convey.So(res, convey.ShouldEqual, 2)
		})

		convey.Convey("set runtime dir invalid status ", func() {
			SetRuntimeDirInvalidStatus()
			res := GetRuntimeDirInvalidStatus()
			convey.So(res, convey.ShouldEqual, 1)
		})
		atomic.StoreInt32(&runtimeDirInvalid, 0)
	})
}
