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

package oom

import (
	"errors"
	"os/exec"
	"reflect"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	mockutils "functioncore/test/utils"
)

func TestStartOOMDetection(t *testing.T) {
	convey.Convey("StartOOMDetection", t, func() {
		oomDetection = 0
		StartOOMDetection()
		convey.So(oomDetection, convey.ShouldEqual, 1)
		oomDetection = 0
	})
}

func TestIsRuntimeOOMKilled(t *testing.T) {
	convey.Convey("IsRuntimeOOMKilled", t, func() {
		convey.Convey("false", func() {
			var data int32 = 0
			patch := ApplyGlobalVar(&oomDetection, data)
			defer patch.Reset()
			flag := IsRuntimeOOMKilled()
			convey.So(flag, convey.ShouldBeFalse)
		})
		convey.Convey("true", func() {
			var data int32 = 1
			patches := []*Patches{
				ApplyGlobalVar(&oomDetection, data),
				ApplyFunc(checkRuntimeOOMInDmesg, func() bool {
					return true
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			flag := IsRuntimeOOMKilled()
			convey.So(flag, convey.ShouldBeTrue)
		})
	})
}

func TestFindPid(t *testing.T) {
	convey.Convey("GetServiceIDFromFuncName", t, func() {
		msg := "0004: Apr 04 07:28:05 000400 kernel: Memory cgroup out of memory: Kill process 28771 (loco-sps-ip-lx-) score 989 or sacrifice child"
		convey.So(findPid(msg), convey.ShouldEqual, "28771")
	})
}

func TestFindPid2(t *testing.T) {
	convey.Convey("GetServiceIDFromFuncName", t, func() {
		msg := "0004: Apr 04 07:28:05 000400 kernel: Memory cgroup out of memory: Killed process 28771 (loco-sps-ip-lx-) score 989 or sacrifice child"
		convey.So(findPid(msg), convey.ShouldEqual, "28771")
	})
}

func TestFindAndMoveForward(t *testing.T) {
	convey.Convey("GetServiceIDFromFuncName", t, func() {
		convey.Convey("true", func() {
			msg := "0004: Apr 04 07:28:05 000400 kernel: Memory cgroup out of memory: Kill process 28771 (loco-sps-ip-lx-) score 989 or sacrifice child"
			substr := "Memory cgroup out of memory: Kill process"
			str, flag := findAndMoveForward(msg, substr)
			convey.So(str, convey.ShouldEqual, " 28771 (loco-sps-ip-lx-) score 989 or sacrifice child")
			convey.So(flag, convey.ShouldBeTrue)
		})
		convey.Convey("false", func() {
			msg := "0004: Apr 04 07:28:05 000400 kernel: Memory cgroup out of memory:"
			substr := "Memory cgroup out of memory: Kill process"
			_, flag := findAndMoveForward(msg, substr)
			convey.So(flag, convey.ShouldBeFalse)
		})
	})
}

func TestCheckRuntimeOOMInDmesg(t *testing.T) {
	convey.Convey("Test Check Runtim eOOM In Dmesg Error", t, func() {
		patches := ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Output", func(c *exec.Cmd) ([]byte, error) {
			return nil, errors.New("sss")
		})
		defer patches.Reset()
		res := checkRuntimeOOMInDmesg()
		convey.So(res, convey.ShouldEqual, false)
	})
	convey.Convey("Test Check Runtim eOOM In Dmesg Error 2", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return nil, nil
			})})
		defer patches.ResetAll()
		res := checkRuntimeOOMInDmesg()
		convey.So(res, convey.ShouldEqual, false)
	})
	//findAndMoveForward
	convey.Convey("Test Check Runtim eOOM In Dmesg", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte("\"0004: Apr 04 07:28:05 000400 kernel(): Memory cgroup out of memory: Kill process 2\" 8771 (loco-sps-ip-lx-) score 989 or sacrifice child\""), nil
			})})
		patches.Append(mockutils.PatchSlice{ApplyFunc(findAndMoveForward, func(s string, substr string) (string, bool) {
			return "nil", true
		})})
		defer patches.ResetAll()
		res := checkRuntimeOOMInDmesg()
		convey.So(res, convey.ShouldEqual, true)
	})
	convey.Convey("Test Check Runtim eOOM In Dmesg error 3", t, func() {
		oldPID = "2\" 8771"
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte("\"0004: Apr 04 07:28:05 000400 kernel(): Memory cgroup out of memory: Kill process 2\" 8771 (loco-sps-ip-lx-) score 989 or sacrifice child\""), nil
			})})
		patches.Append(mockutils.PatchSlice{ApplyFunc(findAndMoveForward, func(s string, substr string) (string, bool) {
			return "nil", true
		})})
		defer patches.ResetAll()
		res := checkRuntimeOOMInDmesg()
		convey.So(res, convey.ShouldEqual, false)
	})
	convey.Convey("Test Check Runtim eOOM In Dmesg v2r9", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte("\"0004: Apr 04 07:28:05 000400 kernel(): Memory cgroup out of memory: Killed process 3\" 8771 (loco-sps-ip-lx-) score 989 or sacrifice child\""), nil
			})})
		patches.Append(mockutils.PatchSlice{ApplyFunc(findAndMoveForward, func(s string, substr string) (string, bool) {
			return "nil", true
		})})
		defer patches.ResetAll()
		res := checkRuntimeOOMInDmesg()
		convey.So(res, convey.ShouldEqual, true)
	})
	convey.Convey("Test Check Runtim eOOM In Dmesg v2r9 greenZone", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{ApplyMethod(reflect.TypeOf(&exec.Cmd{}),
			"Output", func(c *exec.Cmd) ([]byte, error) {
				return []byte("\"oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=e2d04c0e4eaa4d782960bca81d" +
					"e0029e86fd6ac9083181c80647e06009863731,mems_allowed=0-1,oom_memcg=/kubepods/burstable/pod9012793d-" +
					"67de-4abd-bdae-da52ba615df1/e2d04c0e4eaa4d782960bca81de0029e86fd6ac9083181c80647e06009863731" +
					",task_memcg=/kubepods/burstable/pod9012793d-67de-4abd-bdae-da52ba615df1/e2d04c0e4eaa4d782960bca81" +
					"de0029e86fd6ac9083181c80647e06009863731, \nMemory cgroup out of memory: Killed process 940408 (python3.6) " +
					"total-vm:936812kB, anon-rss:758232kB, file-rss:12284kB, shmem-rss:0kB \""), nil
			})})
		defer patches.ResetAll()
		res := checkRuntimeOOMInDmesg()
		convey.So(res, convey.ShouldEqual, true)
	})
}
