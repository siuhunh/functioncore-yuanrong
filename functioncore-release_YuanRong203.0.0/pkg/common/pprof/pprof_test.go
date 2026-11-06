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

// Package options define utils
package pprof

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"

	commonutils "functioncore/pkg/common/utils"
	"functioncore/test/utils"
)

func TestNotifyStartPprof(t *testing.T) {
	convey.Convey("TestNotifyStartPprof_normal", t, func() {
		collectPprofInstanceList := []string{"testPod"}
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "POD_NAME" {
					return "testPod"
				}
				return ""
			}),
			gomonkey.ApplyFunc(collectPprof, func() {
				switchLock.Lock()
				collectPprofFileSwitch = true
				switchLock.Unlock()
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		NotifyStartPprof(collectPprofInstanceList)
		convey.So(collectPprofFileSwitch, convey.ShouldEqual, true)
	})
	convey.Convey("TestNotifyStartPprof_no_POD_NAME", t, func() {
		collectPprofInstanceList := []string{"testPod"}
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return ""
			}),
			gomonkey.ApplyFunc(collectPprof, func() {
				switchLock.Lock()
				collectPprofFileSwitch = true
				switchLock.Unlock()
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		NotifyStartPprof(collectPprofInstanceList)
		convey.So(collectPprofFileSwitch, convey.ShouldEqual, false)
	})
	convey.Convey("TestNotifyStartPprof_no_collectPprofInstanceList", t, func() {
		var collectPprofInstanceList []string
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "POD_NAME" {
					return "testPod"
				}
				return ""
			}),
			gomonkey.ApplyFunc(collectPprof, func() {
				switchLock.Lock()
				collectPprofFileSwitch = true
				switchLock.Unlock()
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		NotifyStartPprof(collectPprofInstanceList)
		convey.So(collectPprofFileSwitch, convey.ShouldEqual, false)
	})
	convey.Convey("TestNotifyStartPprof_stop", t, func() {
		collectPprofInstanceList := []string{"testPod-abc"}
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == "POD_NAME" {
					return "testPod-abc"
				}
				return ""
			}),
			gomonkey.ApplyFunc(collectPprof, func() {
				switchLock.Lock()
				collectPprofFileSwitch = true
				switchLock.Unlock()
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		NotifyStartPprof(collectPprofInstanceList)
		convey.So(collectPprofFileSwitch, convey.ShouldEqual, true)
		collectPprofInstanceList = []string{"testPod-def"}
		NotifyStartPprof(collectPprofInstanceList)
		convey.So(collectPprofFileSwitch, convey.ShouldEqual, false)
	})
}

func TestStopCollectCpu(t *testing.T) {
	convey.Convey("TestNotifyStartPprof_normal", t, func() {
		test := false
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc((*os.File).Close, func(_ *os.File) error {
				test = true
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		file := &os.File{}
		stopCollectCPU(file)
		convey.So(test, convey.ShouldEqual, true)
	})
	convey.Convey("TestNotifyStartPprof_normal", t, func() {
		test := false
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc((*os.File).Close, func(_ *os.File) error {
				test = true
				return errors.New("test")
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		file := &os.File{}
		stopCollectCPU(file)
		convey.So(test, convey.ShouldEqual, true)
	})
	convey.Convey("TestNotifyStartPprof_normal", t, func() {
		test := false
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc((*os.File).Close, func(_ *os.File) error {
				test = true
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		stopCollectCPU(nil)
		convey.So(test, convey.ShouldEqual, false)
	})
}

func TestColletPprof(t *testing.T) {
	convey.Convey("TestColletPprof_already_start", t, func() {
		isInitPprofCollectConfig := false
		switchLock.Lock()
		collectPprofFileSwitch = true
		switchLock.Unlock()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(initPprofCollectConfig, func() {
				isInitPprofCollectConfig = true
				return
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		collectPprof()
		convey.So(isInitPprofCollectConfig, convey.ShouldEqual, false)
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
	})
	convey.Convey("TestColletPprof_normal", t, func() {
		isEnd := false
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(initPprofCollectConfig, func() {
				return
			}),
			gomonkey.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
				return nil
			}),
			gomonkey.ApplyFunc(os.Create, func(name string) (*os.File, error) {
				return &os.File{}, nil
			}),
			gomonkey.ApplyFunc((*os.File).Chmod, func(_ *os.File, mode os.FileMode) error {
				return nil
			}),
			gomonkey.ApplyFunc(pprof.StartCPUProfile, func(w io.Writer) error {
				return nil
			}),
			gomonkey.ApplyFunc(pprof.StopCPUProfile, func() {
				return
			}),
			gomonkey.ApplyFunc(pprof.Lookup, func(name string) *pprof.Profile {
				return &pprof.Profile{}
			}),
			gomonkey.ApplyFunc((*pprof.Profile).WriteTo, func(_ *pprof.Profile, w io.Writer, debug int) error {
				return nil
			}),
			gomonkey.ApplyFunc((*os.File).Close, func(_ *os.File) error {
				return nil
			}),
			gomonkey.ApplyFunc(stopCollectCPU, func(cpuFile *os.File) {
				isEnd = true
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		collectPeriod = 10
		collectPprof()
		time.Sleep(20 * time.Second)
		convey.So(isEnd, convey.ShouldEqual, true)
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
	})
}

func TestInitPprofCollectConfig(t *testing.T) {
	convey.Convey("TestInitPprofCollectConfig_normal", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"/home/sn/test\", \"collectPeriod\":60}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/test")
		convey.So(collectPeriod, convey.ShouldEqual, 60)
	})
	convey.Convey("TestInitPprofCollectConfig_normal_two", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"/home/sn/test2\", \"collectPeriod\":160}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/test2")
		convey.So(collectPeriod, convey.ShouldEqual, 160)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_nil", t, func() {
		pprofFilePath = defaultPprofFilePath
		collectPeriod = defaultCollectPeriod
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return ""
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/pprof")
		convey.So(collectPeriod, convey.ShouldEqual, 600)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_nil", t, func() {
		pprofFilePath = defaultPprofFilePath
		collectPeriod = defaultCollectPeriod
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"/home/sn/test\", \"collectPeriod\":5}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/test")
		convey.So(collectPeriod, convey.ShouldEqual, 600)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_unmarshal_fail", t, func() {
		pprofFilePath = defaultPprofFilePath
		collectPeriod = defaultCollectPeriod
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"/home/sn/test\", \"collectPeriod\":160}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return nil
			}),
			gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
				return errors.New("test")
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/pprof")
		convey.So(collectPeriod, convey.ShouldEqual, 600)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_validstruct_fail", t, func() {
		pprofFilePath = defaultPprofFilePath
		collectPeriod = defaultCollectPeriod
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"/home/sn/test\", \"collectPeriod\":5}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return nil
			}),
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return true, errors.New("test")
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/pprof")
		convey.So(collectPeriod, convey.ShouldEqual, 600)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_ValidateFilePath_fail", t, func() {
		pprofFilePath = defaultPprofFilePath
		collectPeriod = defaultCollectPeriod
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"/home/sn/test\", \"collectPeriod\":200}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return errors.New("test")
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/pprof")
		convey.So(collectPeriod, convey.ShouldEqual, 200)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_filepath_nil", t, func() {
		pprofFilePath = defaultPprofFilePath
		collectPeriod = defaultCollectPeriod
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				if key == pprofLogConfigTAG {
					return "{\"filepath\":\"\", \"collectPeriod\":5}"
				}
				return ""
			}),
			gomonkey.ApplyFunc(commonutils.ValidateFilePath, func(path string) error {
				return errors.New("test")
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		initPprofCollectConfig()
		convey.So(pprofFilePath, convey.ShouldEqual, "/home/sn/pprof")
		convey.So(collectPeriod, convey.ShouldEqual, 600)
	})
}

func TestPprofLookup(t *testing.T) {
	convey.Convey("TestInitPprofCollectConfig_logConfig_create_file_fail", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Create, func(name string) (*os.File, error) {
				return &os.File{}, errors.New("test")
			}),
			gomonkey.ApplyFunc((*os.File).Chmod, func(_ *os.File, mode os.FileMode) error {
				return nil
			}),
			gomonkey.ApplyFunc((*os.File).Close, func(_ *os.File) error {
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := lookupPprof(0, "/test")
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestInitPprofCollectConfig_logConfig_create_file_fail", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(os.Create, func(name string) (*os.File, error) {
				return &os.File{}, nil
			}),
			gomonkey.ApplyFunc((*os.File).Chmod, func(_ *os.File, mode os.FileMode) error {
				return errors.New("test")
			}),
			gomonkey.ApplyFunc((*os.File).Close, func(_ *os.File) error {
				return nil
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := lookupPprof(0, "/test")
		convey.So(err, convey.ShouldNotBeNil)
	})
}
