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

package process

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	commonutil "functioncore/pkg/common/utils"
	"functioncore/pkg/runtime-manager/extends/runtimeargs"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/custom"
	golang "functioncore/pkg/worker/runtime/golang/v1"
	"functioncore/pkg/worker/runtime/web"
	workerutils "functioncore/pkg/worker/utils"
)

func Test_createCustomRuntime(t *testing.T) {
	patches := gomonkey.ApplyFunc(custom.RunProcess,
		func(uid, gid int, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
			return &os.Process{}, nil
		})
	defer patches.Reset()
	c := &customRuntime{}
	process, _, actualErr := c.Create(&types.CreateOptions{})
	assert.Nil(t, actualErr)
	assert.Equal(t, &os.Process{}, process)
	patches = gomonkey.ApplyFunc(custom.RunProcess,
		func(uid, gid int, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
			return nil, errors.New("failed to create runtime")
		})
	process, _, actualErr = c.Create(&types.CreateOptions{})
	assert.Nil(t, process)
}

func Test_createWebRuntime(t *testing.T) {
	patches := gomonkey.ApplyFunc(web.RunProcess,
		func(runProcessParams *runtimemanagerrequest.RunProcessParams, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
			return &os.Process{}, nil
		})
	defer patches.Reset()
	c := &webRuntime{}
	process, _, actualErr := c.Create(&types.CreateOptions{})
	assert.Nil(t, actualErr)
	assert.Equal(t, &os.Process{}, process)
	patches = gomonkey.ApplyFunc(web.RunProcess,
		func(runProcessParams *runtimemanagerrequest.RunProcessParams, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
			return nil, errors.New("failed to create runtime")
		})
	process, _, actualErr = c.Create(&types.CreateOptions{})
	assert.Nil(t, process)
}

func Test_createGORuntime(t *testing.T) {
	patches := gomonkey.ApplyFunc(golang.RunProcess,
		func(uid, gid int, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
			return &os.Process{}, nil
		})
	defer patches.Reset()
	c := &goV1Runtime{}
	process, _, actualErr := c.Create(&types.CreateOptions{})
	assert.Nil(t, actualErr)
	assert.Equal(t, &os.Process{}, process)
	patches = gomonkey.ApplyFunc(golang.RunProcess,
		func(uid, gid int, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
			return nil, errors.New("failed to create runtime")
		})
	process, _, actualErr = c.Create(&types.CreateOptions{})
	assert.Nil(t, process)
}

func Test_createFusionRuntime(t *testing.T) {
	patches := gomonkey.ApplyFunc(exec.LookPath, func(string) (string, error) {
		return "", nil
	})
	patches.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Start", func(_ *exec.Cmd) error {
		return nil
	})
	defer patches.Reset()
	c := &fusionRuntime{}

	_, _, err := c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("unknown")))
	assert.Equal(t, fmt.Errorf("unknown language and version: %s", "unknown"), err)

	patch := gomonkey.ApplyFunc(utils.GetExecPath, func(version string) (string, error) {
		return "", fmt.Errorf("getExecPath error")
	})
	_, _, err = c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("java8")))
	assert.Equal(t, fmt.Errorf("getExecPath error"), err)
	patch.Reset()

	patch = gomonkey.ApplyFunc(runtimeargs.GetBuildArgs, func(par runtimeargs.BuildArgsPars, rmConfig *config.RuntimeConfig) ([]string, error) {
		return []string{}, nil
	})
	_, _, err = c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("java8")))
	assert.Equal(t, fmt.Errorf("failed to get runtime args: %s", "java8"), err)
	patch.Reset()

	patch = gomonkey.ApplyFunc(filepath.Abs, func(path string) (string, error) {
		return "", fmt.Errorf("get file path error")
	})
	stablePatch := gomonkey.ApplyFunc(runtimeargs.GetBuildArgs, func(par runtimeargs.BuildArgsPars, rmConfig *config.RuntimeConfig) ([]string, error) {
		return []string{"build"}, nil
	})
	_, _, err = c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("java8")))
	assert.Equal(t, fmt.Errorf("get file path error"), err)
	patch.Reset()
	defer stablePatch.Reset()

	patch = gomonkey.ApplyFunc(checkSubProcessCMDArgs, func(runtimeType string, params ...string) error {
		return fmt.Errorf("checkSubProcessCMDArgs error")
	})
	_, _, err = c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("java8")))
	assert.Equal(t, fmt.Errorf("checkSubProcessCMDArgs error"), err)
	patch.Reset()

	patch = gomonkey.ApplyFunc(workerutils.SetCmdCredByID, func(cmd *exec.Cmd, uid, gid int) error {
		return fmt.Errorf("SetCmdCredByID error")
	})
	_, _, err = c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("java8")))
	assert.Equal(t, fmt.Errorf("SetCmdCredByID error"), err)
	patch.Reset()

	patch = gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Start", func(_ *exec.Cmd) error {
		return fmt.Errorf("cmdStart error")
	})
	_, _, err = c.Create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, &config.Configuration{},
		types.WithVersion("java8")))
	assert.Equal(t, fmt.Errorf("cmdStart error"), err)
	patch.Reset()
}

func TestCheckSubProcessCMDArgs(t *testing.T) {
	convey.Convey("TestCheckSubProcessCMDArgs", t, func() {
		convey.Convey("TestCheckSubProcessCMDArgs params len error", func() {
			err := checkSubProcessCMDArgs("custom-runtime-rpc", "-exec", "/opt/function/code/app 31589", "9999")
			convey.So(err.Error(), convey.ShouldEqual, "custom-runtime-rpc params len error")
		})
		convey.Convey("TestCheckSubProcessCMDArgs cmd len error", func() {
			err := checkSubProcessCMDArgs("custom-runtime-rpc", "-exec", "/opt/function/code/app 31589 9999")
			convey.So(err.Error(), convey.ShouldEqual, "custom-runtime-rpc cmd len error")
		})
		convey.Convey("TestCheckSubProcessCMDArgs handler error", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(commonutil.ExistPath, func(path string) bool {
					return false
				}),
			}
			err := checkSubProcessCMDArgs("custom-runtime-rpc", "-exec", "/opt/function/code/app 31589")
			snError, ok := err.(snerror.SNError)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(snError.Error(), convey.ShouldEqual, "function entry exception, error: handler not exist")
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("TestCheckSubProcessCMDArgs all ok", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(commonutil.ExistPath, func(path string) bool {
					return true
				}),
			}
			err := checkSubProcessCMDArgs("custom-runtime-rpc", "-exec", "/opt/function/code/app 31589")
			convey.So(err, convey.ShouldBeNil)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func Test_Extensioncreate(t *testing.T) {
	patches := gomonkey.ApplyFunc(runtime.RunExtensionProcess,
		func(createOptions *types.CreateOptions, logReporter func([]byte)) (*os.Process, error) {
			return &os.Process{}, nil
		})
	defer patches.Reset()
	e := &extensionProcess{}
	process, _, actualErr := e.Create(&types.CreateOptions{Name: "extension-test"})
	assert.Nil(t, actualErr)
	assert.Equal(t, &os.Process{}, process)
	patches = gomonkey.ApplyFunc(runtime.RunExtensionProcess,
		func(createOptions *types.CreateOptions, logReporter func([]byte)) (*os.Process, error) {
			return &os.Process{}, errors.New("failed to create extension")
		})
	process, _, actualErr = e.Create(&types.CreateOptions{Name: "extension-test"})
	assert.Nil(t, process)
}

func Test_startProcess(t *testing.T) {
	var (
		patches = gomonkey.NewPatches()
	)

	convey.Convey("test startProcess", t, func() {
		convey.Convey("test startProcess in fastfreeze with checkpoint mode", func() {
			patches.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd { return &exec.Cmd{} })
			cmd := startProcess(constants.RunningModeCheckpoint, "", "", nil, "")
			assert.NotNil(t, cmd)
		})

		convey.Convey("test startProcess in fastfreeze with restore mode", func() {
			patches.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd { return &exec.Cmd{} })
			cmd := startProcess(constants.RunningModeRestore, "", "", nil, "")
			assert.NotNil(t, cmd)
		})

		convey.Convey("test startProcess in faststart with checkpoint mode", func() {
			patches.ApplyFunc(os.Getenv, func(key string) string { return "true" })
			patches.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd { return &exec.Cmd{} })
			cmd := startProcess(constants.RunningModeCheckpoint, "", "", nil, "")
			assert.NotNil(t, cmd)
		})

		convey.Convey("test startProcess in faststart with restore mode", func() {
			patches.ApplyFunc(os.Getenv, func(key string) string { return "true" })
			patches.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd { return &exec.Cmd{} })
			cmd := startProcess(constants.RunningModeRestore, "", "", nil, "")
			assert.NotNil(t, cmd)
		})

		convey.Convey("test startProcess in faststart with restore mode with criuOpts", func() {
			var args string
			patches.ApplyFunc(os.Getenv, func(key string) string { return "true" })
			patches.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
				args = strings.Join(arg, " ")
				return &exec.Cmd{}
			})
			criuOpts := "-v3 --cpu-caps=none"
			cmd := startProcess(constants.RunningModeRestore, "", "", nil, criuOpts)
			assert.NotNil(t, cmd)
			convey.So(strings.Contains(args, criuOpts), convey.ShouldBeTrue)
		})

		convey.Reset(func() {
			patches.Reset()
		})
	})
}
