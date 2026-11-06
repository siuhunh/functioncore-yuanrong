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
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/fsnotify/fsnotify"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	commontypes "functioncore/pkg/common/types"
	utilities "functioncore/pkg/common/utils"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/runtime-manager/extends/debug"
	"functioncore/pkg/runtime-manager/extends/runtimeargs"
	"functioncore/pkg/runtime-manager/extends/usergroup"
	"functioncore/pkg/runtime-manager/functionlog"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker"
	wconfig "functioncore/pkg/worker/config"
	workerconfig "functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
	workerutil "functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

func initManager() {
	Manager.ConnectionWithWorker = &tcpmsg.SocketConn{}
	Manager.conf.RuntimeConfig = wconfig.RuntimeConfig{
		FuncMountConfig: commontypes.FuncMountConfig{
			FuncMountUser: commontypes.FuncMountUser{
				UserID:  1003,
				GroupID: 1003,
			},
		},
		RMConfig: rmconfig.Config{RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
			"unknownVersion":             {Enable: true, CustomArgs: []string{}},
			worker.CustomRuntimeLanguage: {Enable: true, CustomArgs: []string{}},
			utils.Java8:                  {Enable: true, CustomArgs: []string{"apm-javaagent=apm-javaagent.jar"}},
			utils.Nodejs:                 {Enable: false, CustomArgs: []string{}},
			utils.CangJie10:              {Enable: true, CustomArgs: []string{}},
		}},
	}
}

func TestMain(m *testing.M) {
	initManager()
	m.Run()
}

func Test_getRequestBody(t *testing.T) {
	msg := tcpmsg.Message{
		Body: []byte(`{"Conf":null,"Version":"Java8"}`),
	}
	if _, err := getRequestBody(&msg); err != nil {
		t.Errorf("test getRequestBody err:%s", err)
	}
}

func TestGotRuntimeException(t *testing.T) {
	runtimeError.Reset()
	patch := gomonkey.ApplyFunc(oom.IsRuntimeOOMKilled, func() bool { return true })
	defer patch.Reset()

	code, err := GotRuntimeException()
	assert.Equal(t, false, err != nil)
	assert.Equal(t, snerror.RuntimeMemoryExceedLimit, code)
}

func Test_stopRedundancyProcesses(t *testing.T) {
	Manager.Processes.Store("exceptVersion", nil)
	Manager.Processes.Store("versionWithInvalidProcess", &os.Process{})
	err := stopRedundancyProcesses("exceptVersion")
	assert.Equal(t, true, err == nil)
	Manager.Processes.Delete("exceptVersion")
	Manager.Processes.Delete("versionWithInvalidProcess")
	Manager.Processes.Store(-1, nil)
	err = stopRedundancyProcesses("exceptVersion")
	assert.Equal(t, true, err == nil)
	Manager.Processes.Delete(-1)
}

func Test_getLangFromVersion(t *testing.T) {
	assert.Equal(t, utils.Nodejs, getLangFromVersion("nodejs10.15.2"))
	assert.Equal(t, "", getLangFromVersion("unknownVersion"))
}

func Test_ReportLog(t *testing.T) {
	patch := gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(tcp *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	reportLog([]byte("logs string"))
	patch.Reset()

	logReportEnable = false
	reportLog([]byte("logs string"))
}

func Test_PreStartRuntimeProcesses(t *testing.T) {
	defer gomonkey.ApplyFunc(create, func(options *types.CreateOptions) (*os.Process, error) { return nil, nil }).Reset()
	runtimeConfigs := make(map[string]rmconfig.RuntimeConfig, 10)
	runtimeConfigs["java8"] = rmconfig.RuntimeConfig{Enable: true, Port: 23530}
	PreStartRuntimeProcesses(&rmconfig.Config{RuntimeConfigs: runtimeConfigs})

	defer gomonkey.ApplyFunc(create, func(options *types.CreateOptions) (*os.Process, error) {
		return nil, errors.New("failed to start spec runtime process")
	}).Reset()
	PreStartRuntimeProcesses(&rmconfig.Config{})
}

func Test_stopSpecRuntimeProcess(t *testing.T) {
	Manager.Processes.Store("testVersion1", &os.Process{})
	Manager.Processes.Store("testVersion2", &struct{}{})
	defer func() {
		Manager.Processes.Delete("testVersion1")
		Manager.Processes.Delete("testVersion2")
	}()
	defer gomonkey.ApplyMethod(reflect.TypeOf(&os.Process{}), "Kill", func(*os.Process) error { return nil }).Reset()
	assert.Equal(t, nil, stopSpecRuntimeProcess("java"))
	assert.Equal(t, false, stopSpecRuntimeProcess("testVersion1") != nil)
	assert.Equal(t, false, stopSpecRuntimeProcess("testVersion2") != nil)
}

func Test_watchingRuntimeMemoryUsedLoop(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(Manager.ConnectionWithWorker), "SendMessageWithNoResp", func(*tcpmsg.SocketConn, *tcpmsg.Message) error {
		return errors.New("invalid connection")
	})
	runtimeError.Reset()
	defer func() {
		patch.Reset()
		runtimeError.Reset()
	}()
	close(ExitSignal)
	defer func() {
		ExitSignal = make(chan struct{})
	}()
	watchingRuntimeMemoryUsedLoop(math.MaxInt64, "test")
	ExitSignal = make(chan struct{})

	close(ExitSignalOnNoRequest)
	defer func() {
		ExitSignalOnNoRequest = make(chan struct{})
	}()
	watchingRuntimeMemoryUsedLoop(math.MaxInt64, "test")
	watchingRuntimeMemoryUsedLoop(0, "test")
	watchingRuntimeMemoryUsedLoop(math.MaxInt64, "test")
}

func Test_watchingRuntimeMemoryUsedLoop_create_fail(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(monitor.NewCGroupMemoryParser, func() (*monitor.Parser, error) {
		return nil, errors.New("failed to create parser")
	})
	defer patches.Reset()
	watchingRuntimeMemoryUsedLoop(math.MaxInt64, "test")
}

func Test_defaultRuntimeProcess(t *testing.T) {
	patches := gomonkey.ApplyMethod(reflect.TypeOf(&customRuntime{}), "Create",
		func(_ *customRuntime, createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
			return nil, nil, errors.New("mock customRuntime ")
		})
	defer patches.Reset()
	Manager.currentVersion = worker.CustomRuntimeLanguage
	code, _ := getOrStartDefaultRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
	assert.Equal(t, 0, code)

	patches.ApplyMethod(reflect.TypeOf(&webRuntime{}), "Create",
		func(_ *webRuntime, createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
			return nil, nil, nil
		})
	Manager.currentVersion = worker.WebRuntimeLanguage
	code, _ = getOrStartDefaultRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
	assert.Equal(t, 0, code)

	patches.ApplyMethod(reflect.TypeOf(&goV1Runtime{}), "Create",
		func(_ *goV1Runtime, createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
			return nil, nil, nil
		})
	Manager.currentVersion = worker.GOV1RuntimeLanguage
	code, _ = getOrStartDefaultRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
	assert.Equal(t, 0, code)

	patches.ApplyFunc(create, func(options *types.CreateOptions) (*os.Process, error) {
		return &os.Process{}, nil
	})
	Manager.currentVersion = "Test_defaultRuntimeProcess_Version_restart"
	code, _ = getOrStartDefaultRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
	assert.Equal(t, tcpmsg.RuntimeRestarted, code)

	Manager.Processes.Store("Test_Version_exit", &os.Process{})
	defer Manager.Processes.Delete("Test_Version_exit")
	Manager.currentVersion = "Test_Version_exit"
	code, _ = getOrStartDefaultRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
	assert.Equal(t, 0, code)
}

func Test_startRuntimeProcess(t *testing.T) {
	fmConf := commontypes.FuncMountConfig{
		FuncMountUser: commontypes.FuncMountUser{UserID: constants.DefaultRuntimeUID, GroupID: constants.DefaultRuntimeGID},
	}
	runtime := wconfig.RuntimeConfig{
		EnvKey:          "",
		EnvInfo:         "",
		FuncMountConfig: fmConf,
	}
	Manager.conf = &wconfig.Configuration{
		RuntimeConfig: runtime,
	}
	code, err := startRuntimeProcess(100, 100, &runtimemanagerrequest.InitRequestBody{})
	t.Logf("code: %d, err: %s", code, err.Error())
	patch := gomonkey.ApplyFunc(create, func(options *types.CreateOptions) (*os.Process, error) {
		return nil, snerror.New(snerror.RuntimeBootstrapNotFound, snerror.ErrText(snerror.RuntimeBootstrapNotFound))
	})
	Manager.RuntimeCreator = &goV1Runtime{}
	code, err = startRuntimeProcess(constants.DefaultRuntimeUID, constants.DefaultRuntimeUID, &runtimemanagerrequest.InitRequestBody{})
	assert.Equal(t, snerror.RuntimeBootstrapNotFound, code)
	assert.Equal(t, snerror.New(snerror.RuntimeBootstrapNotFound, snerror.ErrText(snerror.RuntimeBootstrapNotFound)), err)
	patch.Reset()
	Manager.RuntimeCreator = &fusionRuntime{}
}

func Test_stopRuntimeProcess(t *testing.T) {
	fmConf := commontypes.FuncMountConfig{
		FuncMountUser: commontypes.FuncMountUser{UserID: constants.DefaultRuntimeUID, GroupID: constants.DefaultRuntimeGID},
	}
	runtime := wconfig.RuntimeConfig{
		EnvKey:          "",
		EnvInfo:         "",
		FuncMountConfig: fmConf,
	}
	conf := &wconfig.Configuration{
		RuntimeConfig: runtime,
	}
	req := &wconfig.RequestBody{
		Conf:    conf,
		Envs:    &workerutil.Environment{},
		Version: "",
	}
	msg := &tcpmsg.Message{}
	msg.Body, _ = json.Marshal(*req)
	dnsMap := make(map[string][]string)
	dnsMap["a"] = []string{"aaa", "bbb"}
	req.Conf.DNSDomainNames = dnsMap
	mockErr := errors.New("mock error")
	debugTag := ""
	isRestore := true
	patches := gomonkey.ApplyFunc(stopRedundancyProcesses, func(exceptVersion string) error { return mockErr })
	patches.ApplyFunc(isCheckPointRestore, func() bool { return isRestore })
	patches.ApplyFunc(watchingRuntimeMemoryUsedLoop, func(hardLimit int64, version string) { return })
	patches.ApplyFunc(watchingRuntimeManagerMemoryUsedLoop, func(hardLimit int64) { return })
	patches.ApplyFunc(watchHostFileLoop, func() { return })
	patches.ApplyFunc(usergroup.CreateMountUser, func(uid, gid int) error { return nil })
	patches.ApplyFunc(os.StartProcess, func(name string, argv []string, attr *os.ProcAttr) (*os.Process, error) { return nil, nil })
	patches.ApplyFunc(processSoftLink, func(exceptVersion string) error { return errors.New("error") })
	patches.ApplyFunc(debug.ExtractDebugTag, func(conf *wconfig.Configuration, currentVersion string) (string, map[string]interface{}) {
		return debugTag, make(map[string]interface{})
	})
	patches.ApplyFunc(prepareRestartWhenStopRuntimeProcess, func() (*prepareRestartPars, error) { return nil, errors.New("prepare error") })
	defer patches.Reset()

	code, err := StopRuntimeProcess(msg)
	assert.Equal(t, true, err != nil)
	assert.Equal(t, 0, code)

	isRestore = false
	code, err = StopRuntimeProcess(msg)
	assert.Equal(t, true, err != nil)
	assert.Equal(t, 0, code)

	mockErr = nil
	debugTag = "true"
	code, err = StopRuntimeProcess(msg)
	assert.Equal(t, true, err == nil)
	assert.Equal(t, 0, code)

	mockErr = errors.New("testErr")
	debugTag = ""
	_, err = StopRuntimeProcess(msg)
	assert.Equal(t, true, err != nil)

	code, err = StopRuntimeProcess(&tcpmsg.Message{Body: nil})
	assert.Equal(t, true, err != nil)
	assert.Equal(t, 0, code)

	patches.ApplyFunc(appendCustomArgs, func(reqBody *wconfig.RequestBody) (bool, error) { return false, errors.New("error") })
	code, err = StopRuntimeProcess(msg)
	assert.Equal(t, true, err != nil)
	assert.Equal(t, snerror.FunctionExceptionCode, code)
}

func Test_inspectProcess(t *testing.T) {
	process := &os.Process{Pid: -1}
	inspectProcess(nil, "", nil)
	inspectProcess(process, "", nil)
	patches := gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return &os.ProcessState{}, nil
	})
	inspectProcess(process, "", nil)
	patches.Reset()
	patches = gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(tcp *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	inspectProcess(process, "", nil)
	patches.Reset()

	patches = gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return &os.ProcessState{}, nil
	})
	patches = gomonkey.ApplyFunc((*os.ProcessState).String, func(o *os.ProcessState) string {
		return "aborted (core dumped)"
	})
	tmp := Manager.currentVersion
	Manager.currentVersion = "nodejs8.10"
	buf := functionlog.NewFixSizeWriter()
	_, _ = buf.Write([]byte(nodejsOOMCoreDumpString))
	inspectProcess(process, "", buf)
	if runtimeError.Get() != snerror.RuntimeMemoryExceedLimit {
		t.Errorf("runtimeError code is %d", runtimeError.Get())
	}
	patches.Reset()
	runtimeError.Reset()
	Manager.currentVersion = tmp

	patches = gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return &os.ProcessState{}, nil
	})
	patches = gomonkey.ApplyFunc((*os.ProcessState).String, func(o *os.ProcessState) string {
		return "aborted (core dumped)"
	})
	tmp1 := Manager.currentVersion
	Manager.currentVersion = "nodejs8.10"
	buf1 := functionlog.NewFixSizeWriter()
	_, _ = buf1.Write([]byte("abc"))
	inspectProcess(process, "", buf1)
	if runtimeError.Get() != 0 {
		t.Errorf("runtimeError code is %d", runtimeError.Get())
	}
	patches.Reset()
	runtimeError.Reset()
	Manager.currentVersion = tmp1
}

func Test_inspectProcessWithRestore(t *testing.T) {
	tmpVersion := Manager.currentVersion
	tmpMode := Manager.currentMode
	process := &os.Process{Pid: 100}
	patches := gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return nil, nil
	})
	gomonkey.ApplyFunc((*exec.Cmd).Output, func(_ *exec.Cmd) ([]byte, error) {
		return []byte("mock"), nil
	})
	gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(tcp *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	Manager.currentVersion = "1"
	Manager.currentMode = restore
	inspectProcess(process, "1", nil)
	inspectProcess(process, "1", nil)
	inspectProcess(process, "1", nil)
	patches.Reset()
	runtimeError.Reset()
	Manager.currentVersion = tmpVersion
	Manager.currentMode = tmpMode
}

func Test_attemptToRestart(t *testing.T) {
	patches := gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return errors.New("failed to send message")
	})
	defer patches.Reset()

	patches1 := gomonkey.ApplyFunc((*RuntimeProcess).restartRuntimeProcessWithRetry, func(r *RuntimeProcess) error {
		return errors.New("failed to restart runtime")
	})
	AttemptToRestart()
	Manager.CurrentRuntime.restartRuntimeProcessWithRetry()
	patches1.Reset()

	patches2 := gomonkey.ApplyFunc((*RuntimeProcess).restartRuntimeProcessWithRetry, func(r *RuntimeProcess) error {
		return nil
	})
	AttemptToRestart()
	patches2.Reset()

	{
		preMode := Manager.currentMode
		Manager.currentMode = restoreDead
		err := AttemptToRestart()
		assert.NotNil(t, err)
		Manager.currentMode = preMode
	}
}

func Test_gracefulShutdown(t *testing.T) {
	ch := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch <- struct{}{}
	}()
	gracefulShutdown(ch)

	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ch)
	}()
	gracefulShutdown(ch)
}

func Test_gracefulShutdownWithExitSignal(t *testing.T) {
	ExitOnce.Do(func() {
		close(ExitSignal)
	})
	ch := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		ch <- struct{}{}
	}()
	gracefulShutdown(ch)

	isChValued := true
	for {
		select {
		case <-ch:
		default:
			isChValued = false
		}
		break
	}
	assert.Equal(t, false, isChValued)
}

func TestGracefulShutdown(t *testing.T) {
	patches := gomonkey.ApplyFunc(gracefulShutdown, func(stopCh <-chan struct{}) { return })
	patches.ApplyMethod(reflect.TypeOf(Manager.ConnectionWithWorker), "SendMessageWithNoResp",
		func(*tcpmsg.SocketConn, *tcpmsg.Message) error {
			return nil
		})
	patches.ApplyFunc(os.Exit, func(code int) { return })

	defer func() {
		patches.Reset()
		ExitSignalOnNoRequest = make(chan struct{})
	}()

	ch := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ch)
	}()
	close(ExitSignalOnNoRequest)
	GracefulShutdown(ch)
	ch2 := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ch2)
	}()
	GracefulShutdown(ch2)
}

func TestGracefulShutdownWithFailedConnect(t *testing.T) {
	isSendMessageFlag := false
	patches := gomonkey.ApplyFunc(gracefulShutdown, func(stopCh <-chan struct{}) { return })
	patches.ApplyMethod(reflect.TypeOf(Manager.ConnectionWithWorker), "SendMessageWithNoResp",
		func(*tcpmsg.SocketConn, *tcpmsg.Message) error {
			isSendMessageFlag = true
			return nil
		})
	patches.ApplyFunc(os.Exit, func(code int) { return })
	Manager.ConnectionWithWorker = nil
	ch3 := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ch3)
	}()
	GracefulShutdown(ch3)
	assert.Equal(t, false, isSendMessageFlag)
	Manager.ConnectionWithWorker = &tcpmsg.SocketConn{}
	patches.Reset()
}

func Test_create(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&fusionRuntime{}), "Create",
			func(_ *fusionRuntime, createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
				return &os.Process{Pid: 10}, nil, nil
			}),
		gomonkey.ApplyFunc(utils.AdjustRuntimeProcess, func(process *os.Process) error {
			return nil
		}),
		gomonkey.ApplyFunc(inspectProcess, func(process *os.Process, version string, buf *functionlog.FixSizeWriter) {
			return
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Manager.currentVersion = "nodejs12.13"
	process, _ := create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, Manager.conf,
		types.WithVersion(Manager.currentVersion)))
	assert.Equal(t, &os.Process{Pid: 10}, process)
	Manager.Processes.Delete(Manager.currentVersion)
}

func Test_checkRuntimeProcessAlive(t *testing.T) {
	Manager.currentVersion = "testVersion"
	defer func() {
		Manager.Processes.Delete("testVersion")
		Manager.currentVersion = ""
	}()
	Manager.Processes.Store("testVersion", nil)
	alive := checkRuntimeProcessAlive()
	assert.Equal(t, false, alive)

	Manager.Processes.Store("testVersion", &os.Process{})
	alive = checkRuntimeProcessAlive()
	assert.Equal(t, false, alive)

	patches := gomonkey.ApplyFunc((*os.Process).Signal, func(os *os.Process, s os.Signal) error { return nil })
	defer patches.Reset()
	alive = checkRuntimeProcessAlive()
	assert.Equal(t, true, alive)
}

func Test_PrintInputLog(t *testing.T) {
	Manager.CurrentRuntime = &RuntimeProcess{Process: &os.Process{}, Version: "testVersion"}
	PrintInputLog()
	Manager.CurrentRuntime = &RuntimeProcess{Process: nil, Version: "testVersion"}
	PrintInputLog()
}

func Test_checkSubProcessCMDArgs(t *testing.T) {
	CMDArgs := []string{"-cp", "-Dlog4j2.configurationFile=file:/home/snuser/config/log4j2.xml", runtimeargs.JavaMainClass}
	err := checkSubProcessCMDArgs("", CMDArgs...)
	assert.Equal(t, nil, err)
	CMDArgs = []string{"-cp", "#", "$"}
	err = checkSubProcessCMDArgs("", CMDArgs...)
	assert.NotEqual(t, nil, err)
}

func Test_KillRuntimeProcess(t *testing.T) {
	convey.Convey("test killRuntimeProcess", t, func() {
		convey.Convey("kill RunningModeRestore success", func() {
			Manager.CurrentRuntime = &RuntimeProcess{}
			Manager.CurrentRuntime.Process = &os.Process{}
			Manager.conf.RunningMode = constants.RunningModeRestore
			count := 0
			patch := gomonkey.ApplyFunc((*RuntimeProcess).ProcessSignal, func(r *RuntimeProcess, sig syscall.Signal) ([]int, error) {
				return nil, errors.New("failed to restart runtime")
			})
			patch.ApplyFunc(syscall.Kill, func(pid int, sig syscall.Signal) (err error) {
				count++
				return nil
			})
			defer patch.Reset()
			responseCode, err := KillRuntimeProcess()
			convey.So(responseCode, convey.ShouldEqual, -1)
			convey.So(err, convey.ShouldBeNil)
			convey.So(count, convey.ShouldEqual, 2)
		})
		convey.Convey("kill pgid success", func() {
			Manager.CurrentRuntime = &RuntimeProcess{}
			Manager.CurrentRuntime.Process = &os.Process{}
			Manager.conf.RunningMode = constants.RunningModeRestore
			count := 0
			getPgidFlag := false
			patch := gomonkey.ApplyFunc((*RuntimeProcess).ProcessSignal, func(r *RuntimeProcess, sig syscall.Signal) ([]int, error) {
				return nil, errors.New("failed to restart runtime")
			})
			patch.ApplyFunc(syscall.Kill, func(pid int, sig syscall.Signal) (err error) {
				count++
				return nil
			})
			patch.ApplyFunc(syscall.Getpgid, func(pid int) (int, error) {
				getPgidFlag = true
				return 12345, nil
			})
			defer patch.Reset()
			responseCode, err := KillRuntimeProcess()
			convey.So(responseCode, convey.ShouldEqual, -1)
			convey.So(err, convey.ShouldBeNil)
			convey.So(count, convey.ShouldEqual, 2)
			convey.So(getPgidFlag, convey.ShouldBeTrue)
		})
	})
}

func Test_initRuntimeProcess(t *testing.T) {
	Manager.conf = &wconfig.Configuration{
		RuntimeConfig: wconfig.RuntimeConfig{
			FuncMountConfig: commontypes.FuncMountConfig{
				FuncMountUser: commontypes.FuncMountUser{
					UserID:  0,
					GroupID: 0,
				},
			},
		},
	}
	defer func() {
		Manager.conf = &wconfig.Configuration{}
	}()
	convey.Convey("test InitRuntimeProcess", t, func() {
		convey.Convey("wrong uid", func() {
			Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.UserID = -100
			code, err := InitRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
			convey.So(code, convey.ShouldEqual, 0)
			convey.So(err, convey.ShouldNotBeNil)
			Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.UserID = 0
		})
		convey.Convey("restore", func() {
			Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.UserID = 0
			patch := gomonkey.ApplyFunc(restoreRuntimeProcess,
				func(uid int, gid int) (int, error) {
					return 0, nil
				})
			patch.ApplyFunc(watchingRuntimeManagerMemoryUsedLoop, func(int64) {})
			patch.ApplyFunc(monitorRuntimeDirMonitor,
				func() error {
					return nil
				})
			defer patch.Reset()
			Manager.conf.RunningMode = constants.RunningModeRestore
			code, err := InitRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(code, convey.ShouldEqual, 0)
		})
		convey.Convey("failed to create dir monitor", func() {
			patch := gomonkey.ApplyFunc(monitorRuntimeDirMonitor,
				func() error {
					return errors.New("test")
				})
			defer patch.Reset()
			code, err := InitRuntimeProcess(&runtimemanagerrequest.InitRequestBody{})
			convey.So(code, convey.ShouldEqual, 0)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func Test_atomicErrorCode_set(t *testing.T) {
	convey.Convey("Test_atomicErrorCode_set", t, func() {
		runtimeError.Set(1)
		convey.So(runtimeError.Get(), convey.ShouldEqual, 1)
	})
}

func Test_atomicErrorCode_override(t *testing.T) {
	convey.Convey("Test_atomicErrorCode_override", t, func() {
		runtimeError.Set(1)
		convey.So(runtimeError.Get(), convey.ShouldEqual, 1)
		runtimeError.Override(2)
		convey.So(runtimeError.Get(), convey.ShouldEqual, 2)
	})
}

func Test_restartRuntimeProcess(t *testing.T) {
	convey.Convey("Test_restartRuntimeProcess", t, func() {
		runtime := &RuntimeProcess{}
		convey.Convey("Test_restartRuntimeProcess_success", func() {
			err := runtime.restartRuntimeProcess()
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func Test_checkRuntimeMemory(t *testing.T) {
	convey.Convey("Test_checkRuntimeMemory", t, func() {
		lastMemoryUsed := 0.0
		convey.Convey("Test_checkRuntimeMemory_tmpMemoryUsed", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(Manager.CurrentRuntime.Process), "Kill", func(_ *os.Process) error {
					return errors.New("kill fail")
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(Manager.ConnectionWithWorker), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
					return errors.New("send message fail")
				}),
				gomonkey.ApplyFunc((*RuntimeProcess).ProcessSignal, func(r *RuntimeProcess, sig syscall.Signal) ([]int, error) {
					return nil, errors.New("failed to restart runtime")
				}),
				gomonkey.ApplyFunc(syscall.Kill, func(pid int, sig syscall.Signal) (err error) { return nil }),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			Manager.CurrentRuntime.Process = &os.Process{
				Pid: 1,
			}
			parser := monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				return uint64(2 * bytesToMB), nil
			})
			checkRuntimeMemory(&lastMemoryUsed, 1, "", parser)
			convey.So(runtimeMemoryWarning, convey.ShouldEqual, true)
			convey.So(lastMemoryUsed, convey.ShouldEqual, 2)

			parser = monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				return uint64(0.5 * bytesToMB), nil
			})
			checkRuntimeMemory(&lastMemoryUsed, 1, "", parser)
			convey.So(runtimeMemoryWarning, convey.ShouldEqual, false)
			convey.So(lastMemoryUsed, convey.ShouldEqual, 0.5)
		})
		convey.Convey("Test_checkRuntimeMemory_parse_fail", func() {
			last := 0.0
			parser := monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				return uint64(0), errors.New("")
			})
			checkRuntimeMemory(&last, 1, "", parser)
			convey.So(last, convey.ShouldEqual, 0)
		})
		convey.Convey("failed to kill same process", func() {
			last := 0.0
			parser := monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				return uint64(1024 * 1024 * 2), nil
			})
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(Manager.CurrentRuntime.Process), "Kill", func(_ *os.Process) error {
					return errors.New("kill fail")
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(Manager.ConnectionWithWorker), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
					return errors.New("send message fail")
				}),
				gomonkey.ApplyFunc(syscall.Getpgid, func(pid int) (pgid int, err error) { return 0, errors.New("test") }),
				gomonkey.ApplyFunc(syscall.Kill, func(pid int, sig syscall.Signal) (err error) { return errors.New("test") }),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
				killProcessFailedTimes = 0
			}()
			Manager.CurrentRuntime.Process = &os.Process{
				Pid: 1,
			}
			latestKillPid = 1
			checkRuntimeMemory(&last, 1, "", parser)
			convey.So(killProcessFailedTimes, convey.ShouldEqual, 1)
		})
	})
}

func Test_getNodeJsCmdStdOutOOMCoreDump(t *testing.T) {
	buf1 := functionlog.NewFixSizeWriter()
	_, _ = buf1.Write([]byte("abc"))
	_, _ = buf1.Write([]byte("abc"))
	buf2 := functionlog.NewFixSizeWriter()
	_, _ = buf2.Write([]byte("FatalProcessOutOfMemory"))
	type args struct {
		currentLanguage string
		buf             *functionlog.FixSizeWriter
		state           *os.ProcessState
	}
	tests := []struct {
		name        string
		args        args
		want        bool
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 other language", args{currentLanguage: utils.Python}, false, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case2 nodejs not coredump",
			args{currentLanguage: utils.Nodejs, state: &os.ProcessState{}}, false, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			}},
		{"case3 buf nil", args{currentLanguage: utils.Nodejs, buf: nil, state: &os.ProcessState{}},
			false, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
						return &os.ProcessState{}, nil
					}),
					gomonkey.ApplyFunc((*os.ProcessState).String, func(o *os.ProcessState) string {
						return "aborted (core dumped)"
					}),
				})
				return patches
			}},
		{"case4 buf not matched", args{currentLanguage: utils.Nodejs, buf: buf1, state: &os.ProcessState{}},
			false, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
						return &os.ProcessState{}, nil
					}),
					gomonkey.ApplyFunc((*os.ProcessState).String, func(o *os.ProcessState) string {
						return "aborted (core dumped)"
					}),
				})
				return patches
			}},
		{"case5 nodejs heap err FatalProcessOutOfMemory",
			args{currentLanguage: utils.Nodejs, buf: buf2, state: &os.ProcessState{}}, true, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
						return &os.ProcessState{}, nil
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := getNodeJsCmdStdOutOOMCoreDump(tt.args.buf, tt.args.state)
			if (err != nil) != tt.wantErr {
				t.Errorf("getNodeJsCmdStdOutOOMCoreDump() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getNodeJsCmdStdOutOOMCoreDump() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_buildRuntimeCreator(t *testing.T) {
	buildRuntimeCreator(worker.GOV1RuntimeLanguage)
	assert.Equal(t, &goV1Runtime{}, Manager.RuntimeCreator)
	buildRuntimeCreator(worker.WebRuntimeLanguage)
	assert.Equal(t, &webRuntime{}, Manager.RuntimeCreator)
	buildRuntimeCreator(worker.CustomRuntimeLanguage)
	assert.Equal(t, &customRuntime{}, Manager.RuntimeCreator)
	buildRuntimeCreator("nodejs12.13")
	assert.Equal(t, &fusionRuntime{}, Manager.RuntimeCreator)
}

func Test_isGeneralizingStartupRuntime(t *testing.T) {
	b := isGeneralizingStartupRuntime(worker.CustomRuntimeLanguage)
	assert.Equal(t, true, b)
	b = isGeneralizingStartupRuntime(utils.Java11)
	assert.Equal(t, false, b)
}

func Test_checkRuntimeManagerMemory(t *testing.T) {
	type args struct {
		suicideLimit float64
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		parser      *monitor.Parser
	}{
		{
			name: "case1 over danger threshold",
			args: args{
				suicideLimit: float64(128) * runtimeManagerMemSuicidePercent,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			parser: monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				// 40.1*1024*1024/4096
				return uint64(10265), nil
			}),
		},
		{
			name: "case2 suicide threshold",
			args: args{
				suicideLimit: float64(128) * runtimeManagerMemSuicidePercent,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Exit, func(code int) {
						return
					}),
				})
				return patches
			},
			parser: monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				// 110*1024*1024/4096
				return uint64(28160), nil
			}),
		},
		{
			name: "case3 failed to parse memory pages",
			args: args{
				suicideLimit: float64(128) * runtimeManagerMemSuicidePercent,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			parser: monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
				return uint64(0), errors.New("failed to read")
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			checkRuntimeManagerMemory(tt.args.suicideLimit, tt.parser)
			patches.ResetAll()
		})

	}
}

func Test_initParser(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(monitor.NewCGroupCPUParser, func() (*monitor.Parser, error) {
		return nil, errors.New("failed to create parser")
	})
	patches.ApplyFunc(monitor.NewSystemCPUParser, func() (*monitor.Parser, error) {
		return nil, errors.New("failed to create parser")
	})
	defer patches.Reset()
	initParser()
}

func Test_watchingRuntimeManagerMemoryUsedLoop(t *testing.T) {
	ExitSignal = make(chan struct{})
	close(ExitSignal)
	defer func() {
		ExitSignal = make(chan struct{})
	}()
	watchingRuntimeManagerMemoryUsedLoop(math.MaxInt64)
	ExitSignal = make(chan struct{})

	ExitSignalOnNoRequest = make(chan struct{})
	close(ExitSignalOnNoRequest)
	defer func() {
		ExitSignalOnNoRequest = make(chan struct{})
	}()
	watchingRuntimeManagerMemoryUsedLoop(math.MaxInt64)
}

func Test_watchingRuntimeManagerMemoryUsedLoop_create_fail(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(monitor.NewProcMemoryParser, func(_ string) (*monitor.Parser, error) {
		return nil, errors.New("failed to create parser")
	})
	defer patches.Reset()
	watchingRuntimeManagerMemoryUsedLoop(0)
}

func Test_checkpointRuntimeByFastFreeze(t *testing.T) {
	convey.Convey("Test_checkpointRuntimeByFastFreeze", t, func() {
		convey.Convey("Test_checkpointRuntimeByFastFreeze_success", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil })
			code, err := CheckpointRuntimeByFastFreeze()
			convey.So(err, convey.ShouldEqual, nil)
			convey.So(code, convey.ShouldEqual, 0)
		})
		convey.Convey("Test_checkpointRuntimeByFastFreeze_fail", func() {
			patches := gomonkey.NewPatches()
			patches.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return fmt.Errorf("abc") })
			code, err := CheckpointRuntimeByFastFreeze()
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(code, convey.ShouldEqual, 1)
		})
	})
}

type createExtensionFailed struct {
}

func (f createExtensionFailed) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	return &os.Process{}, nil, errors.New("test")
}

type createExtensionSucceed struct {
}

func (f createExtensionSucceed) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	return &os.Process{}, nil, nil
}

func Test_createExtension(t *testing.T) {
	Manager = &processMgr{
		conf:             &wconfig.Configuration{},
		RuntimeCreator:   &fusionRuntime{},
		CurrentRuntime:   &RuntimeProcess{},
		CurrentExtension: []*ExtensionProcess{},
		currentMode:      pool,
	}
	convey.Convey("Test_createExtension", t, func() {
		convey.Convey("Test_createExtension_failed", func() {
			Manager.ExtensionCreator = createExtensionFailed{}
			createOpt := &types.CreateOptions{
				Name: "test",
			}
			_, err := createExtension(createOpt)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("Test_createExtension_succeed", func() {
			Manager.ExtensionCreator = createExtensionSucceed{}
			patches := gomonkey.ApplyFunc(inspectExtensionProcess, func(process *os.Process) {
				return
			})
			defer func() {
				time.Sleep(1 * time.Second)
				patches.Reset()
			}()
			createOpt := &types.CreateOptions{
				Name: "test",
			}
			_, err := createExtension(createOpt)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_InitExtensionProcess(t *testing.T) {
	Manager = &processMgr{
		conf:             &wconfig.Configuration{},
		RuntimeCreator:   &fusionRuntime{},
		CurrentRuntime:   &RuntimeProcess{},
		CurrentExtension: []*ExtensionProcess{},
		currentMode:      pool,
	}
	convey.Convey("Test_InitExtensionProcess", t, func() {
		convey.Convey("case1: failed to check extension name", func() {
			patches := gomonkey.ApplyFunc(runtime.CheckExtension,
				func() ([]string, bool) {
					return []string{}, false
				})
			defer patches.Reset()
			err := InitExtensionProcess()
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("case2: failed to exec InitExtensionProcess", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.CheckExtension,
					func() ([]string, bool) {
						return []string{"extension_1.sh", "extension_2.sh"}, true
					}),
				gomonkey.ApplyFunc(buildExtensionCreator,
					func() {
						return
					}),
				gomonkey.ApplyFunc(createExtension,
					func(options *types.CreateOptions) (*os.Process, error) {
						if options.Name == "extension_1.sh" {
							return &os.Process{}, nil
						}
						return &os.Process{}, errors.New("test")
					}),
				gomonkey.ApplyFunc(inspectExtensionProcess, func(process *os.Process) {
					return
				}),
			}
			defer func() {
				time.Sleep(1 * time.Second)
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			err := InitExtensionProcess()
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("case3: succeed to exec InitExtensionProcess", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.CheckExtension,
					func() ([]string, bool) {
						return []string{}, true
					}),
				gomonkey.ApplyFunc(inspectExtensionProcess, func(process *os.Process) {
					return
				}),
			}
			defer func() {
				time.Sleep(1 * time.Second)
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitExtensionProcess()
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("case4: success to exec InitExtensionProcess", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(runtime.CheckExtension,
					func() ([]string, bool) {
						return []string{"extension_1.sh", "extension_2.sh"}, true
					}),
				gomonkey.ApplyFunc(createExtension,
					func(options *types.CreateOptions) (*os.Process, error) {
						if options.Name == "extension_1.sh" || options.Name == "extension_2.sh" {
							return &os.Process{}, nil
						}
						return &os.Process{}, errors.New("test")
					}),
				gomonkey.ApplyFunc(buildExtensionCreator,
					func() {
						return
					}),
				gomonkey.ApplyFunc(inspectExtensionProcess, func(process *os.Process) {
					return
				}),
			}
			defer func() {
				time.Sleep(1 * time.Second)
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			err := InitExtensionProcess()
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(Manager.CurrentExtension), convey.ShouldEqual, 2)
		})
	})
}

func Test_CheckRuntimeCPU(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(notifyMessageWithNoResp,
			func(messageType tcpmsg.MessageType, msg []byte) {
				return
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	// cgroup cpu parse failed
	cgroupCPUParser = monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
		return uint64(0), errors.New("failed to parse cgroup cpu")
	})
	CollectRuntimeCPU(false)

	// system cpu parse failed
	cgroupCPUParser = monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
		return uint64(0), nil
	})
	systemCPUParser = monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
		return uint64(0), errors.New("failed to parse system cpu")
	})
	CollectRuntimeCPU(false)

	// parse ok
	cgroupCPUParser = monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
		return uint64(0), nil
	})
	systemCPUParser = monitor.NewReadSeekerParser(bytes.NewReader(nil), func(reader *bufio.Reader) (interface{}, error) {
		return uint64(0), nil
	})
	CollectRuntimeCPU(false)
}

func Test_WatchingRuntimeCPUUsedLoop(t *testing.T) {
	patch := gomonkey.ApplyFunc(inspectExtensionProcess, func(process *os.Process) {
		return
	})
	defer func() {
		patch.Reset()
	}()
	ExitSignal = make(chan struct{})
	close(ExitSignal)
	defer func() {
		ExitSignal = make(chan struct{})
	}()
	watchingRuntimeCPUUsedLoop()
	ExitSignal = make(chan struct{})

	ExitSignalOnNoRequest = make(chan struct{})
	close(ExitSignalOnNoRequest)
	defer func() {
		ExitSignalOnNoRequest = make(chan struct{})
	}()
	watchingRuntimeCPUUsedLoop()
}

func Test_inspectExtensionProcess(t *testing.T) {
	process := &os.Process{Pid: -1}
	patches := gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return &os.ProcessState{}, errors.New("test")
	})
	inspectExtensionProcess(process)
	patches.Reset()
	patches = gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return &os.ProcessState{}, nil
	})
	patches = gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(tcp *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	inspectExtensionProcess(process)
	patches.Reset()
	patches = gomonkey.ApplyFunc((*os.Process).Wait, func(o *os.Process) (*os.ProcessState, error) {
		return &os.ProcessState{}, nil
	})
	patches = gomonkey.ApplyFunc((*os.ProcessState).String, func(o *os.ProcessState) string {
		return "aborted (core dumped)"
	})
	patches = gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(tcp *tcpmsg.SocketConn,
		msg *tcpmsg.Message) error {
		return nil
	})
	inspectExtensionProcess(process)
	patches.Reset()
}

func Test_appendCustomArgs(t *testing.T) {
	envEmpty := &workerutil.Environment{
		FuncEnvs:    make([]string, 5, 5),
		RuntimeEnvs: make([]string, 5, 5),
	}
	envHasAgent := &workerutil.Environment{
		FuncEnvs:    make([]string, 5, 5),
		RuntimeEnvs: []string{"func-JAVA_TOOL_OPTIONS=  -Dskywalking.asadsa=dsad -javaagent:skywalking-agent.jar  -Dskywalking=bbb"},
	}
	envHasNoAgent := &workerutil.Environment{
		FuncEnvs:    make([]string, 5, 5),
		RuntimeEnvs: []string{"func-JAVA_TOOL_OPTIONS="},
	}
	envHasAgentNoDskywalking := &workerutil.Environment{
		FuncEnvs:    make([]string, 5, 5),
		RuntimeEnvs: []string{""},
	}
	cangjieCustomArgs := &workerutil.Environment{
		FuncEnvs:    make([]string, 5, 5),
		RuntimeEnvs: []string{"func-CANGJIE_CUSTOM_ARGS=ee=ff;aa=bb;cc=dd"},
	}
	cangjieCustomArgsOnlyOne := &workerutil.Environment{
		FuncEnvs:    make([]string, 5, 5),
		RuntimeEnvs: []string{"func-CANGJIE_CUSTOM_ARGS=ee=ff"},
	}
	type args struct {
		reqBody *wconfig.RequestBody
	}

	java_language := &wconfig.Configuration{
		RuntimeConfig: wconfig.RuntimeConfig{Language: "java8"},
	}
	python_language := &wconfig.Configuration{
		RuntimeConfig: wconfig.RuntimeConfig{Language: "python3.6"},
	}
	cangjie_language := &wconfig.Configuration{
		RuntimeConfig: wconfig.RuntimeConfig{Language: utils.CangJie10},
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantEnv []string
	}{
		{
			name: "python_contain_JAVA_TOOL_OPTIONS",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    python_language,
				Envs:    envHasAgent,
				Version: "",
			}},
			want:    false,
			wantEnv: nil,
		},
		{
			name: "not_contain_JAVA_TOOL_OPTIONS",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    java_language,
				Envs:    envEmpty,
				Version: "",
			}},
			want:    false,
			wantEnv: nil,
		},
		{
			name: "contain_JAVA_TOOL_OPTIONS",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    java_language,
				Envs:    envHasAgent,
				Version: utils.Java8,
			}},
			want:    true,
			wantEnv: []string{"-Dskywalking.asadsa=dsad", "-javaagent:skywalking-agent.jar", "-Dskywalking=bbb"},
		},
		{
			name: "contain_JAVA_TOOL_OPTIONS_no_agent",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    java_language,
				Envs:    envHasNoAgent,
				Version: utils.Java8,
			}},
			want:    true,
			wantEnv: []string{},
		},
		{
			name: "contain_JAVA_TOOL_OPTIONS_has_agent_not_all_Dskywalking",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    java_language,
				Envs:    envHasAgentNoDskywalking,
				Version: utils.Java8,
			}},
			want:    false,
			wantEnv: []string{"apm-javaagent=apm-javaagent.jar"},
		},
		{
			name: "contain_CANGJIE_CUSTOM_ARGS_many",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    cangjie_language,
				Envs:    cangjieCustomArgs,
				Version: utils.CangJie10,
			}},
			want:    true,
			wantEnv: []string{"ee=ff", "aa=bb", "cc=dd"},
		},
		{
			name: "contain_CANGJIE_CUSTOM_ARGS_only_one",
			args: args{reqBody: &wconfig.RequestBody{
				Conf:    cangjie_language,
				Envs:    cangjieCustomArgsOnlyOne,
				Version: utils.CangJie10,
			}},
			want:    true,
			wantEnv: []string{"ee=ff"},
		},
	}
	for _, tt := range tests {
		initManager()
		t.Run(tt.name, func(t *testing.T) {
			a, _ := appendCustomArgs(tt.args.reqBody)
			assert.Equalf(t, tt.want, a, "appendCustomArgs(%v)", tt.args.reqBody.Envs.RuntimeEnvs)
			assert.Equal(t, tt.wantEnv, Manager.conf.RuntimeConfig.RMConfig.RuntimeConfigs[tt.args.reqBody.Version].CustomArgs)
		})
	}
}

func Test_prepareRestartWhenStopRuntimeProcess(t *testing.T) {
	tests := []struct {
		name        string
		want        *prepareRestartPars
		wantErr     assert.ErrorAssertionFunc
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "checkID error",
			want: &prepareRestartPars{},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err.Error() == "checkID error" {
					return false
				}
				return true
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(usergroup.CheckID, func(uid, gid int) (int, int, error) {
						return 1003, 1003, errors.New("checkID error")
					}),
				})
				return patches
			},
		},
		{
			name: "successPrepareParameters",
			want: &prepareRestartPars{
				uid:      1005,
				gid:      1003,
				debugEnv: nil,
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				if err != nil {
					return false
				}
				return true
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(usergroup.CheckID, func(uid, gid int) (int, int, error) {
						return 1005, 1003, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		initManager()
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := prepareRestartWhenStopRuntimeProcess()
			if !tt.wantErr(t, err, fmt.Sprintf("prepareRestartWhenStopRuntimeProcess()")) {
				return
			}
			assert.Equalf(t, tt.want, got, "prepareRestartWhenStopRuntimeProcess()")
			patches.ResetAll()
		})
	}
}

func Test_processSoftLink(t *testing.T) {
	tests := []struct {
		name            string
		expectedVersion string
		want            error
		patchesFunc     mockUtils.PatchesFunc
	}{{
		name:            "success to soft-link by nodejs14.18",
		expectedVersion: utils.Nodejs1418,
		want:            nil,
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return nil
				}),
			})
			return patches
		},
	}, {
		name:            "success to soft-link by python3.10",
		expectedVersion: utils.Python310,
		want:            nil,
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return nil
				}),
			})
			return patches
		},
	}, {
		name:            "success to soft-link by python3.11",
		expectedVersion: utils.Python311,
		want:            nil,
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return nil
				}),
			})
			return patches
		},
	}, {
		name:            "success to soft-link by .net3.1",
		expectedVersion: utils.NetCore31,
		want:            nil,
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return nil
				}),
			})
			return patches
		},
	}, {
		name:            "success to soft-link by php8.3",
		expectedVersion: utils.Php83,
		want:            nil,
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return nil
				}),
			})
			return patches
		},
	}, {
		name:            "failed to soft-link 1",
		expectedVersion: utils.Java8,
		want:            errors.New("failed to create soft-link, error: errorMsg"),
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return errors.New("errorMsg")
				}),
			})
			return patches
		},
	}, {
		name:            "failed to soft-link 2",
		expectedVersion: utils.Php73,
		want:            errors.New("failed to create soft-link, error: cmd is nil"),
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return nil
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return errors.New("errorMsg")
				}),
			})
			return patches
		},
	},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got := processSoftLink(tt.expectedVersion)
			assert.Equalf(t, tt.want, got, "processSoftLink()")
			patches.ResetAll()
		})
	}
}

func Test_watchHostFileLoop(t *testing.T) {
	events := make(chan fsnotify.Event)
	errorChan := make(chan error)
	p := gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
		return &fsnotify.Watcher{
			Events: events,
			Errors: errorChan,
		}, nil
	})
	var watcher *fsnotify.Watcher
	p.ApplyMethod(reflect.TypeOf(watcher), "Add", func(_ *fsnotify.Watcher, name string) error { return nil })
	p.ApplyMethod(reflect.TypeOf(watcher), "Remove", func(_ *fsnotify.Watcher, name string) error { return nil })
	p.ApplyMethod(reflect.TypeOf(watcher), "Close", func(_ *fsnotify.Watcher) error { return nil })
	p.ApplyFunc(utilities.GetFileHashInfo, func(path string) (int64, string, error) {
		return 1, "hashValue", nil
	})
	p.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
		return make([]byte, 1), nil
	})
	defer p.Reset()
	ExitSignal = make(chan struct{})
	defer func() {
		ExitSignal = make(chan struct{})
	}()
	close(ExitSignal)
	watchHostFileLoop()
	ExitSignal = make(chan struct{})

	ExitSignalOnNoRequest = make(chan struct{})
	close(ExitSignalOnNoRequest)
	defer func() {
		ExitSignalOnNoRequest = make(chan struct{})
	}()
	watchHostFileLoop()
}

func Test_resetHostsFile(t *testing.T) {
	utilities.HostFile.Sha256 = "test"
	tests := []struct {
		name        string
		want        bool
		event       fsnotify.Event
		patchesFunc mockUtils.PatchesFunc
	}{{
		name: "resetHostsFile error 1",
		want: true,
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utilities.GetFileHashInfo, func(path string) (int64, string, error) {
					return 0, "", errors.New("test err")
				}),
			})
			return patches
		},
		event: fsnotify.Event{Op: fsnotify.Create, Name: "create"},
	},
		{
			name: "resetHostsFile error 2",
			want: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utilities.GetFileHashInfo, func(path string) (int64, string, error) {
						return 1, "test diff", nil
					}),
					gomonkey.ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
						return errors.New("test err")
					}),
				})
				return patches
			},
			event: fsnotify.Event{Op: fsnotify.Create, Name: "create"},
		},
		{
			name: "resetHostsFile success",
			want: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utilities.GetFileHashInfo, func(path string) (int64, string, error) {
						return 1, "test diff", nil
					}),
					gomonkey.ApplyFunc(ioutil.WriteFile, func(filename string, data []byte, perm fs.FileMode) error {
						return nil
					}),
				})
				return patches
			},
			event: fsnotify.Event{Op: fsnotify.Create, Name: "create"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ExitSignal = make(chan struct{})
			defer func() {
				ExitSignal = make(chan struct{})
			}()
			resetHostsFile(tt.event)
			patches.ResetAll()
		})
	}
}

func Test_watchHostFileLoopFail(t *testing.T) {
	var watcher *fsnotify.Watcher
	events := make(chan fsnotify.Event)
	errorChan := make(chan error)
	tests := []struct {
		name        string
		want        error
		patchesFunc mockUtils.PatchesFunc
	}{{
		name: "watchHostFileLoop error 1",
		want: snerror.New(100, "test"),
		patchesFunc: func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(fsnotify.NewWatcher, func() (*fsnotify.Watcher, error) {
					return &fsnotify.Watcher{
						Events: events,
						Errors: errorChan,
					}, snerror.New(100, "test")
				}),
			})
			return patches
		},
	},
		{
			name: "watchHostFileLoop error 2",
			want: snerror.New(100, "test"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(watcher), "Add", func(_ *fsnotify.Watcher, name string) error {
						return snerror.New(100, "test")
					}),
				})
				return patches
			},
		},
		{
			name: "watchHostFileLoop error 3",
			want: snerror.New(100, "test"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(watcher), "Add", func(_ *fsnotify.Watcher, name string) error {
						return nil
					}),
					gomonkey.ApplyFunc(utilities.GetFileHashInfo, func(path string) (int64, string, error) {
						return 1, "test", snerror.New(100, "test")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ExitSignal = make(chan struct{})
			defer func() {
				ExitSignal = make(chan struct{})
			}()
			watchHostFileLoop()
			patches.ResetAll()
		})
	}
}

func Test_monitorRuntimeDirMonitor(t *testing.T) {
	convey.Convey("test monitorRuntimeDirMonitor", t, func() {
		convey.Convey("failed to create dir monitor", func() {
			patch := gomonkey.ApplyFunc(diskusage.NewRuntimeMonitor,
				func(monitorDir []string, stopChan <-chan struct{}, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
					return nil, errors.New("test")
				})
			defer patch.Reset()
			err := monitorRuntimeDirMonitor()
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("succeed to create dir monitor", func() {
			patch := gomonkey.ApplyFunc(diskusage.NewRuntimeMonitor,
				func(monitorDir []string, stopChan <-chan struct{}, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
					return &diskusage.Monitor{}, nil
				})
			defer patch.Reset()
			err := monitorRuntimeDirMonitor()
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestExecPreStopCommand_EmptyCommand(t *testing.T) {
	Manager.conf.PreStop.Handler = ""
	_, err := ExecPreStopCommand()
	if err == nil {
		t.Error("Expected error for empty command, but got nil")
	}
	Manager.conf.PreStop.Handler = "[]"
	_, err = ExecPreStopCommand()
	if err == nil {
		t.Error("Expected error for empty command, but got nil")
	}
}

func TestExecPreStopCommand_InvalidCommand(t *testing.T) {
	Manager.conf.PreStop.Handler = "invalid command"
	_, err := ExecPreStopCommand()
	if err == nil {
		t.Error("Expected error for invalid command, but got nil")
	}
}

func TestExecPreStopCommand_ValidCommand(t *testing.T) {
	patch := gomonkey.ApplyFunc(usergroup.CheckID,
		func(uid, gid int) (int, int, error) {
			return 0, 0, nil
		})
	patch.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Output", func(*exec.Cmd) ([]byte, error) {
		return []byte("\n"), nil
	})
	defer patch.Reset()
	Manager.conf.PreStop.Handler = "[\"echo\"]"
	output, err := ExecPreStopCommand()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if output != "\n" {
		t.Errorf("Expected 'test\n', but got %s", output)
	}
}

func TestExecPreStopCommand_CommandWithArgs(t *testing.T) {
	patch := gomonkey.ApplyFunc(usergroup.CheckID,
		func(uid, gid int) (int, int, error) {
			return 0, 0, nil
		})
	patch.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Output", func(*exec.Cmd) ([]byte, error) {
		return []byte("test\n"), nil
	})
	defer patch.Reset()
	Manager.conf.PreStop.Handler = "[\"echo\", \"test\"]"
	output, err := ExecPreStopCommand()
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
	if output != "test\n" {
		t.Errorf("Expected 'test\n', but got %s", output)
	}
}

func TestExecPreStopCommand_CommandReturnErr(t *testing.T) {
	patch := gomonkey.ApplyFunc(usergroup.CheckID,
		func(uid, gid int) (int, int, error) {
			return 0, 0, nil
		})
	patch.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Output", func(*exec.Cmd) ([]byte, error) {
		return nil, errors.New("error")
	})
	defer patch.Reset()
	Manager.conf.PreStop.Handler = "[\"echo\", \"test\"]"
	_, err := ExecPreStopCommand()
	if err == nil {
		t.Errorf("Expected error, but got %v", err)
	}
}

func Test_watchingRuntimeCaeMetricsLoop(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"test watchingRuntimeCaeMetricsLoop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existSignalTmp := ExitSignal
			ExitSignal = make(chan struct{})
			go func() {
				time.Sleep(time.Second)
				close(ExitSignal)
			}()
			watchingRuntimeCaeMetricsLoop()
			ExitSignal = existSignalTmp
		})
	}
}

func TestCollectCaeMetrics(t *testing.T) {
	netParser, _ := monitor.NewNetParser()

	defer netParser.Close()

	fileSystemParser, _ := monitor.NewFileSystemParser()

	defer fileSystemParser.Close()

	type args struct {
		netParser        *monitor.Parser
		fileSystemParser *monitor.Parser
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test collect cae metrics of nil parser",
			args{},
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
		},
		{
			"test collect cae metrics get parser",
			args{netParser, fileSystemParser},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(notifyMessageWithNoResp, func(messageType tcpmsg.MessageType, msg []byte) {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		CollectCaeMetrics(tt.args.netParser, tt.args.fileSystemParser)
		patches.ResetAll()
	}
}

func Test_inspectProcess1(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&os.Process{}), "Wait", func(process *os.Process) (*os.ProcessState, error) {
			return nil, nil
		}),
		gomonkey.ApplyFunc(notifyMessageWithNoResp, func(messageType tcpmsg.MessageType, msg []byte) {
			return
		}),
	})

	tmpManager := Manager
	tmpInspectProcessChannel := inspectProcessChannel
	inspectProcessChannel = make(chan struct{})

	Manager = &processMgr{
		currentVersion: "nodejs",
		currentMode:    restore,
		conf:           &workerconfig.Configuration{},
	}

	inspectProcess(&os.Process{}, "nodejs", &functionlog.FixSizeWriter{})

	Manager = tmpManager
	inspectProcessChannel = tmpInspectProcessChannel

	tmpManager = Manager
	tmpInspectProcessChannel = inspectProcessChannel
	inspectProcessChannel = make(chan struct{})

	Manager = &processMgr{
		currentVersion: "c#",
		currentMode:    normal,
	}

	inspectProcess(&os.Process{}, "nodejs", &functionlog.FixSizeWriter{})
	inspectProcessChannel = tmpInspectProcessChannel

	Manager = tmpManager
	patches.ResetAll()
}

func Test_getLogPolicy(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{
			name: "test nil",
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getLogPolicy(), "getLogPolicy()")
		})
	}
}
