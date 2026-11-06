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
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/fsnotify/fsnotify"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/tcpmsg"
)

type fakeFileInfo struct{}

func (fakeFileInfo) Name() string       { return "" }
func (fakeFileInfo) Size() int64        { return 0 }
func (fakeFileInfo) Mode() os.FileMode  { return 0750 }
func (fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (fakeFileInfo) IsDir() bool        { return true }
func (fakeFileInfo) Sys() interface{}   { return nil }

func Test_restoreRuntimeProcess(t *testing.T) {
	patches := gomonkey.NewPatches()
	fakeFile := &os.File{}
	patches.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return fakeFileInfo{}, nil
	})
	patches.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeFile), "Readdirnames", func(_ *os.File, n int) (names []string, err error) {
		return []string{"file1", "file2"}, nil
	})
	patches.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	patches.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error { return nil })
	patches.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	fakeMonitor := &diskusage.Monitor{}
	patches.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		// invoke the hook directly
		for _, hook := range hooks {
			hook(fsnotify.Event{})
		}
		return func(*diskusage.Monitor) error { return nil }
	})
	patches.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return fakeMonitor, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeMonitor), "Close", func(_ *diskusage.Monitor) {})
	patches.ApplyFunc(startRuntimeProcess, func(uid int, gid int, reqBody *runtimemanagerrequest.InitRequestBody) (int, error) { return 0, nil })
	patches.ApplyFunc(backupSnapshotLogs, func() { return })
	defer patches.Reset()

	_, err := restoreRuntimeProcess(1, 1)
	assert.NoError(t, err)
	assert.True(t, restoreSuccessFlag)
}

func Test_restoreRuntimeProcessErr(t *testing.T) {
	patches := gomonkey.NewPatches()
	fakeFile := &os.File{}
	patches.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return fakeFileInfo{}, errors.New("my error")
	})
	patches.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeFile), "Readdirnames", func(_ *os.File, n int) (names []string, err error) {
		return []string{"file1", "file2"}, nil
	})
	patches.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	patches.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error { return nil })
	patches.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	fakeMonitor := &diskusage.Monitor{}
	patches.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		// invoke the hook directly
		for _, hook := range hooks {
			hook(fsnotify.Event{})
		}
		return func(*diskusage.Monitor) error { return nil }
	})
	patches.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return fakeMonitor, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeMonitor), "Close", func(_ *diskusage.Monitor) {})
	patches.ApplyFunc(startRuntimeProcess, func(uid int, gid int, reqBody *runtimemanagerrequest.InitRequestBody) (int, error) { return 0, nil })
	patches.ApplyFunc(backupSnapshotLogs, func() { return })
	defer patches.Reset()

	_, err := restoreRuntimeProcess(1, 1)
	assert.Error(t, err)
}

func Test_restoreRuntimeProcessErr2(t *testing.T) {
	patches := gomonkey.NewPatches()
	fakeFile := &os.File{}
	patches.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return fakeFileInfo{}, nil
	})
	patches.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeFile), "Readdirnames", func(_ *os.File, n int) (names []string, err error) {
		return []string{"file1", "file2"}, nil
	})
	patches.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	patches.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error { return nil })
	patches.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	fakeMonitor := &diskusage.Monitor{}
	patches.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		// invoke the hook directly
		for _, hook := range hooks {
			hook(fsnotify.Event{})
		}
		return func(*diskusage.Monitor) error { return nil }
	})
	patches.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return fakeMonitor, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeMonitor), "Close", func(_ *diskusage.Monitor) {})
	patches.ApplyFunc(startRuntimeProcess, func(uid int, gid int, reqBody *runtimemanagerrequest.InitRequestBody) (int, error) {
		return 0, errors.New("my error")
	})
	patches.ApplyFunc(backupSnapshotLogs, func() { return })
	defer patches.Reset()

	_, err := restoreRuntimeProcess(1, 1)
	assert.Error(t, err)
}

func Test_restoreRuntimeProcessRestoreTimeout(t *testing.T) {
	patches := gomonkey.NewPatches()
	isMocked := false
	fakeFile := &os.File{}
	patches.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
		return fakeFileInfo{}, nil
	})
	patches.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeFile), "Readdirnames", func(_ *os.File, n int) (names []string, err error) {
		return []string{"file1", "file2"}, nil
	})
	patches.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	patches.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error { return nil })
	patches.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	fakeMonitor := &diskusage.Monitor{}
	patches.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		return func(*diskusage.Monitor) error { return nil }
	})
	patches.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return fakeMonitor, nil
	})
	patches.ApplyMethod(reflect.TypeOf(fakeMonitor), "Close", func(_ *diskusage.Monitor) {})
	patches.ApplyFunc(context.WithTimeout, func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
		return context.WithDeadline(parent, time.Now().Add(1*time.Millisecond))
	})
	patches.ApplyFunc(startRuntimeProcess, func(uid int, gid int, reqBody *runtimemanagerrequest.InitRequestBody) (int, error) {
		return 0, nil
	})
	patches.ApplyFunc(notifyMessageWithNoResp, func(messageType tcpmsg.MessageType, msg []byte) {
		isMocked = true
		return
	})
	patches.ApplyFunc(backupSnapshotLogs, func() { return })
	defer patches.Reset()

	Manager.CurrentRuntime = &RuntimeProcess{
		Process:  &os.Process{Pid: 1},
		UID:      constants.DefaultRuntimeUID,
		Gid:      constants.DefaultRuntimeGID,
		Version:  Manager.currentVersion,
		Status:   tcpmsg.RuntimeProcessStatusRunning,
		DebugEnv: []string{},
	}

	inspectProcessChannel = make(chan struct{})
	_, err := restoreRuntimeProcess(1, 1)
	assert.NoError(t, err)
	assert.Equal(t, false, isMocked)

	Manager.CurrentRuntime = nil
	_, err = restoreRuntimeProcess(1, 1)
	assert.NoError(t, err)
	assert.Equal(t, true, isMocked)
}
