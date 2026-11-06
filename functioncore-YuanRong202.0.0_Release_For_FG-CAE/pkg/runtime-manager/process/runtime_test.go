package process

import (
	"errors"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/runtime-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestRuntimeProcess_restartRuntimeProcessWithRetry(t *testing.T) {
	patch := gomonkey.ApplyFunc(create, func(options *types.CreateOptions) (*os.Process, error) { return &os.Process{Pid: 1000}, nil })
	tmp := Manager.CurrentRuntime
	Manager.CurrentRuntime = &RuntimeProcess{Process: &os.Process{}}
	defer func() {
		Manager.CurrentRuntime = tmp
		patch.Reset()
	}()
	patch.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	patch.ApplyFunc(syscall.Kill, func(pid int, sig syscall.Signal) (err error) { return nil })
	runtime := &RuntimeProcess{Process: &os.Process{}}
	assert.Nil(t, runtime.restartRuntimeProcessWithRetry())
}

func TestRuntimeProcess_checkProcessPauseState(t *testing.T) {
	type fields struct {
		Process       *os.Process
		DebugEnv      []string
		UID           int
		Gid           int
		Version       string
		Status        tcpmsg.RuntimeProcessStatus
		extensionPids sync.Map
	}
	type args struct {
		pids []int
	}
	var a args
	processIDs := []int{1, 2}
	a.pids = processIDs
	runtime := &RuntimeProcess{}
	runtime.checkProcessPauseState(a.pids)

	patch := gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return nil, errors.New("aaa")
	})
	runtime_recover := &RuntimeProcess{Status: tcpmsg.RuntimeProcessStatusPaused}
	runtime_recover.checkProcessPauseState(a.pids)
	patch.Reset()
}

func TestRuntimeProcess_checkFileInfo(t *testing.T) {
	type fields struct {
		Process       *os.Process
		DebugEnv      []string
		UID           int
		Gid           int
		Version       string
		Status        tcpmsg.RuntimeProcessStatus
		extensionPids sync.Map
	}
	type args struct {
		fileInfos []fs.FileInfo
		pid       int
	}
	var f fields
	var a args
	var info fs.FileInfo
	var infos []fs.FileInfo
	infos = append(infos, info)
	f.Status = tcpmsg.RuntimeProcessStatusRecover
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &RuntimeProcess{
				Process:       tt.fields.Process,
				DebugEnv:      tt.fields.DebugEnv,
				UID:           tt.fields.UID,
				Gid:           tt.fields.Gid,
				Version:       tt.fields.Version,
				Status:        tt.fields.Status,
				extensionPids: tt.fields.extensionPids,
			}
			runtime.checkFileInfo(infos, 0)
		})
	}
}

func TestRuntimeProcess_checkFileInfoFailed(t *testing.T) {
	patch := gomonkey.ApplyFunc(utils.GetFileInfo, func(fileInfo fs.FileInfo) utils.ProcessInfo {
		return utils.ProcessInfo{Pid: 0, State: 0}
	})
	type fields struct {
		Process       *os.Process
		DebugEnv      []string
		UID           int
		Gid           int
		Version       string
		Status        tcpmsg.RuntimeProcessStatus
		extensionPids sync.Map
	}
	type args struct {
		fileInfos []fs.FileInfo
		pid       int
	}
	var f fields
	var a args
	var info fs.FileInfo
	var infos []fs.FileInfo
	infos = append(infos, info)
	f.Status = tcpmsg.RuntimeProcessStatusPaused
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &RuntimeProcess{
				Process:       tt.fields.Process,
				DebugEnv:      tt.fields.DebugEnv,
				UID:           tt.fields.UID,
				Gid:           tt.fields.Gid,
				Version:       tt.fields.Version,
				Status:        tt.fields.Status,
				extensionPids: tt.fields.extensionPids,
			}
			runtime.checkFileInfo(infos, 0)
		})
	}
	patch.Reset()
}

func TestRuntimeProcess_checkFileInfoSuccess(t *testing.T) {
	patch := gomonkey.ApplyFunc(utils.GetFileInfo, func(fileInfo fs.FileInfo) utils.ProcessInfo {
		return utils.ProcessInfo{Pid: 1, State: 1}
	})
	type fields struct {
		Process       *os.Process
		DebugEnv      []string
		UID           int
		Gid           int
		Version       string
		Status        tcpmsg.RuntimeProcessStatus
		extensionPids sync.Map
	}
	type args struct {
		fileInfos []fs.FileInfo
		pid       int
	}
	var f fields
	var a args
	var info fs.FileInfo
	var infos []fs.FileInfo
	infos = append(infos, info)
	f.Status = tcpmsg.RuntimeProcessStatusPaused
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runtime := &RuntimeProcess{
				Process:       tt.fields.Process,
				DebugEnv:      tt.fields.DebugEnv,
				UID:           tt.fields.UID,
				Gid:           tt.fields.Gid,
				Version:       tt.fields.Version,
				Status:        tt.fields.Status,
				extensionPids: tt.fields.extensionPids,
			}
			runtime.checkFileInfo(infos, 0)
		})
	}
	patch.Reset()
}

func Test_getSigNo(t *testing.T) {
	processStatus := getSigNo(syscall.SIGSTOP)
	assert.Equalf(t, tcpmsg.RuntimeProcessStatusPaused, processStatus, "expect RuntimeProcessStatusPaused")
	processStatus = getSigNo(syscall.SIGCONT)
	assert.Equalf(t, tcpmsg.RuntimeProcessStatusRecover, processStatus, "expect RuntimeProcessStatusRecover")
	processStatus = getSigNo(syscall.SIGABRT)
	assert.Equalf(t, tcpmsg.RuntimeProcessStatusRunning, processStatus, "expect RuntimeProcessStatusRunning")
}

func Test_firstGetDirSizeWithTimeOut(t *testing.T) {
	type args struct {
		curDirSizeLimit int32
		monitorDir      string
	}
	tests := []struct {
		name        string
		args        args
		wantSize    int32
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to getDirSize", args{
			curDirSizeLimit: 100,
			monitorDir:      "test",
		}, 100, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(getDirSize, func(monitorDir string) (int, error) {
					return 0, errors.New("test")
				}),
			})
			return patches
		}},
		{"case2 succeed to getDirSize", args{
			curDirSizeLimit: 100,
			monitorDir:      "test",
		}, 10, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(getDirSize, func(monitorDir string) (int, error) {
					return 10, nil
				}),
			})
			return patches
		}},
		{"case3 getDirSize with timeout", args{
			curDirSizeLimit: 100,
			monitorDir:      "test",
		}, 100, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(getDirSize, func(monitorDir string) (int, error) {
					time.Sleep(6 * time.Second)
					return 10, nil
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			atomic.StoreInt32(&firstDirSize, 0)
			patches := tt.patchesFunc()
			firstGetDirSizeWithTimeOut(tt.args.curDirSizeLimit, tt.args.monitorDir)
			firstSize := atomic.LoadInt32(&firstDirSize)
			if firstSize != tt.wantSize {
				t.Errorf("wantSize %v", tt.wantSize)
			}
			patches.ResetAll()
		})
	}
}

func Test_getSnuserDirSizeLimit(t *testing.T) {
	type args struct {
		dirSizeLimit int32
	}
	tests := []struct {
		name     string
		args     args
		wantSize int32
	}{
		{"case1 failed to getDirSize", args{
			dirSizeLimit: 0,
		}, 1024},
		{"case2 succeed to getDirSize", args{
			dirSizeLimit: 100,
		}, 100},
		{"case3 getDirSize with timeout", args{
			dirSizeLimit: -1,
		}, 1024},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := getSnuserDirSizeLimit(tt.args.dirSizeLimit)
			if size != tt.wantSize {
				t.Errorf("wantSize %v", tt.wantSize)
			}
		})
	}
}

func Test_getDirSize(t *testing.T) {
	type args struct {
		monitorDir string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		wantSize    int
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to exec du", args{
			monitorDir: "test",
		}, true, 0, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					return &exec.Cmd{}
				}),
				gomonkey.ApplyFunc((*exec.Cmd).Output, func(_ *exec.Cmd) ([]byte, error) {
					return []byte("mock"), errors.New("test")
				}),
			})
			return patches
		}},
		{"case2 exec du with err output", args{
			monitorDir: "test",
		}, true, 0, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					return &exec.Cmd{}
				}),
				gomonkey.ApplyFunc((*exec.Cmd).Output, func(_ *exec.Cmd) ([]byte, error) {
					return []byte("mock"), nil
				}),
			})
			return patches
		}},
		{"case3 succeed to get dir size", args{
			monitorDir: "test",
		}, false, 17, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					return &exec.Cmd{}
				}),
				gomonkey.ApplyFunc((*exec.Cmd).Output, func(_ *exec.Cmd) ([]byte, error) {
					return []byte("17 \t /home/snuer"), nil
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			size, err := getDirSize(tt.args.monitorDir)
			if size != tt.wantSize && ((err != nil) == tt.wantErr) {
				t.Errorf("wantSize %v", tt.wantSize)
			}
			patches.ResetAll()
		})
	}
}

func Test_CheckSnuserDirSize(t *testing.T) {
	convey.Convey("test check snuser dir size", t, func() {
		convey.Convey("close stop chan", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(firstGetDirSizeWithTimeOut, func(curDirSizeLimit int32, monitorDir string) {
					atomic.StoreInt32(&firstDirSize, 1)
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopChan := make(chan struct{})
			close(stopChan)
			CheckSnuserDirSize(1, stopChan)
		})
		convey.Convey("test less than the size of the first fetch", func() {
			var exitFlag bool
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
				gomonkey.ApplyFunc(getDirSize, func(monitorDir string) (int, error) {
					return 10, nil
				}),
				gomonkey.ApplyFunc(firstGetDirSizeWithTimeOut, func(curDirSizeLimit int32, monitorDir string) {
					atomic.StoreInt32(&firstDirSize, 100)
					return
				}),
				gomonkey.ApplyFunc(exit, func() {
					exitFlag = true
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopChan := make(chan struct{})
			CheckSnuserDirSize(1, stopChan)
			time.Sleep(100 * time.Millisecond)
			close(stopChan)
			convey.So(exitFlag, convey.ShouldBeFalse)
		})
		convey.Convey("test exceed limit exit", func() {
			var exitFlag bool
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
				gomonkey.ApplyFunc(getDirSize, func(monitorDir string) (int, error) {
					return 100, nil
				}),
				gomonkey.ApplyFunc(firstGetDirSizeWithTimeOut, func(curDirSizeLimit int32, monitorDir string) {
					atomic.StoreInt32(&firstDirSize, 1)
					return
				}),
				gomonkey.ApplyFunc(exit, func() {
					exitFlag = true
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopChan := make(chan struct{})
			CheckSnuserDirSize(1, stopChan)
			time.Sleep(100 * time.Millisecond)
			convey.So(exitFlag, convey.ShouldBeTrue)
		})
	})
}
