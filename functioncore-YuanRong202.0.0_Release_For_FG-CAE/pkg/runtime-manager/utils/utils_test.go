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

package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	utilities "functioncore/pkg/common/utils"
	rmconfig "functioncore/pkg/runtime-manager/config"
	wconfig "functioncore/pkg/worker/config"
	mockutils "functioncore/test/utils"
)

func TestAdjustRuntimeProcess(t *testing.T) {
	convey.Convey("test AdjustRuntimeProcess", t, func() {
		convey.Convey("succeed to AdjustRuntimeProcess", func() {
			inputProcess := &os.Process{Pid: 1}
			expectFileName := fmt.Sprintf("/proc/%s/oom_score_adj", strconv.Itoa(inputProcess.Pid))
			expectData := []byte("992")
			actualFileName := ""
			actualData := []byte("1")
			patch := gomonkey.ApplyFunc(ioutil.WriteFile,
				func(filename string, data []byte, perm fs.FileMode) error {
					actualFileName = filename
					actualData = data
					return nil
				})
			defer patch.Reset()
			err := AdjustRuntimeProcess(inputProcess)
			convey.So(err, convey.ShouldBeNil)
			convey.So(actualFileName, convey.ShouldEqual, expectFileName)
			convey.So(actualData, convey.ShouldResemble, expectData)
		})
		convey.Convey("AdjustRuntimeProcess write file return error", func() {
			inputProcess := &os.Process{Pid: 1}
			patch := gomonkey.ApplyFunc(ioutil.WriteFile,
				func(filename string, data []byte, perm fs.FileMode) error {
					return errors.New("failed")
				})
			defer patch.Reset()
			err := AdjustRuntimeProcess(inputProcess)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed")
		})
	})
}

func TestGetServerAddress(t *testing.T) {
	address := GetServerAddress(8080)
	assert.Equal(t, "127.0.0.1:8080", address)
}

func TestDisableOOM(t *testing.T) {
	expectFileName := "/proc/self/oom_score_adj"
	expectData := []byte("-1000")
	actualFileName := ""
	actualData := []byte("1")
	patch := gomonkey.ApplyFunc(ioutil.WriteFile,
		func(filename string, data []byte, perm fs.FileMode) error {
			actualFileName = filename
			actualData = data
			return nil
		})
	defer patch.Reset()
	err := DisableOOM()
	assert.Nil(t, err)
	assert.Equal(t, actualFileName, expectFileName)
	assert.Equal(t, actualData, expectData)
}

func Test_AppendToFile(t *testing.T) {
	err := AppendToFile("/home/append.file", "hello")
	assert.Equal(t, "open /home/append.file: no such file or directory", err.Error())
	os.MkdirAll("/temp/test", 777)
	os.Create("/temp/test/append.file")

	err = AppendToFile("/temp/test/append.file", "hello")
	assert.Equal(t, nil, err)
	os.RemoveAll("/temp/test/append.file")
}

func Test_extractEnv(t *testing.T) {
	assert.Equal(t, true, len(ExtractEnv("nodejs10.15.2", []string{}, &rmconfig.Config{})) != 0)
	os.Setenv("DEPLOY_DIR", "/opt/function/code")
	envs := ExtractEnv("nodejs10.15.2", []string{}, &rmconfig.Config{})
	assert.Equal(t, len(envs) > 1, true)
	envsToString := strings.Join(envs, ",")
	// default runtime memory is 256
	assert.True(t, strings.Contains(envsToString, "cjHeapSize=254MB"))
	assert.True(t, strings.Contains(envsToString, "cjGCThreshold=127MB"))

	cangjieCustomArgs := "aa=bb"
	runtimeConfig := &rmconfig.RuntimeConfig{CustomArgs: []string{"aa=bb"}}
	envs = ExtractEnv(CangJie10, []string{}, &rmconfig.Config{RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
		CangJie10: *runtimeConfig}})
	envsToString = strings.Join(envs, ",")
	assert.True(t, strings.Contains(envsToString, cangjieCustomArgs))
}

func Test_getEnv(t *testing.T) {
	patch := gomonkey.ApplyFunc(getNodePath, func(string) string { return "/user/lib/node_modules" })
	defer patch.Reset()
	assert.Equal(t, []string(nil), getEnv("java1.8", &rmconfig.Config{}))
	assert.Equal(t, []string{"LANG=en_US.UTF-8"}, getEnv(Python, &rmconfig.Config{}))
	assert.Equal(t, true, reflect.DeepEqual(getEnv("nodejs10.15.2", &rmconfig.Config{}), []string{
		"NODE_VERSION=10", "NODE_PATH=/user/lib/node_modules"}))
}

func Test_getNodePath(t *testing.T) {
	convey.Convey("Test_getNodePath", t, func() {
		convey.Convey("test1", func() {
			assert.Equal(t, "", getNodePath("nodejs11"))
			patches := gomonkey.ApplyFunc(filepath.Abs, func(key string) (string, error) {
				return "", errors.New("test error")
			})
			defer patches.Reset()
			assert.Equal(t, "", getNodePath("nodejs11"))
		})
	})
}

func Test_getSocketType(t *testing.T) {
	sockType := GetSocketType(nil, Java8)
	assert.Equal(t, sockType, tcpSocketType)

	sockType = GetSocketType(&wconfig.RuntimeConfig{}, Java8)
	assert.Equal(t, sockType, tcpSocketType)

	runtimeConfig := make(map[string]rmconfig.RuntimeConfig)
	runtimeConfig[Java8] = rmconfig.RuntimeConfig{
		SocketType: unixSocketType,
	}
	conf := &wconfig.RuntimeConfig{
		RMConfig: rmconfig.Config{
			RuntimeConfigs: runtimeConfig,
		},
	}
	sockType = GetSocketType(conf, Java8)
	assert.Equal(t, sockType, unixSocketType)

	conf.RunningMode = constants.RunningModeCheckpoint
	sockType = GetSocketType(conf, Java8)
	assert.Equal(t, sockType, tcpSocketType)

	runtimeConfig = make(map[string]rmconfig.RuntimeConfig)
	runtimeConfig[Java8] = rmconfig.RuntimeConfig{
		SocketType: "xxxx",
	}
	conf = &wconfig.RuntimeConfig{
		RMConfig: rmconfig.Config{
			RuntimeConfigs: runtimeConfig,
		},
	}
	sockType = GetSocketType(conf, Java8)
	assert.Equal(t, sockType, tcpSocketType)
}

func Test_GetExecPath(t *testing.T) {
	patch := gomonkey.ApplyFunc(exec.LookPath, func(string) (string, error) {
		return "", errors.New("failed to lookPath")
	})
	defer patch.Reset()
	path, _ := GetExecPath("java8")
	assert.Equal(t, "", path)
}

func Test_GetExecPath_NetCore31(t *testing.T) {
	patch := gomonkey.ApplyFunc(exec.LookPath, func(string) (string, error) {
		return "", errors.New("failed to lookPath")
	})
	defer patch.Reset()
	path, _ := GetExecPath(NetCore31)
	assert.Equal(t, "", path)
}

func Test_GetExecPath_NetCore21(t *testing.T) {
	patch := gomonkey.ApplyFunc(exec.LookPath, func(string) (string, error) {
		return "", errors.New("failed to lookPath")
	})
	defer patch.Reset()
	path, _ := GetExecPath(NetCore21)
	assert.NotEqual(t, "", path)
}

func Test_GetExecPath_NetCore60(t *testing.T) {
	patch := gomonkey.ApplyFunc(exec.LookPath, func(string) (string, error) {
		return "", errors.New("failed to lookPath")
	})
	defer patch.Reset()
	path, _ := GetExecPath(NetCore60)
	assert.Equal(t, "", path)
}

func Test_GetExecPath_NetCore80(t *testing.T) {
	patch := gomonkey.ApplyFunc(exec.LookPath, func(string) (string, error) {
		return "", errors.New("failed to lookPath")
	})
	defer patch.Reset()
	path, _ := GetExecPath(NetCore80)
	assert.Equal(t, "", path)
}

func TestAppendDomainNameToHostFileError(t *testing.T) {
	convey.Convey("TestAppendDomainNameToHostFileError", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return nil, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := AppendDomainNameToHostFile(nil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestAppendDomainNameToHostFile(t *testing.T) {
	convey.Convey("TestAppendDomainNameToHostFile", t, func() {
		mockWriter := &bufio.Writer{}
		mockFile := &os.File{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return &os.File{}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockFile), "Close", func(_ *os.File) error {
				return nil
			}),
			gomonkey.ApplyFunc(bufio.NewWriter, func(w io.Writer) *bufio.Writer {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockWriter), "Flush", func(_ *bufio.Writer) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockWriter), "WriteString", func(_ *bufio.Writer,
				s string) (int, error) {
				return 0, errors.New("mock error")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := AppendDomainNameToHostFile(map[string][]string{
			"www.baidu.com": {"192.168.0.1", "192.168.0.2"},
			"www.zhihu.com": {"192.168.0.3", "192.168.0.4"},
			"www.qq.com":    {"192.168.0.5"},
		})
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestAppendDomainNameToHostFileSuccess(t *testing.T) {
	convey.Convey("TestAppendDomainNameToHostFileSuccess", t, func() {
		mockWriter := &bufio.Writer{}
		mockFile := &os.File{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return &os.File{}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockFile), "Close", func(_ *os.File) error {
				return nil
			}),
			gomonkey.ApplyFunc(bufio.NewWriter, func(w io.Writer) *bufio.Writer {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockWriter), "Flush", func(_ *bufio.Writer) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockWriter), "WriteString", func(_ *bufio.Writer,
				s string) (int, error) {
				return 1, nil
			}),
			gomonkey.ApplyFunc(utilities.GetFileHashInfo, func(path string) (int64, string, error) {
				return 1, "hashValue", nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
				return make([]byte, 1), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := AppendDomainNameToHostFile(map[string][]string{
			"www.baidu.com": {"192.168.0.1", "192.168.0.2"},
			"www.zhihu.com": {"192.168.0.3", "192.168.0.4"},
			"www.qq.com":    {"192.168.0.5"},
		})
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestPrintContainerID_Success(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "cgroup_test")
	if err != nil {
		t.Fatal("failed to create tmp file: ", err)
	}
	defer os.Remove(tmpFile.Name())
	if _, err := tmpFile.WriteString("13:files:/kubepods/burstable/pod55ff84d3/2b70991c1e4aa0a0ba9d68d6695470cd\n12:cpu:/docker/3a1b2c3d4e5f"); err != nil {
		t.Fatal("failed to write test text: ", err)
	}
	tmpFile.Close()
	originalPath := "/proc/self/cgroup"
	defer func() { os.Setenv("CGROUP_PATH", originalPath) }()
	os.Setenv("CGROUP_PATH", tmpFile.Name())
	if err := PrintContainerID(); err != nil {
		t.Errorf("expected success, but returned error: %v", err)
	}
}

func TestPrintContainerID_NoMatch(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "cgroup_test_invalid")
	if err != nil {
		t.Fatal("failed to create tmp file: ", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString("13:files:/kubepods\n12:cpu:/docker"); err != nil {
		t.Fatal("failed to write test text: ", err)
	}
	tmpFile.Close()

	originalPath := "/proc/self/cgroup"
	defer func() { os.Setenv("CGROUP_PATH", originalPath) }()
	os.Setenv("CGROUP_PATH", tmpFile.Name())
	_ = PrintContainerID()
}

func Test_GetSelfAndSubAndOrphanProcessEntries(t *testing.T) {
	mockMap := make(map[int]*UnixProcessEntry)
	mockMap[0] = &UnixProcessEntry{Pid: 1, Ppid: 0, UID: 1003, ParentProcessEntry: &UnixProcessEntry{Pid: 0, Ppid: 0,
		UID: 0}}
	mockMap[1] = &UnixProcessEntry{Pid: 20, Ppid: 1, UID: 1003}
	mockMap[2] = &UnixProcessEntry{Pid: 120, Ppid: 20, UID: 1003, ChildProcessEntries: map[int]*UnixProcessEntry{0: {Pid: 130, Ppid: 120, UID: 1003}}}

	patches := gomonkey.ApplyFunc(getProcessEntries, func() (map[int]*UnixProcessEntry, error) {
		return mockMap, nil
	})

	defer patches.Reset()
	runtimeProcessEntry, err := GetSelfAndSubAndOrphanProcessEntries(1003, 20)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(runtimeProcessEntry.OrphanProcessEntries))
	assert.Equal(t, 2, len(runtimeProcessEntry.SubProcessEntries))

	mockMap1 := make(map[int]*UnixProcessEntry)
	mockMap1[0] = &UnixProcessEntry{Pid: 1, Ppid: 0, UID: 1003, ParentProcessEntry: &UnixProcessEntry{Pid: 0, Ppid: 0,
		UID: 0}}
	mockMap1[1] = &UnixProcessEntry{Pid: 20, Ppid: 1, UID: 1003, ChildProcessEntries: map[int]*UnixProcessEntry{0: {Pid: 130, Ppid: 20, UID: 1003}, 1: {Pid: 130, Ppid: 20, UID: 1003}}}
	mockMap1[2] = &UnixProcessEntry{Pid: 120, Ppid: 20, UID: 1003, ChildProcessEntries: map[int]*UnixProcessEntry{0: {Pid: 130, Ppid: 120, UID: 1003}}}

	patches1 := gomonkey.ApplyFunc(getProcessEntries, func() (map[int]*UnixProcessEntry, error) {
		return mockMap1, nil
	})

	defer patches1.Reset()
	runtimeProcessEntry1, err1 := GetSelfAndSubAndOrphanProcessEntries(1003, 20)
	assert.Equal(t, nil, err1)
	assert.Equal(t, 3, len(runtimeProcessEntry1.OrphanProcessEntries))
	assert.Equal(t, 2, len(runtimeProcessEntry1.SubProcessEntries))

	patches2 := gomonkey.ApplyFunc(getProcessEntries, func() (map[int]*UnixProcessEntry, error) {
		return nil, errors.New("fail")
	})

	defer patches2.Reset()
	_, err = GetSelfAndSubAndOrphanProcessEntries(1003, 20)
	assert.NotEqual(t, nil, err)
}

func TestPrintContainerID(t *testing.T) {
	patches := gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return nil, errors.New("fail")
	})
	defer patches.Reset()
	tests := []struct {
		name    string
		wantErr bool
	}{
		{"case1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := PrintContainerID(); (err != nil) != tt.wantErr {
				t.Errorf("PrintContainerID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

type fakeFile struct{}

func (fakeFile) Name() string       { return "63" }
func (fakeFile) Size() int64        { return 100 }
func (fakeFile) Mode() fs.FileMode  { return 0755 }
func (fakeFile) ModTime() time.Time { return time.Time{} }
func (fakeFile) IsDir() bool        { return false }
func (fakeFile) Sys() interface{} {
	return &syscall.Stat_t{
		Uid: 63,
	}
}

func TestGetFileInfo(t *testing.T) {
	convey.Convey("TestGetFileInfo", t, func() {
		convey.Convey("process is a zombie process", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
					return []byte("63 (sh) Z 2547594 2547598 2547598 0 -1 4227076 193 0 0 0 0 0"), nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			processInfo := GetFileInfo(&fakeFile{})
			convey.So(processInfo.Pid, convey.ShouldEqual, 0)
			convey.So(processInfo.State, convey.ShouldEqual, 0)
		})
		convey.Convey("failed to read file", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
					return []byte("63 (sh) Z 2547594 2547598 2547598 0 -1 4227076 193 0 0 0 0 0"), errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			processInfo := GetFileInfo(&fakeFile{})
			convey.So(processInfo.Pid, convey.ShouldEqual, 0)
			convey.So(processInfo.State, convey.ShouldEqual, 0)
		})
		convey.Convey("failed to Sscanf", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
					return []byte("63 (sh) Z 2547594 2547598 2547598 0 -1 4227076 193 0 0 0 0 0"), nil
				}),
				gomonkey.ApplyFunc(fmt.Sscanf, func(str string, format string, a ...any) (n int, err error) {
					return 0, errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			processInfo := GetFileInfo(&fakeFile{})
			convey.So(processInfo.Pid, convey.ShouldEqual, 0)
			convey.So(processInfo.State, convey.ShouldEqual, 0)
		})
	})
}

func TestGetLibraryPath(t *testing.T) {
	convey.Convey("TestGetLibraryPath cangjie1.0", t, func() {
		libPath := getLibraryPath(CangJie10)
		convey.So(strings.Contains(libPath, "cangjie1.0"), convey.ShouldBeTrue)
	})
	convey.Convey("TestGetLibraryPath cangjie0.52.3", t, func() {
		libPath := getLibraryPath(CangJie0523)
		convey.So(strings.Contains(libPath, "cangjie0.52.3"), convey.ShouldBeTrue)
	})
	convey.Convey("TestGetLibraryPath custom-runtime-rpc", t, func() {
		libPath := getLibraryPath(CustomRuntimeRPC)
		convey.So(strings.Contains(libPath, "java8"), convey.ShouldBeTrue)
	})
	convey.Convey("TestGetLibraryPath xxx", t, func() {
		_ = os.Setenv("LD_LIBRARY_PATH", "/test/lib/path")
		_ = os.Setenv("DEPLOY_DIR", "/code/deploy/dir")
		libPath := getLibraryPath("xxx")
		convey.So(strings.Contains(libPath, "lib64"), convey.ShouldBeTrue)
		convey.So(strings.Contains(libPath, "/test/lib/path"), convey.ShouldBeTrue)
		convey.So(strings.Contains(libPath, "/code/deploy/dir"), convey.ShouldBeTrue)
	})
}

func TestGetPreservePath(t *testing.T) {
	type args struct {
		logPaths []string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		want        string
	}{
		{
			name: "test cae function preservePath",
			args: args{
				logPaths: []string{"/var/paas/log/hello.log", "/var/paas/log/world.log"},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utilities.IsCAEFunc, func(businessType string) bool {
						return true
					}),
				})
				return patches
			},
			want: constants.FastFreezeCAEPreservePath + ":/var/paas/log",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			assert.Equalf(t, tt.want, GetPreservePath(tt.args.logPaths), "GetPreservePath(%v)", tt.args.logPaths)
		})
	}
}
