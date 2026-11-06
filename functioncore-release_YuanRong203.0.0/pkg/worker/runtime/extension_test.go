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
package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

func Test_buildCmd(t *testing.T) {
	convey.Convey("Test buildCmd has PATH env", t, func() {
		c := &Extension{}
		patches := []*Patches{
			ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, nil
			}),
			ApplyFunc(os.Getenv, func(key string) string {
				return "/bin/bash"
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		cfg := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				RMConfig: rmconfig.Config{
					RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
				},
			},
		}

		cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
		cmd, err := c.buildCmd("extension-test", cfg)
		convey.So(err, convey.ShouldBeNil)
		convey.So(cmd.Args[0], convey.ShouldEqual, "/opt/function/code/extension/extension-test")
		convey.So(cmd.Env, convey.ShouldContain, "EXTENSION_API_ADDR=127.0.0.1:31555")
	})
}

func TestExtension_RunWrapperByID(t *testing.T) {
	convey.Convey("test extension RunWrapperByID 1", t, func() {
		convey.Convey("succeed to run wrapper by id", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),

				ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RMConfig: rmconfig.Config{
						RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
					},
				},
			}
			cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
			createCfg := &types.CreateOptions{
				UID:  1003,
				GID:  1003,
				Conf: cfg,
				Name: "extension-test",
			}
			extension := Extension{
				name:    "test",
				address: "127.0.0.1:31555",
			}
			_, err := extension.RunWrapperByID(createCfg)
			convey.So(err, convey.ShouldBeNil)
		})
	})
	convey.Convey("test extension RunWrapperByID 1 buildCmd error", t, func() {
		convey.Convey("succeed to get buildCmd error", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, errors.New("abc")
				}),

				ApplyFunc(os.IsNotExist, func(err error) bool {
					return true
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RMConfig: rmconfig.Config{
						RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
					},
				},
			}
			cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
			createCfg := &types.CreateOptions{
				UID:  1003,
				GID:  1003,
				Conf: cfg,
				Name: "extension-test",
			}
			extension := Extension{
				name:    "test",
				address: "127.0.0.1:31555",
			}
			_, err := extension.RunWrapperByID(createCfg)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
	convey.Convey("test extension RunWrapperByID SetCmdCredByID start error", t, func() {
		convey.Convey("succeed to SetCmdCredByID start error", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),

				ApplyFunc(utils.SetCmdCredByID, func(cmd *exec.Cmd, uid int, gid int) error {
					return errors.New("error")
				}),
				ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RMConfig: rmconfig.Config{
						RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
					},
				},
			}
			cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
			createCfg := &types.CreateOptions{
				UID:  1003,
				GID:  1003,
				Conf: cfg,
				Name: "extension-test",
			}
			extension := Extension{
				name:    "test",
				address: "127.0.0.1:31555",
			}
			_, err := extension.RunWrapperByID(createCfg)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})

	convey.Convey("test extension RunWrapperByID 1 cmd start error", t, func() {
		convey.Convey("succeed to get cmd start error", func() {
			patches := [...]*Patches{
				ApplyFunc(filepath.Abs, func(path string) (string, error) {
					return "", nil
				}),
				ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
					return nil, nil
				}),

				ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
				ApplyFunc((*exec.Cmd).Start, func(c *exec.Cmd) error {
					return errors.New("error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RMConfig: rmconfig.Config{
						RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
					},
				},
			}
			cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
			createCfg := &types.CreateOptions{
				UID:  1003,
				GID:  1003,
				Conf: cfg,
				Name: "extension-test",
			}
			extension := Extension{
				name:    "test",
				address: "127.0.0.1:31555",
			}
			_, err := extension.RunWrapperByID(createCfg)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestRunExtensionProcess(t *testing.T) {
	convey.Convey("test RunExtensionProcess", t, func() {
		cfg := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				RMConfig: rmconfig.Config{
					RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
				},
			},
		}
		cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
		createCfg := &types.CreateOptions{
			UID:  1003,
			GID:  1003,
			Conf: cfg,
			Name: "extension-test",
		}
		convey.Convey("succeed to run wrapper by id", func() {
			extension := &Extension{}
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(extension), "RunWrapperByID",
					func(_ *Extension, createOptions *types.CreateOptions) (*os.Process, error) {
						return &os.Process{Pid: 100}, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(createCfg.UID, convey.ShouldEqual, 1003)
			test := func([]byte) {
				return
			}
			process, err := RunExtensionProcess(createCfg, test)
			convey.So(err, convey.ShouldBeNil)
			convey.So(process, convey.ShouldNotBeNil)
			convey.So(process.Pid, convey.ShouldEqual, 100)
		})

		convey.Convey("failed to run wrapper by id", func() {
			extension := &Extension{}
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(extension), "RunWrapperByID",
					func(_ *Extension, createOptions *types.CreateOptions) (*os.Process, error) {
						return nil, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(createCfg.UID, convey.ShouldEqual, 1003)
			test := func([]byte) {
				return
			}
			process, err := RunExtensionProcess(createCfg, test)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(process, convey.ShouldBeNil)
		})
	})
}

func TestExtensionManager_SendRequest(t *testing.T) {
	var cli *http.Client
	type fields struct {
		httpServer         *http.Server
		extension          map[string]*ExtensionInfo
		reqMap             map[uint64]*sync.WaitGroup
		Mutex              sync.Mutex
		initChan           chan InitNotify
		funcName           string
		funcVersion        string
		valid              bool
		enable             bool
		healthy            bool
		extNum             int
		invokedFunctionUrn string
	}
	type args struct {
		icID           uint64
		traceID        string
		requestTimeout time.Duration
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeeded to send extension request", fields{
			httpServer: &http.Server{},
			extension:  map[string]*ExtensionInfo{"extension1": &ExtensionInfo{name: "extension1", address: "127.0.0.1:8081"}},
			reqMap:     make(map[uint64]*sync.WaitGroup),
			Mutex:      sync.Mutex{},
			valid:      true,
			enable:     true,
			healthy:    true,
			extNum:     1,
		}, args{
			icID:           1,
			traceID:        "abc",
			requestTimeout: 5,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: ioutil.NopCloser(bytes.NewBufferString("abc"))}, nil
					}),
					ApplyMethod(reflect.TypeOf(new(functionlog.FunctionLogger)), "WriteStdLog", func(_ *functionlog.FunctionLogger, logString,
						timeStamp string, isReserved bool, nanoTimestamp string) {
						return
					}),
				})
				return patches
			}},
		{"case2 send extension request response is nil", fields{
			httpServer: &http.Server{},
			extension:  map[string]*ExtensionInfo{"extension1": &ExtensionInfo{name: "extension1", address: "127.0.0.1:8081"}},
			reqMap:     make(map[uint64]*sync.WaitGroup),
			Mutex:      sync.Mutex{},
			valid:      true,
			enable:     true,
			healthy:    true,
			extNum:     1,
		}, args{
			icID:           1,
			traceID:        "abc",
			requestTimeout: 5,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("response error")
					}),
					ApplyMethod(reflect.TypeOf(new(functionlog.FunctionLogger)), "WriteStdLog", func(_ *functionlog.FunctionLogger, logString,
						timeStamp string, isReserved bool, nanoTimestamp string) {
						return
					}),
				})
				return patches
			}},
		{"case3 send extension request response status code is not 200", fields{
			httpServer: &http.Server{},
			extension:  map[string]*ExtensionInfo{"extension1": &ExtensionInfo{name: "extension1", address: "127.0.0.1:8081"}},
			reqMap:     make(map[uint64]*sync.WaitGroup),
			Mutex:      sync.Mutex{},
			valid:      true,
			enable:     true,
			healthy:    true,
			extNum:     1,
		}, args{
			icID:           1,
			traceID:        "abc",
			requestTimeout: 5,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(cli), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: ioutil.NopCloser(bytes.NewBufferString("abc")),
							StatusCode: http.StatusInternalServerError}, nil
					}),
					ApplyMethod(reflect.TypeOf(new(functionlog.FunctionLogger)), "WriteStdLog", func(_ *functionlog.FunctionLogger, logString,
						timeStamp string, isReserved bool, nanoTimestamp string) {
						return
					}),
				})
				return patches
			}},
		{"case4 NewRequest error", fields{
			httpServer: &http.Server{},
			extension:  map[string]*ExtensionInfo{"extension1": &ExtensionInfo{name: "extension1", address: "127.0.0.1:8081"}},
			reqMap:     make(map[uint64]*sync.WaitGroup),
			Mutex:      sync.Mutex{},
			valid:      true,
			enable:     true,
			healthy:    true,
			extNum:     1,
		}, args{
			icID:           1,
			traceID:        "abc",
			requestTimeout: 5,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
						return nil, errors.New("error")
					}),
					ApplyMethod(reflect.TypeOf(new(functionlog.FunctionLogger)), "WriteStdLog", func(_ *functionlog.FunctionLogger, logString,
						timeStamp string, isReserved bool, nanoTimestamp string) {
						return
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			extMgr := &ExtensionManager{
				httpServer:         tt.fields.httpServer,
				extension:          tt.fields.extension,
				reqMap:             tt.fields.reqMap,
				Mutex:              tt.fields.Mutex,
				initChan:           tt.fields.initChan,
				funcName:           tt.fields.funcName,
				funcVersion:        tt.fields.funcVersion,
				valid:              tt.fields.valid,
				enable:             tt.fields.enable,
				healthy:            tt.fields.healthy,
				extNum:             tt.fields.extNum,
				invokedFunctionUrn: tt.fields.invokedFunctionUrn,
				logger:             new(functionlog.FunctionLogger),
			}
			extMgr.SendRequest(tt.args.icID, tt.args.traceID, tt.args.requestTimeout)
			extMgr.WaitResp(tt.args.icID)
			patches.ResetAll()
		})
	}
}

type MockFile struct {
	name  string
	idDir bool
}

func (m MockFile) Name() string {
	return m.name
}
func (m MockFile) Size() int64 {
	return 20
}
func (m MockFile) Mode() os.FileMode {
	return 2
}
func (m MockFile) ModTime() time.Time {
	return time.Time{}
}
func (m MockFile) IsDir() bool {
	return m.idDir
}
func (m MockFile) Sys() interface{} {
	return MockFile{}
}

func TestCheckExtension(t *testing.T) {
	convey.Convey("test CheckExtension", t, func() {
		convey.Convey("failed to exec CheckExtension, extensionPath is not exist", func() {
			patches := []*Patches{
				ApplyFunc(ioutil.ReadDir,
					func(_ string) ([]os.FileInfo, error) {
						return []os.FileInfo{}, errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, flag := CheckExtension()
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("failed to exec CheckExtension, extension file is invalid", func() {
			patches := []*Patches{
				ApplyFunc(ioutil.ReadDir,
					func(_ string) ([]os.FileInfo, error) {
						return []os.FileInfo{
							MockFile{"extension", true},
							MockFile{"xxx.sh", false},
						}, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, flag := CheckExtension()
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("success to exec  CheckExtension, ", func() {
			patches := []*Patches{
				ApplyFunc(ioutil.ReadDir,
					func(_ string) ([]os.FileInfo, error) {
						return []os.FileInfo{
							MockFile{"extension", true},
							MockFile{"extension_1.sh", false},
						}, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			names, flag := CheckExtension()
			convey.So(flag, convey.ShouldBeTrue)
			convey.So(len(names), convey.ShouldEqual, 1)
		})
	})
}

func TestGetExtensionManager(t *testing.T) {
	type args struct {
		conf *config.Configuration
	}

	tests := []struct {
		name            string
		args            args
		patchesFunc     mockUtils.PatchesFunc
		extensionEnable bool
	}{
		{"case1 succeeded to get extension manager", args{&config.Configuration{RuntimeConfig: config.RuntimeConfig{
			Timeout: 3,
			URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
			RMConfig: rmconfig.Config{
				ServerPort: 1234,
				RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
					worker.ExtensionScript: {
						Port:   8081,
						Enable: true,
					},
				},
			},
		}}}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
			})
			return patches
		}, false},
		{"case2 extension disable", args{conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
			RMConfig: rmconfig.Config{
				ServerPort: 1234,
				RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
					worker.ExtensionScript: {
						Port:   8081,
						Enable: false,
					},
				},
			},
		}}}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
			})
			return patches
		}, false},
		{"case3 extension scripts", args{conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
			RMConfig: rmconfig.Config{
				ServerPort: 1234,
				RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
					worker.ExtensionScript: {
						Port:   8081,
						Enable: true,
					},
				},
			},
		}}}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{}, false
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
			})
			return patches
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := GetExtensionManager(tt.args.conf); !reflect.DeepEqual(got.GetExtensionEnable(), tt.extensionEnable) {
				t.Errorf("GetExtensionManager() = %v, want %v", got.GetExtensionEnable(), tt.extensionEnable)
			}
			patches.ResetAll()
		})
	}
}

func TestInitExtensionProcess(t *testing.T) {
	type args struct {
		conf        *config.Configuration
		timeout     time.Duration
		functionLog *functionlog.FunctionLogger
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{"case1 ExtensionMgr is nil", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: false,
						},
					},
				},
			}},
			timeout:     3,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
			})
			return patches
		}, false},
		{"case2 ExtensionMgr is not healthy", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout:     3,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*ExtensionManager).startExtensionServer, func(extMgr *ExtensionManager, conf *config.Configuration) {
					extMgr.healthy = false
				}),
				ApplyFunc((*ExtensionManager).generateExtensionMgr, func(extMgr *ExtensionManager, conf *config.Configuration) error {
					extMgr.healthy = false
					extMgr.enable = true
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
			})
			return patches
		}, true},
		{"case3 ExtensionMgr logger is nil", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout: 3,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return nil, errors.New("test")
				}),
			})
			return patches
		}, false},
		{"case4 failed to start the extension process ", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout:     3,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(startExtensionProcess, func() tcpmsg.RuntimeManagerRSP {
					return tcpmsg.RuntimeManagerRSP{Err: "error"}
				}),
				ApplyFunc((*ExtensionManager).GetExtensionEnable, func(extMgr *ExtensionManager) bool {
					extMgr.healthy = true
					return true
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
			})
			return patches
		}, true},
		{"case5 extension register error", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout:     3,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(startExtensionProcess, func() tcpmsg.RuntimeManagerRSP {
					return tcpmsg.RuntimeManagerRSP{}
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
				ApplyFunc((*ExtensionManager).GetExtensionEnable, func(extMgr *ExtensionManager) bool {
					extMgr.healthy = true
					notifyRes := InitNotify{
						name: extensionName,
						err:  errors.New("error"),
					}
					extMgr.initChan <- notifyRes
					return true
				}),
			})
			return patches
		}, true},
		{"case6 extension register timeout", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout:     3,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(startExtensionProcess, func() tcpmsg.RuntimeManagerRSP {
					return tcpmsg.RuntimeManagerRSP{}
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
				ApplyFunc((*ExtensionManager).GetExtensionEnable, func(extMgr *ExtensionManager) bool {
					extMgr.healthy = true
					return true
				}),
			})
			return patches
		}, true},

		{"case7 extension process exit", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout:     3,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(startExtensionProcess, func() tcpmsg.RuntimeManagerRSP {
					return tcpmsg.RuntimeManagerRSP{}
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
				ApplyFunc((*ExtensionManager).startExtensionServer, func(extMgr *ExtensionManager, conf *config.Configuration) {
					extMgr.stateChan <- struct{}{}
				}),
			})
			return patches
		}, true},

		{"case8 succeeded to init extension register", args{
			conf: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				Timeout: 3,
				URN:     "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@sccdefault@testliukong001:latest",
				RMConfig: rmconfig.Config{
					ServerPort: 1234,
					RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
						worker.ExtensionScript: {
							Port:   8081,
							Enable: true,
						},
					},
				},
			}},
			timeout:     3 * time.Second,
			functionLog: &functionlog.FunctionLogger{},
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return nil
				}),
				ApplyFunc(CheckExtension, func() ([]string, bool) {
					return []string{"extension1"}, true
				}),
				ApplyFunc(startExtensionProcess, func() tcpmsg.RuntimeManagerRSP {
					return tcpmsg.RuntimeManagerRSP{}
				}),
				ApplyFunc(functionlog.GetFunctionLogger, func(conf *config.Configuration) (*functionlog.FunctionLogger, error) {
					return &functionlog.FunctionLogger{}, nil
				}),
				ApplyFunc((*ExtensionManager).startExtensionServer, func(extMgr *ExtensionManager, conf *config.Configuration) {
					notifyRes := InitNotify{
						name: extensionName,
						err:  nil,
					}
					extMgr.initChan <- notifyRes
				}),
			})
			return patches
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ExtensionMgr = GetExtensionManager(tt.args.conf)
			if err := InitExtensionProcess(tt.args.timeout, tt.args.conf, tt.args.functionLog); (err != nil) != tt.wantErr {
				t.Errorf("InitExtensionProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

type fakeHTTPResponseWriter struct {
	close      bool
	header     http.Header
	body       []byte
	statusCode int
}

func (f *fakeHTTPResponseWriter) Header() http.Header {
	return f.header
}

func (f *fakeHTTPResponseWriter) Write(bytes []byte) (int, error) {
	if f.close {
		return 0, errors.New("fakeResponseWriter closed")
	}
	f.body = append(f.body, bytes...)
	return len(bytes), nil
}

func (f *fakeHTTPResponseWriter) WriteHeader(statusCode int) {
	f.statusCode = statusCode
}

func TestExtensionManager_Register(t *testing.T) {
	type fields struct {
		httpServer         *http.Server
		extension          map[string]*ExtensionInfo
		reqMap             map[uint64]*sync.WaitGroup
		Mutex              sync.Mutex
		initChan           chan InitNotify
		funcName           string
		funcVersion        string
		valid              bool
		enable             bool
		healthy            bool
		extNum             int
		invokedFunctionUrn string
	}
	type args struct {
		w       http.ResponseWriter
		request *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		statusCode  int
		body        RegisterResponse
	}{
		{"case1 extension name or extension address is invalid", fields{
			extension: make(map[string]*ExtensionInfo),
			initChan:  make(chan InitNotify, maxExtensionNum),
		}, args{
			w: &fakeHTTPResponseWriter{},
			request: func() *http.Request {
				req := httptest.NewRequest("POST", "/extension/register", bytes.NewReader([]byte{}))
				return req
			}(),
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		},
			http.StatusOK,
			RegisterResponse{
				ErrorInfo: ExtensionErrorInfo{
					ErrorType: "InvalidExtensionRegistrationParameter",
					ErrorMsg:  "The extension names and address are essential",
				},
			},
		},
		{"case2 ioutil readAll error", fields{
			extension: make(map[string]*ExtensionInfo),
			initChan:  make(chan InitNotify, maxExtensionNum),
		}, args{
			w: &fakeHTTPResponseWriter{},
			request: func() *http.Request {
				req := httptest.NewRequest("POST", "/extension/register", bytes.NewReader([]byte{}))
				req.Header.Set(extensionName, "extension1")
				req.Header.Set(extensionAddress, "127.0.0.1:8081")
				return req
			}(),
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte(""), errors.New("error")
				}),
			})
			return patches
		},
			http.StatusOK,
			RegisterResponse{
				ErrorInfo: ExtensionErrorInfo{
					ErrorType: "InvalidExtensionRegistrationParameter",
					ErrorMsg:  "The registration extension name is incorrect",
				},
			},
		},
		{"case3 current extension name is not exist", fields{
			extension: make(map[string]*ExtensionInfo),
			initChan:  make(chan InitNotify, maxExtensionNum),
		}, args{
			w: &fakeHTTPResponseWriter{},
			request: func() *http.Request {
				req := httptest.NewRequest("POST", "/extension/register", bytes.NewReader([]byte{}))
				req.Header.Set(extensionName, "extension1")
				req.Header.Set(extensionAddress, "127.0.0.1:8081")
				return req
			}(),
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		},
			http.StatusOK,
			RegisterResponse{
				ErrorInfo: ExtensionErrorInfo{
					ErrorType: "InvalidExtensionRegistrationParameter",
					ErrorMsg:  "The registration extension name is incorrect",
				},
			},
		},
		{"case4 succeeded to register", fields{
			extension: map[string]*ExtensionInfo{"extension1": &ExtensionInfo{}},
			initChan:  make(chan InitNotify, maxExtensionNum),
		}, args{
			w: &fakeHTTPResponseWriter{},
			request: func() *http.Request {
				req := httptest.NewRequest("POST", "/extension/register", bytes.NewReader([]byte{}))
				req.Header.Set(extensionName, "extension1")
				req.Header.Set(extensionAddress, "127.0.0.1:8081")
				return req
			}(),
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("error")
				}),
			})
			return patches
		},
			http.StatusOK,
			RegisterResponse{},
		},
		{"case5 succeeded to register", fields{
			extension: map[string]*ExtensionInfo{"extension1": &ExtensionInfo{}},
			initChan:  make(chan InitNotify, maxExtensionNum),
		}, args{
			w: &fakeHTTPResponseWriter{},
			request: func() *http.Request {
				req := httptest.NewRequest("POST", "/extension/register", bytes.NewReader([]byte{}))
				req.Header.Set(extensionName, "extension1")
				req.Header.Set(extensionAddress, "127.0.0.1:8081")
				return req
			}(),
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		},
			http.StatusOK,
			RegisterResponse{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			extMgr := &ExtensionManager{
				httpServer:         tt.fields.httpServer,
				extension:          tt.fields.extension,
				reqMap:             tt.fields.reqMap,
				Mutex:              tt.fields.Mutex,
				initChan:           tt.fields.initChan,
				funcName:           tt.fields.funcName,
				funcVersion:        tt.fields.funcVersion,
				valid:              tt.fields.valid,
				enable:             tt.fields.enable,
				healthy:            tt.fields.healthy,
				extNum:             tt.fields.extNum,
				invokedFunctionUrn: tt.fields.invokedFunctionUrn,
			}
			extMgr.Register(tt.args.w, tt.args.request)
			resp := tt.args.w.(*fakeHTTPResponseWriter)
			var body RegisterResponse
			json.Unmarshal(resp.body, &body)
			if resp.statusCode != tt.statusCode {
				t.Errorf("resp %v", tt.args.w.(*httptest.ResponseRecorder).Result())
			}
			if body.ErrorInfo.ErrorType != tt.body.ErrorInfo.ErrorType {
				t.Errorf("resp body %v", string(resp.body))
			}
			if body.ErrorInfo.ErrorMsg != tt.body.ErrorInfo.ErrorMsg {
				t.Errorf("resp body %v", string(resp.body))
			}
			time.Sleep(5 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestSaveExtensionFailedLog(t *testing.T) {
	convey.Convey("test SaveExtensionFailedLog", t, func() {
		convey.Convey("failed to save extension failed log, LogRecorderBox is empty", func() {
			tmpLogRecorderBox := make([]*functionlog.LogRecorder, 0, 2)
			patches := [...]*Patches{
				ApplyGlobalVar(&functionlog.LogRecorderBox, tmpLogRecorderBox),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RMConfig: rmconfig.Config{
						RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
					},
				},
			}
			cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
			err := snerror.New(snerror.ExtensionExecFailed,
				fmt.Sprintf(snerror.ErrText(snerror.ExtensionExecFailed), "user code execution error"))
			SaveExtensionFailedLog(cfg, err)
		})

		convey.Convey("success to save extension failed log", func() {
			tmpLogRecorderBox := make([]*functionlog.LogRecorder, 0, 2)
			logRecorderCopy := &functionlog.LogRecorder{}
			tmpLogRecorderBox = append(tmpLogRecorderBox, logRecorderCopy)

			patches := [...]*Patches{
				ApplyFunc(functionlog.Sync, func(conf *config.Configuration) {
					return
				}),

				ApplyMethod(reflect.TypeOf(&functionlog.LogRecorder{}), "FinishSave",
					func(_ *functionlog.LogRecorder) {
						return
					}),
				ApplyGlobalVar(&functionlog.LogRecorderBox, tmpLogRecorderBox),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			cfg := &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					RMConfig: rmconfig.Config{
						RuntimeConfigs: make(map[string]rmconfig.RuntimeConfig, 1),
					},
				},
			}
			cfg.RMConfig.RuntimeConfigs[worker.ExtensionScript] = rmconfig.RuntimeConfig{Port: 31555}
			err := snerror.New(snerror.ExtensionExecFailed,
				fmt.Sprintf(snerror.ErrText(snerror.ExtensionExecFailed), "user code execution error"))
			SaveExtensionFailedLog(cfg, err)
		})

	})
}

func Test_isValidAddress(t *testing.T) {
	convey.Convey("test isValidAddress", t, func() {
		convey.Convey("case1： invalid address ", func() {
			address := "127.0.0.1"
			flag := isValidAddress(address)
			convey.So(flag, convey.ShouldBeFalse)
		})
		convey.Convey("case2： invalid address ", func() {
			address := "127.0.0.100:8192"
			flag := isValidAddress(address)
			convey.So(flag, convey.ShouldBeFalse)
		})
		convey.Convey("case3： invalid port ", func() {
			address := "127.0.0.1:31555"
			flag := isValidAddress(address)
			convey.So(flag, convey.ShouldBeFalse)
		})
		convey.Convey("case4： valid address ", func() {
			address := "127.0.0.1:8090"
			flag := isValidAddress(address)
			convey.So(flag, convey.ShouldBeTrue)
		})
	})
}
