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
package custom

import (
	"context"
	"errors"
	"net/http"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime"
	mockutils "functioncore/test/utils"
)

func TestHotLoad(t *testing.T) {
	convey.Convey("TestHotLoad", t, func() {
		convey.Convey("Test 1", func() {
			custom := &Custom{}
			f := &fusionCustom{
				Custom: custom,
			}
			ctx := context.TODO()
			conf := &config.Configuration{}

			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(f.Custom), "HotLoad", func(_ *Custom, ctx context.Context, conf *config.Configuration) error {
					return nil
				}),
				gomonkey.ApplyFunc(runtime.InitExtensionProcess, func(timeout time.Duration, configuration *config.Configuration,
					functionLog *functionlog.FunctionLogger) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := f.HotLoad(ctx, conf)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func TestCreateFusionCustomRuntime(t *testing.T) {
	type args struct {
		conf     *config.Configuration
		handlers runtime.Handlers
	}
	mockCfg := &config.Configuration{}
	mockCfg.RMConfig.RuntimeConfigs = map[string]rmconfig.RuntimeConfig{
		worker.CustomRuntimeLanguage: rmconfig.RuntimeConfig{Port: 8080},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		mockOnce    sync.Once
		want        runtime.LegacyRuntime
		wantErr     bool
	}{
		{
			name: "test001",
			args: args{
				conf: mockCfg,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
						return &runtime.AbstractRuntime{}, nil
					}),
				})
				return patches
			},
			mockOnce: sync.Once{},
			want:     nil,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := CreateFusionCustomRuntime(tt.args.conf, tt.args.handlers)
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateFusionCustomRuntime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestStartFusionCustomHTTPServer(t *testing.T) {
	type args struct {
		conf *config.RuntimeConfig
	}
	mockCfg := &config.RuntimeConfig{}
	mockCfg.RMConfig.RuntimeConfigs = map[string]rmconfig.RuntimeConfig{
		worker.CustomRuntimeLanguage: rmconfig.RuntimeConfig{Port: 8080},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test001",
			args: args{
				conf: mockCfg,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(getServer, func(address string) (*server, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := StartFusionCustomHTTPServer(tt.args.conf); (err != nil) != tt.wantErr {
				t.Errorf("StartFusionCustomHTTPServer() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err := StopFusionCustomHTTPServer(); err != nil {
				t.Errorf("StopFusionCustomHTTPServer() error = %v", err)
			}
		})
	}
}

func TestStopFusionCustomHTTPServer(t *testing.T) {
	convey.Convey("TestStopFusionCustomHTTPServer", t, func() {
		convey.Convey("Test1", func() {
			custom := &Custom{}
			fusionCustomRuntime = &fusionCustom{
				Custom: custom,
			}
			fusionCustomRuntime.server = nil
			tests := []struct {
				name    string
				wantErr bool
			}{
				{
					name:    "test001",
					wantErr: false,
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					if err := StopFusionCustomHTTPServer(); (err != nil) != tt.wantErr {
						t.Errorf("StopFusionCustomHTTPServer() error = %v, wantErr %v", err, tt.wantErr)
					}
				})
			}
		})
		convey.Convey("Test 2", func() {
			custom := &Custom{
				server: &server{
					httpServer: &http.Server{},
				},
			}
			fusionCustomRuntime = &fusionCustom{
				Custom: custom,
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(fusionCustomRuntime.Custom.httpServer), "Shutdown", func(_ *http.Server, ctx context.Context) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := StopFusionCustomHTTPServer()
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("Test 3", func() {
			custom := &Custom{
				server: &server{
					httpServer: &http.Server{},
				},
			}
			fusionCustomRuntime = &fusionCustom{
				Custom: custom,
			}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(fusionCustomRuntime.Custom.httpServer), "Shutdown", func(_ *http.Server, ctx context.Context) error {
					return errors.New("shut down err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := StopFusionCustomHTTPServer()
			convey.So(err.Error(), convey.ShouldEqual, "shut down err")
		})
	})
}

func TestRunProcess(t *testing.T) {
	type args struct {
		uid         int
		gid         int
		conf        *config.Configuration
		logReporter func([]byte)
	}
	mockCfg := &config.Configuration{}
	mockCfg.RMConfig.RuntimeConfigs = map[string]rmconfig.RuntimeConfig{
		worker.CustomRuntimeLanguage: rmconfig.RuntimeConfig{Port: 8080},
	}
	mockCustomRuntime := &Custom{}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test001",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test002",
			args: args{
				conf: mockCfg,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
						return &runtime.AbstractRuntime{}, nil
					}),
					gomonkey.ApplyFunc(getServer, func(address string) (*server, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test003",
			args: args{
				conf: mockCfg,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
						return &runtime.AbstractRuntime{}, nil
					}),
					gomonkey.ApplyFunc(getServer, func(address string) (*server, error) {
						return &server{}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockCustomRuntime), "RunWrapperByID",
						func(_ *Custom, uid, gid int) (*os.Process, error) {
							return nil, errors.New("mock error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test004",
			args: args{
				conf: mockCfg,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(runtime.NewAbstractRuntime, func(conf *config.Configuration) (*runtime.AbstractRuntime, error) {
						return &runtime.AbstractRuntime{}, nil
					}),
					gomonkey.ApplyFunc(getServer, func(address string) (*server, error) {
						return &server{}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockCustomRuntime), "RunWrapperByID",
						func(_ *Custom, uid, gid int) (*os.Process, error) {
							return &os.Process{}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := RunProcess(tt.args.uid, tt.args.gid, tt.args.conf, tt.args.logReporter)
			if (err != nil) != tt.wantErr {
				t.Errorf("RunProcess() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}
