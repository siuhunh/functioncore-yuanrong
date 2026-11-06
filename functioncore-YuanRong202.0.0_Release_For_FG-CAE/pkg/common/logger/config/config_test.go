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

// Package config is common logger client
package config

import (
	"encoding/json"
	"errors"
	"fmt"

	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"gotest.tools/assert"

	mockUtils "functioncore/test/utils"
)

func TestInitConfig(t *testing.T) {
	convey.Convey("TestInitConfig", t, func() {
		convey.Convey("test 1", func() {
			patches := gomonkey.ApplyFunc(GetCoreInfoFromEnv, func() (CoreInfo, error) {
				return defaultCoreInfo, nil
			})
			defer patches.Reset()
			coreInfo, err := GetCoreInfoFromEnv()
			fmt.Printf("log config:%+v\n", coreInfo)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestInitConfigWithReadFileError(t *testing.T) {
	convey.Convey("TestInitConfigWithEmptyPath", t, func() {
		convey.Convey("test 1", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return nil, errors.New("mock read file error")
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			coreInfo, err := GetCoreInfoFromEnv()
			fmt.Printf("error:%s\n", err)
			fmt.Printf("log config:%+v\n", coreInfo)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func Test_getCoreInfoFromEnv(t *testing.T) {
	coreInfo := CoreInfo{
		FilePath:   "/home/sn/log",
		Level:      "ERROR",
		Tick:       10,    // Unit: Second
		First:      10,    // Unit: Number of logs
		Thereafter: 5,     // Unit: Number of logs
		Tracing:    false, // tracing log switch
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(ExtractCoreInfoFromEnv,
			func(env string) (CoreInfo, error) {
				return coreInfo, nil
			}),
	}
	info, err := GetCoreInfoFromEnv()
	assert.Equal(t, err, nil)
	assert.Equal(t, info.Level, "ERROR")
	coreInfo.FilePath = "log"
	for _, p := range patches {
		p.Reset()
	}
	patches = [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(ExtractCoreInfoFromEnv,
			func(env string) (CoreInfo, error) {
				return coreInfo, nil
			}),
	}
	info, err = GetCoreInfoFromEnv()
	assert.Equal(t, info.Level, "INFO")
	for _, p := range patches {
		p.Reset()
	}
}

func TestInitConfigWithErrorJson(t *testing.T) {
	convey.Convey("TestInitConfigWithEmptyPath", t, func() {
		convey.Convey("test 1", func() {
			mockErrorJson := "{\n\"filepath\": \"/home/sn/mock\",\n\"level\": \"INFO\",\n\"maxsize\": " +
				"500,\n\"maxbackups\": 1,\n\"maxage\": 1,\n\"compress\": true\n"
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return []byte(mockErrorJson), nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			coreInfo, err := GetCoreInfoFromEnv()
			fmt.Printf("error:%s\n", err)
			fmt.Printf("log config:%+v\n", coreInfo)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestInitConfigWithEmptyPath(t *testing.T) {
	convey.Convey("TestInitConfigWithEmptyPath", t, func() {
		convey.Convey("test 1", func() {
			mockCfgInfo := "{\n\"filepath\": \"\",\n\"level\": \"INFO\",\n\"maxsize\": " +
				"500,\n\"maxbackups\": 1,\n\"maxage\": 1,\n\"compress\": true\n}"
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return []byte(mockCfgInfo), nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			coreInfo, err := GetCoreInfoFromEnv()
			fmt.Printf("error:%s\n", err)
			fmt.Printf("log config:%+v\n", coreInfo)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestInitConfigWithValidateError(t *testing.T) {
	convey.Convey("TestInitConfigWithEmptyPath", t, func() {
		convey.Convey("test 1", func() {
			mockErrorJson := "{\n\"filepath\": \"some_relative_path\",\n\"level\": \"INFO\",\n\"maxsize\": " +
				"500,\n\"maxbackups\": 1,\n\"maxage\": 1}"
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return []byte(mockErrorJson), nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			coreInfo, err := GetCoreInfoFromEnv()
			fmt.Printf("error:%s\n", err)
			fmt.Printf("log config:%+v\n", coreInfo)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestGetDefaultCoreInfo(t *testing.T) {
	tests := []struct {
		name string
		want CoreInfo
	}{
		{
			name: "test001",
			want: CoreInfo{
				FilePath:   "/home/sn/log",
				Level:      "INFO",
				Tick:       10,    // Unit: Second
				First:      10,    // Unit: Number of logs
				Thereafter: 5,     // Unit: Number of logs
				Tracing:    false, // tracing log switch
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDefaultCoreInfo(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDefaultCoreInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractCoreInfoFromEnv(t *testing.T) {
	normalInfo, _ := json.Marshal(defaultCoreInfo)
	abnormal1 := mockUtils.PatchSlice{}
	abnormalInfo1, _ := json.Marshal(abnormal1)
	abnormal2 := CoreInfo{
		FilePath:   "",
		Level:      "INFO",
		Tick:       10,    // Unit: Second
		First:      10,    // Unit: Number of logs
		Thereafter: 5,     // Unit: Number of logs
		Tracing:    false, // tracing log switch
		Disable:    false, // Disable file logger
	}
	abnormalInfo2, _ := json.Marshal(abnormal2)
	abnormal3 := CoreInfo{
		FilePath:   "/home/sn/log",
		Tick:       10,    // Unit: Second
		First:      10,    // Unit: Number of logs
		Thereafter: 5,     // Unit: Number of logs
		Tracing:    false, // tracing log switch
		Disable:    false, // Disable file logger
	}
	abnormalInfo3, _ := json.Marshal(abnormal3)
	type args struct {
		env string
	}
	tests := []struct {
		name        string
		args        args
		want        CoreInfo
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:    "case1",
			args:    args{LogConfigKey},
			want:    defaultCoreInfo,
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return string(normalInfo)
						}),
				})
				return patches
			},
		},
		{
			name:    "case2",
			args:    args{LogConfigKey},
			want:    defaultCoreInfo,
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return string(abnormalInfo1)
						}),
				})
				return patches
			},
		},
		{
			name:    "case3",
			args:    args{LogConfigKey},
			want:    defaultCoreInfo,
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return string(abnormalInfo2)
						}),
				})
				return patches
			},
		},
		{
			name:    "case3",
			args:    args{LogConfigKey},
			want:    defaultCoreInfo,
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return string(abnormalInfo3)
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := ExtractCoreInfoFromEnv(tt.args.env)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractCoreInfoFromEnv() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ExtractCoreInfoFromEnv() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}
