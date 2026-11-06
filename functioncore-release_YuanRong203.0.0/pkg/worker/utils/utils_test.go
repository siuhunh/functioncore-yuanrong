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
	"archive/zip"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	. "reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
)

func Test_ExtractZipFile(t *testing.T) {
	convey.Convey("ExtractZipFile", t, func() {
		convey.Convey("case1: success", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyFunc(generateZipFile, func(rc io.ReadCloser, fPath string) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldBeNil)
		})
		convey.Convey("case2: ioutil.ReadAll error", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return nil, errors.New("readAll error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldNotBeNil)
			convey.So(res.Error(), convey.ShouldEqual, "read file content error: readAll error")
		})
		convey.Convey("case3: zf.Open error", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						return nil, errors.New("open error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldNotBeNil)
			convey.So(res.Error(), convey.ShouldEqual, "open zip file error: open error")
		})
		convey.Convey("case4: generateZipFile error, os.IsNotExist is false", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyFunc(generateZipFile, func(rc io.ReadCloser, fPath string) error {
					return errors.New("generateZipFile error")
				}),
				gomonkey.ApplyFunc(os.IsNotExist, func(err error) bool {
					return false
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldNotBeNil)
			convey.So(res.Error(), convey.ShouldEqual, "generate zip file error: generateZipFile error")
		})
		convey.Convey("case5: generateZipFile error, os.IsNotExist is true, os.MkdirAll error", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyFunc(generateZipFile, func(rc io.ReadCloser, fPath string) error {
					return errors.New("generateZipFile error")
				}),
				gomonkey.ApplyFunc(os.IsNotExist, func(err error) bool {
					return true
				}),
				gomonkey.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return errors.New("mkdirAll error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldNotBeNil)
			convey.So(res.Error(), convey.ShouldEqual, "make dir for dest file error: mkdirAll error")
		})
		convey.Convey("case6: generateZipFile error twice, os.IsNotExist is true", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyFunc(generateZipFile, func(rc io.ReadCloser, fPath string) error {
					return errors.New("generateZipFile error")
				}),
				gomonkey.ApplyFunc(os.IsNotExist, func(err error) bool {
					return true
				}),
				gomonkey.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldNotBeNil)
			convey.So(res.Error(), convey.ShouldEqual, "generate zip file error: generateZipFile error")
		})
		convey.Convey("case7: generateZipFile error once, os.IsNotExist is true", func() {
			zFile := &zip.File{}
			count := 0
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyFunc(generateZipFile, func(rc io.ReadCloser, fPath string) error {
					count++
					if count == 1 {
						return errors.New("generateZipFile error")
					}
					return nil
				}),
				gomonkey.ApplyFunc(os.IsNotExist, func(err error) bool {
					return true
				}),
				gomonkey.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldBeNil)
		})
		convey.Convey("case8: zf.FileInfo().IsDir is true", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyMethod(TypeOf(zFile.FileInfo()), "IsDir", func() bool {
					return true
				}),
				gomonkey.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldBeNil)
		})
		convey.Convey("case9: zf.FileInfo().IsDir is true, os.Mkdir error", func() {
			zFile := &zip.File{}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return []byte("file"), nil
				}),
				gomonkey.ApplyMethod(TypeOf(zFile), "Open",
					func(_ *zip.File) (io.ReadCloser, error) {
						readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
						defer readCloser.Close()
						return readCloser, nil
					}),
				gomonkey.ApplyMethod(TypeOf(zFile.FileInfo()), "IsDir", func() bool {
					return true
				}),
				gomonkey.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error {
					return errors.New("mkdir error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			res := ExtractZipFile(zFile, "dest")
			convey.So(res, convey.ShouldNotBeNil)
			convey.So(res.Error(), convey.ShouldEqual, "make dir all error: mkdir error")
		})
	})
}

func TestIsMultiRt(t *testing.T) {
	_ = os.Setenv("MULTI_RUNTIME_MODE", "1")
	if !IsMultiRt() {
		t.Errorf("should be multi-runtime mode")
	}
	_ = os.Unsetenv("MULTI_RUNTIME_MODE")
	if IsMultiRt() {
		t.Errorf("should not be multi-runtime mode")
	}
}

func TestGenRuntimeID(t *testing.T) {
	runtimeID, err := GenRuntimeID()
	if err != nil {
		t.Errorf("generate runtime ID error: %s", err.Error())
	}
	fmt.Println("runtimeID", runtimeID)
	if len(runtimeID) != runtimeIDLength {
		t.Errorf("runtime ID length error, expect %d, actual %d", len(runtimeID), runtimeIDLength)
	}
}

func Test_BusID2InvokeID(t *testing.T) {
	id := BusID2InvokeID(10)
	assert.Equal(t, "10", id)
}

func Test_InvokeID2BusID(t *testing.T) {
	id := InvokeID2BusID("10")
	assert.Equal(t, uint64(10), id)
}

func Test_InvokeID2BusID_initializer(t *testing.T) {
	id := InvokeID2BusID("initializer")
	assert.Equal(t, uint64(0), id)
}

func Test_GetResourceLimitsMemory(t *testing.T) {
	GetResourceLimitsMemory()

	patch := gomonkey.ApplyFunc(ioutil.ReadFile, func(string) ([]byte, error) {
		return []byte("60"),
			fmt.Errorf("get limit error")
	})
	defer patch.Reset()
	GetResourceLimitsMemory()

	patch = gomonkey.ApplyFunc(ioutil.ReadFile, func(string) ([]byte, error) { return []byte("60"), nil })
	defer patch.Reset()
	GetResourceLimitsMemory()

	patch = gomonkey.ApplyFunc(strconv.ParseInt, func(s string, base int, bitSize int) (i int64, err error) {
		return 0,
			fmt.Errorf("get limit error")
	})
	defer patch.Reset()
	GetResourceLimitsMemory()
}

func Test_GetCPUMemory(t *testing.T) {
	_ = os.Setenv("PodName", "pool6-4000-16000-java1.8-67f66bbdbc-l7m2h")
	cpu, memory := GetCPUMemory()
	_ = os.Unsetenv("PodName")
	assert.Equal(t, int64(4000), cpu)
	assert.Equal(t, int64(16000), memory)

	resourceMetaData.CPU = 0
	resourceMetaData.Mem = 0
	_ = os.Unsetenv("PodName")
	_ = os.Setenv("PodName", "pool6-0x300-0x400-java1.8-67f66bbdbc-l7m2h")
	cpu, memory = GetCPUMemory()
	_ = os.Unsetenv("PodName")
	assert.Equal(t, int64(0), cpu)
	assert.Equal(t, int64(0), memory)
}

func Test_RestoreCPULimit(t *testing.T) {

	convey.Convey("Test RestoreCPULimit success", t, func() {
		patch := gomonkey.ApplyFunc(ioutil.WriteFile,
			func(filename string, data []byte, perm fs.FileMode) error {
				return nil
			})
		defer patch.Reset()
		_ = os.Setenv("PodName", "pool6-128-300-jdsdac-l7m2h")
		err := RestoreCPULimit()
		_ = os.Unsetenv("PodName")
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("Test RestoreCPULimit success", t, func() {
		patch := gomonkey.ApplyFunc(ioutil.WriteFile,
			func(filename string, data []byte, perm fs.FileMode) error {
				return errors.New("dasda")
			})
		defer patch.Reset()
		_ = os.Setenv("PodName", "pool6-128-300-jdsdac-l7m2h")
		err := RestoreCPULimit()
		_ = os.Unsetenv("PodName")
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("Test RestoreCPULimit skip", t, func() {
		patch := gomonkey.ApplyFunc(IsVpaFeatureEnabled, func() bool {
			return true
		})
		patches := gomonkey.ApplyFunc(utils.IsCAEFunc, func(businessType string) bool {
			return true
		})
		defer patch.Reset()
		defer patches.Reset()
		err := RestoreCPULimit()
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestRemoveGCLog(t *testing.T) {
	RemoveGCLog()

	patch := gomonkey.ApplyFunc(filepath.Glob, func(string) ([]string, error) {
		return nil,
			fmt.Errorf("remove GC log error")
	})
	RemoveGCLog()
	patch.Reset()

	patch = gomonkey.ApplyFunc(filepath.Glob, func(string) ([]string, error) {
		return []string{"apple", "banana", "orange"}, nil
	})
	patch = gomonkey.ApplyFunc(os.Remove, func(string) error {
		return fmt.Errorf("remove GC log error")
	})
	RemoveGCLog()
	patch.Reset()

	patch = gomonkey.ApplyFunc(filepath.Glob, func(string) ([]string, error) {
		return []string{"apple", "banana", "orange"}, nil
	})
	patch = gomonkey.ApplyFunc(os.Remove, func(string) error {
		return nil
	})
	RemoveGCLog()
	patch.Reset()
}

func Test_IsRuntimeEnable(t *testing.T) {
	cfg := &config.RuntimeConfig{Enable: false}
	assert.Equal(t, false, IsRuntimeEnable(cfg))

	cfg = &config.RuntimeConfig{
		Enable:                true,
		ExcludeSpecifications: []int64{16000},
	}
	resourceMetaData.CPU = 0
	resourceMetaData.Mem = 0
	_ = os.Setenv("PodName", "pool6-4000-16000-java1.8-67f66bbdbc-l7m2h")
	flag := IsRuntimeEnable(cfg)
	_ = os.Unsetenv("PodName")
	assert.Equal(t, false, flag)

	cfg = &config.RuntimeConfig{
		Enable:                true,
		ExcludeSpecifications: []int64{1600},
	}

	resourceMetaData.CPU = 0
	resourceMetaData.Mem = 0
	_ = os.Setenv("PodName", "pool6-4000-16000-java1.8-67f66bbdbc-l7m2h")
	flag = IsRuntimeEnable(cfg)
	_ = os.Unsetenv("PodName")
	assert.Equal(t, true, flag)
}

func Test_GenRandID(t *testing.T) {
	_, err := genRandID(5)
	assert.Equal(t, nil, err)

	result, _ := genRandID(-1)
	assert.Equal(t, "", result)

	patch := gomonkey.ApplyFunc(rand.Read, func([]byte) (n int, err error) { return 3, nil })
	defer patch.Reset()
	_, err = genRandID(6)
	assert.Equal(t, "generate random ID length 3, expect 6", err.Error())

	patch = gomonkey.ApplyFunc(rand.Read, func([]byte) (n int, err error) { return 3, fmt.Errorf("error") })
	defer patch.Reset()
	_, err = genRandID(6)
	assert.Equal(t, "error", err.Error())
}

func Test_RemoveDownloadPath(t *testing.T) {
	err := RemoveDownloadPath("", "")
	assert.Equal(t, nil, err)

	err = RemoveDownloadPath("/test/", "test")
	assert.Equal(t, nil, err)

	patch := gomonkey.ApplyFunc(os.RemoveAll, func(path string) error {
		return errors.New("dsad")
	})
	defer patch.Reset()
	err = RemoveDownloadPath("/test/", "test")
	assert.Equal(t, err, err)
}

func Test_GenerateZipFile(t *testing.T) {
	readCloser := io.NopCloser(io.LimitReader(strings.NewReader("xx"), 0))
	defer readCloser.Close()
	err := generateZipFile(readCloser, "/testworker")
	assert.Equal(t, nil, err)

	patch := gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errors.New("aaa")
	})
	defer patch.Reset()
	err = generateZipFile(readCloser, "/test")
	assert.Equal(t, "aaa", err.Error())

	patch = gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, nil
	})
	defer patch.Reset()
	err = generateZipFile(readCloser, "/test")
	assert.Equal(t, "copy zip file error: invalid argument", err.Error())
}

func Test_GetPingHash(t *testing.T) {
	convey.Convey("GetHash ", t, func() {
		convey.Convey("failed to open file", func() {
			value := GetHash("")
			convey.So(value, convey.ShouldEqual, "")
		})

		convey.Convey("failed to copy", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}),
				gomonkey.ApplyFunc((*os.File).Close, func(c *os.File) error {
					return nil
				}),
				gomonkey.ApplyFunc(io.Copy, func(dst io.Writer, src io.Reader) (written int64, err error) {
					return 0, errors.New("test")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			value := GetHash("123")
			convey.So(value, convey.ShouldEqual, "")
		})

		convey.Convey("test change vpc success", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}),
				gomonkey.ApplyFunc((*os.File).Close, func(c *os.File) error {
					return nil
				}),
				gomonkey.ApplyFunc(io.Copy, func(dst io.Writer, src io.Reader) (written int64, err error) {
					return 0, nil
				}),
				gomonkey.ApplyFunc(hex.EncodeToString, func(src []byte) string {
					return "123"
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			value := GetHash("123")
			convey.So(value, convey.ShouldEqual, "123")
		})
	})
}

func TestIsM2SMode(t *testing.T) {
	convey.Convey("IsM2SMode Test ", t, func() {
		convey.Convey("IsM2SMode true ", func() {
			res := IsM2SMode(constants.RunningModeCheckpoint, constants.M2SFunctionPrefix)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Convey("IsM2SMode false", func() {
			res := IsM2SMode(constants.RunningModeNormal, "handler")
			convey.So(res, convey.ShouldBeFalse)
		})
	})
}

func TestIsM2SHeartbeatEnable(t *testing.T) {
	convey.Convey("IsM2SHeartbeatEnable Test", t, func() {
		m2sHeartbeatHandler := "com.huawei.yuanrong.m2s.function.Handler.handleHealthCheck"
		heartbeatHandler := "com.huawei.it.Handler.handleHealthCheck"
		convey.Convey("IsM2SHeartbeatEnable true", func() {
			actual := IsM2SHeartbeatEnable("java8", true, m2sHeartbeatHandler)
			convey.So(actual, convey.ShouldBeTrue)
		})
		convey.Convey("IsM2SHeartbeatEnable unsupported language", func() {
			actual := IsM2SHeartbeatEnable("python", true, m2sHeartbeatHandler)
			convey.So(actual, convey.ShouldBeFalse)
		})
		convey.Convey("IsM2SHeartbeatEnable java8 language with heartbeat unable", func() {
			actual := IsM2SHeartbeatEnable("java8", false, m2sHeartbeatHandler)
			convey.So(actual, convey.ShouldBeFalse)
		})
		convey.Convey("IsM2SHeartbeatEnable java8 language with heartbeat enable but not m2s handler", func() {
			actual := IsM2SHeartbeatEnable("java8", true, heartbeatHandler)
			convey.So(actual, convey.ShouldBeFalse)
		})
	})
}

func TestIsHealthCheckEnable(t *testing.T) {
	convey.Convey("TestIsHealthCheckEnable return true", t, func() {
		ok := IsHealthCheckEnable(types.Heartbeat{
			HealthEnable: true,
			Handler:      "com.huawei.yuanrong.m2s",
		})
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("TestIsHealthCheckEnable return false1", t, func() {
		ok := IsHealthCheckEnable(types.Heartbeat{
			HealthEnable: false,
			Handler:      "com.huawei.yuanrong.m2s",
		})
		convey.So(ok, convey.ShouldBeFalse)
	})
	convey.Convey("TestIsHealthCheckEnable return false2", t, func() {
		ok := IsHealthCheckEnable(types.Heartbeat{
			HealthEnable: true,
			Handler:      "c1om.huawei.yuanrong.m2s",
		})
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestGetUserAgency(t *testing.T) {
	SetUserAgency("test", "testValue")
	assert.Equal(t, "testValue", GetUserAgency("test"))
	assert.Equal(t, "", GetUserAgency("notExists"))
}

func TestNeedCustomContainerPreLoad(t *testing.T) {
	cleanEnv := func() {
		os.Unsetenv(constants.NATConfigures)
		os.Unsetenv(constants.EndpointVpcNATConfigure)
		os.Unsetenv(constants.FuncMountConfig)
	}
	tests := []struct {
		name        string
		patchesFunc func()
		want        bool
	}{
		{
			name: "case1",
			patchesFunc: func() {
				os.Setenv(constants.NATConfigures, "mock_NATConfigures")
				return
			},
			want: true,
		},
		{
			name: "case2",
			patchesFunc: func() {
				os.Setenv(constants.EndpointVpcNATConfigure, "mock_EndpointVpcNATConfigure")
				return
			},
			want: true,
		},
		{
			name: "case3",
			patchesFunc: func() {
				os.Setenv(constants.FuncMountConfig, "mock_FuncMountConfig")
				return
			},
			want: true,
		},
		{
			name: "case4",
			patchesFunc: func() {
				return
			},
			want: false,
		},
		{
			name: "case5",
			patchesFunc: func() {
				os.Setenv(constants.BusinessType, "CAE")
				return
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.patchesFunc()
			assert.Equalf(t, tt.want, NeedCustomContainerPreLoad(), "NeedCustomContainerPreLoad()")
			cleanEnv()
		})
	}
}

func TestGetBusinessType(t *testing.T) {
	assert.Equal(t, "", GetBusinessType())
}

func TestIsDirectFunc(t *testing.T) {
	assert.True(t, IsDirectFunc("javax", false))
	assert.True(t, IsDirectFunc("custom-runtime-rpc", true))
}

func TestIsVpaFeatureEnabled(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"case1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsVpaFeatureEnabled(), "IsVpaFeatureEnabled()")
		})
	}
}

func TestSetVpaResource(t *testing.T) {
	type args struct {
		cpu int64
		mem int64
	}
	var a args
	a.cpu = 300
	a.mem = 128
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetVpaResource(tt.args.cpu, tt.args.mem)
		})
	}
}

func TestEnableLogPath(t *testing.T) {
	convey.Convey("test EnableLogPath", t, func() {
		convey.Convey("case1: true", func() {
			patch := gomonkey.ApplyGlobalVar(&enableLogPath, true)
			defer patch.Reset()
			res := EnableLogPath()
			convey.So(res, convey.ShouldBeTrue)
		})
		convey.Convey("case2: false", func() {
			patch := gomonkey.ApplyGlobalVar(&enableLogPath, false)
			defer patch.Reset()
			res := EnableLogPath()
			convey.So(res, convey.ShouldBeFalse)
		})
	})
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
	DisableOOM()
	assert.Equal(t, actualFileName, expectFileName)
	assert.Equal(t, actualData, expectData)
}

func TestResetOOMScoreAdjToDefault(t *testing.T) {
	expectFileName := "/proc/self/oom_score_adj"
	expectData := []byte("997")
	actualFileName := ""
	actualData := []byte("1")
	patch := gomonkey.ApplyFunc(ioutil.WriteFile,
		func(filename string, data []byte, perm fs.FileMode) error {
			actualFileName = filename
			actualData = data
			return nil
		})
	defer patch.Reset()
	ResetOOMScoreAdjToDefault()
	assert.Equal(t, actualFileName, expectFileName)
	assert.Equal(t, actualData, expectData)
}

func TestGetExtendedInfo(t *testing.T) {
	convey.Convey("test GetExtendedInfo", t, func() {
		convey.Convey("case1: success", func() {
			funcMounts := []types.FuncMount{{
				MountType:    sfs30,
				ExtendedInfo: "127.0.0.1/8",
			}, {
				MountType:    "aaa",
				ExtendedInfo: "127.0.0.2/8",
			}}
			extendedInfo := GetExtendedInfo(funcMounts)
			convey.So(extendedInfo, convey.ShouldEqual, "127.0.0.1/8")
		})
		convey.Convey("case2: mountType is not sfs3.0", func() {
			funcMounts := []types.FuncMount{{
				MountType:    "aaa",
				ExtendedInfo: "127.0.0.2/8",
			}}
			extendedInfo := GetExtendedInfo(funcMounts)
			convey.So(extendedInfo, convey.ShouldEqual, "")
		})
		convey.Convey("case3: mountType is sfs3.0, but extendedInfo is empty", func() {
			funcMounts := []types.FuncMount{{
				MountType:    sfs30,
				ExtendedInfo: "",
			}}
			extendedInfo := GetExtendedInfo(funcMounts)
			convey.So(extendedInfo, convey.ShouldEqual, "")
		})
	})
}
