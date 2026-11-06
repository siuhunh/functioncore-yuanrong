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

package deploy

import (
	"errors"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

func TestSanpshotZipDecompress(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{}, nil
	})
	patches.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), nil })
	patches.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil })
	patches.ApplyFunc(os.Remove, func(name string) error { return nil })
	patches.ApplyFunc(utils.RemoveDownloadPath, func(downloadFilePath, extractPath string) error { return nil })
	defer patches.Reset()

	handler := newSnapshotZipHandler()
	err := handler.Decompress(1, "/abc", []byte("abc"), "/def", false)
	assert.NoError(t, err)

	err = handler.Decompress(readAllLimitSize+1, "/abc", []byte("abc"), "/def", false)
	assert.NoError(t, err)
}

func TestSanpshotZipCompress(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{}, nil
	})
	patches.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), nil })
	patches.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil })
	defer patches.Reset()

	handler := newSnapshotZipHandler()
	err := handler.Compress("/abc", "/def")
	assert.NoError(t, err)
}

func TestGetSnapShotPackKey(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{}, nil
	})
	patches.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), errors.New("error") })
	patches.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil })
	defer patches.Reset()
	handler := newSnapshotZipHandler()
	_, err := handler.getSnapshotPackKey()
	assert.Error(t, err)

}

func TestSanpshotZipCompress2(t *testing.T) {
	convey.Convey("Test decompress 1", t, func() {
		convey.Convey("test SanpshotZipCompress succeed", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{}, nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), nil }),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil }),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			handler := newSnapshotZipHandler()
			err := handler.Compress("/abc", "/def")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("test SanpshotZipCompress failed path", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{}, nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), nil }),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error { return nil }),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			handler := newSnapshotZipHandler()
			err := handler.Compress("abcfdf", "deffdfd")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test SanpshotZipCompress failed command", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{}, nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return []byte("abc"), nil }),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(*exec.Cmd) error {
					return errors.New("")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			handler := newSnapshotZipHandler()
			err := handler.Compress("/abc", "/def")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
