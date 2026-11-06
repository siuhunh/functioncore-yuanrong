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

// Package deploy
package deploy

import (
	"errors"
	"os/exec"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/utils"
)

func TestZstdDecompress(t *testing.T) {
	convey.Convey("test decompress", t, func() {
		convey.Convey("test decompressFile 1", func() {
			patches := [...]*gomonkey.Patches{
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
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			objectSize := int64(readAllLimitSize + 1)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			zstdHandler := newZstdHandler()
			zstdHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
		})
		convey.Convey("test decompressFile 2", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return errors.New("run cmd error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			objectSize := int64(readAllLimitSize + 1)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			zstdHandler := newZstdHandler()
			err := zstdHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			convey.So(err.Error(), convey.ShouldEqual, "run cmd error")
		})
	})
	convey.Convey("test decompressBuffer", t, func() {
		convey.Convey("test decompressBuffer 1", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.WriteFileToPath, func(writePath string, buffer []byte) error {
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
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			objectSize := int64(readAllLimitSize)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			zstdHandler := newZstdHandler()
			err := zstdHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test decompressBuffer 2", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.WriteFileToPath, func(writePath string, buffer []byte) error {
					return errors.New("write file to path err")
				}),
				gomonkey.ApplyFunc(exec.Command, func(name string, arg ...string) *exec.Cmd {
					cmd := &exec.Cmd{
						Path: name,
						Args: append([]string{name}, arg...),
					}
					return cmd
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Run", func(_ *exec.Cmd) error {
					return errors.New("run cmd error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			objectSize := int64(readAllLimitSize)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			zstdHandler := newZstdHandler()
			err := zstdHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			convey.So(err.Error(), convey.ShouldEqual, "write file to path err")
		})
		convey.Convey("test decompressBuffer 3", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(utils.WriteFileToPath, func(writePath string, buffer []byte) error {
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
					return errors.New("run cmd error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			objectSize := int64(readAllLimitSize)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			zstdHandler := newZstdHandler()
			err := zstdHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			convey.So(err.Error(), convey.ShouldEqual, "run cmd error")
		})
	})
}
