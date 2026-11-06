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
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	utilities "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
	mockutils "functioncore/test/utils"
)

func TestZipDecompress(t *testing.T) {
	convey.Convey("Test decompress 1", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(zip.NewReader, func(r io.ReaderAt, size int64) (*zip.Reader, error) {
				zr := new(zip.Reader)
				return zr, errors.New("can not find the file")
			}),
			gomonkey.ApplyFunc(zip.OpenReader, func(name string) (*zip.ReadCloser, error) {
				r := new(zip.ReadCloser)
				return r, errors.New("can not open the reader")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("test decompressBuffer", func() {
			zipHandler := newZipHandler()
			objectSize := int64(readAllLimitSize)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			err := zipHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			fmt.Println(err)
			convey.So(err.Error(), convey.ShouldEqual, "can not find the file")
		})
		convey.Convey("test decompressFile", func() {
			zipHandler := newZipHandler()
			objectSize := int64(readAllLimitSize + 1)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			err := zipHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			fmt.Println(err)
			convey.So(err.Error(), convey.ShouldEqual, "can not open the reader")
		})
	})
	convey.Convey("Test decompress 2", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(zip.NewReader, func(r io.ReaderAt, size int64) (*zip.Reader, error) {
				zr := new(zip.Reader)
				return zr, nil
			}),
			gomonkey.ApplyFunc(zip.OpenReader, func(name string) (*zip.ReadCloser, error) {
				r := new(zip.ReadCloser)
				return r, nil
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return nil, errors.New("can not get the config")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("test decompressBuffer", func() {
			zipHandler := newZipHandler()
			objectSize := int64(readAllLimitSize)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			err := zipHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			fmt.Println(err)
			convey.So(err.Error(), convey.ShouldEqual, "can not get the config")
		})
		convey.Convey("test decompressFile", func() {
			zipHandler := newZipHandler()
			objectSize := int64(readAllLimitSize + 1)
			downloadFilePath := ""
			buffer := make([]byte, 0)
			extractPath := ""
			err := zipHandler.Decompress(objectSize, downloadFilePath, buffer, extractPath, false)
			fmt.Println(err)
			convey.So(err.Error(), convey.ShouldEqual, "can not get the config")
		})
	})
}

func TestValidateFilePath(t *testing.T) {
	convey.Convey("Test validateFilePath", t, func() {
		convey.Convey("test validateFilePath", func() {
			path := "/usr/test/"
			zipfile := "testZipFile"
			depth := 2
			err := validateFilePath(path, zipfile, depth)
			convey.So(err, convey.ShouldEqual, true)
		})
	})
	convey.Convey("Test relativeFilePath", t, func() {
		convey.Convey("test relativeFilePath", func() {
			path := "/usr/test/"
			zipfile := "../testZipFile"
			depth := 2
			err := validateFilePath(path, zipfile, depth)
			convey.So(err, convey.ShouldEqual, false)
		})
		convey.Convey("test relativeFilePath 2", func() {
			path := "/usr/test/"
			zipfile := "abc/../../../testZipFile"
			depth := 10
			err := validateFilePath(path, zipfile, depth)
			convey.So(err, convey.ShouldEqual, false)
		})
		convey.Convey("test relativeFilePath 3", func() {
			path := "/usr/test/"
			zipfile := "abc/def/../testZipFile"
			depth := 10
			err := validateFilePath(path, zipfile, depth)
			convey.So(err, convey.ShouldEqual, true)
		})
	})
}

func TestCheckAndUnzip(t *testing.T) {
	convey.Convey("test check and unzip 1", t, func() {
		targetFilePath := "/usr/test/"
		cfg := &config.Configuration{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return cfg, nil
			}),
			gomonkey.ApplyFunc(utilities.ValidateFilePath, func(path string) error {
				return nil
			}),
			gomonkey.ApplyFunc(utils.ExtractZipFile, func(zf *zip.File, dest string) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		f1 := &zip.File{}
		f1.Name = "testZipFile1"
		f1.UncompressedSize64 = 1
		f2 := &zip.File{}
		f2.Name = "testZipFile2"
		cfg.DeploymentConfig.CodePackage.FileCountsMax = 1
		cfg.DeploymentConfig.CodePackage.UnzipFileSizeMaxMB = 5
		cfg.DeploymentConfig.CodePackage.DirDepthMax = 3

		zipHandler := newZipHandler()

		file := make([]*zip.File, 0)
		file = append(file, f1)
		err := zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err, convey.ShouldEqual, nil)
		file = append(file, f2)
		err = zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err.Error(), convey.ShouldEqual, "amount of files in package is over 1 ,exceeding the limit")

		cfg.DeploymentConfig.CodePackage.FileCountsMax = 2
		f2.UncompressedSize64 = 1
		err = zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err, convey.ShouldEqual, nil)

		f2.UncompressedSize64 = uint64(10) << 20
		err = zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err.Error(), convey.ShouldEqual, "unzip file size is out of range 5")

		targetFilePath = "/usr/test/testFile/"
		f2.UncompressedSize64 = 1
		err = zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err, convey.ShouldEqual, nil)

	})

	convey.Convey("test check and unzip 2", t, func() {
		targetFilePath := "/usr/test/"
		cfgErr := errors.New("get config error")
		cfg := &config.Configuration{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return cfg, cfgErr
			}),
			gomonkey.ApplyFunc(utilities.ValidateFilePath, func(path string) error {
				return nil
			}),
			gomonkey.ApplyFunc(utils.ExtractZipFile, func(zf *zip.File, dest string) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		f1 := &zip.File{}
		f1.Name = "testZipFile3"
		f1.UncompressedSize64 = 1
		f2 := &zip.File{}
		f2.Name = "testZipFile4"
		cfg.DeploymentConfig.CodePackage.FileCountsMax = 1
		cfg.DeploymentConfig.CodePackage.UnzipFileSizeMaxMB = 5
		cfg.DeploymentConfig.CodePackage.DirDepthMax = 3

		zipHandler := newZipHandler()

		file := make([]*zip.File, 0)
		file = append(file, f1)
		err := zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err, convey.ShouldEqual, cfgErr)
	})

	convey.Convey("test check and unzip 2", t, func() {
		targetFilePath := "/usr/test/"
		cfg := &config.Configuration{}
		cfgErr := errors.New("failed to unzip the file")
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
				return cfg, nil
			}),
			gomonkey.ApplyFunc(utilities.ValidateFilePath, func(path string) error {
				return nil
			}),
			gomonkey.ApplyFunc(utils.ExtractZipFile, func(zf *zip.File, dest string) error {
				return cfgErr
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		f1 := &zip.File{}
		f1.Name = "testZipFile5"
		f1.UncompressedSize64 = 1
		f2 := &zip.File{}
		f2.Name = "testZipFile6"
		cfg.DeploymentConfig.CodePackage.FileCountsMax = 2
		cfg.DeploymentConfig.CodePackage.UnzipFileSizeMaxMB = 5
		cfg.DeploymentConfig.CodePackage.DirDepthMax = 3

		zipHandler := newZipHandler()

		file := make([]*zip.File, 0)
		file = append(file, f1)
		err := zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err, convey.ShouldEqual, nil)

		cfg.DeploymentConfig.CodePackage.DirDepthMax = 1
		file = append(file, f2)
		err = zipHandler.checkAndUnzip(file, targetFilePath)
		convey.So(err, convey.ShouldEqual, nil)
	})
}

func TestZipHandler_Decompress(t *testing.T) {
	type fields struct {
		filePathMap map[string]struct{}
	}
	type args struct {
		objectSize       int64
		downloadFilePath string
		buffer           []byte
		extractPath      string
		isLayer          bool
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name:   "test zip format is invalid",
			fields: fields{},
			args: args{
				objectSize:       readAllLimitSize - 1,
				downloadFilePath: "mockDownloadFilePath",
				buffer:           []byte("mockBuffer"),
				extractPath:      "mockExtractPath",
				isLayer:          false,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(zip.NewReader, func(r io.ReaderAt, size int64) (*zip.Reader, error) {
						return nil, zip.ErrFormat
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
		{
			name:   "test zip file format is invalid",
			fields: fields{},
			args: args{
				objectSize:       readAllLimitSize + 1,
				downloadFilePath: "mockDownloadFilePath",
				buffer:           []byte("mockBuffer"),
				extractPath:      "mockExtractPath",
				isLayer:          true,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(zip.OpenReader, func(name string) (*zip.ReadCloser, error) {
						return nil, zip.ErrFormat
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := &ZipHandler{
				filePathMap: tt.fields.filePathMap,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.wantErr(t, z.Decompress(tt.args.objectSize, tt.args.downloadFilePath, tt.args.buffer, tt.args.extractPath, tt.args.isLayer), fmt.Sprintf("Decompress(%v, %v, %v, %v, %v)", tt.args.objectSize, tt.args.downloadFilePath, tt.args.buffer, tt.args.extractPath, tt.args.isLayer))
		})
	}
}
