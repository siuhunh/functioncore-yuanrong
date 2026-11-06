/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	mockUtils "functioncore/test/utils"
)

func TestSnapshotZstdDecompress(t *testing.T) {
	tests := []struct {
		name         string
		downloadFile string
		objectSize   int64
		extract      string
		buffer       []byte
		patchesFunc  mockUtils.PatchesFunc
		wantErr      error
	}{
		{
			name:         "test decompress zstd file path failed",
			downloadFile: "abc",
			objectSize:   1,
			extract:      "/def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: errors.New("invalid file path, expect to be configured as an absolute path"),
		},
		{
			name:         "test decompress zstd source file path failed",
			downloadFile: "/abc",
			objectSize:   1,
			extract:      "def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: errors.New("invalid file path, expect to be configured as an absolute path"),
		},
		{
			name:         "test decompress zstd by chmod dir failed",
			downloadFile: "/abc",
			objectSize:   1,
			extract:      "/def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return errors.New("chmod dir failed")
					}),
				})
				return patches
			},
			wantErr: errors.New("chmod dir failed"),
		},
		{
			name:         "test decompress zstd write file failed",
			downloadFile: "/abc",
			objectSize:   1,
			extract:      "/def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(os.WriteFile, func(name string, data []byte, perm os.FileMode) error {
						return errors.New("write file failed")
					}),
				})
				return patches
			},
			wantErr: errors.New("write file failed"),
		},
		{
			name:         "test decompress zstd write file success",
			downloadFile: "/abc",
			objectSize:   1,
			extract:      "/def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(os.WriteFile, func(name string, data []byte, perm os.FileMode) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: nil,
		},
		{
			name:         "test decompress zstd rename file failed",
			downloadFile: "/abc",
			objectSize:   1 + readAllLimitSize,
			extract:      "/def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(os.Rename, func(oldpath, newpath string) error {
						return errors.New("rename failed")
					}),
				})
				return patches
			},
			wantErr: errors.New("rename failed"),
		},
		{
			name:         "test decompress zstd rename file success",
			downloadFile: "/abc",
			objectSize:   1 + readAllLimitSize,
			extract:      "/def",
			buffer:       []byte("abc"),
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(os.Rename, func(oldpath, newpath string) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handler := newSnapshotZstdHandler()
			err := handler.Decompress(tt.objectSize, tt.downloadFile, tt.buffer, tt.extract, false)
			assert.Equal(t, err, tt.wantErr)
			patches.ResetAll()
		})
	}
}

func TestSnapshotZstdCompress(t *testing.T) {
	convey.Convey("Test compress by zstd", t, func() {
		convey.Convey("test SnapshotZstdCompress succeed", func() {
			handler := newSnapshotZstdHandler()
			err := handler.Compress("/abc", "/def")
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("test SnapshotZstdCompress zstd file path failed", func() {
			handler := newSnapshotZstdHandler()
			err := handler.Compress("abcd", "deffdfd")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test SnapshotZstdCompress zstd source file path failed", func() {
			handler := newSnapshotZstdHandler()
			err := handler.Compress("/abc", "def")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}
