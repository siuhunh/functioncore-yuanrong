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

// Package obs is the obs client
package obs

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/agiledragon/gomonkey"
	"github.com/stretchr/testify/assert"
)

func TestDownloadAllFileToPath(t *testing.T) {
	// 创建临时目录
	tmpDir := t.TempDir()
	downloadPath := filepath.Join(tmpDir, "testfile.txt")
	body := []byte("test content")

	t.Run("test success", func(t *testing.T) {
		// 测试用例1：正常情况
		patches := gomonkey.ApplyFunc(os.MkdirAll, func(string, os.FileMode) error {
			return nil
		})
		patches.ApplyFunc(os.Create, func(string) (*os.File, error) {
			return os.Stdout, nil
		})
		patches.ApplyFunc((*os.File).Write, func(*os.File, []byte) (int, error) {
			return len(body), nil
		})
		defer patches.Reset()

		err := DownloadAllFileToPath(downloadPath, body)
		assert.NoError(t, err)
	})

	t.Run("create dir failed", func(t *testing.T) {
		// 测试用例2：目录创建失败
		patches := gomonkey.ApplyFunc(os.MkdirAll, func(string, os.FileMode) error {
			return fmt.Errorf("failed to make dir")
		})
		defer patches.Reset()
		err := DownloadAllFileToPath(downloadPath, body)
		assert.Error(t, err)
	})

	t.Run("create file failed", func(t *testing.T) {
		// 测试用例3：文件创建失败
		patches := gomonkey.ApplyFunc(os.MkdirAll, func(string, os.FileMode) error {
			return nil
		})
		defer patches.Reset()
		patches.ApplyFunc(os.Create, func(string) (*os.File, error) {
			return nil, fmt.Errorf("failed to create file")
		})
		err := DownloadAllFileToPath(downloadPath, body)
		assert.Error(t, err)
	})

	t.Run("write file failed", func(t *testing.T) {
		// 测试用例4：文件写入失败
		patches := gomonkey.ApplyFunc(os.MkdirAll, func(string, os.FileMode) error {
			return nil
		})
		defer patches.Reset()
		patches.ApplyFunc(os.Create, func(string) (*os.File, error) {
			return os.Stdout, nil
		})
		patches.ApplyFunc((*os.File).Write, func(*os.File, []byte) (int, error) {
			return 0, fmt.Errorf("failed to write file")
		})
		err := DownloadAllFileToPath(downloadPath, body)
		assert.Error(t, err)
	})
}
