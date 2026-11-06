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

// Package verify is verification of function params accuracy
package verify

import (
	"archive/zip"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"functioncore/pkg/cli/utils"
)

const (
	// file path max length 4096
	filePathMaxLength  = 4096
	readFileHeaderSize = 4
	// The maximum number of files in a zip file is 30000
	zipMaxFileNums = 30000
	mb             = 20
	// The maximum size of the decompressed zip file is 400 MB.
	uncompressedMaxSize = 600 << mb
	// The maximum number of directories in a zip file.
	maxFileDepths = 20
	// The maximum size of a zip file is 200 MB.
	defaultZIPMaxSize = 300 << mb
)

// CheckFilePath check file path
func CheckFilePath(path string) error {
	if path == "" {
		return errors.New("file path is empty, please enter path")
	}
	if len(path) > filePathMaxLength {
		return errors.New("file path length is too long")
	}
	if !utils.Exists(path) {
		return errors.New("file path doesn't exist")
	}
	return nil
}

// CheckZIPFile check zip file
func CheckZIPFile(filePath string) error {
	if err := CheckFilePath(filePath); err != nil {
		return err
	}

	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return err
	}
	if err := CheckFile(fileInfo); err != nil {
		return err
	}
	if err := zipLimit(filePath); err != nil {

		return err
	}

	return nil
}

// CheckFile check file
func CheckFile(fileInfo os.FileInfo) error {
	if !fileInfo.Mode().IsRegular() {
		return errors.New("file is not a regular file")
	}
	if fileInfo.Size() == 0 {
		return errors.New("file is empty")
	}
	return nil
}

func zipLimit(zipPath string) error {
	var totalWrote uint64

	fileInfo, err := os.Stat(zipPath)
	if err != nil {
		return err
	}
	if fileInfo.Size() > defaultZIPMaxSize {
		return fmt.Errorf("the maximum size of the ZIP archive is %d MB", defaultZIPMaxSize>>mb)
	}

	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer reader.Close()
	if len(reader.File) > zipMaxFileNums {
		return fmt.Errorf("the maximum number of files in the zip archive is %d", zipMaxFileNums)
	}

	for _, file := range reader.File {
		dirPath := file.Name
		depths := strings.Split(dirPath, string(filepath.Separator))
		if len(depths) > maxFileDepths {
			return fmt.Errorf("the maximum depth of the ZIP archive directory is %d", maxFileDepths)
		}

		if file.FileInfo().IsDir() {
			continue
		}
		totalWrote += file.UncompressedSize64
		if totalWrote > uncompressedMaxSize {
			return fmt.Errorf("the maximum size of the ZIP archive after decompression is %d MB",
				uncompressedMaxSize>>mb)
		}
	}
	return nil
}
