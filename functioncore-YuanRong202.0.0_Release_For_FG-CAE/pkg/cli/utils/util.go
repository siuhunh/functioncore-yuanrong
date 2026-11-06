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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"golang.org/x/text/encoding/simplifiedchinese"
)

// MkdirFileMode 创建目录权限 只给所有者读写执行权限，必须有执行权限否则无法进入目录
const MkdirFileMode = 0700

// UniqueID -
func UniqueID() string {
	return uuid.New().String()
}

// Exists exists Whether the path exists
func Exists(path string) bool {
	if _, err := filepath.Abs(path); err != nil {
		return false
	}

	if _, err := os.Stat(path); err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}

	return true
}

// RawFormat -
func RawFormat(raw string) string {
	if len(raw) == 0 {
		return ""
	}

	// Remove the first line \n
	if raw[0] == '\n' {
		raw = raw[1:]
	}
	// split raw by \n
	lines := strings.Split(raw, "\n")

	// convert \t at the beginning of each line to two space indentation
	for i, line := range lines {
		if strings.HasPrefix(line, "\t") {
			lines[i] = strings.Replace(line, "\t", "  ", 1)
		}
		if strings.HasPrefix(line, "    ") {
			lines[i] = strings.Replace(line, "    ", "  ", 1)
		}
	}

	return strings.Join(lines, "\n")
}

// ConvertToUTF8 converts the given encoded bytes to UTF-8
func ConvertToUTF8(bytes []byte) ([]byte, error) {
	if utf8.Valid(bytes) {
		return bytes, nil
	}

	if isGBK(bytes) {
		// gbk -> utf-8
		decodeByte, err := simplifiedchinese.GBK.NewDecoder().Bytes(bytes)
		if err != nil {
			return nil, err
		}
		return decodeByte, nil
	}

	return nil, errors.New("only supports automatic conversion of gbk encoding to utf-8 encoding")
}

func isGBK(data []byte) bool {
	length := len(data)
	doubleByte := 2
	for i := 0; i < length; {
		if data[i] <= 0x7f {
			// 编码0~127,只有一个字节的编码，兼容ASCII码
			i++
			continue
		}
		// 大于127的使用双字节编码，落在gbk编码范围内的字符
		if i < length-1 {
			if data[i] >= 0x81 && data[i] <= 0xfe &&
				data[i+1] >= 0x40 && data[i+1] <= 0xfe &&
				data[i+1] != 0xf7 {
				i += doubleByte
				continue
			}
			return false
		}
	}
	return true
}

// Zip Traverse files and directories in the src directory and compress them to the dst file.
func Zip(dst, src string) error {
	fz, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, MkdirFileMode)
	if err != nil {
		return err
	}
	defer fz.Close()

	w := zip.NewWriter(fz)
	defer w.Close()

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if path == src {
			return nil
		}
		if info.IsDir() {
			if err := zipDir(path, src, w); err != nil {
				return err
			}
		} else {
			if err := zipFile(path, src, w); err != nil {
				return err
			}
		}
		return nil
	})
}

// ZipOneFile -
func ZipOneFile(dst, src string) error {
	fz, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, MkdirFileMode)
	if err != nil {
		return err
	}
	defer fz.Close()

	w := zip.NewWriter(fz)
	defer w.Close()

	destPath := path.Base(src)
	fDest, err := w.Create(destPath)
	if err != nil {
		return err
	}
	fSrc, err := os.Open(src)
	if err != nil {
		return err
	}
	defer fSrc.Close()
	_, err = io.Copy(fDest, fSrc)
	if err != nil {
		return err
	}
	return nil
}

func zipFile(path, src string, w *zip.Writer) error {
	destPath := filepath.ToSlash(path[len(src)+1:])
	fDest, err := w.Create(destPath)
	if err != nil {
		return err
	}
	fSrc, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fSrc.Close()
	_, err = io.Copy(fDest, fSrc)
	if err != nil {
		return err
	}
	return nil
}

func zipDir(path, src string, w *zip.Writer) error {
	dirPath := path[len(src)+1:]
	if !strings.HasSuffix(dirPath, string(filepath.Separator)) {
		dirPath += string(filepath.Separator)
	}
	dirPath = filepath.ToSlash(dirPath)
	_, err := w.Create(dirPath)
	if err != nil {
		return err
	}
	return nil
}

// DefaultLinuxResourcePath the default resource directory in Linux
const DefaultLinuxResourcePath = "~/.functioncore/pkg/cli/pkg/cli/resource/"

// GetResourcePath obtain the resource directory
func GetResourcePath() (string, error) {
	var err error
	resourcePath := os.Getenv("ResourcePath")
	if resourcePath == "" {
		// 获取可执行文件所在目录下的resource的路径
		resourcePath, err = filepath.Abs(filepath.Dir(os.Args[0]))
		if err != nil {
			return "", errors.New("invalid resource path")
		}
		resourcePath = strings.Replace(resourcePath, "\\", "/", -1)
		resourcePath = path.Join(path.Dir(resourcePath), "resource")
	} else {
		resourcePath = strings.Replace(resourcePath, "\\", "/", -1)
	}

	// 如果目录不存在,设置成默认目录
	if !Exists(resourcePath) && runtime.GOOS == "linux" {
		resourcePath = DefaultLinuxResourcePath
	}
	// 如果目录还是不存在则返回错误
	if !Exists(resourcePath) {
		return "", errors.New("resource path not exists")
	}

	return resourcePath, nil
}

// CopyTree 复制src目录下所有文件内容到dst目录下
func CopyTree(src, dst string) error {
	if !Exists(dst) {
		return fmt.Errorf("the dst directory does not exist")
	}
	if !Exists(src) {
		return fmt.Errorf("the src directory does not exist")
	}
	src = filepath.Clean(src)
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if path == src {
			return nil
		}
		relativePath := strings.Replace(path, src, "", 1)
		if strings.HasPrefix(relativePath, "\\") {
			relativePath = strings.TrimPrefix(relativePath, "\\")
		}
		if info.IsDir() {
			dstPath := filepath.Join(dst, relativePath)
			if err := os.Mkdir(dstPath, MkdirFileMode); err != nil {
				return err
			}
		} else {
			dstPath := filepath.Join(dst, relativePath)
			err := CopyFile(path, dstPath)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// CopyFile copy srcFile to destFile
func CopyFile(srcFileName string, destFileName string) error {
	srcFile, err := os.Open(srcFileName)

	if err != nil {
		log.Println("Invalid File")
		return err
	}

	defer srcFile.Close()

	// 文件为代码文件，需要有可执行权限
	dstFile, err := os.OpenFile(destFileName, os.O_WRONLY|os.O_CREATE, MkdirFileMode)
	if err != nil {
		log.Println("Invalid File")
		return err
	}

	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}
