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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

// ZipHandler -
type ZipHandler struct {
	filePathMap map[string]struct{}
}

func newZipHandler() *ZipHandler {
	return &ZipHandler{
		filePathMap: make(map[string]struct{}),
	}
}

// Decompress implements Decompressor.Decompress
func (z *ZipHandler) Decompress(objectSize int64, downloadFilePath string, buffer []byte,
	extractPath string, isLayer bool) error {
	log.GetLogger().Debugf("start to decompress by zip, downloadFilePath: %s, objectSize: %d, isLayer: %p",
		downloadFilePath, objectSize, isLayer)
	var err error
	if objectSize > readAllLimitSize || isLayer {
		err = z.decompressFile(downloadFilePath, extractPath)
	} else {
		err = z.decompressBuffer(buffer, extractPath)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to decompress: %s", err.Error())
		return err
	}
	return utils.RemoveDownloadPath(downloadFilePath, extractPath)
}

func (z *ZipHandler) decompressFile(zipFilePath, extractPath string) error {
	zipReader, err := zip.OpenReader(zipFilePath)
	if err != nil {
		log.GetLogger().Errorf("failed to open zip file: %s", err.Error())
		return snerror.New(snerror.ZipFormatInvalid, err.Error())
	}

	defer func() {
		err := zipReader.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close zip reader: %s", err.Error())
		}

		// Add a large runtime.GC to reduce the memory consumption by 50 MB.
		// (46 MB before decompression and 170 MB after decompression)
		runtime.GC()
	}()

	err = z.checkAndUnzip(zipReader.File, extractPath)
	if err != nil {
		return err
	}

	if err = os.Remove(zipFilePath); err != nil {
		return err
	}

	return nil
}

func (z *ZipHandler) decompressBuffer(buffer []byte, extractPath string) error {
	zipReader, err := zip.NewReader(bytes.NewReader(buffer), int64(len(buffer)))
	if err != nil {
		log.GetLogger().Errorf("failed to transfer from zip file: %s", err.Error())
		return snerror.New(snerror.ZipFormatInvalid, err.Error())
	}
	return z.checkAndUnzip(zipReader.File, extractPath)
}

func (z *ZipHandler) checkAndUnzip(file []*zip.File, targetFilePath string) error {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config: %s", err.Error())
		return err
	}
	if len(file) > conf.DeploymentConfig.CodePackage.FileCountsMax {
		log.GetLogger().Warnf("failed to deploy function, error :too many file in package")
		return snerror.NewWithFmtMsg(snerror.TooManyDependencyFiles, snerror.ErrText(snerror.TooManyDependencyFiles),
			conf.DeploymentConfig.CodePackage.FileCountsMax)
	}
	unzipMax := int64(conf.DeploymentConfig.CodePackage.UnzipFileSizeMaxMB << mbShift)
	var total int64
	for _, zipFile := range file {
		if !validateFilePath(targetFilePath, zipFile.Name, conf.DeploymentConfig.CodePackage.DirDepthMax) {
			log.GetLogger().Warnf("zip file from [%s] to [%s] is invalid", zipFile.Name, targetFilePath)
			continue
		}
		total += int64(zipFile.UncompressedSize64)
		if total > unzipMax {
			log.GetLogger().Warnf("failed to deploy function, error : zip file unzip is too larger")
			return fmt.Errorf("unzip file size is out of range %d",
				conf.DeploymentConfig.CodePackage.UnzipFileSizeMaxMB)
		}
		_, ok := z.filePathMap[zipFile.Name]
		if ok {
			// If file exists skip it
			log.GetLogger().Warnf("file %s has already exist", zipFile.Name)
			continue
		}

		z.filePathMap[zipFile.Name] = struct{}{}
		if err := utils.ExtractZipFile(zipFile, targetFilePath); err != nil {
			log.GetLogger().Warnf("unzipFileFail | %s", err.Error())
			continue
		}
	}
	return nil
}

func validateFilePath(dir, zipfile string, depth int) bool {
	if zipfile == "" ||
		strings.Contains(zipfile, "~/") ||
		len(strings.Split(zipfile, string(filepath.Separator))) > depth {
		return false
	}

	// Make sure the joined path is still inside the directory
	path := filepath.Join(dir, zipfile)
	return path != dir && strings.HasPrefix(path, dir)
}
