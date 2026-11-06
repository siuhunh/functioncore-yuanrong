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
	"os/exec"
	"runtime"

	"functioncore/pkg/common/logger/log"
	utilities "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/utils"
)

// ZstdHandler -
type ZstdHandler struct {
}

func newZstdHandler() *ZstdHandler {
	return &ZstdHandler{}
}

// Decompress implements Decompressor.Decompress
func (z *ZstdHandler) Decompress(objectSize int64, downloadFilePath string, buffer []byte,
	extractPath string, isLayer bool) error {
	var err error
	defer func() {
		runtime.GC()
	}()
	log.GetLogger().Debugf("start to decompress by zstd, downloadFilePath: %s, objectSize: %d",
		downloadFilePath, objectSize)
	if objectSize > readAllLimitSize || isLayer {
		err = z.decompressFile(downloadFilePath, extractPath)
	} else {
		err = z.decompressBuffer(buffer, downloadFilePath, extractPath)
	}
	if err != nil {
		return err
	}
	return utils.RemoveDownloadPath(downloadFilePath, extractPath)
}

func (z *ZstdHandler) decompressFile(downloadFilePath, pathOfSource string) error {
	cmd := exec.Command("tar", "-I", "zstd", "-xf", downloadFilePath, "-C", pathOfSource)
	err := cmd.Run()
	if err != nil {
		log.GetLogger().Errorf("failed decompress zstd file, error %s ", err.Error())
		return err
	}
	return nil
}

func (z *ZstdHandler) decompressBuffer(buffer []byte, downloadFilePath string, extractPath string) error {
	err := utilities.WriteFileToPath(downloadFilePath, buffer)
	if err != nil {
		log.GetLogger().Errorf("failed to write file to path error: %s", err.Error())
		return err
	}
	return z.decompressFile(downloadFilePath, extractPath)
}
