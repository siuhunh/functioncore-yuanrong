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
	"os"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

// SnapshotZstdHandler -
type SnapshotZstdHandler struct {
}

func newSnapshotZstdHandler() *SnapshotZstdHandler {
	return &SnapshotZstdHandler{}
}

// Decompress implements Decompressor.Decompress
func (z *SnapshotZstdHandler) Decompress(objectSize int64, downloadFile string, buffer []byte,
	extract string, isLayer bool) error {
	if err := utils.ValidateFilePath(downloadFile); err != nil {
		log.GetLogger().Errorf("failed to validate download file path: %s, %s", downloadFile, err.Error())
		return err
	}
	if err := utils.ValidateFilePath(extract); err != nil {
		log.GetLogger().Errorf("failed to validate extract directory path: %s, %s", extract, err.Error())
		return err
	}
	if err := os.Chmod("/runtime/var-tmp/snapshot/snapshot", perm); err != nil {
		log.GetLogger().Errorf("failed to chmod snapshot directory %s", err.Error())
		return err
	}
	snapshotFileName := "/runtime/var-tmp/snapshot/snapshot/snapshot.tar.zst"
	if objectSize > readAllLimitSize || isLayer {
		log.GetLogger().Infof("decompress snapshot from file with file path: %s, extract: %s, isLayer: %t",
			downloadFile, extract, isLayer)
		err := os.Rename(downloadFile, snapshotFileName)
		if err != nil {
			log.GetLogger().Errorf("rename snapshot file path: %s, err: %s", downloadFile, err.Error())
			return err
		}
	} else {
		// small object comes from buffer
		log.GetLogger().Infof("decompress snapshot from buffer with file path: %s, extract: %s, isLayer: %t",
			downloadFile, extract, isLayer)
		err := os.WriteFile(snapshotFileName, buffer, defaultFilePerm)
		if err != nil {
			log.GetLogger().Errorf("wite snapshot file path: %s, err: %s", snapshotFileName, err.Error())
			return err
		}
	}
	return nil
}

// Compress -
func (z *SnapshotZstdHandler) Compress(zstdFile, src string) error {
	if err := utils.ValidateFilePath(zstdFile); err != nil {
		log.GetLogger().Errorf("failed to validate zstd file path: %s, %s", zstdFile, err.Error())
		return err
	}
	if err := utils.ValidateFilePath(src); err != nil {
		log.GetLogger().Errorf("failed to validate source directory path: %s, %s", src, err.Error())
		return err
	}

	return nil
}
