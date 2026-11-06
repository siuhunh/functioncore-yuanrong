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
	"bytes"
	"errors"
	"os"
	"os/exec"
	"unsafe"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

// SnapshotZipHandler -
type SnapshotZipHandler struct {
}

func newSnapshotZipHandler() *SnapshotZipHandler {
	return &SnapshotZipHandler{}
}

func (z *SnapshotZipHandler) getSnapshotPackKey() (string, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config: %s", err.Error())
		return "", err
	}

	decryptKey, err := localauth.Decrypt(cfg.SnapshotPackKey)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt: %s", err.Error())
		return "", err
	}

	return *(*string)(unsafe.Pointer(&decryptKey)), nil
}

// Decompress implements Decompressor.Decompress
func (z *SnapshotZipHandler) Decompress(objectSize int64, downloadFile string, buffer []byte,
	extract string, isLayer bool) error {

	key, err := z.getSnapshotPackKey()
	if err != nil {
		return err
	}
	defer commonutils.ClearStringMemory(key)

	if err := commonutils.ValidateFilePath(downloadFile); err != nil {
		log.GetLogger().Errorf("failed to validate download file path: %s, %s", downloadFile, err.Error())
		return err
	}
	if err := commonutils.ValidateFilePath(extract); err != nil {
		log.GetLogger().Errorf("failed to validate extract directory path: %s, %s", extract, err.Error())
		return err
	}

	var cmd *exec.Cmd
	if objectSize > readAllLimitSize || isLayer {
		log.GetLogger().Infof("decompress snapshot from file with file path: %s, extract: %s, isLayer: %t",
			downloadFile, extract, isLayer)
		cmd = exec.Command(constants.SnapshotUnpackScriptPath, key, downloadFile, extract)
	} else {
		// small object comes from buffer
		log.GetLogger().Infof("decompress snapshot from buffer with file path: %s, extract: %s, isLayer: %t",
			downloadFile, extract, isLayer)
		cmd = exec.Command(constants.SnapshotUnpackScriptPath, key, "", extract)
		cmd.Stdin = bytes.NewBuffer(buffer)
	}
	if cmd == nil {
		log.GetLogger().Errorf("failed to create command with file path: %s, extract: %s, isLayer: %t",
			downloadFile, extract, isLayer)
		return errors.New("failed to create command")
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		log.GetLogger().Errorf("failed to decompress: %s, stderr: %s", err.Error(), stderr.String())
		return err
	}

	if err := os.Remove(downloadFile); err != nil {
		return err
	}

	return utils.RemoveDownloadPath(downloadFile, extract)
}

// Compress -
func (z *SnapshotZipHandler) Compress(zipFile string, src string) error {
	key, err := z.getSnapshotPackKey()
	if err != nil {
		return err
	}
	defer commonutils.ClearStringMemory(key)

	if err := commonutils.ValidateFilePath(zipFile); err != nil {
		log.GetLogger().Errorf("failed to validate zip file path: %s, %s", zipFile, err.Error())
		return err
	}
	if err := commonutils.ValidateFilePath(src); err != nil {
		log.GetLogger().Errorf("failed to validate source directory path: %s, %s", src, err.Error())
		return err
	}

	cmd := exec.Command(constants.SnapshotPackScriptPath, key, zipFile, src)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		log.GetLogger().Errorf("execute zipCommand error: %s", err.Error())
		return err
	}

	return nil
}
