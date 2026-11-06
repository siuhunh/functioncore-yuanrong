/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package deploy deploys Layers and functions
package deploy

import (
	"encoding/json"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
)

const (
	defaultFilePerm = 0644
	bytesToMB       = 1024 * 1024
)

// DownloadSnapshotFromDs -
func (d *s3Deployer) DownloadSnapshotFromDs(bucketID, ObjectID string) error {
	start := time.Now()
	fileID := bucketID + "@" + ObjectID
	log.GetLogger().Infof("begin to download snapshot file from data system with %s", fileID)
	fileListKey := fileID + "@files"
	fileListStr, err := d.storageCli.Get(fileListKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get value by key %s from data system with %s, error %s",
			fileListKey, fileID, err.Error())
		return err
	}
	snapshotFileList := make([]string, 0, 4)
	err = json.Unmarshal([]byte(fileListStr), &snapshotFileList)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal json for %s with error %s", fileID, err.Error())
		return err
	}
	var fileName string
	var filePath string
	var fileSize int
	var fileContentBytes []byte
	totalFileSize := 0
	for i := range snapshotFileList {
		fileName = fileID + "@" + snapshotFileList[i]
		fileContentStr, err := d.storageCli.Get(fileName)
		if err != nil {
			log.GetLogger().Errorf("failed to get file from data system with error %s", fileName, err.Error())
			return err
		}
		fileContentBytes = []byte(fileContentStr)
		fileSize = len(fileContentBytes)
		totalFileSize += fileSize
		filePath = filepath.Join(constants.SnapshotImagePathForWorker, snapshotFileList[i])
		err = os.WriteFile(filePath, []byte(fileContentStr), defaultFilePerm)
		if err != nil {
			log.GetLogger().Errorf("failed to write file %s to %s with error %s", fileName, filePath, err.Error())
			return err
		}
		log.GetLogger().Infof("succeed to write file %s to %s with file size %f MB",
			fileName, filePath, float64(fileSize)/bytesToMB)
	}
	log.GetLogger().Infof("succeed to download snapshot file from data system with %s and file size %f MB cost %s",
		fileID, float64(totalFileSize)/bytesToMB, time.Since(start).String())
	return nil
}

// NoCompressANDUploadToDS -
func (d *s3Deployer) NoCompressANDUploadToDS(bucketID, ObjectID string) (int64, string, error) {
	snapshotDir := constants.SnapshotImagePathForWorker
	fileID := bucketID + "@" + ObjectID
	log.GetLogger().Infof("begin to upload snapshot to data system with file id %s", fileID)

	files, err := ioutil.ReadDir(snapshotDir)
	if err != nil {
		log.GetLogger().Errorf("failed to read dir from %s with error %s", snapshotDir, err.Error())
		return 0, "", err
	}
	var filePath string
	var file fs.FileInfo
	totalFileSize := int64(0)
	snapshotFileList := make([]string, 0, 4)
	for i := range files {
		file = files[i]
		totalFileSize += file.Size()
		filePath = filepath.Join(snapshotDir, file.Name())
		fileBytes, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.GetLogger().Errorf("failed to read file from %s with error %s", filePath, err.Error())
			return 0, "", err
		}
		fileName := fileID + "@" + file.Name()
		if err = d.storageCli.Set(fileName, string(fileBytes)); err != nil {
			log.GetLogger().Errorf("failed to upload %s to data system with error %s", fileName, err.Error())
			return 0, "", err
		}
		snapshotFileList = append(snapshotFileList, file.Name())
		log.GetLogger().Infof("succeed to upload %s to data system with size %f MB",
			fileName, float64(file.Size())/bytesToMB)
	}
	value, err := json.Marshal(snapshotFileList)
	if err != nil {
		return 0, "", err
	}
	if err = d.storageCli.Set(fileID+"@files", string(value)); err != nil {
		log.GetLogger().Errorf("failed to upload %s to data system with error %s", fileID+"@files", err.Error())
		return 0, "", err
	}
	log.GetLogger().Infof("succeed to upload %s to data system with total size %f MB",
		fileID+"@files", float64(totalFileSize)/bytesToMB)
	return totalFileSize, "", nil
}
