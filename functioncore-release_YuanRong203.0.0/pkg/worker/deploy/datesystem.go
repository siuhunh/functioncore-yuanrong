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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/storage"
	"functioncore/pkg/common/utils"
)

const (
	defaultFilePerm        = 0644
	bytesToMB              = 1024 * 1024
	defaultUploadTimeout   = 5 * time.Second
	defaultDownloadTimeout = 2 * time.Second
	defaultDeleteTimeout   = 5 * time.Second

	snapshotDSKeySeparator = "@"
	snapshotDSListKeySplit = "@files"

	errDeleteSnapshotFromDS = "failed to delete fileID %s with value: %s from ds, error: %s"
)

// DownloadSnapshotFromDS -
func DownloadSnapshotFromDS(bucketID, objectID string) error {
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), defaultDownloadTimeout)
	defer cancel()
	go func() {
		done <- downloadSnapshotFromDS(bucketID, objectID)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		log.GetLogger().Warnf("failed to download snapshot from bucketID '%s' and objectID '%s' with timeout %v",
			bucketID, objectID, defaultDownloadTimeout)
		return errors.New("download snapshot from data system timeout")
	}
}

func downloadSnapshotFromDS(bucketID, objectID string) error {
	start := time.Now()
	fileID := bucketID + snapshotDSKeySeparator + objectID
	log.GetLogger().Infof("begin to download snapshot file from data system with %s", fileID)
	fileListKey := fileID + snapshotDSListKeySplit
	storageClient := storage.GetStorageClient()
	if storageClient == nil {
		return errors.New("storage client is nil")
	}
	fileListStr, err := storageClient.Get(fileListKey)
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
		fileName = fileID + snapshotDSKeySeparator + snapshotFileList[i]
		fileContentStr, err := storageClient.Get(fileName)
		if err != nil {
			log.GetLogger().Errorf("failed to get file %s from data system with error %s", fileName, err.Error())
			return err
		}
		fileContentBytes = []byte(fileContentStr)
		fileSize = len(fileContentBytes)
		totalFileSize += fileSize
		filePath = filepath.Join(utils.GetSnapshotImagePathForWorker(), snapshotFileList[i])
		err = os.WriteFile(filePath, []byte(fileContentStr), defaultFilePerm)
		if err != nil {
			log.GetLogger().Errorf("failed to write file %s from data system to %s with error %s",
				fileName, filePath, err.Error())
			return err
		}
		log.GetLogger().Infof("succeed to write file %s from data system to %s with file size %f MB",
			fileName, filePath, float64(fileSize)/bytesToMB)
	}
	log.GetLogger().Infof("succeed to download snapshot file from data system with %s and file size %f MB cost %s",
		fileID, float64(totalFileSize)/bytesToMB, time.Since(start).String())
	return nil
}

// UploadSnapshotToDS upload snapshot to data system
func UploadSnapshotToDS(bucketID, objectID string) (int64, string, error) {
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), defaultUploadTimeout)
	defer cancel()
	var fileSize int64
	var fileHash string
	go func() {
		var uploadErr error
		fileSize, fileHash, uploadErr = noCompressANDUploadToDS(bucketID, objectID)
		done <- uploadErr
	}()
	select {
	case err := <-done:
		return fileSize, fileHash, err
	case <-ctx.Done():
		log.GetLogger().Warnf("failed to upload snapshot to bucketID '%s' and objectID '%s' with timeout %v",
			bucketID, objectID, defaultDownloadTimeout)
		return 0, "", errors.New("upload snapshot to data system timeout")
	}
}

func noCompressANDUploadToDS(bucketID, ObjectID string) (int64, string, error) {
	start := time.Now()
	snapshotDir := utils.GetSnapshotImagePathForWorker()
	fileID := bucketID + snapshotDSKeySeparator + ObjectID
	log.GetLogger().Infof("begin to upload snapshot to data system with file id %s", fileID)
	storageClient := storage.GetStorageClient()
	if storageClient == nil {
		return 0, "", errors.New("storage client is nil")
	}
	if err := deleteOldDSFileIfExists(storageClient, fileID); err != nil {
		log.GetLogger().Errorf("failed to get/delete old file from ds with error %s", err.Error())
		return 0, "", err
	}
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
		fileName := fileID + snapshotDSKeySeparator + file.Name()
		if err = storageClient.Set(fileName, string(fileBytes)); err != nil {
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
	if err = storageClient.Set(fileID+snapshotDSListKeySplit, string(value)); err != nil {
		log.GetLogger().Errorf("failed to upload %s to data system with error %s", fileID+snapshotDSListKeySplit, err.Error())
		return 0, "", err
	}
	log.GetLogger().Infof("succeed to upload %s to data system with total size %f MB, cost time: %s",
		fileID+snapshotDSListKeySplit, float64(totalFileSize)/bytesToMB, time.Since(start).String())
	return totalFileSize, "", nil
}

// DeleteSnapshotFromDS delete snapshot to data system
func DeleteSnapshotFromDS(bucketID, objectID string, needRemoveObjectIDs []string) error {
	if len(needRemoveObjectIDs) == 0 {
		log.GetLogger().Infof("there is no history snapshot need delete with bucketID '%s' and objectID '%s'",
			bucketID, objectID)
		return nil
	}

	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), defaultDeleteTimeout)
	defer cancel()
	go func() {
		var err error
		err = deleteSnapshotFromDS(bucketID, needRemoveObjectIDs)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		log.GetLogger().Warnf("failed to delete snapshot to bucketID '%s' and objectID '%s' with timeout %v",
			bucketID, objectID, defaultDeleteTimeout)
		return errors.New("delete snapshot to data system timeout")
	}
}

func deleteOldDSFileIfExists(storageClient storage.Storage, fileID string) error {
	fileListKey := fileID + snapshotDSListKeySplit
	fileListStr, err := storageClient.Get(fileListKey)
	if err != nil {
		log.GetLogger().Errorf("key %s alreay deleted or not exist from data system with %s, error %s",
			fileListKey, fileID, err.Error())
		return nil
	}
	snapshotFileList := make([]string, 0, 5)
	err = json.Unmarshal([]byte(fileListStr), &snapshotFileList)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal json for %s with error %s", fileID, err.Error())
		return err
	}
	var fileName string
	for i := range snapshotFileList {
		fileName = fileID + snapshotDSKeySeparator + snapshotFileList[i]
		err = storageClient.Delete(fileName)
		if err != nil {
			log.GetLogger().Errorf("failed to del file %s from data system with error %s", fileName, err.Error())
			return fmt.Errorf(errDeleteSnapshotFromDS, fileID, fileListStr, err.Error())
		}
		log.GetLogger().Infof("succeed to delete file %s from data system", fileName)
	}
	return nil
}

func deleteSnapshotFromDS(bucketID string, needRemoveObjectIDs []string) error {
	storageClient := storage.GetStorageClient()
	if storageClient == nil {
		return errors.New("storage client is nil")
	}

	for i := 0; i < len(needRemoveObjectIDs); i++ {
		fileID := bucketID + snapshotDSKeySeparator + needRemoveObjectIDs[i]
		err := deleteOldDSFileIfExists(storageClient, fileID)
		if err != nil {
			return err
		}
	}
	return nil
}
