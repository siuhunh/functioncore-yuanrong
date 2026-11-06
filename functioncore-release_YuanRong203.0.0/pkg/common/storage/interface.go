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

// Package storage provides common storage interface
package storage

import (
	"os"

	"functioncore/pkg/common/constants"
)

var (
	storageClient       Storage
	snapshotStorageType = os.Getenv("SNAPSHOT_STORAGE_TYPE")
	config              storageConfig
)

// Storage define storage interface
type Storage interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
}

type storageConfig struct {
	enableTenantAuth bool
	tenantID         string
	iamToken         string
}

// GetStorageClient get storage client
func GetStorageClient() Storage {
	if storageClient != nil {
		return storageClient
	}
	if snapshotStorageType == constants.DataSystemStorageType {
		if client := GetDSClient(); client != nil {
			storageClient = client
		}
	}
	return storageClient
}

// GetStorageType get storage type env
func GetStorageType() string {
	return snapshotStorageType
}

// SetConfig set config
func SetConfig(enableTenantAuth bool, tenantID, iamToken string) {
	if !enableTenantAuth {
		return
	}
	config = storageConfig{
		enableTenantAuth: enableTenantAuth,
		tenantID:         tenantID,
		iamToken:         iamToken,
	}
}
