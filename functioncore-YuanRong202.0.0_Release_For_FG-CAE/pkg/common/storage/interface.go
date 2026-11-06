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

const (
	dataSystemStorageType = "data-system"
)

// Storage define storage interface
type Storage interface {
	Get(key string) (string, error)
	Set(key, value string) error
}

type emptyStorage struct {
}

// Get -
func (m *emptyStorage) Get(key string) (string, error) {
	return "empty storage", nil
}

// Set -
func (m *emptyStorage) Set(key, value string) error {
	return nil
}

// GetStorageClient get storage client
func GetStorageClient(storageType string) Storage {
	if storageType == dataSystemStorageType {
		return GetDSClient()
	}
	return &emptyStorage{}
}
