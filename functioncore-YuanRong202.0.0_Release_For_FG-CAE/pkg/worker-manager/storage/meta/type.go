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

// Package meta is the meta data of function
package meta

import "encoding/json"

// StorageFilter storage filter
type StorageFilter struct {
	Filters map[string]string
}

// Storage meta storage
type Storage interface {
	Type() (string, error)
	Data() (string, error)
}

// ListMeta meta list
type ListMeta struct {
	SelfLink string `json:"selfLink,omitempty" protobuf:"bytes,1,opt,name=selfLink"`

	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`

	Continue string `json:"continue,omitempty" protobuf:"bytes,3,opt,name=continue"`
}

// StorageList meta storage list
type StorageList struct {
	ListMeta `json:"metadata,omitempty"`
	Items    []Storage `json:"items"`
}

// Data return string Marshal Meta Storage List
func (l *StorageList) Data() (string, error) {
	data, err := json.Marshal(l)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
