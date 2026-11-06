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

package instance

// Info -
type Info struct {
	InstanceMetadata EtcdInstanceMetaData `json:"instanceMetaData"`
}

// EtcdInstanceMetaData -
type EtcdInstanceMetaData struct {
	MaxInstance  int64  `json:"maxInstance" valid:",optional"`
	MinInstance  int64  `json:"minInstance" valid:",optional"`
	InstanceType string `json:"instanceType" valid:",omitempty"`
	IdleMode     bool   `json:"idleMode" valid:",omitempty"`
}
