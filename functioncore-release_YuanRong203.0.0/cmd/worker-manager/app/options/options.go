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

// Package options define options of worker-manager
package options

// APIServerOptions defines some server options for server startup
type APIServerOptions struct {
	ListenAddress  string
	EnableAPIV1    bool   `valid:",optional"`
	EnableTenant   bool   `valid:",optional"`
	StorageType    string `valid:"stringlength(1|64)"`
	ProtocolType   string `valid:"stringlength(1|64)"`
	ConfigFile     string `valid:",optional"`
	IsClusterMode  bool   `valid:",optional"`
	ClusterType    string `valid:"stringlength(1|64)"`
	LogPath        string `valid:",optional"`
	IsSnapshotMode bool   `valid:",optional"`
}

// NewAPIServerOptions New API Server Options
func NewAPIServerOptions() *APIServerOptions {
	s := APIServerOptions{
		ListenAddress:  "",
		EnableAPIV1:    true,
		EnableTenant:   false,
		ProtocolType:   "http",
		ConfigFile:     "/home/sn/config/worker-manager.conf",
		IsClusterMode:  false,
		ClusterType:    "etcd",
		LogPath:        "/home/sn/log",
		IsSnapshotMode: false,
	}
	return &s
}
