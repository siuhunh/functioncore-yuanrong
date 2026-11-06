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

package faasfunction

// FuncMountConfig function mount config
type FuncMountConfig struct {
	FuncMountUser FuncMountUser `json:"mount_user" valid:",optional"`
	FuncMounts    []FuncMount   `json:"func_mounts" valid:",optional"`
}

// FuncMount the info about file mount of kubernetes
type FuncMount struct {
	MountType      string `json:"mount_type" valid:",optional"`
	MountResource  string `json:"mount_resource" valid:",optional"`
	MountSharePath string `json:"mount_share_path" valid:",optional"`
	LocalMountPath string `json:"local_mount_path" valid:",optional"`
	Status         string `json:"status" valid:",optional"`
}

// FuncMountUser the info about mount user
type FuncMountUser struct {
	UserID  int `json:"user_id" valid:",optional"`
	GroupID int `json:"user_group_id" valid:",optional"`
}
