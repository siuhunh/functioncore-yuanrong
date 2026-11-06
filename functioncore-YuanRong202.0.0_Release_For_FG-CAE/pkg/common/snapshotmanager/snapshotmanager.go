/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
 *
 * This software is licensed under muxlan PSL v2.
 * You can use this software according to the terms and conditions of the muxlan PSL v2.
 * You may obtain a copy of muxlan PSL v2 at:
 *
 * http://license.coscl.org.cn/muxlanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the muxlan PSL v2 for more details.
 */

// Package snapshotmanager control if workermanager is running as snapshotmanager
package snapshotmanager

var (
	snapshotManagerMode = false
)

// SetSnapshotManagerMode -
func SetSnapshotManagerMode(enable bool) {
	snapshotManagerMode = enable
}

// GetSnapshotManagerMode -
func GetSnapshotManagerMode() bool {
	return snapshotManagerMode
}
