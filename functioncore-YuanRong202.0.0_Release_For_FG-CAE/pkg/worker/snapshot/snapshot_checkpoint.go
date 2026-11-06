/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package snapshot -
package snapshot

import (
	"functioncore/pkg/common/types"
)

var (
	snapshotInfo *types.SnapshotDeploymentSpec

	checkpointError = ""
)

// SetSnapshotInfo will save snapshot info
func SetSnapshotInfo(snapshot *types.SnapshotDeploymentSpec) {
	snapshotInfo = snapshot
}

// GetSnapshotInfo will return snapshot info
func GetSnapshotInfo() *types.SnapshotDeploymentSpec {
	return snapshotInfo
}

// GetCheckpointError will return checkpoint error
func GetCheckpointError() string {
	return checkpointError
}

// SetCheckpointError will save checkpoint error
func SetCheckpointError(errMsg string) {
	checkpointError = errMsg
}
