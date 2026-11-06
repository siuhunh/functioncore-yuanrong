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

// Package monitor provide memory and file monitor
package monitor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlkioStatEntrys_Append(t *testing.T) {
	blkioStateEntrys := InitBlkioStateEntrys()
	blkioStateEntrys.Append(BlkioStatEntry{
		Major: 100,
		Minor: 1,
		Op:    "Read",
		Value: 100,
	})

	blkioStateEntrys.Append(BlkioStatEntry{
		Major: 100,
		Minor: 2,
		Op:    "Read",
		Value: 100,
	})

	blkioStateEntrys.Append(BlkioStatEntry{
		Major: 100,
		Minor: 2,
		Op:    "Write",
		Value: 101,
	})

	blkioStateEntrys.Append(BlkioStatEntry{
		Major: 100,
		Minor: 2,
		Op:    "Write",
		Value: 200,
	})

	readBytes, writeBytes := blkioStateEntrys.GetReadAndWriteBytes()
	assert.Equalf(t, uint64(200), readBytes, "GetReadAndWriteBytes()")
	assert.Equalf(t, uint64(301), writeBytes, "GetReadAndWriteBytes()")
}

func TestCaeMetrics_DecodeAndEncode(t *testing.T) {
	caeMetrics := CaeMetrics{
		ReceiveBytes:         100,
		ReceivePackage:       200,
		TransmitBytes:        300,
		TransmitPackage:      400,
		FileSystemReadBytes:  500,
		FileSystemWriteBytes: 600,
		TimestampMs:          700,
	}

	encodeStr := caeMetrics.Encode()

	caeMetricsNew := CaeMetrics{}

	caeMetricsNew.DecodeFrom(encodeStr)

	assert.Equalf(t, caeMetrics, caeMetricsNew, "caeMetricsEncode")
}
