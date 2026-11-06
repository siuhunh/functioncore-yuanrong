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

import "encoding/binary"

const (
	uint64size             = 8
	caeMetricsEncodeLength = uint64size * 7
)

const (
	blkioStatEntryOpRead  = "Read"
	blkioStatEntryOpWrite = "Write"
)

// CaeMetrics -
type CaeMetrics struct {
	ReceiveBytes         uint64
	ReceivePackage       uint64
	TransmitBytes        uint64
	TransmitPackage      uint64
	FileSystemReadBytes  uint64
	FileSystemWriteBytes uint64
	TimestampMs          uint64
}

// Encode encodes CaeMetrics to bytes
func (c *CaeMetrics) Encode() []byte {
	data := make([]byte, caeMetricsEncodeLength, caeMetricsEncodeLength)
	binary.BigEndian.PutUint64(data, c.ReceiveBytes)
	offset := uint64size
	binary.BigEndian.PutUint64(data[offset:], c.ReceivePackage)
	offset += uint64size
	binary.BigEndian.PutUint64(data[offset:], c.TransmitBytes)
	offset += uint64size
	binary.BigEndian.PutUint64(data[offset:], c.TransmitPackage)
	offset += uint64size
	binary.BigEndian.PutUint64(data[offset:], c.FileSystemReadBytes)
	offset += uint64size
	binary.BigEndian.PutUint64(data[offset:], c.FileSystemWriteBytes)
	offset += uint64size
	binary.BigEndian.PutUint64(data[offset:], c.TimestampMs)
	return data
}

// DecodeFrom decodes CaeMetrics from bytes
func (c *CaeMetrics) DecodeFrom(b []byte) {
	c.ReceiveBytes = binary.BigEndian.Uint64(b)
	offset := uint64size
	c.ReceivePackage = binary.BigEndian.Uint64(b[offset:])
	offset += uint64size
	c.TransmitBytes = binary.BigEndian.Uint64(b[offset:])
	offset += uint64size
	c.TransmitPackage = binary.BigEndian.Uint64(b[offset:])
	offset += uint64size
	c.FileSystemReadBytes = binary.BigEndian.Uint64(b[offset:])
	offset += uint64size
	c.FileSystemWriteBytes = binary.BigEndian.Uint64(b[offset:])
	offset += uint64size
	c.TimestampMs = binary.BigEndian.Uint64(b[offset:])
}

// NetData the data of net metric
type NetData struct {
	ReceiveBytes    uint64
	ReceivePackage  uint64
	TransmitBytes   uint64
	TransmitPackage uint64
}

// BlkioStatEntry the data get from blkio
type BlkioStatEntry struct {
	Major uint64 `json:"major,omitempty"`
	Minor uint64 `json:"minor,omitempty"`
	Op    string `json:"op,omitempty"`
	Value uint64 `json:"value,omitempty"`
}

// BlkioStatEntrys slice of BlkioStatEntry
type BlkioStatEntrys []BlkioStatEntry

// InitBlkioStateEntrys get Init of BlkioStatEntrys
func InitBlkioStateEntrys() BlkioStatEntrys {
	return make(BlkioStatEntrys, 0)
}

// GetReadAndWriteBytes get read bytes and write bytes from BlkioStatEntrys
func (bes *BlkioStatEntrys) GetReadAndWriteBytes() (readBytes, writeBytes uint64) {
	for _, blkioStateEntry := range *bes {
		if blkioStateEntry.Op == blkioStatEntryOpRead {
			readBytes += blkioStateEntry.Value
		}
		if blkioStateEntry.Op == blkioStatEntryOpWrite {
			writeBytes += blkioStateEntry.Value
		}
	}
	return
}

// Append append data to BlkioStatEntrys
func (bes *BlkioStatEntrys) Append(be BlkioStatEntry) {
	*bes = append(*bes, be)
}
