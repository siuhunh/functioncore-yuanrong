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

// Package functionlog defines log module
package functionlog

import "errors"

const (
	defaultFixSizeWriterSize = 3072
)

// FixSizeWriter -
type FixSizeWriter struct {
	byteSlice    []byte
	recycleIndex int
	isCovered    bool
}

// NewFixSizeWriter -
func NewFixSizeWriter() *FixSizeWriter {
	return &FixSizeWriter{
		byteSlice: make([]byte, defaultFixSizeWriterSize),
	}
}

// Read -
func (w *FixSizeWriter) Read() []byte {
	if w.isCovered {
		return append(w.byteSlice[w.recycleIndex:], w.byteSlice[0:w.recycleIndex]...)
	}
	return w.byteSlice[0:w.recycleIndex]
}

// Write -
func (w *FixSizeWriter) Write(p []byte) (int, error) {
	startIndex := 0
	if len(w.byteSlice) == 0 {
		return len(p), errors.New("slice byte length is 0")
	}
	for startIndex < len(p) {
		if w.recycleIndex >= len(w.byteSlice) {
			return len(p), errors.New("recycleIndex error")
		}
		n := copy(w.byteSlice[w.recycleIndex:], p[startIndex:])
		startIndex += n
		w.recycleIndex += n
		if !w.isCovered && w.recycleIndex >= len(w.byteSlice) {
			w.isCovered = true
		}
		w.recycleIndex %= len(w.byteSlice)
	}
	return len(p), nil
}
