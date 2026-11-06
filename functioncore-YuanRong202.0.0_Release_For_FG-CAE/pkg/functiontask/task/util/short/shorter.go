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

// Package short uses the first three bytes of the hash value to identify the original hash value, which is mainly used in logs.
package short

import (
	"encoding/hex"
	"strings"

	"functioncore/pkg/common/logger/log"
)

// the max length of short bytes.
const shortBytes = 3

// the max length of short characters.
const shortCharacters = shortBytes * 2

// Hash returns the first three bytes of the hash value (expressed in hexadecimal) to identify the original hash value.
func Hash(hash []byte) string {
	if len(hash) > shortBytes {
		return hex.EncodeToString(hash[:shortBytes])
	}
	return hex.EncodeToString(hash)
}

// Str returns first six character of the original string.
func Str(s string) string {
	if len(s) > shortCharacters {
		return s[:shortCharacters]
	}
	return s
}

func writeString(b *strings.Builder, s string) {
	_, err := b.WriteString(s)
	if err != nil {
		log.GetLogger().Warnf("write String err, err: %s", err.Error())
	}
}

// List returns a list separated by comma, and each element is a short hash.
func List(l []string) string {
	var b strings.Builder
	for i, reqID := range l {
		if i != 0 {
			writeString(&b, ",")
		}
		writeString(&b, Str(reqID))
	}
	return b.String()
}

// Lists returns a list of List.
func Lists(l [][]string) string {
	var b strings.Builder
	for i, reqID := range l {
		if i != 0 {
			writeString(&b, ",")
		}
		writeString(&b, "[")
		writeString(&b, List(reqID))
		writeString(&b, "]")
	}
	return b.String()
}
