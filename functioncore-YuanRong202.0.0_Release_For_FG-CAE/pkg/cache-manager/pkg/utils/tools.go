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
package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"strings"

	"functioncore/pkg/cache-manager/config"
)

// GenerateChecksum generate Checksum
func GenerateChecksum(buffer []byte, info *config.FunctionMetaInfo) string {
	hasher := sha512.New()
	if info.FuncMetaData.CodeSha512 == "" {
		hasher = sha256.New()
	}
	n, err := hasher.Write(buffer)
	if n != len(buffer) || err != nil {
		return ""
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// GenerateMD5 generate MD5
func GenerateMD5(str string) string {
	h := md5.New()
	_, err := h.Write([]byte(str))
	if err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ParseObjectID trim file name from objectID
func ParseObjectID(objectID string, isLayer bool) string {
	// The prefixes of the dependency packages are the same.
	if isLayer {
		return objectID
	}
	idx := strings.LastIndexByte(objectID, '/')
	if idx == -1 {
		return objectID
	}
	return objectID[:idx]
}
