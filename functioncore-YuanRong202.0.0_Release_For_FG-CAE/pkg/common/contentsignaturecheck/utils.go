/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package contentsignaturecheck implements content signature check
package contentsignaturecheck

import (
	"encoding/base64"
	"strconv"

	"functioncore/pkg/common/logger/log"
)

const (
	payloadCheckLen   = 20 // if payload length <= 20, use payload as content signature
	payloadExtractLen = 10 // use the first 10 bytes and the last 10 bytes as content signature
)

// GetContentSignature get content signature from input payload
func GetContentSignature(payload []byte) string {
	payloadLen := len(payload)
	var extractStr string
	if payloadLen <= payloadCheckLen {
		extractStr = string(payload) + strconv.Itoa(payloadLen)
	} else {
		extractStr = string(payload[:payloadExtractLen]) + string(payload[payloadLen-payloadExtractLen:]) +
			strconv.Itoa(payloadLen)
	}
	return base64.StdEncoding.EncodeToString([]byte(extractStr))
}

// CheckContentSignature check content signature when worker content signature is inconsistent
func CheckContentSignature(headerSignature, contentSignature, traceID, name string) {
	if headerSignature == contentSignature {
		log.GetLogger().Warnf("worker content is inconsistent, %s content is consistent, content signature: %s, "+
			"traceID: %s", name, contentSignature, traceID)
	} else {
		log.GetLogger().Warnf("worker content is inconsistent, %s content is inconsistent, header signature: %s, "+
			"content signature: %s, traceID: %s", name, headerSignature, contentSignature, traceID)
	}
}
