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

package contentsignaturecheck

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetContentSignature(t *testing.T) {
	payload1 := []byte("123")
	signature1 := GetContentSignature(payload1)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("1233")), signature1)

	payload2 := []byte("012345678901234567890123456789")
	signature2 := GetContentSignature(payload2)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("0123456789012345678930")), signature2)
}

func TestCheckContentSignature(t *testing.T) {
	CheckContentSignature("a", "a", "traceID", "name")
	CheckContentSignature("a", "b", "traceID", "name")
}
