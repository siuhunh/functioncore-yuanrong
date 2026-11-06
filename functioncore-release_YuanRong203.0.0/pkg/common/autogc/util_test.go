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

package autogc

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRSS(t *testing.T) {
	r := bytes.NewReader([]byte("367597 12113 4058 3810 0 47257 0\n"))
	buffer := make([]byte, KB)

	for i := 0; i < 10; i++ {
		rss, err := parseRSS(r, buffer)
		assert.Nil(t, err, "parseRSS should return no error")
		assert.Equal(t, uint64(12113*os.Getpagesize()), rss)
	}

	r = bytes.NewReader([]byte("123"))
	_, err := parseRSS(r, make([]byte, KB))
	assert.Error(t, err, "parseRSS should failed")

	r = bytes.NewReader([]byte("123 abcde 132"))
	_, err = parseRSS(r, make([]byte, KB))
	assert.Error(t, err, "parseRSS should failed")
}
