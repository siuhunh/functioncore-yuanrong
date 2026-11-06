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

package strutil

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBytesToString(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		var randBytes []byte
		l := rnd.Intn(100) + 1
		for j := 0; j < l; j++ {
			randBytes = append(randBytes, byte(rnd.Intn(100)+1))
		}
		s := string(randBytes)
		assert.Equal(t, s, BytesToString(randBytes))
	}
}

func TestStringToBytes(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 100; i++ {
		randString := ""
		l := rnd.Intn(100) + 1
		for j := 0; j < l; j++ {
			randString += string(byte(rnd.Intn(100) + 1))
		}
		bytes := []byte(randString)
		assert.Equal(t, bytes, StringToBytes(randString))
	}
}
