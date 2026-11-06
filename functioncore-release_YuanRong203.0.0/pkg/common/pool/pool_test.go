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

package pool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBufferPool_PutGet(t *testing.T) {
	// Verify the basic functions of pool pkg, Input:NA, OutPut:NA

	// Get empty
	p := BufferPool{}
	assert.Nil(t, p.Get())

	// Get after Put
	b1 := bytes.Buffer{}
	b1.Grow(10)
	p.Put(&b1)
	assert.Equal(t, b1.Len(), 0)
	assert.NotNil(t, p.Get())
	assert.Nil(t, p.Get())

	// Put overflow
	b1.Grow(MaxBufferSize + 1)
	p.Put(&b1)
	assert.Nil(t, p.Get())
	assert.Equal(t, b1.Len(), 0)
}
