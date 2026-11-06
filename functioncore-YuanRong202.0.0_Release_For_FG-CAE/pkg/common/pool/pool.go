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

// Package pool wrappers sync.Pool for easy use
package pool

import (
	"bytes"
	"sync"
)

// MaxBufferSize the pool max size
const MaxBufferSize = 1024

// BuffPool is global buffer pool
var BuffPool = BufferPool{
	Pool: sync.Pool{New: func() interface{} {
		return new(bytes.Buffer)
	}},
}

// BufferPool wrappers sync.Pool that provides *bytes.Buffer
type BufferPool struct {
	sync.Pool
}

// Get gets *bytes.Buffer from pool
func (b *BufferPool) Get() *bytes.Buffer {
	ret, ok := b.Pool.Get().(*bytes.Buffer)
	if !ok {
		return nil
	}
	return ret
}

// Put resets buffer and puts it back to pool
func (b *BufferPool) Put(buf *bytes.Buffer) {
	if buf.Cap() > MaxBufferSize {
		buf = nil
		return
	}
	buf.Reset()
	b.Pool.Put(buf)
}
