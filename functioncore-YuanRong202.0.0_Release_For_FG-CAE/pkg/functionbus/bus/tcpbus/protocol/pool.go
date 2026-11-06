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

package protocol

import (
	"sync"
)

var bp = bytePool{
	Pool: sync.Pool{New: func() interface{} {
		return make([]byte, intLen, intLen)
	}},
}

type bytePool struct {
	sync.Pool
}

// Get gets []byte from pool
func (b *bytePool) Get() []byte {
	ret, ok := b.Pool.Get().([]byte)
	if !ok {
		return nil
	}
	return ret
}

// Put puts []byte back to pool
func (b *bytePool) Put(data []byte) {
	b.Pool.Put(data)
}
