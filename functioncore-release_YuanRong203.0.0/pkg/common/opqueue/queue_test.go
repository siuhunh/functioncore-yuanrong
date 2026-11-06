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

package opqueue

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewOperationQueue(t *testing.T) {
	count1 := uint32(0)
	count2 := uint32(0)
	queue := NewOperationQueue(100)
	// assert operations with same key will be processed consecutively
	for i := 0; i < 20; i++ {
		go func() {
			queue.Push(Operation{
				Key: "key1",
				Run: func() {
					atomic.AddUint32(&count1, 1)
					time.Sleep(10 * time.Millisecond)
				},
			})
		}()
	}
	time.Sleep(30 * time.Millisecond)
	assert.True(t, atomic.LoadUint32(&count1) < 5)
	assert.True(t, atomic.LoadUint32(&count1) > 0)

	// operations with different key will be parallelized
	for i := 0; i < 10; i++ {
		go func(i int) {
			queue.Push(Operation{
				Key: "other" + strconv.Itoa(i),
				Run: func() {
					atomic.AddUint32(&count2, 1)
				},
			})
		}(i)
	}
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, uint32(10), atomic.LoadUint32(&count2))
}
