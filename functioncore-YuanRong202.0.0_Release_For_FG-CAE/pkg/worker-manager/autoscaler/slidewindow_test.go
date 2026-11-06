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

package autoscaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRingBuffer(t *testing.T) {
	rb := newRingBuffer(2)
	elem := rb.get(1)
	elem.val = 1
	elem = rb.get(2)
	elem.val = 2

	elem = rb.get(3)
	assert.Equal(t, 1, elem.val)
	rb.rangeAll(func(offset int, elem *ringElem) {
		if offset == 0 {
			assert.Equal(t, 2, elem.val)
		} else if offset == 1 {
			assert.Equal(t, 1, elem.val)
		} else {
			t.Fail()
		}
	})

	rb.advance()

	elem = rb.get(1)
	assert.Equal(t, 2, elem.val)
	elem = rb.get(2)
	assert.Equal(t, 1, elem.val)
	rb.rangeAll(func(offset int, elem *ringElem) {
		if offset == 0 {
			assert.Equal(t, 1, elem.val)
		} else if offset == 1 {
			assert.Equal(t, 2, elem.val)
		} else {
			t.Fail()
		}
	})

	rb.advance()

	rb.rangeAll(func(offset int, elem *ringElem) {
		if offset == 0 {
			assert.Equal(t, 2, elem.val)
		} else if offset == 1 {
			assert.Equal(t, 1, elem.val)
		} else {
			t.Fail()
		}
	})
}
