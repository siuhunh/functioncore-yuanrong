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

package uchan

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnboundedChan(t *testing.T) {
	ch := NewUnboundedChan(5)

	// assert we can push more element than init cap
	for i := 0; i < 10; i++ {
		ch.In <- i
	}
	for i := 0; i < 10; i++ {
		assert.Equal(t, i, <-ch.Out)
	}

	for i := 0; i < 10; i++ {
		ch.In <- i
	}
	// assert we can get close the input channel and get notified in output channel
	close(ch.In)
	for i := 0; i < 10; i++ {
		assert.Equal(t, i, <-ch.Out)
	}
	_, ok := <-ch.Out
	assert.Equal(t, false, ok)
}
