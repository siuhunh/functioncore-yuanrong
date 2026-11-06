/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsContain(t *testing.T) {
	items := []string{"apple", "banana", "cherry"}
	assert.Equal(t, true, IsContain(items, "apple"))
	assert.Equal(t, true, IsContain(items, "banana"))
	assert.Equal(t, true, IsContain(items, "cherry"))
	assert.Equal(t, false, IsContain(items, "orange"))
}
