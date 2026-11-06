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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultAlg(t *testing.T) {
	assert.Equal(t, 4*GB, 4294967296)

	alg := DefaultAlg{}
	alg.Init(4*GB, 3200*MB)

	tests := []struct {
		current  uint64
		excepted int
	}{
		{
			current:  40 * MB,
			excepted: defaultMaxGOGC,
		},
		{
			current:  3200 * MB,
			excepted: 1,
		},
		{
			current:  3201 * MB,
			excepted: 1,
		},
		{
			current:  3100 * MB,
			excepted: 3,
		},
		{
			current:  2000 * MB,
			excepted: 60,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.excepted, alg.NextGOGC(test.current, 0))
	}
}
