/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package dataaggregation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAtomicAverage(t *testing.T) {
	a := NewAtomicAverage(-1)

	assert.Equal(t, int64(0), a.GetValue())

	a.AddSample(0)
	assert.Equal(t, int64(1), a.GetValue())

	a.AddSample(10)
	assert.Equal(t, int64(5), a.GetValue())

	for i := 2; i < maxWindowCapacity; i++ {
		a.AddSample(0)
	}
	assert.Equal(t, int64(1), a.GetValue())

	a.AddSample(10)
	a.AddSample(10)
	assert.Equal(t, int64(10), a.GetValue())

	a.Reset()
	assert.Equal(t, int64(0), a.GetValue())

	a.AddSample(0)
	assert.Equal(t, int64(1), a.GetValue())

	a.Reset()
	a.AddSamples([]int64{30000, 10000, 20000})
	assert.Equal(t, int64(20000), a.GetValue())

	samples := make([]int64, maxWindowCapacity)
	for i := range samples {
		samples[i] = 1
	}
	a.AddSamples(samples)
	assert.Equal(t, int64(1), a.GetValue())
}
