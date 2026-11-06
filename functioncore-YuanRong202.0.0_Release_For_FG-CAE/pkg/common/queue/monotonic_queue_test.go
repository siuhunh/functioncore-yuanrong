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

package queue

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type monotonicQueueCase struct {
	input    []int
	k        int
	excepted []int
}

func generateMonotonicQueueCase(rnd *rand.Rand) monotonicQueueCase {
	n := 10 + rnd.Intn(100)
	k := 1 + rnd.Intn(10)

	input := make([]int, n, n)
	for i := range input {
		input[i] = rnd.Intn(1000)
	}
	output := make([]int, n, n)

	for i := range output {
		max := input[i]
		for j := i - k + 1; j <= i; j++ {
			if j < 0 {
				continue
			}
			if input[j] > max {
				max = input[j]
			}
		}
		output[i] = max
	}
	return monotonicQueueCase{
		input:    input,
		k:        k,
		excepted: output,
	}
}

func TestMonotonicQueue(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < 10; i++ {
		t.Run("TestMonotonicQueue_case_"+strconv.Itoa(i), func(t *testing.T) {
			c := generateMonotonicQueueCase(rnd)
			q := NewMonotonicQueue(c.k)
			for idx := range c.input {
				q.Push(c.input[idx])
				assert.Equal(t, c.excepted[idx], q.GetMax(), fmt.Sprintf("case %v index %d getMax %d", c, idx, q.GetMax()))
			}
		})
	}
}

func TestMonotonicQueue_Resize(t *testing.T) {
	data := []int{626, 672, 816, 350, 880, 814, 826, 799, 435, 75, 195, 887, 991, 145, 832, 407, 229, 337}
	excepted := []int{626, 672, 816, 816, 880, 880, 880, 880, 880, 880, 826, 887, 991, 991, 832, 832, 407, 337}
	q := NewMonotonicQueue(5)
	for i := range data {
		if i == 6 {
			q.Resize(6)
		}
		if i == 12 {
			q.Resize(2)
		}
		q.Push(data[i])
		assert.Equal(t, excepted[i], q.GetMax(), fmt.Sprintf("index %d", i))
	}
}
