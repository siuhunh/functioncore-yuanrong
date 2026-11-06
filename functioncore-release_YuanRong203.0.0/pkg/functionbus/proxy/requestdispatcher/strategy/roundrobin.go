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

// Package strategy -
package strategy

// RoundRobin select next index from instanceArray
func RoundRobin[T, M, N any](index int, array []T, used M, enable N, checkFunction func(T, M, N) (bool, string, string)) int {
	arrayNums := len(array)
	if arrayNums == 0 {
		return -1
	}
	for i := 0; i < len(array); i++ {
		index = nextIndex(index, arrayNums)
		ok, _, _ := checkFunction(array[index], used, enable)
		if ok {
			return index
		}
	}
	ok, _, _ := checkFunction(array[index], used, enable)
	if !ok {
		return -1
	}
	return index
}

func nextIndex(index int, arrayNums int) int {
	index++
	if index >= arrayNums {
		index = 0
	}
	return index
}
