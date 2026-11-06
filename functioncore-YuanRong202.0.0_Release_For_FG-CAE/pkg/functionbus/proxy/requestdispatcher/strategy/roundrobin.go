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
func RoundRobin[T, M any](index int, array []T, used M, checkFunction func(T, M) bool) int {
	arrayNums := len(array)
	if arrayNums == 0 {
		return -1
	}
	for i := 0; i < len(array); i++ {
		index = nextIndex(index, arrayNums)
		if checkFunction(array[index], used) {
			return index
		}
	}
	if !checkFunction(array[index], used) {
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
