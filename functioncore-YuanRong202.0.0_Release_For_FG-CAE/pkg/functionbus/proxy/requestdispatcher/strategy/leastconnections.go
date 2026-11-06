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

import "math"

// LeastConnections select least connects index from instanceArray
func LeastConnections[T, M any](array []T, used M, countConnections func(T, M) int, checkFunction func(T, M) bool) int {
	index := findLeastCons(array, used, countConnections)
	if index < 0 {
		return index
	}
	// find not leastConnections then return -1
	if checkFunction(array[index], used) {
		return index
	} else {
		return -1
	}
}

func findLeastCons[T, M any](array []T, used M, countConnections func(T, M) int) int {
	minConnections := math.MaxInt64
	index := -1
	for i, t := range array {
		connectionsNum := countConnections(t, used)
		if connectionsNum < minConnections {
			minConnections = connectionsNum
			index = i
		}
	}
	return index
}
