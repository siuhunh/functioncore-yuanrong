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

// Package utils gets function services from URNs
package utils

import "math"

const min = 0.00001

// FloatAccuracy float accuracy
var FloatAccuracy Accuracy = func() float64 {
	return min
}

// Accuracy accuracy
type Accuracy func() float64

// Equal check if a is equal b return true
func (ac Accuracy) Equal(a, b float64) bool {
	return math.Abs(a-b) < ac()
}

// Greater check if a is greater b return true
func (ac Accuracy) Greater(a, b float64) bool {
	return math.Max(a, b) == a && math.Abs(a-b) > ac()
}

// Smaller check if a is smaller b return true
func (ac Accuracy) Smaller(a, b float64) bool {
	return math.Max(a, b) == b && math.Abs(a-b) > ac()
}

// GreaterOrEqual check if a is greater or equal b return true
func (ac Accuracy) GreaterOrEqual(a, b float64) bool {
	return math.Max(a, b) == a || math.Abs(a-b) < ac()
}

// SmallerOrEqual check if a is smaller or equal b return true
func (ac Accuracy) SmallerOrEqual(a, b float64) bool {
	return math.Max(a, b) == b || math.Abs(a-b) < ac()
}
