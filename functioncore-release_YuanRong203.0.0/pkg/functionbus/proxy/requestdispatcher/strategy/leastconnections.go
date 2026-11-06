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

import (
	"math"

	"functioncore/pkg/common/logger/log"
)

// LeastConnections select least connects index from instanceArray
func LeastConnections[T, M, N any](array []T, used M, enable N,
	countConnections func(T, M, N) int, checkFunction func(T, M, N) (bool, string, string)) int {
	minConnections := math.MaxInt64
	index := -1
	checkResult := make(map[string]string)
	for i, t := range array {
		ok, obj, reason := checkFunction(array[i], used, enable)
		if !ok {
			checkResult[obj] = reason
			continue
		}
		connectionsNum := countConnections(t, used, enable)
		if connectionsNum < minConnections {
			minConnections = connectionsNum
			index = i
		}
	}
	if len(checkResult) > 0 && index == -1 {
		log.GetLogger().Warnf("least conn schedule failed:%#v", checkResult)
	}
	return index
}
