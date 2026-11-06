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

// Package predicate provide predicate algorithms
package predicate

import (
	"fmt"
)

const (
	maxAlgorithmsNumber = 10
)

var (
	algorithms map[string]AbsPredicateAlgorithm
)

func init() {
	algorithms = make(map[string]AbsPredicateAlgorithm, maxAlgorithmsNumber)
	// Register all predicate algorithms here
	algorithms[overloadFilterName] = &overloadFilter{}
}

// NewPredicateAlgorithm new a predicate algorithm
func NewPredicateAlgorithm(name string) (AbsPredicateAlgorithm, error) {
	algorithm, ok := algorithms[name]
	if ok {
		return algorithm, nil
	}
	return nil, fmt.Errorf("predicate algorithm: %s not exist", name)
}
