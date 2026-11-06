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

// Package priority provide priority algorithms
package priority

import "fmt"

const (
	maxScorerNumber = 10
)

var (
	algorithms map[string]AbsPriorityAlgorithm
)

func init() {
	algorithms = make(map[string]AbsPriorityAlgorithm, maxScorerNumber)
	// Register all predicate algorithms here
	algorithms[naiveAverageScorerName] = &naiveAverageScorer{}
	algorithms[directStateLoadScorerName] = &directStateLoadScorer{}
	algorithms[indirectStateLoadScorerName] = &indirectStateLoadScorer{}
	// Add new predicate algorithms here...
}

// NewPriorityAlgorithm new a priority algorithm
func NewPriorityAlgorithm(name string, conf interface{}) (AbsPriorityAlgorithm, error) {
	algorithm, ok := algorithms[name]
	if !ok {
		return nil, fmt.Errorf("priority algorithm %s not exist", name)
	}
	err := algorithm.Init(conf)
	if err != nil {
		return nil, err
	}
	return algorithm, nil
}
