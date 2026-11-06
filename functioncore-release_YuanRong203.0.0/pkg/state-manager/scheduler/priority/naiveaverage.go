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

package priority

import (
	"errors"
	"math"
	"math/rand"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/scheduler/metadata"
)

const (
	naiveAverageScorerName = "naiveAverageScorer"
	sameStateTypeScore     = 100
	leastStateInsScore     = 50
	randScoreRange         = 10
)

type naiveAverageScorer struct{}

// Init with config
func (f *naiveAverageScorer) Init(conf interface{}) error {
	return nil
}

// The node with same state type will get "sameStateTypeScore" score,
// the first node with least state instance will get "leastStateInsScore" score,
// others get 0
func (f *naiveAverageScorer) Score(
	target *metadata.TargetInfo, infos []*metadata.NodeInfo) ([]*metadata.NodeScore, error) {
	if target == nil {
		return nil, errors.New("target info is empty")
	}
	scores := make([]*metadata.NodeScore, len(infos), len(infos))
	minIns := math.MaxInt32
	minIdx := -1
	for idx := range infos {
		if infos[idx] == nil {
			log.GetLogger().Warnf("nodeInfo is nil of %d", idx)
			continue
		}
		scores[idx] = &metadata.NodeScore{
			ID:    infos[idx].ID,
			Score: rand.Int31n(randScoreRange),
		}
		nodes, err := metadata.GetAffinityNode(target.StateType)
		if err != nil {
			log.GetLogger().Debugf("failed to get affinity node, error %s", err.Error())
		} else {
			_, ok := nodes[infos[idx].ID]
			if ok {
				scores[idx].Score += sameStateTypeScore
			}
		}
		ins, err := metadata.GetNodeAssignedData(infos[idx].ID)
		if err != nil {
			log.GetLogger().Warnf("failed to get state instance, error: %s", err.Error())
			continue
		}
		log.GetLogger().Debugf("get %d instance in %s", len(ins), infos[idx].ID)
		if len(ins) < minIns {
			minIns = len(ins)
			minIdx = idx
		}
	}
	if minIdx != -1 {
		scores[minIdx].Score += leastStateInsScore
	}
	return scores, nil
}
