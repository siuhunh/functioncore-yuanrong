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

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/scheduler/metadata"
	"functioncore/pkg/state-manager/scheduler/predicate"
)

const (
	indirectStateLoadScorerName = "indirectStateLoadScorer"
)

type indirectStateLoadScorer struct {
	cpuScoreFactor   float32
	memScoreFactor   float32
	stateScoreFactor float32
	affinityFactor   float32
}

// Init with config
func (s *indirectStateLoadScorer) Init(conf interface{}) error {
	log.GetLogger().Infof("init indirectStateLoadScorer")
	const maxFactor = 1.0
	cfg, ok := conf.(config.Scorer)
	if !ok {
		log.GetLogger().Warnf("init failed, config is empty")
		return nil
	}
	check := checkFactor(cfg.CPUScoreFactor, cfg.MemScoreFactor, cfg.StateScoreFactor)
	if cfg.AffinityFactor > maxFactor || !check {
		log.GetLogger().Warnf("args error: state: %v, cpu: %v, mem: %v, using default",
			cfg.StateScoreFactor, cfg.CPUScoreFactor, cfg.MemScoreFactor)
		return nil
	}
	s.affinityFactor = cfg.AffinityFactor
	s.stateScoreFactor = cfg.StateScoreFactor
	s.cpuScoreFactor = cfg.CPUScoreFactor
	s.memScoreFactor = cfg.MemScoreFactor
	return nil
}

// The algorithm takes into account CPU, memory usage and state load
// Similar to directStateLoadScorerName
func (s *indirectStateLoadScorer) Score(
	target *metadata.TargetInfo, infos []*metadata.NodeInfo) ([]*metadata.NodeScore, error) {
	if target == nil {
		return nil, errors.New("target info is empty")
	}
	scores := make([]*metadata.NodeScore, len(infos), len(infos))
	curRealMaxLoad := s.getMaxIndirectStateLoad(infos)

	for idx := range infos {
		if infos[idx] == nil {
			log.GetLogger().Warnf("nodeInfo is nil of %v", idx)
			continue
		}
		scores[idx] = &metadata.NodeScore{
			ID:    infos[idx].ID,
			Score: 0,
		}
		nodes, err := metadata.GetAffinityNode(target.StateType)
		if err == nil {
			if _, ok := nodes[infos[idx].ID]; ok {
				score := affinityScore * s.affinityFactor
				scores[idx].Score += int32(score)
			}
		}
		log.GetLogger().Debugf("get affinity node result %v", err)

		if cache := infos[idx].Cache; cache != nil {
			// The higher the memory usage rate, the lower the score
			memScore := maxMemScore * (1 - cache.(predicate.ResourceUsage).MEMRatio)
			scores[idx].Score += int32(memScore * s.memScoreFactor)

			// The higher the cpu usage rate, the lower the score
			cpuScore := maxCPUScore * (1 - cache.(predicate.ResourceUsage).CPURatio)
			scores[idx].Score += int32(cpuScore * s.cpuScoreFactor)

			log.GetLogger().Debugf("nodeID: %v, cpuScore: %v, memScore: %v",
				infos[idx].ID, cpuScore, memScore)
		}

		stateScore := maxStateLoad
		curLoad := metadata.GetIndirectStateLoad(infos[idx].ID)
		if curLoad != 0 && !isZero(curRealMaxLoad) {
			stateScore -= curLoad * maxStateLoad / curRealMaxLoad
		}
		scores[idx].Score += int32(stateScore * s.stateScoreFactor)
		log.GetLogger().Debugf("nodeID: %v, stateScore: %v", infos[idx].ID, stateScore)
		log.GetLogger().Debugf("nodeID: %v, Score: %v", infos[idx].ID, scores[idx].Score)
	}
	return scores, nil
}

func (s *indirectStateLoadScorer) getMaxIndirectStateLoad(infos []*metadata.NodeInfo) float32 {
	var maxLoad float32
	for idx := range infos {
		if infos[idx] == nil {
			continue
		}
		curLoad := metadata.GetIndirectStateLoad(infos[idx].ID)
		log.GetLogger().Debugf("nodeID: %v, curLoad: %v", infos[idx].ID, curLoad)
		if curLoad > maxLoad {
			maxLoad = curLoad
		}
	}
	log.GetLogger().Debugf("maxLoad: %f", maxLoad)
	return maxLoad
}

func isZero(number float32) bool {
	const tolerance = 0.01
	if number-0 < tolerance {
		return true
	}
	return false
}
