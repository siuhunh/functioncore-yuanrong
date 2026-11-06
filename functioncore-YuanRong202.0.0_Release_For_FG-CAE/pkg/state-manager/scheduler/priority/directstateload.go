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
	directStateLoadScorerName = "directStateLoadScorer"

	maxStateLoad  float32 = 100
	maxCPUScore   float32 = 100
	maxMemScore   float32 = 100
	affinityScore float32 = 100
)

// default cpuScoreFactor is 0.0
// default memScoreFactor is 0.0
// default stateScoreFactor is 1.0
// default affinityFactor is 0.3
type directStateLoadScorer struct {
	cpuScoreFactor   float32
	memScoreFactor   float32
	stateScoreFactor float32
	affinityFactor   float32
}

// Init with config
func (s *directStateLoadScorer) Init(conf interface{}) error {
	log.GetLogger().Infof("begin to init directStateLoadScorer")
	const maxFactor = 1.0
	cfg, ok := conf.(config.Scorer)
	if !ok {
		log.GetLogger().Warnf("failed to init directStateLoadScorer, using default args")
		return nil
	}
	if cfg.AffinityFactor > maxFactor || !checkFactor(cfg.CPUScoreFactor, cfg.MemScoreFactor, cfg.StateScoreFactor) {
		log.GetLogger().Warnf("check args error: cpu: %v, mem: %v, state: %v, using default",
			cfg.CPUScoreFactor, cfg.MemScoreFactor, cfg.StateScoreFactor)
		return nil
	}
	s.cpuScoreFactor = cfg.CPUScoreFactor
	s.memScoreFactor = cfg.MemScoreFactor
	s.stateScoreFactor = cfg.StateScoreFactor
	s.affinityFactor = cfg.AffinityFactor

	return nil
}

// The algorithm takes into account CPU, memory usage and state load
func (s *directStateLoadScorer) Score(
	target *metadata.TargetInfo, infos []*metadata.NodeInfo) ([]*metadata.NodeScore, error) {
	if target == nil {
		return nil, errors.New("target info is empty")
	}
	scores := make([]*metadata.NodeScore, len(infos), len(infos))
	for idx := range infos {
		if infos[idx] == nil {
			log.GetLogger().Warnf("nodeInfo is nil of %d", idx)
			continue
		}
		scores[idx] = &metadata.NodeScore{
			ID:    infos[idx].ID,
			Score: 0,
		}
		nodes, err := metadata.GetAffinityNode(target.StateType)
		if err != nil {
			log.GetLogger().Debugf("failed to get affinity node, error: %s", err.Error())
		} else {
			_, ok := nodes[infos[idx].ID]
			if ok {
				score := affinityScore * s.affinityFactor
				scores[idx].Score += int32(score)
			}
		}

		if cache := infos[idx].Cache; cache != nil {
			// The higher the cpu usage rate, the lower the score
			cpuScore := maxCPUScore * (1 - cache.(predicate.ResourceUsage).CPURatio)
			scores[idx].Score += int32(cpuScore * s.cpuScoreFactor)

			// The higher the memory usage rate, the lower the score
			memScore := maxMemScore * (1 - cache.(predicate.ResourceUsage).MEMRatio)
			scores[idx].Score += int32(memScore * s.memScoreFactor)

			log.GetLogger().Debugf("nodeID: %v, cpuScore: %v, memScore: %v",
				infos[idx].ID, cpuScore, memScore)
		}

		stateScore := maxStateLoad - metadata.GetDirectStateLoad(infos[idx].ID)
		scores[idx].Score += int32(stateScore * s.stateScoreFactor)
		log.GetLogger().Debugf("nodeID: %v, stateScore: %v", infos[idx].ID, stateScore)
		log.GetLogger().Debugf("nodeID: %v, Score: %v", infos[idx].ID, scores[idx].Score)
	}
	return scores, nil
}

func checkFactor(cpuFactor, memFactor, stateFactor float32) bool {
	const normalized = 1.0
	const tolerance = 0.01
	total := cpuFactor + memFactor + stateFactor
	if total-normalized < tolerance {
		return true
	}
	return false
}
