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

/*
Package scheduler schedule state instance to priority node

This is an algorithmic framework for resource application and calculates a specific node.
1. The framework supports schedule functions and function states.
   Currently, only basic algorithms for state allocation are supported.
2. Call method "GetPriorityNode" to get the specific nodeID.

The framework mainly contains two types of algorithms:
1. Predicate algorithm:
   Filter out some nodes that meet the requirements
2. Priority algorithm:
   Scoring all nodes, framework will eventually choose the highest scoring node
While add an algorithm, only need to inherit the abstract method and add it to the corresponding algorithm set map
in "init" method. The framework can pass the user's cached data in two algorithms.

The framework also records the type of resources allocated on all nodes
*/

// Package scheduler implements stateID and nodeID router scheduler
package scheduler

import (
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/scheduler/metadata"
	"functioncore/pkg/state-manager/scheduler/predicate"
	"functioncore/pkg/state-manager/scheduler/priority"
	"functioncore/pkg/state-manager/utils"
)

const (
	defaultFilterName = "overloadFilter"
	defaultScorerName = "naiveAverageScorer"
	defaultServiceID  = "default"
)

var (
	filter predicate.AbsPredicateAlgorithm
	scorer priority.AbsPriorityAlgorithm

	// minFunctionInstanceMap key: FunctionName  value: []NodeID
	minFunctionInstanceMap sync.Map
	// availableNodes nodes can be routed in etcd
	availableNodes sync.Map
)

// IsAvailableNode check if nodeID is available
func IsAvailableNode(nodeID string) bool {
	_, ok := availableNodes.Load(nodeID)
	return ok
}

// AddAvailableNodes add node info to availableNodes
func AddAvailableNodes(nodeID string, value []byte) {
	// parse map from value
	var nodeMap map[string]string
	err := json.Unmarshal(value, &nodeMap)
	if err != nil {
		log.GetLogger().Errorf("init AvailableNodeMap, Unmarshal err %s ", err.Error())
		return
	}
	if nodeID != "" {
		availableNodes.Store(nodeID, nodeMap)
		log.GetLogger().Infof("add one node info to availableNodes, nodeID: %s", nodeID)
	}
}

// DelAvailableNodes delete node info from availableNodes
func DelAvailableNodes(nodeID string) {
	availableNodes.Delete(nodeID)
	log.GetLogger().Infof("delete one node info from availableNodes, nodeID: %s", nodeID)
}

// GetPriorityNode get priority node
func GetPriorityNode(funcName string) (string, error) {
	log.GetLogger().Debugf("get priority node of %s", funcName)
	target, err := metadata.GetTargetInfo(funcName)
	if err != nil {
		return "", err
	}

	filteredNodes, err := getFilteredNodes(target)
	if err != nil {
		return "", err
	}

	// prefer the smallest function instance node
	instanceNodes := GetMinInstanceNode(funcName)
	nodeID := ""
	if instanceNodes != nil {
		nodeID = instanceNodes[0]
		DeleteInstanceMap(funcName, nodeID)
	}
	if nodeID == "" {
		nodeScores, err := scorer.Score(target, filteredNodes)
		if err != nil {
			return "", err
		}
		nodeID, err = selectNode(nodeScores)
		if err != nil {
			return "", err
		}
	}
	start := time.Now()
	metadata.AddNode(utils.GetServiceID(funcName), nodeID)
	log.GetLogger().Debugf("add node cost time: %v", time.Now().Sub(start))
	return nodeID, nil
}

// GetPriorityNodeWithoutFuncName gets priority node without funcName
func GetPriorityNodeWithoutFuncName() (string, error) {
	log.GetLogger().Debugf("get priority node without funcName")
	target := &metadata.TargetInfo{
		FuncName:  "function-task",
		StateType: "default",
	}

	filteredNodes, err := getFilteredNodes(target)
	if err != nil {
		return "", err
	}

	nodeID := ""
	nodeScores, err := scorer.Score(target, filteredNodes)
	if err != nil {
		return "", err
	}
	nodeID, err = selectNode(nodeScores)
	if err != nil {
		return "", err
	}
	start := time.Now()
	metadata.AddNode(target.StateType, nodeID)
	log.GetLogger().Debugf("add node cost time: %v", time.Now().Sub(start))
	return nodeID, nil
}

func getFilteredNodes(target *metadata.TargetInfo) ([]*metadata.NodeInfo, error) {
	nodes, err := metadata.GetNodes()
	if err != nil {
		return nil, err
	}
	// available nodes
	onlineNodes := make([]*metadata.NodeInfo, 0, len(nodes))
	availableNodes.Range(func(key, value interface{}) bool {
		nodeID, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to convert interface to string, key: %v", key)
			return false
		}
		onlineNodes = append(onlineNodes, &metadata.NodeInfo{ID: nodeID})
		return true
	})
	if filter == nil || scorer == nil {
		return nil, errors.New("scheduler is not initial")
	}
	filteredNodes, err := filter.Filter(target, onlineNodes)
	if err != nil {
		return nil, err
	}
	return filteredNodes, nil
}

// InitScheduler init scheduler
func InitScheduler(conf *config.FConfig) error {
	log.GetLogger().Infof("begin to init scheduler")
	if conf == nil {
		return errors.New("config is nil")
	}
	var err error
	realFilter := conf.ScheCfg.Filter.AlgorithmName
	realScorer := conf.ScheCfg.Scorer.AlgorithmName
	// Init filter, if not exist, using default
	if filter, err = predicate.NewPredicateAlgorithm(realFilter); err != nil {
		filter, err = predicate.NewPredicateAlgorithm(defaultFilterName)
		if err != nil {
			log.GetLogger().Errorf("init default filter error: %s", err.Error())
			return err
		}
		log.GetLogger().Infof("init default filter instead")
	}

	// Init scorer, if not exist, using default
	scorer, err = priority.NewPriorityAlgorithm(realScorer, conf.ScheCfg.Scorer)
	if err != nil {
		scorer, err = priority.NewPriorityAlgorithm(defaultScorerName, nil)
		if err != nil {
			log.GetLogger().Errorf("init default scorer error: %s", err.Error())
			return err
		}
		log.GetLogger().Infof("init default scorer instead")
	}

	// Init data synchronization
	err = metadata.InitSync(conf)
	if err != nil {
		log.GetLogger().Errorf("init data synchronization error: %s", err.Error())
		return err
	}
	return nil
}

// selectNode select the node with highest score
func selectNode(scores []*metadata.NodeScore) (string, error) {
	priorityScore := ""
	highestScore := int32(math.MinInt32)
	for idx := range scores {
		if scores[idx] == nil {
			continue
		}
		if scores[idx].Score > highestScore {
			highestScore = scores[idx].Score
			priorityScore = scores[idx].ID
		}
	}
	if priorityScore == "" {
		return "", errors.New("can't find available node")
	}
	log.GetLogger().Debugf("selected node: %v, score: %v", priorityScore, highestScore)
	return priorityScore, nil
}

// AddInstanceMap add instance to minFunctionInstanceMap
func AddInstanceMap(functionName string, nodeID string, eventValue []byte) {
	if functionName == "" {
		return
	}
	var valueMap map[string]string
	err := json.Unmarshal(eventValue, &valueMap)
	if err != nil {
		log.GetLogger().Errorf("failed to add function instance to Map, Unmarshal err %s", err.Error())
		return
	}
	// MinInstanceNode is determined by the existence of the ownerIP field
	if ownerIP, ok := valueMap["ownerIP"]; ok && ownerIP != "" {
		return
	}
	nodes := GetMinInstanceNode(functionName)
	for _, item := range nodes {
		if item == nodeID {
			return
		}
	}
	nodes = append(nodes, nodeID)
	minFunctionInstanceMap.Store(functionName, nodes)
}

// DeleteInstanceMap delete instance from minFunctionInstanceMap
func DeleteInstanceMap(functionName string, nodeID string) {
	nodes := GetMinInstanceNode(functionName)
	tmp := make([]string, 0, len(nodes))
	deleteKey := true
	for _, item := range nodes {
		if item != nodeID {
			tmp = append(tmp, item)
			deleteKey = false
		}
	}
	if deleteKey {
		minFunctionInstanceMap.Delete(functionName)
		return
	}
	minFunctionInstanceMap.Store(functionName, tmp)
}

// GetMinInstanceNode get instance from minFunctionInstanceMap
func GetMinInstanceNode(functionName string) []string {
	if value, ok := minFunctionInstanceMap.Load(functionName); ok {
		return value.([]string)
	}
	return nil
}
