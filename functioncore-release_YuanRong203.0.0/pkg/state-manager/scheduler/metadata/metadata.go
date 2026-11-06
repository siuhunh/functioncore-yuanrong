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

// Package metadata cache node info here
package metadata

import (
	"errors"
	"sync"

	"functioncore/pkg/common/logger/log"
	rm "functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/utils"
)

const (
	defaultNodeAmount = 128
)

var (
	affinityTable = struct {
		sync.RWMutex
		// dataType : (nodeID1, nodeID2...)
		table map[string]map[string]struct{}
	}{}
	getStateIDFunc func(string) (map[string]struct{}, error)

	stateLoadTable = struct {
		sync.RWMutex
		directLoad   map[string]float32
		indirectLoad map[string]float32
	}{}
)

// TargetInfo is information about target state instance
type TargetInfo struct {
	// FuncName is the name of the function which will use this state instance
	FuncName string
	// StateType is state type of this state instance
	StateType string
}

// NodeInfo is information about a node
type NodeInfo struct {
	ID string
	// Used to transfer data between predicate and priority
	Cache interface{}
}

// NodeScore stores score of a node
type NodeScore struct {
	ID    string
	Score int32
}

func init() {
	affinityTable.table = make(map[string]map[string]struct{})
	stateLoadTable.directLoad = make(map[string]float32, defaultNodeAmount)
	stateLoadTable.indirectLoad = make(map[string]float32, defaultNodeAmount)
}

// Register get stateID function
func Register(callback func(string) (map[string]struct{}, error)) {
	getStateIDFunc = callback
}

// GetTargetInfo get target state instance info
func GetTargetInfo(funcName string) (*TargetInfo, error) {
	target := &TargetInfo{FuncName: funcName}
	target.StateType = utils.GetServiceID(funcName)
	if target.StateType == "" {
		return nil, errors.New("state type not bind")
	}
	return target, nil
}

// GetNodes get all the nodes's info in cluster
func GetNodes() ([]*NodeInfo, error) {
	// Get node id firstly
	nodeIDs, err := rm.GetAllNodes()
	if err != nil {
		return nil, err
	}
	// Construct node infos
	nodeLen := len(nodeIDs)
	nodes := make([]*NodeInfo, nodeLen, nodeLen)
	for idx := range nodes {
		nodes[idx] = &NodeInfo{}
		nodes[idx].ID = nodeIDs[idx]
	}
	return nodes, nil
}

// GetNodeResource get resource info of node
func GetNodeResource(nodeID string) (*rm.NodeResourceInfo, error) {
	return rm.GetAllResource(nodeID)
}

// GetNodeAssignedData get assigned data of node
func GetNodeAssignedData(nodeID string) (map[string]struct{}, error) {
	if getStateIDFunc == nil {
		return nil, errors.New("get stateID func nil")
	}
	nodeIDs, err := getStateIDFunc(nodeID)
	if err != nil {
		return nil, err
	}
	return nodeIDs, nil
}

// GetAffinityNode get affinity node
// How to clear the table cache when stateType is deleted?
func GetAffinityNode(dataType string) (map[string]struct{}, error) {
	affinityTable.RLock()
	defer affinityTable.RUnlock()

	nodes, ok := affinityTable.table[dataType]
	if !ok {
		return nil, errors.New("can't find node of this data type")
	}
	newNodes := make(map[string]struct{}, len(nodes))
	for id := range nodes {
		newNodes[id] = struct{}{}
	}

	return newNodes, nil
}

// AddNode add nodeID to stateType in table
func AddNode(dataType string, nodeID string) {
	affinityTable.Lock()
	nodes, ok := affinityTable.table[dataType]
	if !ok || nodes == nil {
		affinityTable.table[dataType] = make(map[string]struct{})
	}
	affinityTable.table[dataType][nodeID] = struct{}{}
	affinityTable.Unlock()
}

// GetDirectStateLoad get direct state load
func GetDirectStateLoad(nodeID string) float32 {
	stateLoadTable.RLock()
	defer stateLoadTable.RUnlock()

	if stateLoadTable.directLoad == nil {
		log.GetLogger().Warnf("state load table is nil")
		return 0
	}
	value, ok := stateLoadTable.directLoad[nodeID]
	if !ok {
		log.GetLogger().Debugf("no direct state load of %s", nodeID)
		return 0
	}
	return value
}

// GetIndirectStateLoad get indirect state load
func GetIndirectStateLoad(nodeID string) float32 {
	stateLoadTable.RLock()
	defer stateLoadTable.RUnlock()

	if stateLoadTable.indirectLoad == nil {
		log.GetLogger().Warnf("state load table is nil")
		return 0
	}
	value, ok := stateLoadTable.indirectLoad[nodeID]
	if !ok {
		log.GetLogger().Debugf("no indirect state load of %s", nodeID)
		return 0
	}
	return value
}
