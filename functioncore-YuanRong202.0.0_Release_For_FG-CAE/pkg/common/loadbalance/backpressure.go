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

package loadbalance

import "sync"

// NodeRR provide RoundRobin for nodes in backpressure
type NodeRR struct {
	nodes   []string
	counter map[string]int
	index   map[string]int
	l       sync.RWMutex
}

// NewNodeRR create a new NodeRR
func NewNodeRR() *NodeRR {
	return &NodeRR{
		nodes:   make([]string, 0),
		counter: make(map[string]int),
		index:   make(map[string]int),
	}
}

// Add increase node count by name
func (n *NodeRR) Add(name string) {
	n.l.Lock()
	defer n.l.Unlock()
	if val, ok := n.counter[name]; ok {
		n.counter[name] = val + 1
	} else {
		n.counter[name] = 1
		n.index[name] = len(n.nodes)
		n.nodes = append(n.nodes, name)
	}
}

// Del decrease node count by name
func (n *NodeRR) Del(name string) {
	n.l.Lock()
	defer n.l.Unlock()
	val, ok := n.counter[name]
	if !ok {
		return
	}
	if val > 1 {
		n.counter[name] = val - 1
		return
	}
	delete(n.counter, name)
	i := 0
	for ; i < len(n.nodes); i++ {
		if n.nodes[i] == name {
			n.nodes = append(n.nodes[:i], n.nodes[i+1:]...)
			break
		}
	}
	length := len(n.nodes)
	for ; i < length; i++ {
		n.index[n.nodes[i]] = i
	}
}

// Next get next node name
func (n *NodeRR) Next(name string) string {
	n.l.RLock()
	defer n.l.RUnlock()
	if len(n.nodes) == 0 {
		return name
	}
	index := n.index[name] + 1
	if index >= len(n.nodes) {
		index = 0
	}
	return n.nodes[index]
}
