/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistentHashWithVirtualNode consistent hash with virtual node
// NOTE: this struct is NOT thread-safe
type ConsistentHashWithVirtualNode struct {
	replica int
	keys    []int
	hash    map[int]*Node
}

// NewConsistentHashWithVirtualNode create consistent hash with replica size of virtual node
func NewConsistentHashWithVirtualNode(replica int) *ConsistentHashWithVirtualNode {
	return &ConsistentHashWithVirtualNode{
		replica: replica,
		hash:    make(map[int]*Node),
	}
}

// Add adds node to hash ring
func (c *ConsistentHashWithVirtualNode) Add(node *Node) error {
	for i := 0; i < c.replica; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(node.Key + "_" + strconv.Itoa(i))))
		if _, ok := c.hash[hash]; ok {
			continue
		}
		c.keys = append(c.keys, hash)
		c.hash[hash] = node
	}
	sort.Ints(c.keys)
	return nil
}

// Next -
func (c *ConsistentHashWithVirtualNode) Next(key string) *Node {
	if len(c.keys) == 0 {
		return nil
	}

	keyHash := int(crc32.ChecksumIEEE([]byte(key)))
	hash := c.keys[c.search(keyHash)]
	return c.hash[hash]
}

func (c *ConsistentHashWithVirtualNode) search(key int) int {
	idx := sort.Search(len(c.keys), func(i int) bool {
		if i >= len(c.keys) {
			return false
		}
		return c.keys[i] > key
	})
	if idx >= len(c.keys) {
		idx = 0
	}
	return idx
}

// Delete delete node from hash ring
func (c *ConsistentHashWithVirtualNode) Delete(key string) *Node {
	var deleted *Node

	for i := 0; i < c.replica; i++ {
		hash := int(crc32.ChecksumIEEE([]byte(key + "_" + strconv.Itoa(i))))
		val, ok := c.hash[hash]
		if !ok || val.Key != key {
			continue
		}
		delete(c.hash, hash)
		deleted = val
		for j, h := range c.keys {
			if h == hash {
				c.keys = append(c.keys[:j], c.keys[j+1:]...)
				break
			}
		}
	}
	return deleted
}

// Node -
type Node struct {
	Obj  interface{}
	Key  string
	hash uint32
}

// NoLockLoadBalance -
type NoLockLoadBalance interface {
	Add(node *Node) error
	Next(key string) *Node
	Delete(nodeKey string) *Node
}

// CreateNoLockLB -
func CreateNoLockLB() NoLockLoadBalance {
	return &ConsistentHash{
		nodes: make([]*Node, 0),
		cache: createHashCache(),
	}
}

type nodeSlice []*Node

// Len returns the size
func (s nodeSlice) Len() int {
	return len(s)
}

// Swap will swap two elements
func (s nodeSlice) Swap(i, j int) {
	if i < 0 || i >= len(s) || j < 0 || j >= len(s) {
		return
	}
	s[i], s[j] = s[j], s[i]
}

// Less returns true if i less than j
func (s nodeSlice) Less(i, j int) bool {
	if i < 0 || i >= len(s) || j < 0 || j >= len(s) {
		return false
	}
	return s[i].hash < s[j].hash
}

// ConsistentHash -
type ConsistentHash struct {
	cache *hashCache
	nodes nodeSlice
}

// Add -
func (c *ConsistentHash) Add(newNode *Node) error {
	newNode.hash = getHashKeyCRC32(newNode.Key)
	for _, node := range c.nodes {
		if node.Key == newNode.Key {
			return errors.New("node already exist")
		}
		if node.hash == newNode.hash {
			return errors.New("node hash already exist")
		}
	}

	c.nodes = append(c.nodes, newNode)
	sort.Sort(c.nodes)
	return nil
}

// Next -
func (c *ConsistentHash) Next(key string) *Node {
	if len(c.nodes) == 0 {
		return nil
	}

	keyHash := c.cache.getHash(key)
	index := c.search(keyHash)
	return c.nodes[index]
}

func (c *ConsistentHash) search(keyHash uint32) int {
	f := func(x int) bool {
		if x >= len(c.nodes) {
			return false
		}
		return c.nodes[x].hash > keyHash
	}
	index := sort.Search(len(c.nodes), f)
	if index >= len(c.nodes) {
		return 0
	}
	return index
}

// Delete -
func (c *ConsistentHash) Delete(nodeKey string) *Node {
	for i, node := range c.nodes {
		if node.Key == nodeKey {
			c.nodes = append(c.nodes[:i], c.nodes[i+1:]...)
			return node
		}
	}
	return nil
}
