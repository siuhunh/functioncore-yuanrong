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

// Package loadbalance provides consistent hash alogrithm
package loadbalance

import (
	"hash/crc32"
	"sort"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
)

const (
	// MaxInstanceSize is the max instance size be stored in hash ring
	MaxInstanceSize = 100
	defaultMapSize  = 100
)

type uint32Slice []uint32

// Len returns the size
func (u uint32Slice) Len() int {
	return len(u)
}

// Swap will swap two elements
func (u uint32Slice) Swap(i, j int) {
	if i < 0 || i >= len(u) || j < 0 || j >= len(u) {
		return
	}
	u[i], u[j] = u[j], u[i]
}

// Less returns true if i less than j
func (u uint32Slice) Less(i, j int) bool {
	if i < 0 || i >= len(u) || j < 0 || j >= len(u) {
		return false
	}
	return u[i] < u[j]
}

type anchorInfo struct {
	instanceHash uint32
	instanceKey  string
}

// CHGeneric is the generic consistent hash
type CHGeneric struct {
	anchorPoint map[string]*anchorInfo
	instanceMap map[uint32]string
	hashPool    uint32Slice
	insMutex    sync.RWMutex
	anchorMutex sync.Mutex
}

// NewCHGeneric creates generic consistent hash
func NewCHGeneric() *CHGeneric {
	return &CHGeneric{
		hashPool:    make([]uint32, 0, MaxInstanceSize),
		instanceMap: make(map[uint32]string, defaultMapSize),
		anchorPoint: make(map[string]*anchorInfo, defaultMapSize),
	}
}

// Done -
func (c *CHGeneric) Done(node interface{}, cost time.Duration) {}

// Next returns the next scheduled node of a function
func (c *CHGeneric) Next(name string, move bool) interface{} {
	c.anchorMutex.Lock()
	anchor, exist := c.anchorPoint[name]
	if !exist {
		anchor = c.addAnchorPoint(name)
		c.anchorMutex.Unlock()
		return anchor.instanceKey
	}
	c.insMutex.RLock()
	if move {
		c.moveAnchorPoint(name, anchor.instanceHash)
	}
	_, exist = c.instanceMap[anchor.instanceHash]
	c.insMutex.RUnlock()
	// check if node still exists, no maxReqCount limitation
	if !exist {
		c.moveAnchorPoint(name, anchor.instanceHash)
	}
	c.anchorMutex.Unlock()
	return anchor.instanceKey
}

// Add will add a node into hash ring
func (c *CHGeneric) Add(node interface{}, weight int) {
	c.insMutex.Lock()
	defer c.insMutex.Unlock()
	name, ok := node.(string)
	if !ok {
		log.GetLogger().Errorf("unable to convert %T to string", node)
		return
	}
	hashKey := getHashKeyCRC32(name)
	_, exist := c.instanceMap[hashKey]
	if exist {
		return
	}
	c.instanceMap[hashKey] = name
	c.hashPool = append(c.hashPool, hashKey)
	sort.Sort(c.hashPool)
	log.GetLogger().Infof("add node %s to hash ring", name)
}

// Remove will remove a node from hash ring
func (c *CHGeneric) Remove(node interface{}) {
	name, ok := node.(string)
	if !ok {
		log.GetLogger().Errorf("unable to convert %T to string", node)
	}
	hashKey := getHashKeyCRC32(name)
	c.insMutex.Lock()
	delete(c.instanceMap, hashKey)
	for i, hash := range c.hashPool {
		if hash == hashKey {
			copy(c.hashPool[i:], c.hashPool[i+1:])
			c.hashPool[len(c.hashPool)-1] = 0
			c.hashPool = c.hashPool[:len(c.hashPool)-1]
			break
		}
	}
	log.GetLogger().Infof("delete node %s from hash ring", name)
	c.insMutex.Unlock()
	return
}

// RemoveAll will remove all nodes from hash ring
func (c *CHGeneric) RemoveAll() {
	c.insMutex.Lock()
	c.hashPool = make([]uint32, 0, MaxInstanceSize)
	c.instanceMap = make(map[uint32]string, defaultMapSize)
	c.insMutex.Unlock()
	return
}

// Reset will clean all anchor infos
func (c *CHGeneric) Reset() {
	c.anchorMutex.Lock()
	c.anchorPoint = make(map[string]*anchorInfo, defaultMapSize)
	c.anchorMutex.Unlock()
	return
}

func (c *CHGeneric) addAnchorPoint(name string) *anchorInfo {
	// need to be called in a thread safe context
	hashKey := getHashKeyCRC32(name)
	c.insMutex.RLock()
	instanceHash := c.getNextHashKey(hashKey)
	c.insMutex.RUnlock()
	newAnchor := &anchorInfo{
		instanceHash: instanceHash,
		instanceKey:  c.instanceMap[instanceHash],
	}
	c.anchorPoint[name] = newAnchor
	return newAnchor
}

func (c *CHGeneric) moveAnchorPoint(name string, curHash uint32) {
	c.insMutex.RLock()
	instanceHash := c.getNextHashKey(curHash)
	c.anchorPoint[name].instanceHash = instanceHash
	c.anchorPoint[name].instanceKey = c.instanceMap[instanceHash]
	c.insMutex.RUnlock()
}

func (c *CHGeneric) getNextHashKey(hashKey uint32) uint32 {
	// need to be called with insMutex locked
	if len(c.hashPool) == 0 {
		return 0
	}
	nextHashKey := c.hashPool[0]
	for _, v := range c.hashPool {
		if v > hashKey {
			nextHashKey = v
			break
		}
	}
	log.GetLogger().Infof("next hash key: %d, instance: %s", nextHashKey, c.instanceMap[nextHashKey])
	return nextHashKey
}

// NextWithRequest -
func (c *CHGeneric) NextWithRequest(req *Request, move bool) interface{} {
	return c.Next(req.Name, move)
}

// SetConcurrency -
func (c *CHGeneric) SetConcurrency(concurrency int) {}

// SetIsLongRun -
func (c *CHGeneric) SetIsLongRun(isLongRun bool) {}

// SetIsCaeFunc -
func (c *CHGeneric) SetIsCaeFunc(isCaeFunc bool) {}

// Start -
func (c *CHGeneric) Start() {}

// Stop -
func (c *CHGeneric) Stop() {}

// NoLock -
func (c *CHGeneric) NoLock() bool {
	return false
}

func getHashKeyCRC32(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// NewConcurrentCHGeneric return ConcurrentCHGeneric with given concurrency
func NewConcurrentCHGeneric(concurrency int) *ConcurrentCHGeneric {
	return &ConcurrentCHGeneric{
		CHGeneric:   NewCHGeneric(),
		concurrency: concurrency,
		counter:     make(map[string]*concurrentCounter),
	}
}

type concurrentCounter struct {
	count int
	last  time.Time
}

// ConcurrentCHGeneric is concurrency balanced
type ConcurrentCHGeneric struct {
	*CHGeneric
	counter     map[string]*concurrentCounter
	countMutex  sync.Mutex
	concurrency int
}

// Next returns the next scheduled node
func (c *ConcurrentCHGeneric) Next(name string, move bool) interface{} {
	c.countMutex.Lock()
	defer c.countMutex.Unlock()
	l, ok := c.counter[name]
	if !ok {
		c.counter[name] = &concurrentCounter{
			last: time.Now(),
		}
		return c.CHGeneric.Next(name, move)
	}
	l.count++
	if l.count >= c.concurrency {
		now := time.Now()
		l.count = 0
		if now.Sub(l.last) < 1*time.Second {
			move = true
		}
		l.last = now
	}
	return c.CHGeneric.Next(name, move)
}

// Add add a node to hash ring
func (c *ConcurrentCHGeneric) Add(node interface{}, weight int) {
	c.CHGeneric.Add(node, weight)
}

// Remove remove a node from hash ring
func (c *ConcurrentCHGeneric) Remove(node interface{}) {
	c.countMutex.Lock()
	defer c.countMutex.Unlock()
	delete(c.counter, node.(string))
	c.CHGeneric.Remove(node)
}

// RemoveAll remove all nodes from hash ring
func (c *ConcurrentCHGeneric) RemoveAll() {
	c.countMutex.Lock()
	defer c.countMutex.Unlock()
	c.counter = make(map[string]*concurrentCounter)
	c.CHGeneric.RemoveAll()
}

// Reset clean all anchor infos and counters
func (c *ConcurrentCHGeneric) Reset() {
	c.countMutex.Lock()
	defer c.countMutex.Unlock()
	c.counter = make(map[string]*concurrentCounter)
	c.CHGeneric.Reset()
}
