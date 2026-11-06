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

package utils

import (
	"bufio"
	"container/list"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"functioncore/pkg/common/logger/log"
)

const (
	memStatPath          = "/sys/fs/cgroup/memory/memory.stat"
	hierarchicalMem      = "hierarchical_memory_limit"
	memContextLen        = 2
	cachePodDefaultMem   = 4
	defaultMaxUsePercent = 0.2
	float64BitSize       = 64
)

var maxUsePercent = parseMaxUsePercent()

func parseMaxUsePercent() float64 {
	env := os.Getenv("MAX_USE_PERCENT")
	if env == "" {
		return defaultMaxUsePercent
	}
	val, err := strconv.ParseFloat(env, float64BitSize)
	if err != nil || val <= 0 {
		return defaultMaxUsePercent
	}
	return val
}

// The maximum default value of cacheMem is 80% of that of 4 Gi.
var defaultCacheMemMax float64 = cachePodDefaultMem * maxUsePercent * 1024 * 1024 * 1024

// LoadFunc is func that load value if lru cache missed
type LoadFunc func(key interface{}, isLayer bool) (value interface{}, size int64, err error)

// Lru is the lru structure of cacheMap.
type Lru struct {
	list        *list.List
	cache       *sync.Map
	lock        *sync.Mutex
	cacheMemMax int64
	codeSize    int64
	load        LoadFunc
}

// LruOption option for LRU cache
type LruOption interface {
	apply(*Lru)
}

// WithLRULoadFunc return option that apply load function to lru
func WithLRULoadFunc(f LoadFunc) LruOption {
	return &lruLoadOpt{load: f}
}

type lruLoadOpt struct {
	load LoadFunc
}

func (llo *lruLoadOpt) apply(lru *Lru) {
	lru.load = llo.load
}

// ListNode is the list unit structure of the LRU.
type ListNode struct {
	key     interface{}
	val     interface{}
	valSize int64
}

func getMemFromLine(line string, field string) int64 {
	if strings.Contains(line, field) {
		context := strings.Fields(line)
		if len(context) == memContextLen {
			if res, err := strconv.ParseInt(context[1], 10, 64); err == nil {
				return int64(float64(res) * maxUsePercent)
			}
		}
		return 0
	}
	return -1
}

func getMemFromStat(field string) int64 {
	file, err := os.Open(memStatPath)
	if err != nil {
		log.GetLogger().Warnf("failed to open the file")
		return 0
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.GetLogger().Warnf("failed to read line: %s", err.Error())
			return 0
		}
		line = strings.TrimSpace(line)
		if res := getMemFromLine(line, field); res != -1 {
			return res
		}
	}
	return 0
}

func getMemcacheMemMax() int64 {
	cacheMemMax := getMemFromStat(hierarchicalMem)
	if cacheMemMax == 0 {
		log.GetLogger().Warnf("failed to obtain cacheMemMax from memory.stat")
		return int64(defaultCacheMemMax)
	}
	return cacheMemMax
}

// NewLru is used to generate the LRU structure of the CacheMap.
func NewLru(cache *sync.Map, opts ...LruOption) *Lru {
	cacheMemMax := getMemcacheMemMax()
	log.GetLogger().Infof("maximum cache allowed by server.CacheMap: %d", cacheMemMax)
	l := &Lru{
		cacheMemMax: cacheMemMax,
		codeSize:    0,
		list:        list.New(),
		cache:       cache,
		lock:        new(sync.Mutex),
	}
	for _, opt := range opts {
		opt.apply(l)
	}
	return l
}

func (lru *Lru) removeOld(key interface{}, old interface{}) {
	oldElement, ok := old.(*list.Element)
	if !ok {
		return
	}
	oldListNode, ok := oldElement.Value.(ListNode)
	if !ok {
		return
	}
	lru.codeSize -= oldListNode.valSize
	lru.cache.Delete(key)
	lru.list.Remove(oldElement)
}

// AddCache is used to load new elements.
func (lru *Lru) AddCache(key interface{}, value interface{}, valSize int64) {
	if lru.list == nil {
		log.GetLogger().Errorf("failed to init lru list")
		return
	}
	lru.lock.Lock()
	defer lru.lock.Unlock()

	if old, ok := lru.cache.Load(key); ok {
		lru.removeOld(key, old)
	}
	lru.addToCache(key, value, valSize)
	return
}

func (lru *Lru) addToCache(key interface{}, value interface{}, valSize int64) {
	needSize := lru.codeSize + valSize
	for ele := lru.list.Back(); ele != nil && needSize > lru.cacheMemMax; ele = lru.list.Back() {
		listEleNode, ok := ele.Value.(ListNode)
		if !ok {
			log.GetLogger().Errorf("addCache failed to get listEleNode")
			break
		}

		lru.codeSize = lru.codeSize - listEleNode.valSize
		lru.list.Remove(ele)
		_, ok = lru.cache.Load(listEleNode.key)
		if !ok {
			continue
		}
		lru.cache.Delete(listEleNode.key)
		log.GetLogger().Infof("removed from the LRU linked list, objectID: %s", listEleNode.key)
		needSize = lru.codeSize + valSize
	}
	ele := lru.list.PushFront(ListNode{
		key:     key,
		val:     value,
		valSize: valSize,
	})
	lru.codeSize += valSize
	lru.cache.Store(key, ele)
	log.GetLogger().Infof("lru cache, valSize %d, codeSize %d", valSize, lru.codeSize)
}

// GetCache is used to get new elements.
func (lru *Lru) GetCache(key interface{}, isLayer bool) (interface{}, bool) {
	if lru.cache == nil {
		log.GetLogger().Errorf("getCache failed to get new lru")
		return nil, false
	}
	lru.lock.Lock()
	defer lru.lock.Unlock()

	ele, ok := lru.cache.Load(key)
	if !ok && lru.load != nil {
		val, size, err := lru.load(key, isLayer)
		if err == nil {
			lru.addToCache(key, val, size)
			ele, ok = lru.cache.Load(key)
		}
	}
	if ok {
		listEle, ok := ele.(*list.Element)
		if !ok || listEle == nil {
			log.GetLogger().Errorf("getCache failed to get listEle")
			return nil, false
		}
		listEleNode, ok := listEle.Value.(ListNode)
		if !ok {
			log.GetLogger().Errorf("getCache failed to get listEleNode")
			return nil, false
		}

		lru.list.MoveToFront(listEle)
		return listEleNode.val, true
	}
	return nil, false
}

// DelCache is used to delete elements.
func (lru *Lru) DelCache(key interface{}) {
	if lru.cache == nil {
		log.GetLogger().Errorf("delCache failed to get new lru")
		return
	}
	lru.lock.Lock()
	defer lru.lock.Unlock()
	ele, ok := lru.cache.Load(key)
	if ok {
		listEle, ok := ele.(*list.Element)
		if !ok || listEle == nil {
			log.GetLogger().Errorf("delCache failed to get listEle")
			return
		}
		listEleNode, ok := listEle.Value.(ListNode)
		if !ok {
			log.GetLogger().Errorf("delCache failed to get listEleNode")
			return
		}
		lru.list.Remove(listEle)
		lru.cache.Delete(key)
		lru.codeSize = lru.codeSize - listEleNode.valSize
	}
}
