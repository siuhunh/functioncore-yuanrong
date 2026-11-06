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

// Package workersview monitor remote worker and choose remote proxy
package workersview

import (
	"crypto/rand"
	"math/big"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/strategy"
)

// RandomWeightMap  map of randomWeights
type RandomWeightMap struct {
	// funcKeyWithResource:*RandomWeight
	randomWeights sync.Map
}

// RandomWeight - collect instance distribution of function:functionKey
type RandomWeight struct {
	nodes               map[string]int64
	mutex               sync.RWMutex
	funcKeyWithResource string
	randomPods          RandomPods
	index               int
}

var (
	once            sync.Once
	randomWeightMap *RandomWeightMap
)

// GetRandomWeightMap get singleton of RandomWeightMap
func getRandomWeightMap() *RandomWeightMap {
	once.Do(func() {
		randomWeightMap = &RandomWeightMap{
			randomWeights: sync.Map{},
		}
	})

	return randomWeightMap
}

// ChooseRemoteNode choose remote proxy for function
func ChooseRemoteNode(funcKeyWithResource string, used map[string]struct{}) interface{} {
	nodeIf := getRandomWeightMap().next(funcKeyWithResource, used)
	if nodeIf == nil {
		return nil
	}
	if _, ok := workersview.enabledNodes.Load(nodeIf); !ok {
		return nil
	}
	return nodeIf
}

// ChooseRemoteInstanceSkipUsed choose remote instance and nodeIP, and skip instance used
func ChooseRemoteInstanceSkipUsed(funcKeyWithResource string, used map[string]struct{}) (string, string) {
	pod, nodeIP := getRandomWeightMap().nextSkipUsed(funcKeyWithResource, used)
	if pod == "" || nodeIP == "" {
		return "", ""
	}
	if _, ok := workersview.enabledNodes.Load(nodeIP); !ok {
		return "", ""
	}
	return pod, nodeIP
}

func (rwm *RandomWeightMap) addWorker(funcKeyWithResource, podName, nodeIP string) {
	if funcKeyWithResource == "" || nodeIP == "" {
		return
	}
	obj, _ := rwm.randomWeights.LoadOrStore(funcKeyWithResource, &RandomWeight{
		nodes:               make(map[string]int64, 0),
		mutex:               sync.RWMutex{},
		funcKeyWithResource: funcKeyWithResource,
		randomPods:          RandomPods{},
		index:               -1,
	})
	if obj, ok := obj.(*RandomWeight); ok {
		obj.increaseNodeWeight(podName, nodeIP)
	}
	return
}

func (rwm *RandomWeightMap) delRemoteWorker(funcKeyWithResource, podName, nodeIP string) {
	if funcKeyWithResource == "" || nodeIP == "" {
		return
	}
	obj, ok := rwm.randomWeights.Load(funcKeyWithResource)
	if ok {
		if randomWeight, ok := obj.(*RandomWeight); ok {
			randomWeight.decreaseNodeWeight(podName, nodeIP)
			if len(randomWeight.nodes) == 0 {
				rwm.randomWeights.Delete(funcKeyWithResource)
			}
			return
		}
	}

	log.GetLogger().Warnf("workersview RandomWeightMap decrease Function instance abnormal, "+
		"function %s, node:%s", urnutils.AnonymizeTenantKey(funcKeyWithResource), nodeIP)
	return
}

func (rwm *RandomWeightMap) nextSkipUsed(funcKeyWithResource string, used map[string]struct{}) (string, string) {
	if funcKeyWithResource == "" {
		return "", ""
	}
	obj, ok := rwm.randomWeights.Load(funcKeyWithResource)
	if ok {
		if randomWeight, ok := obj.(*RandomWeight); ok {
			return randomWeight.nextSkipUsed(used)
		}
	}
	return "", ""
}

func (rw *RandomWeight) nextSkipUsed(used map[string]struct{}) (string, string) {
	rw.mutex.RLock()
	defer rw.mutex.RUnlock()

	rw.index = strategy.RoundRobin(rw.index, rw.randomPods, used,
		func(entry RandomPod, usedInstance map[string]struct{}) bool {
			if _, ok := used[entry.podName]; ok {
				return false
			}
			return true
		})

	if rw.index < 0 || rw.index >= len(rw.randomPods) {
		return "", ""
	}

	return rw.randomPods[rw.index].podName, rw.randomPods[rw.index].nodeIP
}

func (rwm *RandomWeightMap) next(funcKeyWithResource string, used map[string]struct{}) interface{} {
	if funcKeyWithResource == "" {
		return nil
	}
	obj, ok := rwm.randomWeights.Load(funcKeyWithResource)
	if ok {
		if randomWeight, ok := obj.(*RandomWeight); ok {
			return randomWeight.next(used)
		}
	}
	return nil
}

func (rw *RandomWeight) next(used map[string]struct{}) interface{} {
	rw.mutex.RLock()
	defer rw.mutex.RUnlock()

	nodes := rw.nodes
	var weights int64 = 0

	for nodeIP, weight := range nodes {
		if _, ok := used[nodeIP]; ok {
			continue
		}
		weights += weight
	}

	if weights <= 0 {
		return nil
	}

	n, err := rand.Int(rand.Reader, big.NewInt(weights))
	if err != nil {
		log.GetLogger().Errorf("failed to get random number, err: %s", err.Error())
		return nil
	}
	random := n.Int64()

	random++

	for proxy, weight := range nodes {
		if _, ok := used[proxy]; ok {
			continue
		}
		if random > weight {
			random -= weight
		} else {
			return proxy
		}
	}

	return nil
}

// add nodes and pods, but nodes is for faasScheduler, if isLocal do not add nodes
func (rw *RandomWeight) increaseNodeWeight(podName, nodeIP string) {
	if nodeIP == "" {
		return
	}

	isLocal := nodeIP == internal.SelfNodeIP

	rw.mutex.Lock()
	defer rw.mutex.Unlock()

	oldIP, ok := rw.randomPods.getNodeIPByPodName(podName)
	if ok && !isLocal {
		if _, ok := rw.nodes[oldIP]; ok {
			rw.nodes[oldIP]--
		}
	}
	rw.randomPods = rw.randomPods.updateNodeIPByPodName(podName, nodeIP)
	if isLocal {
		return
	}
	if _, ok := rw.nodes[nodeIP]; !ok {
		rw.nodes[nodeIP] = 1
	} else {
		rw.nodes[nodeIP]++
	}
	if rw.nodes[oldIP] == 0 {
		delete(rw.nodes, oldIP)
	}
}

func (rw *RandomWeight) decreaseNodeWeight(podName, nodeIP string) {
	if nodeIP == "" {
		return
	}
	rw.mutex.Lock()
	defer rw.mutex.Unlock()
	if _, ok := rw.randomPods.getNodeIPByPodName(podName); !ok {
		return
	}
	rw.randomPods = rw.randomPods.deleteRandomPodsByPodName(podName)
	if _, ok := rw.nodes[nodeIP]; !ok {
		log.GetLogger().Warnf("workersview randomweight decrease node weight abnormal, function %s, node:%s",
			urnutils.AnonymizeTenantKey(rw.funcKeyWithResource), nodeIP)
	} else {
		rw.nodes[nodeIP]--
		if rw.nodes[nodeIP] == 0 {
			delete(rw.nodes, nodeIP)
		}
	}
	return
}

// RandomPod -
type RandomPod struct {
	nodeIP  string
	podName string
}

// RandomPods -
type RandomPods []RandomPod

func (randomPods RandomPods) getNodeIPByPodName(podName string) (string, bool) {
	for _, randomPod := range randomPods {
		if randomPod.podName == podName {
			return randomPod.nodeIP, true
		}
	}

	return "", false
}

func (randomPods RandomPods) deleteRandomPodsByPodName(podName string) RandomPods {
	for index, randomPod := range randomPods {
		if randomPod.podName == podName {
			randomPods[index] = randomPods[len(randomPods)-1]
			return randomPods[:len(randomPods)-1]
		}
	}
	return randomPods
}

func (randomPods RandomPods) updateNodeIPByPodName(podName, nodeIP string) RandomPods {
	for index, randomPod := range randomPods {
		if randomPod.podName == podName {
			randomPods[index].nodeIP = nodeIP
			return randomPods
		}
	}
	return append(randomPods, RandomPod{podName: podName, nodeIP: nodeIP})
}
