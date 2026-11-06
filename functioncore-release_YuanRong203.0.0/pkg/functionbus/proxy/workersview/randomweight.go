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
	"os"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/strategy"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

var clusterID = os.Getenv("CLUSTER_ID")

const (
	workerManagerApplier = "worker-manager"
	workerIDPrefix       = "defaultaz-#-"
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

// ChooseSpecificRemoteInstance choose specific remote instance and nodeIP
func ChooseSpecificRemoteInstance(funcKeyWithResource string, instanceID string) (string, string) {
	randomWeightIf, ok := getRandomWeightMap().randomWeights.Load(funcKeyWithResource)
	if !ok {
		log.GetLogger().Errorf("load randomWeight failed with %s", funcKeyWithResource)
		return "", ""
	}
	randomWeight, ok := randomWeightIf.(*RandomWeight)
	if !ok {
		log.GetLogger().Errorf("%#v assert RandomWeight failed", randomWeight)
		return "", ""
	}
	workerID := utils.ParseWorkerIDFromInstanceID(instanceID)
	randomWeight.mutex.RLock()
	nodeIP, ok := randomWeight.randomPods.getNodeIPByPodName(workerID)
	randomWeight.mutex.RUnlock()
	if !ok {
		log.GetLogger().Errorf("can't find worker %s record", workerID)
		return "", ""
	}
	_, ok = workersview.enabledNodes.Load(nodeIP)
	if !ok {
		log.GetLogger().Errorf("node ip %s of %s is disable", nodeIP, workerID)
		return "", ""
	}
	log.GetLogger().Debugf("choose %s as target remote node, node ip %s", workerID, nodeIP)
	return workerID, nodeIP
}

// GetAllRemoteInstances get all remote instances
func GetAllRemoteInstances(funcKeyWithResource string) map[string]string {
	if funcKeyWithResource == "" {
		return nil
	}
	return getRandomWeightMap().getInstances(funcKeyWithResource)
}

// CheckOnlyOneInstanceLeft check only one instance left
func CheckOnlyOneInstanceLeft(funcKeyWithResource, podName string) bool {
	if funcKeyWithResource == "" || podName == "" {
		return false
	}

	return getRandomWeightMap().checkOnlyOneInstanceLeft(funcKeyWithResource, podName)
}

func (rwm *RandomWeightMap) addWorker(funcKeyWithResource, podName string, provider *servicediscovery.Provider) {
	if funcKeyWithResource == "" || provider.OwnerIP == "" {
		return
	}
	log.GetLogger().Infof("add worker %s to randomWeights, funcKeyWithResource:%s", podName, funcKeyWithResource)
	obj, _ := rwm.randomWeights.LoadOrStore(funcKeyWithResource, &RandomWeight{
		nodes:               make(map[string]int64, 0),
		mutex:               sync.RWMutex{},
		funcKeyWithResource: funcKeyWithResource,
		randomPods:          RandomPods{},
		index:               -1,
	})
	if obj, ok := obj.(*RandomWeight); ok {
		obj.increaseNodeWeight(podName, provider)
	}
	return
}

func (rwm *RandomWeightMap) delWorker(funcKeyWithResource, podName, nodeIP string) {
	if funcKeyWithResource == "" || nodeIP == "" {
		return
	}
	obj, ok := rwm.randomWeights.Load(funcKeyWithResource)
	if ok {
		if randomWeight, ok := obj.(*RandomWeight); ok {
			randomWeight.decreaseNodeWeight(podName, nodeIP)
			if len(randomWeight.nodes) == 0 && len(randomWeight.randomPods) == 0 {
				log.GetLogger().Infof("delete worker %s from randomWeights, funcKeyWithResource:%s",
					podName, funcKeyWithResource)
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

func (rwm *RandomWeightMap) getInstances(funcKeyWithResource string) map[string]string {
	obj, ok := rwm.randomWeights.Load(funcKeyWithResource)
	if ok {
		if randomWeight, ok := obj.(*RandomWeight); ok {
			return randomWeight.getInstances()
		}
	}
	return nil
}

func (rwm *RandomWeightMap) checkOnlyOneInstanceLeft(funcKeyWithResource, podName string) bool {
	obj, ok := rwm.randomWeights.Load(funcKeyWithResource)

	if ok {
		if randomWeight, ok := obj.(*RandomWeight); ok {
			return randomWeight.checkOnlyOneInstanceLeft(podName)
		}
	}
	return false
}

func (rw *RandomWeight) nextSkipUsed(used map[string]struct{}) (string, string) {
	rw.mutex.RLock()
	funcKeyWithResource := rw.funcKeyWithResource
	rw.mutex.RUnlock()
	circuitBreakerEnabled := workerbroker.GetCircuitBreakerEnabled(funcKeyWithResource)
	rw.mutex.RLock()
	defer rw.mutex.RUnlock()
	rw.index = strategy.RoundRobin(rw.index, rw.randomPods, used, circuitBreakerEnabled,
		func(entry RandomPod, usedInstance map[string]struct{}, circuitBreakerEnabled bool) (bool, string, string) {
			if entry.willDelete {
				return false, entry.podName, "marked willDelete"
			}
			if _, ok := used[entry.podName]; ok {
				return false, entry.podName, "used instance"
			}
			workerID := workerIDPrefix + entry.podName
			if circuitBreakerEnabled && workerbroker.GetInstance().GetWorkerBrokerCPUOverloaded(workerID) {
				return false, entry.podName, "cpu overload"
			}
			return true, entry.podName, "pass"
		})

	if rw.index < 0 || rw.index >= len(rw.randomPods) {
		return "", ""
	}

	return rw.randomPods[rw.index].podName, rw.randomPods[rw.index].nodeIP
}

func (rw *RandomWeight) getInstances() map[string]string {
	rw.mutex.RLock()
	defer rw.mutex.RUnlock()

	pods := make(map[string]string, len(rw.randomPods))
	for _, pod := range rw.randomPods {
		nodeIP := pod.nodeIP
		if nodeIP == internal.SelfNodeIP {
			continue
		}
		if _, ok := workersview.enabledNodes.Load(nodeIP); ok {
			pods[pod.podName] = nodeIP
		}
	}
	return pods
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
func (rw *RandomWeight) increaseNodeWeight(podName string, provider *servicediscovery.Provider) {
	if provider.OwnerIP == "" {
		return
	}

	isLocal := provider.OwnerIP == internal.SelfNodeIP

	rw.mutex.Lock()
	defer rw.mutex.Unlock()

	oldIP, ok := rw.randomPods.getNodeIPByPodName(podName)
	if ok && !isLocal {
		if _, ok := rw.nodes[oldIP]; ok {
			rw.nodes[oldIP]--
		}
	}
	rw.randomPods = rw.randomPods.updatePodInfo(podName, provider)
	if isLocal {
		return
	}
	if _, ok := rw.nodes[provider.OwnerIP]; !ok {
		rw.nodes[provider.OwnerIP] = 1
	} else {
		rw.nodes[provider.OwnerIP]++
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

	// if is local, not handler nodes
	log.GetLogger().Infof("nodeIp is %s, selfNodeIP is %s", nodeIP, internal.SelfNodeIP)
	if nodeIP == internal.SelfNodeIP {
		return
	}

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

func (rw *RandomWeight) checkOnlyOneInstanceLeft(podName string) bool {
	rw.mutex.Lock()
	defer rw.mutex.Unlock()

	return rw.randomPods.checkOnlyOneInstanceLeft(podName)
}

// RandomPod -
type RandomPod struct {
	nodeIP     string
	podName    string
	willDelete bool
	Applier    string
	Cluster    string
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

func (randomPods RandomPods) updatePodInfo(podName string, provider *servicediscovery.Provider) RandomPods {
	log.GetLogger().Infof("%s info changed, nodeIp:%s, willDelete status:%t",
		podName, provider.OwnerIP, provider.WillDelete)

	for index, randomPod := range randomPods {
		if randomPod.podName == podName {
			randomPods[index].nodeIP = provider.OwnerIP
			if provider.WillDelete {
				randomPods[index].willDelete = provider.WillDelete
			}
			return randomPods
		}
	}

	randomPod := RandomPod{
		podName: podName,
		nodeIP:  provider.OwnerIP,
		Applier: provider.Applier,
		Cluster: provider.Cluster,
	}

	if provider.WillDelete {
		randomPod.willDelete = provider.WillDelete
	}
	return append(randomPods, randomPod)
}

func (randomPods RandomPods) checkOnlyOneInstanceLeft(podName string) bool {
	log.GetLogger().Debugf("randomPods is %+v", randomPods)

	thisClusterPodNum := 0
	for _, v := range randomPods {
		if v.Cluster == clusterID {
			thisClusterPodNum++
			if thisClusterPodNum > 1 {
				return false
			}

			if v.podName != podName || v.Applier == workerManagerApplier {
				return false
			}
		}
	}

	if thisClusterPodNum == 0 {
		return false
	}

	return true
}
