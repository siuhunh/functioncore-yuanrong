/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package poolmanager

import (
	"math"
	"sort"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/nodebalance"
	"functioncore/pkg/worker-manager/utils"
)

const (
	memoryGB   = 1024 * 1024 * 1024
	cpuCore    = 1000
	allPercent = 100
	minFloat   = 0.0000001
)

// NormalPriorityPacking normal bin packing
type NormalPriorityPacking struct {
	enableTMPExpansion bool
	funcName           string
	noScheduleNodes    []string
}

// NodeScore the score of node
type NodeScore struct {
	node  nodeName
	score float64
}

// Recover recover data
func (n *NormalPriorityPacking) Recover() SelectPodRule {
	n.enableTMPExpansion = false
	n.funcName = ""
	return n
}

// WithFuncName with funcName
func (n *NormalPriorityPacking) WithFuncName(funcName string) SelectPodRule {
	n.funcName = funcName
	return n
}

// WithEnableTMPExpansion with enable tmp expansion
func (n *NormalPriorityPacking) WithEnableTMPExpansion(enableTMPExpansion bool) SelectPodRule {
	n.enableTMPExpansion = enableTMPExpansion
	return n
}

// WithNoScheduleNodes with no schedule nodes
func (n *NormalPriorityPacking) WithNoScheduleNodes(noScheduleNodes []string) SelectPodRule {
	n.noScheduleNodes = noScheduleNodes
	return n
}

// GetEnableTMPExpansion get enable tmp expansion
func (n *NormalPriorityPacking) GetEnableTMPExpansion() bool {
	return n.enableTMPExpansion
}

// GetNoScheduleNodes get no schedule nodes
func (n *NormalPriorityPacking) GetNoScheduleNodes() []string {
	return n.noScheduleNodes
}

// GetFuncName get funcName
func (n *NormalPriorityPacking) GetFuncName() string {
	return n.funcName
}

// SelectPod select pod
func (n *NormalPriorityPacking) SelectPod(p *Pool) *corev1.Pod {
	// if enableTMPExpansion, find the node support tmpExpansion
	tmpExpansionNodes := map[string]*cache.NodeInfo{}
	if n.enableTMPExpansion {
		// the temp cap will be desc when list tempExpansion, so we need RecoverRemainCapacityStorage before return
		tmpExpansionNodes, _ = cache.GetAvailableNodesForTMPExpansion()
	}

	p.mux.Lock()
	defer p.mux.Unlock()
	// prefer to select pod on no taints node
	noTaintsNodeList, hasTaintsNodeList := getTaintsNodeListAndNoTaintsNodeList(p.PodList)
	noScheduleNodes := n.GetNoScheduleNodes()
	updatedNoTaintsNodeList := filterNoScheduleNode(noTaintsNodeList, noScheduleNodes)
	updatedHasTaintsNodeList := filterNoScheduleNode(hasTaintsNodeList, noScheduleNodes)
	podList := n.selectAvailableNodeList(updatedNoTaintsNodeList, tmpExpansionNodes, p.PodList)
	if len(podList) == 0 {
		podList = n.selectAvailableNodeList(updatedHasTaintsNodeList, tmpExpansionNodes, p.PodList)
	}

	pod := n.scoreNodesAndSelectPod(podList)

	if pod != nil {
		p.processSelectedPod(pod)
	}
	// recover the tmp cap in cache, if pod is nil, pod will be recovered too
	go cache.ReduceNodeStorage(tmpExpansionNodes, pod.Spec.NodeName)
	return pod
}

// selectAvailableNodeList depend on node select
func (n *NormalPriorityPacking) selectAvailableNodeList(nodeList []nodeName,
	tmpExpansionNodes map[string]*cache.NodeInfo, podList map[nodeName]*NodePod) map[nodeName][]*corev1.Pod {

	availableNodeList := make(map[nodeName][]*corev1.Pod)

	for _, nodeID := range nodeList {
		nodePod, nodeExist := podList[nodeID]
		if !nodeExist || nodePod.readyPodNum == 0 {
			continue
		}

		// if is not a tmpExpansion function, we think this node is ok
		// if is a tmpExpansion function, we choose this node by tmpExpansionNodeExist
		if _, tmpExpansionNodeExist := tmpExpansionNodes[string(nodeID)]; !n.enableTMPExpansion || tmpExpansionNodeExist {

			if availablePodList :=
				nodePod.podsQueue.getAvailablePodList(); len(availablePodList) != 0 {
				availableNodeList[nodeID] = availablePodList
			}
		}

	}
	return availableNodeList
}

// select pod by scale
func (n *NormalPriorityPacking) scoreNodesAndSelectPod(availableNodeList map[nodeName][]*corev1.Pod) *corev1.Pod {
	if len(availableNodeList) == 0 {
		return nil
	}
	nodesScore := make([]*NodeScore, len(availableNodeList))
	profilingContent := monitoring.GetFunctionProfilingDataByFuncName(n.funcName)

	i := 0
	for node, pods := range availableNodeList {
		podMemory := int64(0)
		podCPU := int64(0)
		if len(pods) > 0 {
			podReqs, _ := resourcehelper.PodRequestsAndLimits(pods[0])
			podMemory = podReqs.Memory().Value()
			podCPU = podReqs.Cpu().MilliValue()
		}
		nodesScore[i] = &NodeScore{
			score: GetScoreByNodeAndProfiling(string(node), profilingContent, podMemory, podCPU),
			node:  node,
		}
		i++
	}

	sort.Slice(nodesScore, func(i, j int) bool {
		return nodesScore[i].score > nodesScore[j].score
	})

	for index, node := range nodesScore {
		log.GetLogger().Debugf("packing: node score by sort , index : %d, score: %f, nodeName is %s",
			index, node.score, node.node)
	}

	if pods, ok := availableNodeList[nodesScore[0].node]; ok {
		selectPod := pods[0]
		_, podLimits := resourcehelper.PodRequestsAndLimits(pods[0])
		selectPodMemory := podLimits.Memory().Value()
		selectPodCPU := podLimits.Cpu().MilliValue()
		if profilingContent == nil {
			nodebalance.UpdateNodeState(string(nodesScore[0].node), selectPodCPU, selectPodMemory)
		} else {
			nodebalance.UpdateNodeState(string(nodesScore[0].node), profilingContent.CPU.Average, selectPodMemory)
		}
		return selectPod
	}
	return nil
}

// GetScoreByNodeAndProfiling get score by node and profiling
func GetScoreByNodeAndProfiling(node string, profilingContent *monitoring.ProfilingContent,
	podMemory, podCPU int64) float64 {
	// for calculated the score of node, we need get data of node usage and funcProfiling
	// 1. get node state
	nodeStateForPacking := nodebalance.GetNodesState(node)
	allCPU := nodeStateForPacking.AllCPU
	usedCPU := nodeStateForPacking.UsedCPU
	allMemory := nodeStateForPacking.AllMemory
	requestMemory := nodeStateForPacking.RequestMemory

	exceptNodeCPUPercent := nodebalance.DefaultExpectNodeCPUPercent
	cfg, err := config.GetConfig()
	if err == nil {
		exceptNodeCPUPercent = atomic.LoadInt64(&cfg.NodeBalance.ExpectNodeCPUPercent)
	}

	// if it can't get node state, we all return 0, all nodes get same score
	poolMemoryInNode := getPoolMemoryInNode(nodeName(node))
	if allCPU == 0 || usedCPU == 0 || allMemory == 0 || requestMemory == 0 {
		log.GetLogger().Debugf("packing: can't get node state now")
		return 0
	}

	currentRatio := (float64(requestMemory-poolMemoryInNode) * cpuCore) /
		(float64(usedCPU) * memoryGB)
	// exceptNodeCpuPercent is a value 0 - 100
	exceptRatio := (float64(allMemory) * cpuCore * allPercent) /
		(float64(allCPU) * memoryGB * float64(exceptNodeCPUPercent))

	var afterRatio float64
	if profilingContent == nil || profilingContent.CPU.Average == 0 {
		afterRatio = (float64(requestMemory-poolMemoryInNode+podMemory) * cpuCore) /
			(float64(usedCPU+podCPU) * memoryGB)
	} else {
		// get this node memory excludes pool
		afterRatio = (float64(requestMemory-poolMemoryInNode+podMemory) * cpuCore) /
			(float64(usedCPU+profilingContent.CPU.Average) * memoryGB)
	}

	log.GetLogger().Infof("packing: get score by node and profiling,node is %s, allCPU is %d, "+
		"usedCPU is %d, allMemory is %d requestMemory is %d, currentRatio is %f, afterRatio is %f, "+
		"exceptRatio is %f, profilingContent %t, poolMemoryInNode is %d", node, allCPU, usedCPU, allMemory,
		requestMemory, currentRatio, afterRatio, exceptRatio, profilingContent != nil, poolMemoryInNode)
	return getScoreByRatio(currentRatio, afterRatio, exceptRatio)

}

func getPoolMemoryInNode(node nodeName) int64 {
	poolMemoryInNode := int64(0)
	poolList := GetPoolManager().GetPoolList()
	for _, pool := range poolList {
		nodePodList, ok := pool.PodList[node]
		if !ok {
			continue
		}
		for _, pod := range nodePodList.podsQueue.items {
			// if usedFlag, pod was selected but not generalized, the usage of cpu is 0
			// so when we choose pod, we consider the pod is not generalized now
			if utils.IsPodReady(pod.pod) {
				podReqs, _ := resourcehelper.PodRequestsAndLimits(pod.pod)
				poolMemoryInNode += podReqs.Memory().Value()
			}
		}

	}
	return poolMemoryInNode
}

func getScoreByRatio(currentRatio, afterRatio, exceptRatio float64) float64 {
	if exceptRatio < minFloat {
		return 0
	}
	return (math.Abs(currentRatio-exceptRatio) - math.Abs(afterRatio-exceptRatio)) / exceptRatio
}
