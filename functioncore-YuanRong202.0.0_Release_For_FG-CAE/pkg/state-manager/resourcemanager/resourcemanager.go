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

// Package resourcemanager implements methods
// to manager nodes in cluster.
package resourcemanager

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"

	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/config"
)

// NodeEvent represents the node event type
type NodeEvent int32
type nodeEventCB func(interface{})

const (
	// NodeOnline represents node online
	NodeOnline NodeEvent = 0
	// NodeOffline represents node offline
	NodeOffline NodeEvent = 1

	resourceCPU     = "cpu"
	resourceMemory  = "memory"
	monitorInterval = 5 * time.Second
)

// NodeResourceInfo is used to store all resource info of a node
type NodeResourceInfo struct {
	CPU            int64
	Memory         int64
	CPUCapacity    int64
	MemoryCapacity int64
	CPURemain      int64
	MemoryRemain   int64
	NetBandwidth   float64
}

type nodeInfo struct {
	nodeName string
	ready    bool
	NodeResourceInfo
}

var (
	nodesMutex      sync.RWMutex
	nodesMap        map[string]*nodeInfo
	nodesSubMutex   sync.RWMutex
	nodeSubscribers map[NodeEvent][]nodeEventCB
	factory         informers.SharedInformerFactory
)

// GetAllNodes is used to get the names of all nodes
func GetAllNodes() ([]string, error) {
	nodesMutex.RLock()
	result := make([]string, 0, len(nodesMap))
	for nodeName, nodeInfo := range nodesMap {
		if nodeInfo.ready {
			result = append(result, nodeName)
		}
	}
	nodesMutex.RUnlock()
	return result, nil
}

// GetCPU is used to get the current CPU of a node
func GetCPU(nodeName string) (int64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return nodeInfo.CPU, nil
}

// GetMemory is used to get the current memory of a node
func GetMemory(nodeName string) (int64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return nodeInfo.Memory, nil
}

// GetCPUCapacity is used to get the CPU capacity of a node
func GetCPUCapacity(nodeName string) (int64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return nodeInfo.CPUCapacity, nil
}

// GetMemoryCapacity is used to get the memory capacity of a node
func GetMemoryCapacity(nodeName string) (int64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return nodeInfo.MemoryCapacity, nil
}

// GetCPURemain is used to get the remain CPU of a node
func GetCPURemain(nodeName string) (int64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return nodeInfo.CPURemain, nil
}

// GetMemoryRemain is used to get the remain memory of a node
func GetMemoryRemain(nodeName string) (int64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return nodeInfo.MemoryRemain, nil
}

// GetNetBandwidth is used to get the bandwidth of a node
func GetNetBandwidth(nodeName string) (float64, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return 0, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return 0, errors.New("node " + nodeName + " is not ready")
	}
	return 0, nil
}

// GetAllResource is used to get all resource info of a node
func GetAllResource(nodeName string) (*NodeResourceInfo, error) {
	nodesMutex.RLock()
	defer nodesMutex.RUnlock()
	nodeInfo, exists := nodesMap[nodeName]
	if !exists {
		return nil, errors.New("node " + nodeName + " is not in cluster")
	}
	if !nodeInfo.ready {
		return nil, errors.New("node " + nodeName + " is not ready")
	}
	return &(nodeInfo.NodeResourceInfo), nil
}

// RegisterNodeEvent is used to notify state-manager the online event or offline event of a node
func RegisterNodeEvent(event NodeEvent, callback func(interface{})) error {
	nodesSubMutex.Lock()
	nodeSubscribers[event] = append(nodeSubscribers[event], callback)
	nodesSubMutex.Unlock()
	return nil
}

// Start is used to init resourcemanager
func Start(stopper <-chan struct{}) error {
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	// creates the in-cluster config
	inClusterConfig, err := k8sclient.GetKubeConfig()
	if err != nil {
		return err
	}
	if cfg.KubeCfgTimeout >= 0 {
		inClusterConfig.Timeout = time.Duration(cfg.KubeCfgTimeout) * time.Second
	}
	// creates the clientSet
	clientSet, err := kubernetes.NewForConfig(inClusterConfig)
	if err != nil {
		return err
	}
	// create metrics
	mc, err := metrics.NewForConfig(inClusterConfig)
	if err != nil {
		return err
	}

	if nodesMap == nil {
		nodesMap = make(map[string]*nodeInfo)
	}
	if nodeSubscribers == nil {
		nodeSubscribers = make(map[NodeEvent][]nodeEventCB)
	}

	// initialize informer
	factory = informers.NewSharedInformerFactory(clientSet, 0)
	nodeInformer := factory.Core().V1().Nodes()
	informer := nodeInformer.Informer()
	defer runtime.HandleCrash()

	// start informer to list and watch
	go factory.Start(stopper)
	// sync resource from apiserver
	if !cache.WaitForCacheSync(stopper, informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return errors.New("timed out waiting for caches to sync")
	}
	// register node event callback
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAdd,
		UpdateFunc: onUpdate,
		DeleteFunc: onDelete,
	})

	// get node resource list
	err = getNodeInfoList(nodeInformer)
	if err != nil {
		return err
	}
	// start monitor node's change in cluster
	go monitorNodes(mc, stopper)

	return nil
}

// GetNodeCPUMilliValue gets the CPU value of the specified node
func GetNodeCPUMilliValue(nodeID string) int64 {
	nodeInformer, err := factory.Core().V1().Nodes().Lister().Get(nodeID)
	if err != nil {
		log.GetLogger().Errorf("get nodeInformer error: %s", err.Error())
		return 0
	}
	return nodeInformer.Status.Capacity.Cpu().MilliValue()
}

// GetNodeMemoryMilliValue gets the Memory value of the specified node
func GetNodeMemoryMilliValue(nodeID string) int64 {
	nodeInformer, err := factory.Core().V1().Nodes().Lister().Get(nodeID)
	if err != nil {
		log.GetLogger().Errorf("get nodeInformer error: %s", err.Error())
		return 0
	}
	return nodeInformer.Status.Capacity.Memory().MilliValue()
}

// AddNodeToNodeMap when the proxy is online, add node to nodemap
func AddNodeToNodeMap(nodeID string, cpuValue int64, memValue int64) {
	if nodesMap == nil {
		nodesMap = make(map[string]*nodeInfo)
	}

	nodesMutex.Lock()
	_, exists := nodesMap[nodeID]
	if !exists {
		nodesMap[nodeID] = &nodeInfo{
			nodeName: nodeID,
			ready:    false,
			NodeResourceInfo: NodeResourceInfo{
				CPUCapacity:    cpuValue,
				MemoryCapacity: memValue,
			},
		}
	}
	nodesMutex.Unlock()
}

// ClearNodeMap is used to delete the information of the specified node in the map
func ClearNodeMap(nodeID string) {
	if nodesMap == nil {
		return
	}

	nodesMutex.Lock()
	delete(nodesMap, nodeID)
	nodesMutex.Unlock()
}

func isDataPlaneNode(node *corev1.Node) bool {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return false
	}
	if conf.ScheCfg.NodeAffinity.NodeAffinityKey == "" || conf.ScheCfg.NodeAffinity.NodeAffinityValue == "" {
		return true
	}
	if value, ok := node.Labels[conf.ScheCfg.NodeAffinity.NodeAffinityKey]; ok {
		if value == conf.ScheCfg.NodeAffinity.NodeAffinityValue {
			return true
		}
	}
	return false
}

func getNodeReadyStatus(nodeID string) bool {
	nodeInformer, err := factory.Core().V1().Nodes().Lister().Get(nodeID)
	if err != nil {
		log.GetLogger().Errorf("get nodeInformer error: %s", err.Error())
		return false
	}
	for _, condition := range nodeInformer.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func getNodeInfoList(nodeInformer v1.NodeInformer) error {
	nodeLister := nodeInformer.Lister()
	nodeList, err := nodeLister.List(labels.Everything())
	if err != nil {
		return err
	}
	nodesMutex.Lock()
	for _, node := range nodeList {
		if isDataPlaneNode(node) {
			cpuValue := node.Status.Capacity.Cpu().MilliValue()
			memValue := node.Status.Capacity.Memory().Value()
			nodesMap[node.Name] = &nodeInfo{
				nodeName: node.Name,
				ready:    getNodeReadyStatus(node.Name),
				NodeResourceInfo: NodeResourceInfo{
					CPUCapacity:    cpuValue,
					MemoryCapacity: memValue,
					CPURemain:      cpuValue,
					MemoryRemain:   memValue,
				},
			}
		}
	}
	nodesMutex.Unlock()

	return nil
}

func updateNodesMap(mc *metrics.Clientset) {
	nodesRes, err := mc.MetricsV1beta1().NodeMetricses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.GetLogger().Warnf("failed to get resource list from metrics, error: %s", err.Error())
		return
	}
	nodesMutex.Lock()
	for _, nodeRes := range nodesRes.Items {
		if node, exists := nodesMap[nodeRes.Name]; exists {
			cpuQ := nodeRes.Usage[resourceCPU]
			node.CPU = (&cpuQ).MilliValue()
			node.CPURemain = node.CPUCapacity - node.CPU
			memoryQ := nodeRes.Usage[resourceMemory]
			node.Memory = (&memoryQ).Value()
			node.MemoryRemain = node.MemoryCapacity - node.Memory
			node.ready = getNodeReadyStatus(nodeRes.Name)
		}
	}
	nodesMutex.Unlock()
}

func monitorNodes(mc *metrics.Clientset, stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stop channel is nil")
		return
	}
	tick := time.NewTicker(monitorInterval)
	for {
		select {
		case <-tick.C:
			updateNodesMap(mc)
		case _, ok := <-stopChan:
			if !ok {
				tick.Stop()
				log.GetLogger().Infof("stopChan was closed")
				return
			}
		}
	}
}

func onAdd(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		log.GetLogger().Errorf("interface obj transfer to corev1.Node error")
		return
	}
	if !isDataPlaneNode(node) {
		log.GetLogger().Warnf("onAdd node %s isn't data plane node and ignore it", node.Name)
		return
	}
	cpuValue := node.Status.Capacity.Cpu().MilliValue()
	memValue := node.Status.Capacity.Memory().Value()
	AddNodeToNodeMap(node.Name, cpuValue, memValue)

	nodesSubMutex.RLock()
	for _, callback := range nodeSubscribers[NodeOnline] {
		callback(node.Name)
	}
	nodesSubMutex.RUnlock()
}

func onUpdate(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*corev1.Node)
	if !ok {
		log.GetLogger().Errorf("oldObj transfer to corev1.Node error")
		return
	}
	newNode, ok := newObj.(*corev1.Node)
	if !ok {
		log.GetLogger().Errorf("newObj transfer to corev1.Node error")
		return
	}
	if !isDataPlaneNode(newNode) {
		log.GetLogger().Warnf("onUpdate node %s isn't data plane node and ignore it", newNode.Name)
		return
	}
	if oldNode.Name != newNode.Name {
		log.GetLogger().Errorf("old node name %s and new node name %s is different", oldNode.Name, newNode.Name)
		return
	}

	doUpdateCallback(oldNode, newNode)
}

func doUpdateCallback(oldNode, newNode *corev1.Node) {
	oldNodeStatus := NodeOffline
	for _, c := range oldNode.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			oldNodeStatus = NodeOnline
			break
		}
	}
	newNodeStatus := NodeOffline
	for _, c := range newNode.Status.Conditions {
		if c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue {
			newNodeStatus = NodeOnline
			break
		}
	}

	if oldNodeStatus == NodeOffline && newNodeStatus == NodeOnline {
		onAdd(newNode)
	}
	if oldNodeStatus == NodeOnline && newNodeStatus == NodeOffline {
		onDelete(oldNode)
	}
}

func onDelete(obj interface{}) {
	node, ok := obj.(*corev1.Node)
	if !ok {
		log.GetLogger().Errorf("interface obj transfer to corev1.Node error")
		return
	}
	if !isDataPlaneNode(node) {
		log.GetLogger().Warnf("onDelete node %s isn't data plane node and ignore it", node.Name)
		return
	}
	log.GetLogger().Infof("No need to process node %s delete event now, waiting for bus proxy offline event", node.Name)
}
