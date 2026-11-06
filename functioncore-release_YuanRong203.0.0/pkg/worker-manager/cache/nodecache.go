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

package cache

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

var (
	nodeCache = &NodeCache{
		NodeList:               make(map[string]*NodeInfo, utils.DefaultMapSize),
		LabelRecord:            make(map[string]map[string]map[string]struct{}, utils.DefaultMapSize),
		needReserveStorageNums: 0,
	}
	nodeCacheSingleton sync.Once
	// NodeK8sCache - nodeCache as k8s *corev1.Node type
	NodeK8sCache sync.Map
	// NodeTaintCache store the ds-worker failure cache, and check again if exist after 60sec, ok do long-time failure
	NodeTaintCache sync.Map
	// NodeMirroredDiskCache store the mirror disk space report from function-task, key is nodeIP and value is disk space
	NodeMirroredDiskCache sync.Map
)

const (
	listNodeTimout     = 10 * time.Second
	defaultNodesMapCap = 10
	// a single pod with tmp storage extension enabled consumes 10 GB
	singlePodStorage = 10 * 1024 * 1024 * 1024
	// maximum pre-allocated storage capacity for normal instances
	normalInsStorageCapacity = 50 * 1024 * 1024 * 1024

	defaultCacheNodesPercentage = 1
	threshold                   = 1e-9

	// ScheduleLabelSuffix schedule label suffix
	ScheduleLabelSuffix = "-Schedule"
	// NoScheduleLabelSuffix no schedule label suffix
	NoScheduleLabelSuffix = "-NoSchedule"
	// PreferredScheduleLabelSuffix preferred schedule label suffix
	PreferredScheduleLabelSuffix = "-PreferredSchedule"
	// DefaultLabelKey -
	DefaultLabelKey = "default"
	// DefaultLabelValue -
	DefaultLabelValue = "default-NoSchedule"
	// DefaultSSDLabelKey -
	DefaultSSDLabelKey = "ssd-node-key"
	// DefaultSSDLabelValue -
	DefaultSSDLabelValue = "ssd-node-value"
)

const (
	convertKToB = 1024
	convertMToB = 1024 * 1024
	convertGToB = 1024 * 1024 * 1024
	convertTToB = 1024 * 1024 * 1024 * 1024
)

// InitNodeCache -
func InitNodeCache(pods []corev1.Pod) error {
	var err error
	nodeCacheSingleton.Do(func() {
		err = initNodeCache(pods)
	})
	return err
}

func initNodeCache(pods []corev1.Pod) error {
	ctx, cancel := context.WithTimeout(context.Background(), listNodeTimout)
	nodeListMeta, err := utils.GetKubernetesClient().CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "node-role",
	})
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to list node, err: %s", err.Error())
		return err
	}
	for _, node := range nodeListMeta.Items {
		AddNode(&node)
	}
	// update the node cache based on the pod label
	nodeCache.mux.Lock()
	for _, pod := range pods {
		if enable, exist := pod.Labels[utils.LabelEnableTMPExpansion]; !exist || enable == utils.FalseString {
			continue
		}
		node := nodeCache.NodeList[pod.Spec.NodeName]
		if node == nil {
			continue
		}
		node.RemainingCapacityStorageDec()
	}
	nodeCache.mux.Unlock()
	return nil
}

// GetAvailableNodesForTMPExpansion is obtain nodes with more than 10G available storage
// If the node resources are sufficient, reserve a node with 10G storage
func GetAvailableNodesForTMPExpansion() (map[string]*NodeInfo, []string) {
	nodes := make(map[string]*NodeInfo, defaultNodesMapCap)
	nodeList := make([]string, 0, defaultNodesMapCap)
	nodeCache.mux.Lock()
	defer nodeCache.mux.Unlock()
	if singlePodStorage == 0 {
		return nodes, nodeList
	}
	currentSinglePodStorageNum := 0
	for nodeName, node := range nodeCache.NodeList {
		tmpSinglePodStorageNum := int(atomic.LoadInt64(&node.remainingCapacityStorage) / singlePodStorage)
		if tmpSinglePodStorageNum > 0 {
			nodes[nodeName] = node
			nodeList = append(nodeList, nodeName)
			currentSinglePodStorageNum += tmpSinglePodStorageNum
		}
	}
	// insufficient resources
	if nodeCache.needReserveStorageNums+1 > currentSinglePodStorageNum {
		return make(map[string]*NodeInfo, 0), []string{}
	}
	// If the node resources are sufficient, reserve a node with 10G storage
	nodeCache.needReserveStorageNums++
	return nodes, nodeList
}

// ReduceNodeStorage is used with the GetAvailableNodesForTMPExpansion function and is to reduce 10g  storage resources
// of the node where the pod is successfully.
func ReduceNodeStorage(nodes map[string]*NodeInfo, nodeName string) {
	nodeCache.mux.Lock()
	if nodeInfo, exist := nodes[nodeName]; exist {
		nodeInfo.remainingCapacityStorage -= singlePodStorage
	}
	// The number of reserved 10g storage is reduced by one
	nodeCache.needReserveStorageNums--
	nodeCache.mux.Unlock()
}

// GetNodeNameList get a name list of nodes
func GetNodeNameList(nodeList map[string]*NodeInfo) []string {
	nodeNameList := make([]string, 0, len(nodeList))
	for nodeName := range nodeList {
		nodeNameList = append(nodeNameList, nodeName)
	}
	return nodeNameList
}

// RecoverNodeRemainCapacityStorage recover 10G storage
func RecoverNodeRemainCapacityStorage(nodeName string, podName string) {
	nodeCache.mux.RLock()
	nodeInfo, exist := nodeCache.NodeList[nodeName]
	if !exist {
		log.GetLogger().Warnf("node %s does not exist", nodeName)
		nodeCache.mux.RUnlock()
		return
	}
	nodeCache.mux.RUnlock()
	previousCapacityStorage := atomic.LoadInt64(&(nodeInfo.remainingCapacityStorage))
	nodeInfo.RemainingCapacityStorageInc()
	log.GetLogger().Infof("recycling pod %s storage resources, node  %s add 10G available storage, current "+
		"remainingCapacityStorage: %d, previous remainingCapacityStorage: %d", podName, nodeName,
		atomic.LoadInt64(&(nodeInfo.remainingCapacityStorage)), previousCapacityStorage)
}

// NodeInfo stores node info
type NodeInfo struct {
	Label                    map[string]string
	Taints                   []corev1.Taint
	NodeIP                   string
	AllocatableCPU           int64
	AllocatableMemory        int64
	AllocatableStorage       int64
	CapacityCPU              int64
	CapacityMemory           int64
	CapacityStorage          int64
	remainingCapacityStorage int64
	RequestCPU               int64
	RequestMemory            int64
	LimitCPU                 int64
	LimitMemory              int64
	AllocatablePods          int64
	UsedPods                 int64
	IsReady                  bool
	RequestNvidiaGPU         int64 // nvidia.com/gpu
	CapacityNvidiaGPU        int64
	RequestGPUPercentage     int64 // volcano.sh/gpu-core.percentage
	CapacityGPUPercentage    int64
	RequestGPUMem128         int64 // volcano.sh/gpu-mem.128Mi
	CapacityGPUMem128        int64
	GPUType                  string
	IsDefaultLabel           bool
	IsSSDLabel               bool
	MigrateTimestamp         int64
}

// RemainingCapacityStorageInc remaining capacity storage increase
func (node *NodeInfo) RemainingCapacityStorageInc() int64 {
	if node == nil {
		return 0
	}
	return atomic.AddInt64(&(node.remainingCapacityStorage), singlePodStorage)
}

// RemainingCapacityStorageDec remaining capacity storage decrease
func (node *NodeInfo) RemainingCapacityStorageDec() int64 {
	if node == nil {
		return 0
	}
	return atomic.AddInt64(&(node.remainingCapacityStorage), -singlePodStorage)
}

// NodeCache records node infos
type NodeCache struct {
	NodeList map[string]*NodeInfo
	// LabelRecord key is labelKey, labelValue, value is node name list
	LabelRecord            map[string]map[string]map[string]struct{}
	needReserveStorageNums int
	mux                    sync.RWMutex
}

// GetNodeInfo get node info from node cache
func GetNodeInfo(nodeName string) *NodeInfo {
	nodeCache.mux.RLock()
	nodeInfo := nodeCache.NodeList[nodeName]
	if nodeInfo == nil {
		nodeCache.mux.RUnlock()
		return nil
	}
	copied := *nodeInfo
	nodeCache.mux.RUnlock()
	return &copied
}

// GetRandomNodeFromNodeList select a random node for stateID
func GetRandomNodeFromNodeList() string {
	var nodeList []string
	nodeCache.mux.RLock()
	for nodeName, node := range nodeCache.NodeList {
		if utils.IsInSlice(node.Label[constants.NodeRoleLabelKey], constants.GetValidNodeRoleValues()) {
			nodeList = append(nodeList, nodeName)
		}
	}
	nodeCache.mux.RUnlock()
	if len(nodeList) == 0 {
		return ""
	}
	rand.Seed(time.Now().UnixNano())
	randomIndex := rand.Intn(len(nodeList))
	return nodeList[randomIndex]
}

// UpdateNodeMigrateTimestamp update migrateTimestamp of node
func UpdateNodeMigrateTimestamp(nodeName string, timestamp int64) {
	nodeCache.mux.RLock()
	defer nodeCache.mux.RUnlock()
	nodeInfo := nodeCache.NodeList[nodeName]
	if nodeInfo == nil {
		return
	}
	atomic.StoreInt64(&nodeInfo.MigrateTimestamp, timestamp)
	return
}

func (n *NodeCache) recordLabel(nodeName string, labels map[string]string) {
	for key, value := range labels {
		valueMap, exist := n.LabelRecord[key]
		if !exist {
			valueMap = make(map[string]map[string]struct{}, utils.DefaultMapSize)
			n.LabelRecord[key] = valueMap
		}
		nodeList, exist := valueMap[value]
		if !exist {
			nodeList = make(map[string]struct{}, utils.DefaultMapSize)
		}
		nodeList[nodeName] = struct{}{}
		valueMap[value] = nodeList
	}
}

func (n *NodeCache) deleteLabel(nodeName string) {
	labels := n.NodeList[nodeName].Label
	for key, value := range labels {
		nodes := n.LabelRecord[key][value]
		delete(nodes, nodeName)
	}
}

// GetNodesByLabel gets node list according to given labels
func GetNodesByLabel(labels []map[string]string) []string {
	nodeCache.mux.RLock()
	defer nodeCache.mux.RUnlock()
	result := make([]string, 0, utils.DefaultSliceSize)
	for nodeName, node := range nodeCache.NodeList {
		if isMatchedNode(node, labels) {
			result = append(result, nodeName)
		}
	}
	return result
}

// GetNodesWithFilters returns nodes with filters
func GetNodesWithFilters(filters ...Filter) []string {
	res := make([]string, 0, utils.DefaultSliceSize)
	nodeCache.mux.RLock()
	for name, node := range nodeCache.NodeList {
		match := true
		for _, filter := range filters {
			if !filter.match(node) {
				match = false
				break
			}
		}
		if match {
			res = append(res, name)
		}
	}
	nodeCache.mux.RUnlock()
	return res
}

// Filter validate whether NodeInfo match some requirements
type Filter interface {
	match(*NodeInfo) bool
}

type labelValueIn struct {
	key    string
	values []string
}

func (l *labelValueIn) match(info *NodeInfo) bool {
	if len(l.values) == 0 {
		return true
	}
	val, ok := info.Label[l.key]
	if !ok {
		return false
	}
	for _, v := range l.values {
		if v == val {
			return true
		}
	}
	return false
}

// WithLabelValueIn filters NodeInfo that contains label of key,
// and value of that labels is in values set.
func WithLabelValueIn(key string, values ...string) Filter {
	return &labelValueIn{
		key:    key,
		values: values,
	}
}

type labelValueEq struct {
	labels map[string]string
}

func (l *labelValueEq) match(info *NodeInfo) bool {
	for k, v := range l.labels {
		if actual, ok := info.Label[k]; !ok || actual != v {
			return false
		}
	}
	return true
}

// WithLabelValueEq filters NodeInfo that has all (key, value) labels provided
func WithLabelValueEq(labels map[string]string) Filter {
	return &labelValueEq{labels: labels}
}

func isMatchedNode(node *NodeInfo, labels []map[string]string) bool {
	for _, label := range labels {
		for key, expectValue := range label {
			value, exist := node.Label[key]
			if !exist || value != expectValue {
				return false
			}
		}
	}
	return true
}

// IncludeNoScheduleLabels check whether there is a scheduling label
func IncludeNoScheduleLabels(labels map[string]string) bool {
	for _, value := range labels {
		if strings.HasSuffix(value, NoScheduleLabelSuffix) {
			return true
		}
	}
	return false
}

func isNoDefaultScheduleNode(labels map[string]string) bool {
	for key, value := range labels {
		if key == DefaultLabelKey && value == DefaultLabelValue {
			return true
		}
	}
	return false
}

func isSSDNode(labels map[string]string) bool {
	for key, value := range labels {
		if key == DefaultSSDLabelKey && value == DefaultSSDLabelValue {
			return true
		}
	}
	return false
}

// AddNode adds node to cache
func AddNode(node *corev1.Node) {
	nodeCache.mux.Lock()
	defer nodeCache.mux.Unlock()
	if _, exist := nodeCache.NodeList[node.Name]; exist {
		log.GetLogger().Warnf("node %s already exists in node list", node.Name)
		return
	}
	nodeInfo := &NodeInfo{
		NodeIP:                utils.GetNodeIPAddress(node),
		AllocatableCPU:        node.Status.Allocatable.Cpu().MilliValue(),
		AllocatableMemory:     node.Status.Allocatable.Memory().Value(),
		AllocatableStorage:    node.Status.Allocatable.StorageEphemeral().Value(),
		CapacityCPU:           node.Status.Capacity.Cpu().MilliValue(),
		CapacityMemory:        node.Status.Capacity.Memory().Value(),
		CapacityStorage:       node.Status.Capacity.StorageEphemeral().Value(),
		Label:                 node.Labels,
		Taints:                utils.CopyTaints(node.Spec.Taints),
		AllocatablePods:       node.Status.Allocatable.Pods().Value(),
		IsReady:               utils.IsNodeReady(node),
		CapacityNvidiaGPU:     node.Status.Capacity.Name(utils.NvidiaGPU, resource.DecimalSI).Value(),
		CapacityGPUPercentage: node.Status.Capacity.Name(utils.GpuIsolationCoreKey, resource.DecimalSI).Value(),
		CapacityGPUMem128:     node.Status.Capacity.Name(utils.GpuIsolationMemKey, resource.DecimalSI).Value(),
		IsDefaultLabel:        isNoDefaultScheduleNode(node.Labels),
		IsSSDLabel:            isSSDNode(node.Labels),
		MigrateTimestamp:      time.Now().Unix(),
	}
	atomic.StoreInt64(&(nodeInfo.remainingCapacityStorage), nodeInfo.CapacityStorage-normalInsStorageCapacity)
	nodeCache.NodeList[node.Name] = nodeInfo
	nodeCache.recordLabel(node.Name, node.Labels)
	NodeK8sCache.Store(node.Name, node.DeepCopy())
	NodeMirroredDiskCache.Store(utils.GetNodeIPAddress(node), "")
	log.GetLogger().Infof("add node %s to node list, remainingCapacityStorage: %d, taints: %v, gpuType: %s",
		node.Name, atomic.LoadInt64(&(nodeInfo.remainingCapacityStorage)), nodeInfo.Taints,
		node.Labels[utils.GpuTypeAffinityKey])
}

// DeleteNode deletes node from cache
func DeleteNode(node *corev1.Node) {
	nodeCache.mux.Lock()
	defer nodeCache.mux.Unlock()
	if _, exist := nodeCache.NodeList[node.Name]; !exist {
		log.GetLogger().Warnf("node %s does not exist in node list", node.Name)
		return
	}
	nodeCache.deleteLabel(node.Name)
	delete(nodeCache.NodeList, node.Name)
	NodeK8sCache.Delete(node.Name)
	NodeMirroredDiskCache.Delete(utils.GetNodeIPAddress(node))
	log.GetLogger().Debugf("delete node %s from node list", node.Name)
}

// UpdateNode update node in cache
func UpdateNode(node *corev1.Node) {
	nodeCache.mux.Lock()
	defer nodeCache.mux.Unlock()

	nodeInfo, exist := nodeCache.NodeList[node.Name]
	if !exist {
		nodeInfo = &NodeInfo{}
		nodeCache.NodeList[node.Name] = nodeInfo
	}
	// deal with deleted label
	for key, value := range nodeInfo.Label {
		if node.Labels[key] == "" && nodeCache.LabelRecord[key][value] != nil {
			delete(nodeCache.LabelRecord[key][value], node.Name)
		}
	}
	oldCapacityStorage := nodeInfo.CapacityStorage
	nodeInfo.Taints = utils.CopyTaints(node.Spec.Taints)
	nodeInfo.NodeIP = utils.GetNodeIPAddress(node)
	nodeInfo.Label = node.Labels
	nodeInfo.AllocatableCPU = node.Status.Allocatable.Cpu().MilliValue()
	nodeInfo.AllocatableMemory = node.Status.Allocatable.Memory().Value()
	nodeInfo.AllocatableStorage = node.Status.Allocatable.StorageEphemeral().Value()
	nodeInfo.CapacityCPU = node.Status.Capacity.Cpu().MilliValue()
	nodeInfo.CapacityMemory = node.Status.Capacity.Memory().Value()
	nodeInfo.CapacityStorage = node.Status.Capacity.StorageEphemeral().Value()
	nodeInfo.AllocatablePods = node.Status.Allocatable.Pods().Value()
	nodeInfo.CapacityNvidiaGPU = node.Status.Capacity.Name(utils.NvidiaGPU, resource.DecimalSI).Value()
	nodeInfo.CapacityGPUPercentage = node.Status.Capacity.Name(utils.GpuIsolationCoreKey, resource.DecimalSI).Value()
	nodeInfo.CapacityGPUMem128 = node.Status.Capacity.Name(utils.GpuIsolationMemKey, resource.DecimalSI).Value()
	nodeInfo.IsReady = utils.IsNodeReady(node)
	nodeInfo.IsDefaultLabel = isNoDefaultScheduleNode(node.Labels)
	nodeInfo.IsSSDLabel = isSSDNode(node.Labels)
	diffStorage := nodeInfo.CapacityStorage - oldCapacityStorage
	// previousRemainingCapacityStorage for log printing
	previousRemainingCapacityStorage := nodeInfo.remainingCapacityStorage
	atomic.AddInt64(&(nodeInfo.remainingCapacityStorage), diffStorage)
	nodeCache.recordLabel(node.Name, node.Labels)
	NodeK8sCache.Store(node.Name, node.DeepCopy())
	log.GetLogger().Infof("node %s is updated, current remainingCapacityStorage: %d, previous "+
		"remainingCapacityStorage: %d, taints: %v, gpuType: %s", node.Name,
		atomic.LoadInt64(&(nodeInfo.remainingCapacityStorage)), previousRemainingCapacityStorage, nodeInfo.Taints,
		node.Labels[utils.GpuTypeAffinityKey])
	log.GetLogger().Infof("node current allocatable cpu:%d, mem:%d", nodeInfo.AllocatableCPU,
		nodeInfo.AllocatableMemory)
}

// UpdateNodeInfoByNodeID -
func UpdateNodeInfoByNodeID(nodeID string, info *config.NodeInfo) {
	nodeCache.mux.Lock()
	nodeInfo, exist := nodeCache.NodeList[nodeID]
	if !exist {
		nodeInfo = &NodeInfo{}
		nodeCache.NodeList[nodeID] = nodeInfo
	}
	nodeInfo.RequestCPU = info.CPUUsed
	nodeInfo.RequestMemory = info.MemoryUsed
	nodeInfo.LimitCPU = info.CPUTotal
	nodeInfo.LimitMemory = info.MemoryTotal
	nodeInfo.UsedPods = info.UsedPods
	nodeInfo.RequestGPUPercentage = info.RequestGPUPercentage
	nodeInfo.RequestGPUMem128 = info.RequestGPUMem128
	nodeInfo.RequestNvidiaGPU = info.RequestNvidiaGPU
	nodeCache.mux.Unlock()
}

// GetTotalNodeInfoWithFilters sums NodeInfo with filters
func GetTotalNodeInfoWithFilters(filters ...Filter) *config.NodeInfo {
	info := &config.NodeInfo{
		CPUTotal:    0,
		CPUUsed:     0,
		MemoryTotal: 0,
		MemoryUsed:  0,
	}
	nodeList := GetNodesWithFilters(filters...)
	nodeCache.mux.RLock()
	for _, nodeName := range nodeList {
		nodeInfo, exist := nodeCache.NodeList[nodeName]
		if !exist {
			continue
		}
		info.CPUTotal += nodeInfo.LimitCPU
		info.CPUUsed += nodeInfo.RequestCPU
		info.MemoryTotal += nodeInfo.LimitMemory
		info.MemoryUsed += nodeInfo.RequestMemory
	}
	nodeCache.mux.RUnlock()
	return info
}

// IsOverload check cpu mem is over threshold by label
func IsOverload(threshold float64, filters ...Filter) bool {
	info := GetTotalNodeInfoWithFilters(filters...)
	if info.CPUUsed == 0 || info.CPUTotal == 0 || info.MemoryUsed == 0 || info.MemoryTotal == 0 {
		log.GetLogger().Errorf("resource data is 0 with request cpu: %d, limit cpu: %d, "+
			"request memory: %d, limit memory: %d will not scale up",
			info.CPUUsed, info.CPUTotal, info.MemoryUsed, info.MemoryTotal)
		return true
	}
	if info.CPUUsed*constants.Magnification/info.CPUTotal > int64(threshold*constants.Magnification) {
		log.GetLogger().Warnf("cpu usage is over threshold: %f with used cpu: %d, total cpu: %d",
			threshold, info.CPUUsed, info.CPUTotal)
		return true
	}
	if info.MemoryUsed*constants.Magnification/info.MemoryTotal > int64(threshold*constants.Magnification) {
		log.GetLogger().Warnf("memory usage is over threshold: %f with used memory: %d, total memory: %d",
			threshold, info.MemoryUsed, info.MemoryTotal)
		return true
	}
	return false
}

// GetK8sNodeCacheByDeepCopy -
func GetK8sNodeCacheByDeepCopy(nodeName string) *corev1.Node {
	nc, ok := NodeK8sCache.Load(nodeName)
	if !ok {
		log.GetLogger().Warnf("node: %s is not exist in node cache.", nodeName)
		return nil
	}
	nodeK8sCache, ok := nc.(*corev1.Node)
	if !ok {
		log.GetLogger().Warnf("node: %s in node cache is not *corev1.Node struct", nodeName)
		return nil
	}
	return nodeK8sCache.DeepCopy()
}

// GetNodeNameByNodeIP -
func GetNodeNameByNodeIP(nodeIP string) string {
	nodeCache.mux.RLock()
	defer nodeCache.mux.RUnlock()
	for nodeName, nodeInfo := range nodeCache.NodeList {
		if nodeInfo.NodeIP == nodeIP {
			return nodeName
		}
	}

	return ""
}

// ConvertNodeMirroredDiskCacheToBytes -
func ConvertNodeMirroredDiskCacheToBytes(nodeMirroredDiskStr string) int64 {
	if nodeMirroredDiskStr == "" || nodeMirroredDiskStr == "0" {
		return 0
	}

	nodeMirroredDiskStrVal := nodeMirroredDiskStr[:len(nodeMirroredDiskStr)-1]

	nodeMirroredDiskFloatVal, err := strconv.ParseFloat(nodeMirroredDiskStrVal, 64)
	if err != nil {
		log.GetLogger().Errorf("failed to parse node mirrored disk str val: %s", nodeMirroredDiskStrVal)
		return 0
	}

	suffix := nodeMirroredDiskStr[len(nodeMirroredDiskStr)-1]

	switch suffix {
	case 'B':
		return int64(nodeMirroredDiskFloatVal)
	case 'K':
		return int64(nodeMirroredDiskFloatVal * convertKToB)
	case 'M':
		return int64(nodeMirroredDiskFloatVal * convertMToB)
	case 'G':
		return int64(nodeMirroredDiskFloatVal * convertGToB)
	case 'T':
		return int64(nodeMirroredDiskFloatVal * convertTToB)
	default:
		return 0
	}
}

type nodeInfo struct {
	nodeIP           string
	leftSpaceOfBytes int64
}

// ObtainingNodesSuitableForCache -
func ObtainingNodesSuitableForCache(cacheNodesPercentage float64, cacheNodesNumber int) []string {
	if cacheNodesPercentage <= threshold || cacheNodesPercentage > 1 {
		cacheNodesPercentage = defaultCacheNodesPercentage
	}

	var nodeList []nodeInfo
	var suitableNodes []string
	NodeMirroredDiskCache.Range(func(key, value any) bool {
		nodeIP, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to assert nodeIP of mirrored disk key: %v", key)
			return true
		}

		leftSpace, ok := value.(string)
		if !ok {
			log.GetLogger().Errorf("failed to assert left space of mirrored disk value: %v", value)
			return true
		}
		leftSpaceOfBytes := ConvertNodeMirroredDiskCacheToBytes(leftSpace)

		log.GetLogger().Debugf("left space of bytes is %s:%d, nodeIp: %s", leftSpace, leftSpaceOfBytes, nodeIP)

		if leftSpaceOfBytes <= 0 {
			log.GetLogger().Warnf("node: %s has no remaining space", nodeIP)
			return true
		}
		nodeList = append(nodeList, nodeInfo{nodeIP: nodeIP, leftSpaceOfBytes: leftSpaceOfBytes})
		return true
	})

	// Sort nodes based on the remaining space
	sort.Slice(nodeList, func(i, j int) bool {
		return nodeList[i].leftSpaceOfBytes > nodeList[j].leftSpaceOfBytes
	})

	if cacheNodesNumber == 0 {
		cacheNodesNumber = len(nodeList)
	}

	// Obtaining the Number of Nodes to Be Output
	numNodes := int(float64(len(nodeList)) * cacheNodesPercentage)
	if numNodes > cacheNodesNumber {
		numNodes = cacheNodesNumber
	}

	for i := 0; i < numNodes; i++ {
		suitableNodes = append(suitableNodes, nodeList[i].nodeIP)
	}
	return suitableNodes
}
