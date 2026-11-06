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

// Package nodebalance balances pressure of node(such as cpu usage of nodes) in cluster
package nodebalance

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/utils"
)

var (
	defaultBalancer *Balancer
)

const (
	defaultCPUThreshold           = 50
	defaultCPULowThreshold        = 35
	defaultPressureCountThreshold = 5
	defaultRefreshTime            = 60
	usedPodsLimitPercent          = 85
	requestLimitPercent           = 85

	// DefaultExpectNodeCPUPercent default cpu percent
	DefaultExpectNodeCPUPercent = int64(45)

	percent = 100
)

func init() {
	config.RegisterHotLoadNodeBalanceCallBack(hotLoadNodeBalance)
}

func hotLoadNodeBalance(config *config.FaasConfig, loadConfig *config.HotLoadConfig) {
	if loadConfig.NodeBalance.NodePressureThreshold == 0 {
		loadConfig.NodeBalance.NodePressureThreshold = defaultPressureCountThreshold
	}
	if loadConfig.NodeBalance.CPUPercentThreshold == 0 {
		loadConfig.NodeBalance.CPUPercentThreshold = defaultCPUThreshold
	}
	if loadConfig.NodeBalance.RefreshInterval == 0 {
		loadConfig.NodeBalance.RefreshInterval = defaultRefreshTime
	}
	if loadConfig.NodeBalance.ExpectNodeCPUPercent == 0 {
		loadConfig.NodeBalance.ExpectNodeCPUPercent = DefaultExpectNodeCPUPercent
	}
	if loadConfig.NodeBalance.CPUPercentLowThreshold == 0 {
		loadConfig.NodeBalance.CPUPercentLowThreshold = defaultCPULowThreshold
	}

	old := atomic.LoadInt64(&config.NodeBalance.NodePressureThreshold)
	newCfg := loadConfig.NodeBalance.NodePressureThreshold
	if old != newCfg {
		log.GetLogger().Infof("update nodePressureThreshold from %d to %d", old, newCfg)
		atomic.StoreInt64(&config.NodeBalance.NodePressureThreshold, newCfg)
		UpdatePressureCountThreshold(newCfg)
	}
	old = atomic.LoadInt64(&config.NodeBalance.CPUPercentThreshold)
	newCfg = loadConfig.NodeBalance.CPUPercentThreshold
	if old != newCfg {
		log.GetLogger().Infof("update cpuPercentThreshold from %d to %d", old, newCfg)
		atomic.StoreInt64(&config.NodeBalance.CPUPercentThreshold, newCfg)
		UpdateCPUThreshold(newCfg)
	}
	old = atomic.LoadInt64(&config.NodeBalance.CPUPercentLowThreshold)
	newCfg = loadConfig.NodeBalance.CPUPercentLowThreshold
	if old != newCfg {
		log.GetLogger().Infof("update cpuPercentLowThreshold from %d to %d", old, newCfg)
		atomic.StoreInt64(&config.NodeBalance.CPUPercentLowThreshold, newCfg)
		UpdateCPULowThreshold(newCfg)
	}
	old = atomic.LoadInt64(&config.NodeBalance.RefreshInterval)
	newCfg = loadConfig.NodeBalance.RefreshInterval
	if old != newCfg {
		log.GetLogger().Infof("update refreshInterval from %d to %d", old, newCfg)
		atomic.StoreInt64(&config.NodeBalance.RefreshInterval, newCfg)
		UpdateRefreshTime(newCfg)
	}
	oldDisabled := atomic.LoadUint32(&config.NodeBalance.Disabled)
	newDisabled := loadConfig.NodeBalance.Disabled
	if oldDisabled != newDisabled {
		log.GetLogger().Infof("update node balance disabled from %d to %d", oldDisabled, newDisabled)
		atomic.StoreUint32(&config.NodeBalance.Disabled, newDisabled)
		UpdateDisableStatus(newDisabled)
	}
	oldExpectNodeCPUPercent := atomic.LoadInt64(&config.NodeBalance.ExpectNodeCPUPercent)
	newExpectNodeCPUPercent := loadConfig.NodeBalance.ExpectNodeCPUPercent
	if oldExpectNodeCPUPercent != newExpectNodeCPUPercent {
		log.GetLogger().Infof("update node balance expectNodeCPUPercent from %d to %d",
			oldExpectNodeCPUPercent, newExpectNodeCPUPercent)
		atomic.StoreInt64(&config.NodeBalance.ExpectNodeCPUPercent, newExpectNodeCPUPercent)
	}
}

// Run runs the default balancer
func Run(stopCh <-chan struct{}, migratePodToSpecificNodeFunc func(pod corev1.Pod, node string),
	scoreFunc func(node string, profilingContent *monitoring.ProfilingContent, podMemory, podCPU int64) float64) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config, err: %s", err.Error())
		return
	}
	opt := Option{
		MetricsClient:                 utils.GetKubernetesMetricsClient(),
		PodLister:                     utils.GetPodInformer().Lister(),
		StopCh:                        stopCh,
		CPUThreshold:                  atomic.LoadInt64(&cfg.NodeBalance.CPUPercentThreshold),
		CPULowThreshold:               atomic.LoadInt64(&cfg.NodeBalance.CPUPercentLowThreshold),
		PressureCountThreshold:        atomic.LoadInt64(&cfg.NodeBalance.NodePressureThreshold),
		RefreshTime:                   atomic.LoadInt64(&cfg.NodeBalance.RefreshInterval),
		Disabled:                      atomic.LoadUint32(&cfg.NodeBalance.Disabled),
		MigratePodToSpecificNodeFunc:  migratePodToSpecificNodeFunc,
		ScoreFunc:                     scoreFunc,
		IsMatchMigrationWithLabelFunc: isMatchMigrationWithLabel,
	}
	defaultBalancer = NewBalancer(opt)
	go defaultBalancer.Run()
}

// UpdateCPUThreshold update CPU threshold of default balancer
func UpdateCPUThreshold(newCfg int64) {
	if defaultBalancer == nil {
		return
	}
	defaultBalancer.UpdateCPUThreshold(newCfg)
}

// UpdateCPULowThreshold update CPU low threshold of default balancer
func UpdateCPULowThreshold(newCfg int64) {
	if defaultBalancer == nil {
		return
	}
	defaultBalancer.UpdateCPULowThreshold(newCfg)
}

// UpdatePressureCountThreshold update pressure count threshold of default balancer
func UpdatePressureCountThreshold(newCfg int64) {
	if defaultBalancer == nil {
		return
	}
	defaultBalancer.UpdatePressureCountThreshold(newCfg)
}

// UpdateRefreshTime update refresh time of default balancer
func UpdateRefreshTime(newCfg int64) {
	if defaultBalancer == nil {
		return
	}
	defaultBalancer.UpdateRefreshTime(newCfg)
}

// UpdateDisableStatus update disable status of default balancer
func UpdateDisableStatus(disabled uint32) {
	if defaultBalancer == nil {
		return
	}
	defaultBalancer.UpdateDisableStatus(disabled)
}

type nodeState struct {
	name                    string
	allCPU                  int64
	usedCPU                 int64
	allMemory               int64
	requestMemory           int64
	requestCPU              int64
	usedPods                int64
	allocatePods            int64
	pressureCount           int64
	lowPressureCount        int64 // if over low percent, count++
	pods                    []podState
	score                   float64
	requestNvidiaGPU        int64
	requestGPUPercentage    int64
	requestGPUMem128        int64
	capacityNvidiaGPU       int64
	capacityGPUPercentage   int64
	capacityGPUMem128       int64
	gpuType                 string
	nodeType                string
	isNoDefaultScheduleNode bool
	antiAffinityPods        []podState
}

func (n *nodeState) canMigrate() bool {
	if n.usedPods*percent > n.allocatePods*usedPodsLimitPercent {
		return false
	}
	if n.requestCPU*percent > n.allCPU*requestLimitPercent {
		return false
	}
	if n.requestMemory*percent > n.allMemory*requestLimitPercent {
		return false
	}
	return true
}

func (n *nodeState) tryAddPods(pod podState) bool {
	if n.requestNvidiaGPU+pod.requestNvidiaGPU > n.capacityNvidiaGPU ||
		n.requestGPUPercentage+pod.requestGPUPercentage > n.capacityGPUPercentage ||
		n.requestGPUMem128+pod.requestGPUMem128 > n.capacityGPUMem128 {
		return false
	}
	// GPU node can't be shared by these types at the same time
	if n.requestNvidiaGPU+pod.requestNvidiaGPU > 0 && n.requestGPUPercentage+pod.requestGPUPercentage > 0 {
		return false
	}
	n.usedPods++
	n.requestCPU += pod.podCPU
	n.requestMemory += pod.podMemory
	n.usedCPU += pod.usedCPU
	n.requestGPUMem128 += pod.requestGPUMem128
	n.requestGPUPercentage += pod.requestGPUPercentage
	n.requestNvidiaGPU += pod.requestNvidiaGPU
	return true
}

func (n *nodeState) tryRemovePods(pod podState) {
	n.usedPods--
	n.requestCPU -= pod.podCPU
	n.requestMemory -= pod.podMemory
	n.usedCPU -= pod.usedCPU
	n.requestNvidiaGPU -= pod.requestNvidiaGPU
	n.requestGPUPercentage -= pod.requestGPUPercentage
	n.requestGPUMem128 -= pod.requestGPUMem128
}

type podState struct {
	name                 string
	usedCPU              int64
	funcName             string
	podMemory            int64
	podCPU               int64
	requestNvidiaGPU     int64
	requestGPUPercentage int64
	requestGPUMem128     int64
	podType              string
	podScheduleInfo      cache.PodScheduleInfo
}

// Option is options to create Balancer
type Option struct {
	MetricsClient                metrics.Interface
	PodLister                    listercorev1.PodLister
	StopCh                       <-chan struct{}
	CPUThreshold                 int64
	CPULowThreshold              int64
	PressureCountThreshold       int64
	RefreshTime                  int64
	Disabled                     uint32
	MigratePodToSpecificNodeFunc func(pod corev1.Pod, node string)
	ScoreFunc                    func(node string, profilingContent *monitoring.ProfilingContent,
		podMemory, podCPU int64) float64
	IsMatchMigrationWithLabelFunc func(nodeInfo *nodeState, podInfo podState) bool
}

// NewBalancer create new Balancer with given options
func NewBalancer(opt Option) *Balancer {
	if opt.PressureCountThreshold <= 0 {
		opt.PressureCountThreshold = defaultPressureCountThreshold
	}
	if opt.CPUThreshold <= 0 {
		opt.CPUThreshold = defaultCPUThreshold
	}
	if opt.RefreshTime <= 0 {
		opt.RefreshTime = defaultRefreshTime
	}
	if opt.CPULowThreshold <= 0 {
		opt.CPULowThreshold = defaultCPULowThreshold
	}
	return &Balancer{
		mc:                            opt.MetricsClient,
		podLister:                     opt.PodLister,
		stopCh:                        opt.StopCh,
		cpuThreshold:                  opt.CPUThreshold,
		cpuLowThreshold:               opt.CPULowThreshold,
		pressureCountThreshold:        opt.PressureCountThreshold,
		nodes:                         make(map[string]*nodeState),
		nodesMutex:                    &sync.RWMutex{},
		ticker:                        time.NewTicker(time.Duration(opt.RefreshTime) * time.Second),
		disabled:                      opt.Disabled,
		migratePodToSpecificNodeFunc:  opt.MigratePodToSpecificNodeFunc,
		scoreFunc:                     opt.ScoreFunc,
		isMatchMigrationWithLabelFunc: opt.IsMatchMigrationWithLabelFunc,
	}
}

// Balancer balance node pressure in cluster
type Balancer struct {
	ticker                       *time.Ticker
	mc                           metrics.Interface
	podLister                    listercorev1.PodLister
	stopCh                       <-chan struct{}
	cpuThreshold                 int64
	cpuLowThreshold              int64
	pressureCountThreshold       int64
	nodes                        map[string]*nodeState
	nodesMutex                   *sync.RWMutex
	disabled                     uint32
	migratePodToSpecificNodeFunc func(pod corev1.Pod, node string)
	scoreFunc                    func(node string, profilingContent *monitoring.ProfilingContent,
		podMemory, podCPU int64) float64
	isMatchMigrationWithLabelFunc func(tarGetNodeInfo *nodeState, podInfo podState) bool
}

// Run runs the node-balancer until stopCh closed
func (b *Balancer) Run() {
	for {
		select {
		case <-b.stopCh:
			return
		case <-b.ticker.C:
			if atomic.LoadUint32(&b.disabled) != 0 {
				continue
			}
			b.calculateMigrate()
		}
	}
}

func (b *Balancer) scaleNoScheduleLabelsMigratePod(noScheduleLabelNodes []string) {
	log.GetLogger().Infof("scale no schedule labels migrate pod, noScheduleLabelNodes %v", noScheduleLabelNodes)
	b.nodesMutex.RLock()
	defer b.nodesMutex.RUnlock()
	for _, nodeName := range noScheduleLabelNodes {
		allowMigrateNodes := make([]*nodeState, 0, len(b.nodes))
		nodeInfo, ok := b.nodes[nodeName]
		if !ok || (ok && len(nodeInfo.antiAffinityPods) == 0) {
			continue
		}
		for _, nodestate := range b.nodes {
			if nodestate.name != nodeName {
				allowMigrateNodes = append(allowMigrateNodes, nodestate)
			}
		}
		log.GetLogger().Debugf("current allowMigrateNodes %v, antiAffinityPod length %d",
			allowMigrateNodes, len(nodeInfo.antiAffinityPods))
		b.migratePodWithNoScheduleLabels(nodeName, allowMigrateNodes)
	}
}

func isInNodeList(nodeList []string, node string) bool {
	if len(nodeList) == 0 {
		return false
	}
	for _, n := range nodeList {
		if n == node {
			return true
		}
	}
	return false
}

func (b *Balancer) migratePodWithNoScheduleLabels(nodeName string, migratableNodes []*nodeState) {
	binPackEnable := false
	cfg, err := config.GetConfig()
	if err == nil {
		binPackEnable = cfg.BinPackEnable
	}

	_, cpuThreshold, _ := b.getMigrateConfig()
	taggedLabelNode := b.nodes[nodeName]
	b.migratePodToScheduleNodes(taggedLabelNode, migratableNodes, binPackEnable, cpuThreshold)
}

func (b *Balancer) migratePodToScheduleNodes(taggedLabelNode *nodeState, migratableNodes []*nodeState,
	binPackEnable bool, threshold int64) {
	if taggedLabelNode == nil || len(migratableNodes) == 0 {
		log.GetLogger().Warnf("not enough node to migrate, defaultLabelNode is nil, migratableNodes len: %d",
			len(migratableNodes))
		return
	}
	// categorize node by GPU resource
	lowPressureNodes := make(map[string][]*nodeState)
	for i, node := range migratableNodes {
		lowPressureNodes[node.gpuType] = append(lowPressureNodes[node.gpuType], migratableNodes[i])
	}

	// first try to migrate high pressure node to low pressure node
	log.GetLogger().Infof("try to migrate pod from taggedLabel node %s with schedule label of gpu type %s",
		taggedLabelNode.name, taggedLabelNode.gpuType)
	targetNodes, ok := lowPressureNodes[taggedLabelNode.gpuType]
	if !ok || len(targetNodes) == 0 {
		log.GetLogger().Infof("no candidate node of gpu type %s for node %s",
			taggedLabelNode.gpuType, taggedLabelNode.name)
		taggedLabelNode.pressureCount = 0
		taggedLabelNode.lowPressureCount = 0
		return
	}

	if binPackEnable {
		b.migrateWithScoreByNodeLabel(taggedLabelNode, targetNodes, threshold)
	} else {
		b.migrateByNodeLabel(taggedLabelNode, targetNodes, threshold)
	}
	// avoid migrate frequently, if this node is migrated this time, avoid to migrate this node in immediately
	taggedLabelNode.pressureCount = 0
	taggedLabelNode.lowPressureCount = 0
}

// UpdateCPUThreshold updates the CPU threshold of balancer
func (b *Balancer) UpdateCPUThreshold(new int64) {
	log.GetLogger().Infof("node balance update cpu threshold to %d", new)
	atomic.StoreInt64(&b.cpuThreshold, new)
}

// UpdateCPULowThreshold updates the CPU low threshold of balancer
func (b *Balancer) UpdateCPULowThreshold(new int64) {
	log.GetLogger().Infof("node balance update cpu threshold to %d", new)
	atomic.StoreInt64(&b.cpuLowThreshold, new)
}

// UpdatePressureCountThreshold updates the pressure count threshold of balancer
func (b *Balancer) UpdatePressureCountThreshold(new int64) {
	log.GetLogger().Infof("node balance update pressure count threshold to %d", new)
	atomic.StoreInt64(&b.pressureCountThreshold, new)
}

// UpdateRefreshTime update refresh time of balancer
func (b *Balancer) UpdateRefreshTime(new int64) {
	log.GetLogger().Infof("node balance update refresh time to %ds", new)
	b.ticker.Reset(time.Duration(new) * time.Second)
}

// UpdateDisableStatus update disable status of balancer
func (b *Balancer) UpdateDisableStatus(disabled uint32) {
	log.GetLogger().Infof("node balance update disabled status to %d", disabled)
	atomic.StoreUint32(&b.disabled, disabled)
}

func (b *Balancer) getMigrateConfig() (nodeThreshold, cpuThreshold, cpuLowThreshold int64) {
	nodeThreshold = atomic.LoadInt64(&b.pressureCountThreshold)
	cpuThreshold = atomic.LoadInt64(&b.cpuThreshold)
	cpuLowThreshold = atomic.LoadInt64(&b.cpuLowThreshold)
	log.GetLogger().Debugf("getMigrateConfig, nodeThreshold is %d, cpuThreshold is %d, cpuLowThreshold is %d",
		nodeThreshold, cpuThreshold, cpuLowThreshold)
	return
}

func (b *Balancer) calculateMigrate() {
	binPackEnable := false
	cfg, err := config.GetConfig()
	if err == nil {
		binPackEnable = cfg.BinPackEnable
	}

	nodeThreshold, cpuThreshold, cpuLowThreshold := b.getMigrateConfig()

	clusterCPUUsage, noScheduleLabelNodes := b.refreshNodeState(cpuThreshold, cpuLowThreshold)
	if clusterCPUUsage < 0 {
		log.GetLogger().Warn("can't get cluster cpu usage in migrate!")
		return
	}
	log.GetLogger().Infof("start migrate, the cluster cpu usage is %d", clusterCPUUsage)

	if err = b.refreshPodState(); err != nil {
		log.GetLogger().Errorf("refresh pod state error: %s", err.Error())
		return
	}

	if clusterCPUUsage >= cpuThreshold {
		// high pressure cluster
		log.GetLogger().Infof("cluster is high pressure, try to migrate")
		highPressure, targets := b.calculateMigrateTarget(nodeThreshold, cpuThreshold, true)
		b.migrateByPressureNodes(highPressure, targets, binPackEnable, cpuThreshold)
	} else if clusterCPUUsage >= cpuLowThreshold {
		// middle pressure cluster
		// first: migrate high to (low + middle)
		log.GetLogger().Infof("cluster is middle pressure, try to migrate")
		highPressure, targets := b.calculateMigrateTarget(nodeThreshold, cpuThreshold, true)
		b.migrateByPressureNodes(highPressure, targets, binPackEnable, cpuThreshold)
		// second: migrate (middle + high) to low
		// because after first migrate, there will exist(10% and 70%), not balance, all lower than cpuThreshold
		log.GetLogger().Infof("deal the second migrate")
		highAndMiddlePressure, LowTargets := b.calculateMigrateTarget(nodeThreshold, cpuLowThreshold, false)
		b.migrateByPressureNodes(highAndMiddlePressure, LowTargets, binPackEnable, cpuLowThreshold)
	} else {
		log.GetLogger().Infof("cluster is low pressure, try to migrate")
		// low pressure cluster
		highAndMiddlePressure, targets := b.calculateMigrateTarget(nodeThreshold, cpuLowThreshold, false)
		b.migrateByPressureNodes(highAndMiddlePressure, targets, binPackEnable, cpuLowThreshold)
	}
	// migration determination based on labels
	b.scaleNoScheduleLabelsMigratePod(noScheduleLabelNodes)
}

func (b *Balancer) migrateByPressureNodes(highPressure, lowPressure []*nodeState, binPackEnable bool, threshold int64) {
	if len(highPressure) == 0 || len(lowPressure) == 0 {
		log.GetLogger().Infof("not enough node to migrate, high pressure len: %d, low pressure len: %d",
			len(highPressure), len(lowPressure))
		return
	}
	// categorize node by GPU resource
	lowPressureNodes := make(map[string][]*nodeState)
	for i, node := range lowPressure {
		lowPressureNodes[node.gpuType] = append(lowPressureNodes[node.gpuType], lowPressure[i])
	}

	// sort by cpu left
	sort.Slice(highPressure, func(i, j int) bool {
		return highPressure[i].allCPU-highPressure[i].usedCPU < highPressure[j].allCPU-highPressure[j].usedCPU
	})

	// first try to migrate high pressure node to low pressure node
	for i := range highPressure {
		log.GetLogger().Infof("try to migrate pod from node %s of gpu type %s",
			highPressure[i].name, highPressure[i].gpuType)
		targetNodes, ok := lowPressureNodes[highPressure[i].gpuType]
		if !ok || len(targetNodes) == 0 {
			log.GetLogger().Infof("no candidate node of gpu type %s for node %s",
				highPressure[i].gpuType, highPressure[i].name)
			highPressure[i].pressureCount = 0
			highPressure[i].lowPressureCount = 0
			continue
		}
		if binPackEnable {
			b.migrateWithScore(highPressure[i], targetNodes, threshold)
		} else {
			b.migrate(highPressure[i], targetNodes, threshold)
		}
		// avoid migrate frequently, if this node is migrated this time, avoid to migrate this node in immediately
		highPressure[i].pressureCount = 0
		highPressure[i].lowPressureCount = 0
	}
}

// calculateMigrateTarget calculate migrate nodes, threshold is cpuThreshold or cpuLowThreshold
func (b *Balancer) calculateMigrateTarget(nodeThreshold, threshold int64, isHighLine bool) (
	[]*nodeState, []*nodeState) {
	highPressure := make([]*nodeState, 0, len(b.nodes))
	targets := make([]*nodeState, 0, len(b.nodes))

	b.nodesMutex.RLock()
	defer b.nodesMutex.RUnlock()

	for _, nodestate := range b.nodes {
		if isHighLine {
			if nodestate.pressureCount >= nodeThreshold {
				highPressure = append(highPressure, nodestate)
			}
		} else {
			if nodestate.lowPressureCount >= nodeThreshold {
				highPressure = append(highPressure, nodestate)
			}
		}

		if !isNodePressured(nodestate, threshold) && nodestate.canMigrate() && !nodestate.isNoDefaultScheduleNode {
			targets = append(targets, nodestate)
		}
		log.GetLogger().Debugf("node: %s, canMigrate is %t, threshold is %d, pressureCount is %d,"+
			"lowPressureCount is %d, allCpu is %d, usedCpu is %d, pod len is %d, usedPods is %d, requestCpu is %d",
			nodestate.name, nodestate.canMigrate(), threshold, nodestate.pressureCount, nodestate.lowPressureCount,
			nodestate.allCPU, nodestate.usedCPU, len(nodestate.pods), nodestate.usedPods, nodestate.requestCPU)
	}

	return highPressure, targets
}

func (b *Balancer) migrateByNodeLabel(migrateNode *nodeState, targets []*nodeState, threshold int64) {
	// sort by cpu left
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].allCPU-targets[i].usedCPU > targets[j].allCPU-targets[j].usedCPU
	})
	index := 0
	for i, podInfo := range migrateNode.antiAffinityPods {
		index = 0
		for j, node := range targets {
			index++
			if !b.isMatchMigrationWithLabelFunc(node, podInfo) {
				log.GetLogger().Infof("node %s not match condition pod %v", node.name, podInfo.name)
				continue
			}
			log.GetLogger().Infof("node %s match condition pod %v", node.name, podInfo.name)
			if b.tryMigrate(migrateNode.antiAffinityPods[i], migrateNode, targets[j], threshold) {
				// after success migrate, we refresh sort of targets
				sort.Slice(targets, func(i, j int) bool {
					return targets[i].allCPU-targets[i].usedCPU > targets[j].allCPU-targets[j].usedCPU
				})
				break
			}
		}
		if index == len(targets) {
			log.GetLogger().Infof("pod %s funcName %s failed to migrate, not have available nodes",
				podInfo.name, podInfo.funcName)
		}
	}
}

func (b *Balancer) migrate(migrateNode *nodeState, targets []*nodeState, threshold int64) {
	// sort by migrateNode pod used cpu
	sort.Slice(migrateNode.pods, func(i, j int) bool {
		return migrateNode.pods[i].usedCPU > migrateNode.pods[j].usedCPU
	})
	sortByPodType(migrateNode)

	// sort by cpu left
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].allCPU-targets[i].usedCPU > targets[j].allCPU-targets[j].usedCPU
	})

	index := 0
	for i, podInfo := range migrateNode.pods {
		index = 0
		for j, node := range targets {
			index++
			if !b.isMatchMigrationWithLabelFunc(node, podInfo) {
				log.GetLogger().Infof("node %s not match condition pod %v", node.name, podInfo.name)
				continue
			}
			log.GetLogger().Infof("node %s match condition pod %v", node.name, podInfo.name)
			if b.tryMigrate(migrateNode.pods[i], migrateNode, targets[j], threshold) {
				// after success migrate, we refresh sort of targets
				sort.Slice(targets, func(i, j int) bool {
					return targets[i].allCPU-targets[i].usedCPU > targets[j].allCPU-targets[j].usedCPU
				})
				break
			}
		}
		if !isNodePressured(migrateNode, threshold) {
			log.GetLogger().Infof("node %s is not pressured now, so stop this node migrate", migrateNode.name)
			break
		}

	}
}

func sortByPodType(migrateNode *nodeState) {
	if migrateNode.nodeType != utils.CAENodeAffinityValue && migrateNode.nodeType != utils.CAENodeAntiAffinityValue {
		return
	}
	var caeReservePods []podState
	var caePods []podState
	var faaSReservePods []podState
	var faaSPods []podState
	var defaultPods []podState

	for _, pod := range migrateNode.pods {
		log.GetLogger().Debugf("pod name %s ,pod.podType is %s.", pod.name, pod.podType)
		switch pod.podType {
		case utils.PodTypeCAEReserve:
			caeReservePods = append(caeReservePods, pod)
		case utils.PodTypeCAE:
			caePods = append(caePods, pod)
		case utils.PodTypeFaaSReserve:
			faaSReservePods = append(faaSReservePods, pod)
		case utils.PodTypeFaaS:
			faaSPods = append(faaSPods, pod)
		default:
			defaultPods = append(defaultPods, pod)
		}
	}

	if migrateNode.nodeType == utils.CAENodeAntiAffinityValue {
		migrateNode.pods = append(append(append(append(caeReservePods, caePods...), faaSReservePods...),
			faaSPods...), defaultPods...)
	} else {
		migrateNode.pods = append(append(append(append(faaSPods, faaSReservePods...), caePods...),
			caeReservePods...), defaultPods...)
	}
}

// return true to indicate that the pods can be migrated to this node
func isMatchMigrationWithLabel(tarGetNodeInfo *nodeState, podInfo podState) bool {
	if tarGetNodeInfo == nil {
		log.GetLogger().Warnf("tarGetNodeInfo is nil")
		return false
	}
	cached := cache.GetNodeInfo(tarGetNodeInfo.name)
	if cached == nil {
		log.GetLogger().Warnf("tarGetNodeInfo.name %s is not in cache", tarGetNodeInfo.name)
		return false
	}
	// functions that are not tagged can only migrate nodes that are not tagged by default
	if !podInfo.podScheduleInfo.TaggedScheduleLabel && !cached.IsDefaultLabel {
		log.GetLogger().Infof("podInfo %s need to migrate with default label, pod AffinityNodes %v, AntiAffinityNodes %v",
			podInfo.name, podInfo.podScheduleInfo.AffinityNodes, podInfo.podScheduleInfo.AntiAffinityNodes)
		return true
	}
	// if the tagged function does not have a strong affinity,
	// it can only be migrated to a node without a strong affinity,
	// and a node with a strong affinity can only be migrated to a strong affinity node.
	// if you don't have any, you can move at will
	if podInfo.podScheduleInfo.TaggedScheduleLabel &&
		(len(podInfo.podScheduleInfo.AffinityNodes) == 0 ||
			(len(podInfo.podScheduleInfo.AffinityNodes) != 0 &&
				utils.IsInSlice(tarGetNodeInfo.name, podInfo.podScheduleInfo.AffinityNodes))) &&
		(len(podInfo.podScheduleInfo.AntiAffinityNodes) == 0 ||
			(len(podInfo.podScheduleInfo.AntiAffinityNodes) != 0 &&
				!utils.IsInSlice(tarGetNodeInfo.name, podInfo.podScheduleInfo.AntiAffinityNodes))) {
		log.GetLogger().Infof("podInfo %s can migrate, pod AffinityNodes %v, AntiAffinityNodes %v",
			podInfo.name, podInfo.podScheduleInfo.AffinityNodes, podInfo.podScheduleInfo.AntiAffinityNodes)
		return true
	}

	return false
}

func (b *Balancer) migrateWithScoreByNodeLabel(migrateNode *nodeState, targets []*nodeState, threshold int64) {
	for i, podInfo := range migrateNode.antiAffinityPods {
		for j := range targets {
			profilingContent := monitoring.GetFunctionProfilingDataByFuncName(migrateNode.antiAffinityPods[i].funcName)
			targets[j].score = b.scoreFunc(targets[j].name, profilingContent,
				migrateNode.antiAffinityPods[i].podMemory, migrateNode.antiAffinityPods[i].podCPU)
		}

		sort.Slice(targets, func(i, j int) bool {
			return targets[i].score > targets[j].score
		})

		for j, node := range targets {
			if !b.isMatchMigrationWithLabelFunc(node, podInfo) {
				continue
			}
			if b.tryMigrate(migrateNode.antiAffinityPods[i], migrateNode, targets[j], threshold) {
				break
			}
		}
	}
}

func (b *Balancer) migrateWithScore(migrateNode *nodeState, targets []*nodeState, threshold int64) {
	sort.Slice(migrateNode.pods, func(i, j int) bool {
		return migrateNode.pods[i].usedCPU > migrateNode.pods[j].usedCPU
	})
	sortByPodType(migrateNode)

	for i, podInfo := range migrateNode.pods {
		for j := range targets {
			profilingContent := monitoring.GetFunctionProfilingDataByFuncName(migrateNode.pods[i].funcName)
			targets[j].score = b.scoreFunc(targets[j].name, profilingContent,
				migrateNode.pods[i].podMemory, migrateNode.pods[i].podCPU)
		}

		sort.Slice(targets, func(i, j int) bool {
			return targets[i].score > targets[j].score
		})

		for j, node := range targets {
			if !b.isMatchMigrationWithLabelFunc(node, podInfo) {
				continue
			}
			if b.tryMigrate(migrateNode.pods[i], migrateNode, targets[j], threshold) {
				break
			}
		}
		if !isNodePressured(migrateNode, threshold) {
			break
		}
	}
}

func (b *Balancer) tryMigrate(pod podState, from, to *nodeState, threshold int64) bool {
	if !to.tryAddPods(pod) {
		log.GetLogger().Infof("pod name %s, try to add Migrate pods failed!.", pod.name)
		return false
	}

	if isNodePressured(to, threshold) || !to.canMigrate() {
		log.GetLogger().Infof("try to migrate pod %s from %s to %s, is pressure: %t, canMigrate: %t, so cancel",
			pod.name, from.name, to.name, isNodePressured(to, threshold), to.canMigrate())
		to.tryRemovePods(pod)
		return false
	}

	p, err := b.podLister.Pods("default").Get(pod.name)
	if err != nil {
		log.GetLogger().Errorf("failed to get pod %s from lister, err: %s", pod.name, err.Error())
		to.tryRemovePods(pod)
		return false
	}
	log.GetLogger().Infof("start to migrate pod %s from %s to %s", pod.name, from.name, to.name)
	go b.migratePodToSpecificNodeFunc(*p, to.name)
	from.tryRemovePods(pod)
	return true
}

func isNodePressured(node *nodeState, threshold int64) bool {
	return node.usedCPU*percent >= node.allCPU*threshold
}

func hasTaints(node *cache.NodeInfo) bool {
	if len(node.Taints) == 0 {
		return false
	}
	for i := range node.Taints {
		// check if node has taints besides "node-type"="gpu"
		if node.Taints[i].Key != "node-type" || node.Taints[i].Value != "gpu" {
			return true
		}
	}
	return false
}

// refreshNodeState return cluster cpu usage percent
func (b *Balancer) refreshNodeState(cpuThreshold, cpuLowThreshold int64) (int64, []string) {
	nodes, err := b.mc.MetricsV1beta1().NodeMetricses().List(context.TODO(), v1.ListOptions{
		LabelSelector: nodeLabelSelector,
	})
	if err != nil {
		log.GetLogger().Warnf("failed to list node metrics, err: %s", err.Error())
		return -1, []string{}
	}

	newNodes := make(map[string]*nodeState, len(b.nodes))
	var noScheduleLabelNode []string

	clusterCPUUsage, clusterAllCPU := int64(0), int64(0)

	for _, node := range nodes.Items {
		cached := cache.GetNodeInfo(node.Name)
		// skip node that has taints
		if cached == nil || hasTaints(cached) {
			log.GetLogger().Warnf("can't get cache of node %s", node.Name)
			continue
		}

		cpu := node.Usage["cpu"]
		oldState, ok := b.nodes[node.Name]
		if !ok {
			oldState = &nodeState{}
		}

		// pressureCount ,lowPressureCount in newState default is 0
		newState := newNodeState(cached)
		newState.name = node.Name
		newState.usedCPU = cpu.MilliValue()

		// if node pressured for pressureCount consecutive times, to do migrate
		if isNodePressured(newState, cpuThreshold) {
			newState.pressureCount = oldState.pressureCount + 1
		}

		// if node pressured for lowPressureCount consecutive times, to do migrate
		if isNodePressured(newState, cpuLowThreshold) {
			newState.lowPressureCount = oldState.lowPressureCount + 1
		}

		newNodes[node.Name] = newState

		clusterCPUUsage += cpu.MilliValue()
		clusterAllCPU += cached.CapacityCPU

		isNoScheduleLabel := cache.IncludeNoScheduleLabels(cached.Label)
		if isNoScheduleLabel {
			noScheduleLabelNode = append(noScheduleLabelNode, node.Name)
		}
	}
	b.nodesMutex.Lock()
	b.nodes = newNodes
	b.nodesMutex.Unlock()

	if clusterAllCPU == 0 {
		return clusterAllCPU, noScheduleLabelNode
	}

	return clusterCPUUsage * percent / clusterAllCPU, noScheduleLabelNode
}

func newNodeState(nodeInfo *cache.NodeInfo) *nodeState {
	return &nodeState{
		allCPU:                  nodeInfo.CapacityCPU,
		allMemory:               nodeInfo.CapacityMemory,
		requestCPU:              nodeInfo.RequestCPU,
		requestMemory:           nodeInfo.RequestMemory,
		usedPods:                nodeInfo.UsedPods,
		allocatePods:            nodeInfo.AllocatablePods,
		requestGPUMem128:        nodeInfo.RequestGPUMem128,
		requestGPUPercentage:    nodeInfo.RequestGPUPercentage,
		requestNvidiaGPU:        nodeInfo.RequestNvidiaGPU,
		capacityGPUMem128:       nodeInfo.CapacityGPUMem128,
		capacityGPUPercentage:   nodeInfo.CapacityGPUPercentage,
		capacityNvidiaGPU:       nodeInfo.CapacityNvidiaGPU,
		gpuType:                 nodeInfo.Label[utils.GpuTypeAffinityKey],
		nodeType:                nodeInfo.Label[utils.PoolNodeLabelKey],
		isNoDefaultScheduleNode: nodeInfo.IsDefaultLabel,
	}
}

func (b *Balancer) refreshPodState() error {
	pods, err := b.mc.MetricsV1beta1().PodMetricses("default").List(context.TODO(), v1.ListOptions{
		LabelSelector: "isPoolPod=false",
	})
	if err != nil {
		log.GetLogger().Errorf("failed to list pod metrics, err: %s", err.Error())
		return err
	}
	for _, pod := range pods.Items {
		p, err := b.podLister.Pods("default").Get(pod.Name)
		if err != nil {
			log.GetLogger().Errorf("failed get pod %s from lister, err: %s", pod.Name, err.Error())
			continue
		}
		funcName := utils.GetFullFuncNameByPod(p)
		// 1. pod deleted 2. check point pod 3. pod don't have funcName label (maybe update label failed)
		// 4. stateful function
		if p.DeletionTimestamp != nil || p.Labels[utils.StandardLabelKey] == utils.CheckpointPodStandard ||
			funcName == "" || p.Labels[utils.LabelStateID] != "" {
			continue
		}
		used := int64(0)
		for _, c := range pod.Containers {
			if c.Name == "worker" {
				cpu := c.Usage["cpu"]
				// consider 2*worker's cpu as (worker+bus)'s cpu used by this pod
				used += cpu.MilliValue() << 1
			} else if c.Name == "runtime" {
				cpu := c.Usage["cpu"]
				used += cpu.MilliValue()
			}
		}

		nodes, ok := b.nodes[p.Spec.NodeName]
		if !ok {
			log.GetLogger().Warnf("skip non-exit node %s for pod %s", p.Spec.NodeName, p.Name)
			continue
		}

		podType := b.getPodType(pod, funcName)

		podReqs, _ := resourcehelper.PodRequestsAndLimits(p)
		funSvcID := utils.CrName(p.Labels[utils.LabelBusinessID], p.Labels[utils.LabelTenantID], funcName,
			p.Labels[utils.LabelFunctionVersion])
		podScheduleInfo := cache.WorkerCache.GetScheduleLabelNodes(funSvcID)
		nodes.pods = append(nodes.pods, podState{
			name:                 pod.Name,
			usedCPU:              used,
			funcName:             funcName,
			podMemory:            podReqs.Memory().Value(),
			podCPU:               podReqs.Cpu().MilliValue(),
			requestNvidiaGPU:     podReqs.Name(utils.NvidiaGPU, resource.DecimalSI).Value(),
			requestGPUPercentage: podReqs.Name(utils.GpuIsolationCoreKey, resource.DecimalSI).Value(),
			requestGPUMem128:     podReqs.Name(utils.GpuIsolationMemKey, resource.DecimalSI).Value(),
			podType:              podType,
			podScheduleInfo:      podScheduleInfo,
		})
		if isInNodeList(podScheduleInfo.AntiAffinityNodes, p.Spec.NodeName) {
			nodes.antiAffinityPods = append(nodes.antiAffinityPods, podState{
				name:                 pod.Name,
				usedCPU:              used,
				funcName:             funcName,
				podMemory:            podReqs.Memory().Value(),
				podCPU:               podReqs.Cpu().MilliValue(),
				requestNvidiaGPU:     podReqs.Name(utils.NvidiaGPU, resource.DecimalSI).Value(),
				requestGPUPercentage: podReqs.Name(utils.GpuIsolationCoreKey, resource.DecimalSI).Value(),
				requestGPUMem128:     podReqs.Name(utils.GpuIsolationMemKey, resource.DecimalSI).Value(),
				podType:              podType,
				podScheduleInfo:      podScheduleInfo,
			})
		}
	}

	return nil
}

func (b *Balancer) getPodType(pod v1beta1.PodMetrics, funcName string) string {
	funcSvcName := utils.CrName(pod.Labels[utils.LabelBusinessID], pod.Labels[utils.LabelTenantID],
		funcName, pod.Labels[utils.LabelFunctionVersion])
	funcMeta := cache.WorkerCache.GetFuncMeta(funcSvcName)
	if funcMeta == nil {
		log.GetLogger().Warnf("pod %s funcMeta is null, funcSvcName %s", pod.Name, funcSvcName)
	}
	if funcMeta != nil && pod.Labels[utils.IsPoolPodLabelKey] == utils.FalseString {
		if pod.Labels[utils.LabelApplier] == utils.AppID {
			if funcMeta.Spec.Configuration.BusinessType == constants.BusinessTypeCAE {
				return utils.PodTypeCAEReserve
			}
			return utils.PodTypeFaaSReserve
		}
		if funcMeta.Spec.Configuration.BusinessType == constants.BusinessTypeCAE {
			return utils.PodTypeCAE
		}
		return utils.PodTypeFaaS
	}
	return utils.PodTypeDefault
}

// NodeStateForPacking node state when packing
type NodeStateForPacking struct {
	AllCPU        int64
	UsedCPU       int64
	AllMemory     int64
	RequestMemory int64
}

// GetNodesState get node state
func GetNodesState(nodeName string) *NodeStateForPacking {
	nodestateForPacking := &NodeStateForPacking{}
	if defaultBalancer == nil {
		return nodestateForPacking
	}

	defaultBalancer.nodesMutex.RLock()
	defer defaultBalancer.nodesMutex.RUnlock()

	if state, ok := defaultBalancer.nodes[nodeName]; ok {
		nodestateForPacking.AllCPU = state.allCPU
		nodestateForPacking.UsedCPU = state.usedCPU
		nodestateForPacking.AllMemory = state.allMemory
		nodestateForPacking.RequestMemory = state.requestMemory
	}

	return nodestateForPacking
}

// UpdateNodeState update node state
func UpdateNodeState(nodeName string, usedCPU, requestsMemory int64) {
	if defaultBalancer == nil {
		return
	}

	defaultBalancer.nodesMutex.Lock()
	defer defaultBalancer.nodesMutex.Unlock()
	if state, ok := defaultBalancer.nodes[nodeName]; ok {
		state.usedCPU += usedCPU
		state.requestMemory += requestsMemory
		defaultBalancer.nodes[nodeName] = state
	}
}
