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

// Package poolmanager manages pool
package poolmanager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

const (
	defaultNotReadyNodeNum      = 10
	minNodeNotReadyEventsCount  = 3
	legalNodeNotReadyTimeWindow = 10 * time.Second
)

var (
	notReadyNodes = &NotReadyNodes{record: make(map[string]*nodeEvent, defaultNotReadyNodeNum)}
)

type nodeEvent struct {
	count     int
	timestamp time.Time
}

// NotReadyNodes -
type NotReadyNodes struct {
	record map[string]*nodeEvent
	sync.RWMutex
}

// GetEventsCount get events count
func (n *NotReadyNodes) GetEventsCount(nodeName string) int {
	count := 0
	n.RLock()
	event := n.record[nodeName]
	if event == nil {
		count = 0
	} else {
		count = event.count
	}
	n.RUnlock()
	return count
}

// Reset -
func (n *NotReadyNodes) Reset(nodeName string) {
	n.Lock()
	delete(n.record, nodeName)
	n.Unlock()
}

// RecordAndCheck record and check node is need to be dealt
func (n *NotReadyNodes) RecordAndCheck(nodeName string) bool {
	n.Lock()
	defer n.Unlock()
	event := n.record[nodeName]
	// beyond time window will reset node event count to 1
	if event == nil || event.count == 0 || time.Now().After(event.timestamp.Add(legalNodeNotReadyTimeWindow)) {
		n.record[nodeName] = &nodeEvent{timestamp: time.Now(), count: 1}
		return false
	}
	if event.count+1 >= minNodeNotReadyEventsCount {
		delete(n.record, nodeName)
		return true
	}
	n.record[nodeName].count++
	return false
}

// CleanPodsByNodeFunc -
type CleanPodsByNodeFunc func(node *corev1.Node, labelSelector, traceID string) error

// DoActionsFunc -
type DoActionsFunc func(funcSvcID string, actions []cache.Action, metadata cache.Metadata)

// SetBillingSwitchToWorkerFunc -
type SetBillingSwitchToWorkerFunc func(instance v1alpha1.WorkerInstance, enableBilling bool, traceID string)

var cleanPodsByNodeFunc CleanPodsByNodeFunc

var doActionsFunc DoActionsFunc

var setBillingSwitchToWorkerFunc SetBillingSwitchToWorkerFunc

// RegisterCleanPodsCallBack -
func RegisterCleanPodsCallBack(cleanPodsFunc CleanPodsByNodeFunc) {
	cleanPodsByNodeFunc = cleanPodsFunc
}

// RegisterDoActionsCallBack -
func RegisterDoActionsCallBack(callbackFunc DoActionsFunc) {
	doActionsFunc = callbackFunc
}

// RegisterSetBillingSwitchToWorkerCallBack -
func RegisterSetBillingSwitchToWorkerCallBack(callbackFunc SetBillingSwitchToWorkerFunc) {
	setBillingSwitchToWorkerFunc = callbackFunc
}

// AddNode add node
func AddNode(node *corev1.Node) error {
	if utils.HasMigratingTaints(node.Spec.Taints) {
		// it will migrate all instance when node has migrating taints
		if err := prepareMigratingPods(node); err != nil {
			return err
		}
	} else {
		// it will notify all reserved instance on the node to enable billing
		// when the node has no migrating taints
		notifyMigratingReservedIns(node)
	}
	return nil
}

func needMigratePodOnNode(oldNode *cache.NodeInfo, node *corev1.Node) bool {
	oldTaints := utils.HasMigratingTaints(oldNode.Taints)
	newTaints := utils.HasMigratingTaints(node.Spec.Taints)

	// current node don't have taints, no need to migrate
	if !newTaints {
		return false
	}

	// migrated taints is added
	if !oldTaints {
		return true
	}

	// node should be ready
	if !utils.IsNodeReady(node) {
		return false
	}

	// in case we add some taints manually to node that already has taints
	// mark CA tainted node unschedulable, e.g.
	if len(node.Spec.Taints) > len(oldNode.Taints) {
		return true
	}

	cfg, err := config.GetConfig()
	if err != nil {
		return false
	}
	taintsNodeCheckInterval := atomic.LoadInt64(&cfg.TaintsNodeCheckInterval)
	if taintsNodeCheckInterval <= 0 {
		return false
	}

	lastMigrateTime := atomic.LoadInt64(&oldNode.MigrateTimestamp)
	if time.Now().Unix()-lastMigrateTime >= taintsNodeCheckInterval {
		log.GetLogger().Infof("try to check taints node %s", node.Name)
		return true
	}

	return false
}

// UpdateNode update node
func UpdateNode(node *corev1.Node) error {
	oldNode := cache.GetNodeInfo(node.Name)
	if oldNode == nil {
		return nil
	}
	// migrated taints is deleted
	if utils.HasMigratingTaints(oldNode.Taints) && !utils.HasMigratingTaints(node.Spec.Taints) {
		// notify all migrating reserved instance with enable billing
		notifyMigratingReservedIns(node)
	}
	// migrated taints is added
	if needMigratePodOnNode(oldNode, node) {
		if err := prepareMigratingPods(node); err != nil {
			return err
		}
		cache.UpdateNodeMigrateTimestamp(node.Name, time.Now().Unix())
	}
	if isUnLabeled(node) {
		log.GetLogger().Warnf("node label is deleted and start to clean pods on node: %s", node.Name)
		return cleanPodsByNodeFunc(node, "isPoolPod", "node-offline-clean-"+node.Name)
	}

	return dealWithNotReadyNodeEvent(node)
}

func dealWithNotReadyNodeEvent(node *corev1.Node) error {
	if utils.IsNodeReady(node) {
		notReadyNodes.Reset(node.Name)
		return nil
	}
	log.GetLogger().Warnf("node %s has been not ready for %d times",
		node.Name, notReadyNodes.GetEventsCount(node.Name))
	if notReadyNodes.RecordAndCheck(node.Name) {
		if err := deletePoolPodsByForceOnNode(node.Name); err != nil {
			return err
		}
		// migrate all instances from not ready node to ready node
		return prepareMigratingPods(node)
	}
	return nil
}

func deletePoolPodsByForceOnNode(nodeName string) error {
	podList, err := utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: "isPoolPod=true", FieldSelector: "spec.nodeName=" + nodeName})
	if err != nil {
		log.GetLogger().Errorf("failed to list all pool pods in node: %s with error: %s", nodeName, err.Error())
		return err
	}
	log.GetLogger().Warnf("it will clean %d pool pods by force on node %s", len(podList.Items), nodeName)
	for _, pod := range podList.Items {
		if err = utils.ForciblyDeletePod(&pod); err != nil {
			return err
		}
	}
	return nil
}

// DeleteNode delete node
func DeleteNode(node *corev1.Node) error {
	notReadyNodes.Reset(node.Name)
	log.GetLogger().Warnf("node is deleted and start to clean pods on node: %s", node.Name)

	return cleanPodsByNodeFunc(node, "isPoolPod", "node-offline-clean-"+node.Name)
}

func isUnLabeled(node *corev1.Node) bool {
	oldNode := cache.GetNodeInfo(node.Name)
	if oldNode == nil {
		return false
	}
	if oldNode.Label[constants.NodeRoleLabelKey] != "" && node.Labels[constants.NodeRoleLabelKey] == "" {
		return true
	}
	return false
}

func shortPodName(name string) string {
	if len(name) <= utils.MaxLabelValueLength {
		return name
	}
	short := name[:utils.MaxLabelValueLength]
	log.GetLogger().Infof("pod name %s exceed limit and short it to %s", name, short)
	return short
}

// MigratePodToSpecificNode migrate pod to specific node
func MigratePodToSpecificNode(pod corev1.Pod, node string) {
	// pod is not being deleted or not ready will not be migrated
	if !utils.IsPodReady(&pod) {
		return
	}

	if pod.Labels[utils.LabelMCPFunc] == utils.TrueString {
		return
	}

	template := utils.GeneratePodTemplate(&pod)
	utils.ConfigDemandNodes(template, []string{node})
	template.Labels[utils.LabelMigratedFrom] = shortPodName(pod.Name)
	template.Name = utils.GenerateMigratePodName(pod.Name)

	funcSvcID, ok := utils.OwnedByFunctionService(&pod)
	if !ok || funcSvcID == "" {
		return
	}
	workerIns := utils.MakeWorkerInsInfo(&pod)
	// warning: Do not modify the pod's labels, which cause fatal error: concurrent map read and map write
	workerIns.IsMigrating = true
	if workerIns.Applier == constants.WorkerManagerApplier {
		go setBillingSwitchToWorkerFunc(workerIns, false, "migrating-"+pod.Name)
	}
	// instance updated event will look up migrating label and generate actions by migrating label
	actions, metadata := cache.WorkerCache.HandleMigratingInstance(funcSvcID,
		cache.NewInstanceMeta(workerIns, eventsource.Updated), template)
	doActionsFunc(funcSvcID, actions, metadata)
}

func prepareMigratingPods(node *corev1.Node) error {
	traceID := "migrating-" + node.Name
	// clean pool pod
	if err := cleanPodsByNodeFunc(node, "isPoolPod=true", traceID); err != nil {
		log.GetLogger().Errorf("failed to clean pool pods by node %s with %s and %s",
			node.Name, traceID, err.Error())
		return err
	}
	podList, err := utils.GetKubernetesClient().CoreV1().Pods(utils.DefaultNamespace).List(context.TODO(),
		metav1.ListOptions{LabelSelector: "isPoolPod=false", FieldSelector: "spec.nodeName=" + node.Name})
	if err != nil {
		log.GetLogger().Errorf("failed to get instance pods by node %s with %s and %s",
			node.Name, traceID, err.Error())
		return err
	}
	log.GetLogger().Infof("there are %d migrating pods on %s with %s", len(podList.Items), node.Name, traceID)
	for _, instancePod := range podList.Items {
		// pod is not being deleted or not ready will not be migrated
		if !utils.IsPodReady(&instancePod) {
			continue
		}
		if instancePod.Labels[utils.LabelMCPFunc] == utils.TrueString {
			continue
		}
		if utils.GetFullFuncNameByPod(&instancePod) == "" {
			continue
		}
		funcSvcID, ok := utils.OwnedByFunctionService(&instancePod)
		if !ok || funcSvcID == "" {
			continue
		}
		workerIns := utils.MakeWorkerInsInfo(&instancePod)
		workerIns.IsMigrating = true
		if workerIns.Applier == constants.WorkerManagerApplier && utils.IsNodeReady(node) {
			go setBillingSwitchToWorkerFunc(workerIns, false, traceID)
		}
		// instance updated event will look up migrating label and generate actions by migrating label
		actions, metadata := cache.WorkerCache.HandleMigratingInstance(funcSvcID,
			cache.NewInstanceMeta(workerIns, eventsource.Updated), nil)
		doActionsFunc(funcSvcID, actions, metadata)
	}
	return nil
}

func notifyMigratingReservedIns(node *corev1.Node) {
	traceID := "migrating-" + node.Name
	reservedInsLabelSelector := fmt.Sprintf("%s=%s", utils.LabelApplier, constants.WorkerManagerApplier)
	podList, err := utils.GetKubernetesClient().CoreV1().Pods(utils.DefaultNamespace).List(context.TODO(),
		metav1.ListOptions{LabelSelector: reservedInsLabelSelector, FieldSelector: "spec.nodeName=" + node.Name})
	if err != nil {
		log.GetLogger().Errorf("failed to get instance pods by node %s with %s and %s",
			node.Name, traceID, err.Error())
		return
	}
	log.GetLogger().Infof("it will notify %d reserved instances with enable billing on %s, traceID %s",
		len(podList.Items), node.Name, traceID)
	for _, instancePod := range podList.Items {
		// workerIns will record pod migrating label
		workerIns := utils.MakeWorkerInsInfo(&instancePod)
		go setBillingSwitchToWorkerFunc(workerIns, true, traceID)
	}
}
