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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	commonvpa "functioncore/pkg/common/vpa"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/preload"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
)

const (
	defaultNotReadyNodeNum      = 10
	minNodeNotReadyEventsCount  = 3
	legalNodeNotReadyTimeWindow = 10 * time.Second

	checkPodIsDeletedRetryTimes = 10
	vpaLowThreshold             = 0.5
	vpaHighThreshold            = 0.85
	defaultCheckNodeStateTimes  = 5
	defaultRefreshNodeTime      = 60
	waitListPodTimeout          = 10 * time.Second
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

type migratePod struct {
	pod    *corev1.Pod
	cpu    int64
	memory int64
}

type migrateNode struct {
	node     corev1.Node
	cpuRatio float64
	memRatio float64
	score    float64
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
		if err := cleanPodsByNodeFunc(node, "app=vpa-pause-pod", "node-offline-clean-vpa-pause-pod"); err != nil {
			log.GetLogger().Warnf("failed to clean vpa pause pod on node: %s", node.Name)
		}
		return cleanPodsByNodeFunc(node, "isPoolPod", "node-offline-clean-"+node.Name)
	}
	if isVPAPausePodTurnToFaults(node) {
		log.GetLogger().Warnf("vpa pause pod turn to faults and start to clean pods on node: %s", node.Name)
		if err := cleanPodsByNodeFunc(node, "isPoolPod=true", "vpa-pause-pod-faults-clean-pool-pod"); err != nil {
			log.GetLogger().Warnf("failed to clean pool pod on node: %s err %s, "+
				"when vpa pause pod turn to faults", node.Name, err.Error())
		}
	}
	if isVPAPausePodTurnToInsufficient(node) {
		log.GetLogger().Warnf("vpa pause pod request resource is insufficient "+
			"and start to clean pods on migrate node: %s", node.Name)
		go MigrateVpaPodsOnNode(node)
	}

	if isNodeTurnToSchedule(oldNode, node) {
		log.GetLogger().Infof("node updated and now can schedule, %s", node.Name)
		err := vpa.NewVpaPausePod().AddVpaPod(context.Background(), node.Name, nil)
		if err != nil {
			log.GetLogger().Warnf("failed to add vpa-pause-pod on node: %s, error: %s", node.Name, err.Error())
		}
		go preload.GetClient().AddAllPreloadToOneNode(node)
	}

	return dealWithNotReadyNodeEvent(node)
}

func isVPAPausePodTurnToFaults(node *corev1.Node) bool {
	oldNode := cache.GetNodeInfo(node.Name)
	if oldNode == nil {
		return false
	}
	if oldNode.Label[constants.NodeVpaPausePodFaultsLabelKey] == "" &&
		node.Labels[constants.NodeVpaPausePodFaultsLabelKey] != "" {
		return true
	}
	return false
}

func isVPAPausePodTurnToInsufficient(node *corev1.Node) bool {
	oldNode := cache.GetNodeInfo(node.Name)
	if oldNode == nil {
		return false
	}
	if oldNode.Label[constants.NodeVpaPausePodFaultsLabelKey] !=
		constants.NodeVpaPausePodResourceInsufficientLabelValue &&
		node.Labels[constants.NodeVpaPausePodFaultsLabelKey] ==
			constants.NodeVpaPausePodResourceInsufficientLabelValue {
		return true
	}
	return false
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

func isNodeTurnToSchedule(oldNode *cache.NodeInfo, node *corev1.Node) bool {
	// first assert current node can schedule
	if node.Labels[constants.NodeRoleLabelKey] == "" || utils.HasMigratingTaints(node.Spec.Taints) {
		return false
	}
	// then check old node can't schedule
	if oldNode.Label[constants.NodeRoleLabelKey] == "" || utils.HasMigratingTaints(oldNode.Taints) {
		return true
	}
	return false
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

	template := utils.GeneratePodTemplate(&pod)
	utils.ConfigDemandNodes(template, []string{node})
	template.Labels[utils.LabelMigratedFrom] = shortPodName(pod.Name)
	template.Name = utils.GenerateMigratePodName(pod)

	// add migrating label to instance pod
	pod.Labels[utils.LabelMigrating] = utils.TrueString
	funcSvcID, ok := utils.OwnedByFunctionService(&pod)
	if !ok || funcSvcID == "" {
		return
	}
	// workerIns will record pod migrating label
	workerIns := utils.MakeWorkerInsInfo(&pod)
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
		if utils.GetFullFuncNameByPod(&instancePod) == "" {
			continue
		}
		// add migrating label to instance pod
		instancePod.Labels[utils.LabelMigrating] = utils.TrueString
		funcSvcID, ok := utils.OwnedByFunctionService(&instancePod)
		if !ok || funcSvcID == "" {
			continue
		}
		if !cache.WorkerCache.CheckMigratingInstanceShouldBeContinued(funcSvcID, instancePod.Name, traceID) {
			log.GetLogger().Warnf("pod %s is already migrating from node %s with %s", instancePod.Name, node.Name, traceID)
			continue
		}

		// workerIns will record pod migrating label
		workerIns := utils.MakeWorkerInsInfo(&instancePod)
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

// MigrateVpaPodsOnNode migrate the vpa pod on the vpa faults node
func MigrateVpaPodsOnNode(node *corev1.Node) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config, err: %s", err.Error())
		return
	}
	checkNodeTimes := atomic.LoadInt64(&cfg.NodeBalance.NodePressureThreshold)
	if checkNodeTimes == 0 {
		checkNodeTimes = defaultCheckNodeStateTimes
	}
	refreshTime := atomic.LoadInt64(&cfg.NodeBalance.RefreshInterval)
	if refreshTime == 0 {
		refreshTime = defaultRefreshNodeTime
	}

	// check the node status for one minute each time. if the node is still abnormal after five checks,
	// migrate the pods of the node.
	for i := 0; i < int(checkNodeTimes); i++ {
		time.Sleep(time.Duration(refreshTime) * time.Second)
		updateNode, err := utils.GetNodeInformer().Lister().Get(node.Name)
		if err != nil {
			log.GetLogger().Warnf("failed to get migrate node: %s", node.Name)
			return
		}
		_, ok := updateNode.Labels[constants.NodeVpaPausePodFaultsLabelKey]
		if !ok && checkCurNodeResourcePressure(node, "migrating-"+node.Name) {
			log.GetLogger().Infof("migrate node: %s has normal status", node.Name)
			return
		}
	}

	log.GetLogger().Warnf("node %s status is still unhealthy by recheck %d times, "+
		"begin to migrate pod on the node", node.Name, checkNodeTimes)
	if err := prepareMigratingVpaPods(node); err != nil {
		log.GetLogger().Warnf("failed to migrate pool pod on migrate node: %s", node.Name)
	}
}

func prepareMigratingVpaPods(node *corev1.Node) error {
	traceID := "migrating-" + node.Name
	// clean pool pod
	if err := cleanPodsByNodeFunc(node, "isPoolPod=true", traceID); err != nil {
		log.GetLogger().Errorf("failed to clean pool pods with traceID %s error %s", traceID, err.Error())
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), waitListPodTimeout)
	defer cancel()
	podList, err := utils.GetKubernetesClient().CoreV1().Pods(utils.DefaultNamespace).List(ctx,
		metav1.ListOptions{LabelSelector: "isPoolPod=false", FieldSelector: "spec.nodeName=" + node.Name})
	if err != nil {
		log.GetLogger().Errorf("failed to get instance pods with traceID %s error %s", traceID, err.Error())
		return err
	}

	migratePodList, err := sortMigratePod(node, podList, traceID)
	if err != nil {
		return err
	}

	log.GetLogger().Infof("there are %d migrating pods on node %s with %s", len(podList.Items), node.Name, traceID)
	for _, migratePod := range migratePodList {
		// pod is not being deleted or not ready will not be migrated
		if !utils.IsPodReady(migratePod.pod) {
			continue
		}
		if utils.GetFullFuncNameByPod(migratePod.pod) == "" {
			continue
		}

		funcSvcID, ok := utils.OwnedByFunctionService(migratePod.pod)
		if !ok || funcSvcID == "" {
			continue
		}

		err = selectNodeToMigrate(node, migratePod, funcSvcID, traceID)
		if err != nil {
			return err
		}
		if checkCurNodeResourcePressure(node, traceID) {
			log.GetLogger().Infof("curNode %s cpu and memory usage lower than %f, stop the instance migrate with %s",
				node.Name, vpaHighThreshold, traceID)
			return nil
		}
	}
	return nil
}

func selectNodeToMigrate(node *corev1.Node, migratePod migratePod, funcSvcID, traceID string) error {
	migrateNode, err := selectMigrateNode(funcSvcID, traceID)
	if err != nil {
		log.GetLogger().Errorf("failed to get avaliable new node for node %s to migrate, traceID %s ,error %s",
			node.Name, traceID, err.Error())
		return err
	}
	log.GetLogger().Infof("begin to migate pod %s in node %s to migrate %s, traceID %s", migratePod.pod.Name,
		node.Name, migrateNode.Name, traceID)
	MigratePodToSpecificNode(*migratePod.pod, migrateNode.Name)
	waitPodDeletedOrTimeout(migratePod.pod.Name, traceID)
	return nil
}

func checkCurNodeResourcePressure(curNode *corev1.Node, traceID string) bool {
	cachedNode := cache.GetNodeInfo(curNode.Name)
	resource, err := computeNodeResource(cachedNode, curNode.Name, traceID)
	if err != nil {
		return false
	}
	if resource.score > vpaHighThreshold {
		return false
	}
	vpaPod := vpa.NewVpaPausePod()
	pod, err := vpaPod.GetVpaPod(context.Background(), utils.VpaPausePodPrefix+curNode.Name)
	if err != nil || pod.Status.Phase != corev1.PodRunning {
		log.GetLogger().Warnf("vpa-pause-pod in node %s is unhealthy with %s", curNode.Name, traceID)
		return false
	}

	return true
}

func waitPodDeletedOrTimeout(podName, traceID string) {
	for i := 0; i < checkPodIsDeletedRetryTimes; i++ {
		_, err := utils.GetPodInformer().Lister().Pods(utils.FunctionNameSpace()).Get(podName)
		if err == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		if !utils.IsPodNotFound(err) {
			log.GetLogger().Warnf("get pod %s with traceID: %s, error: %s", podName, traceID, err.Error())
			time.Sleep(1 * time.Second)
			continue
		}
		log.GetLogger().Infof("pod %s is deleted after retry %d times with traceID: %s", podName, i+1, traceID)
		return
	}
	log.GetLogger().Warnf("pod %s is deleted failed by retry %d times with traceID: %s", podName,
		checkPodIsDeletedRetryTimes, traceID)
	return
}

func sortMigratePod(node *corev1.Node, podList *corev1.PodList, traceID string) ([]migratePod, error) {
	var migratePodList = make([]migratePod, 0, len(podList.Items))
	for _, instancePod := range podList.Items {
		// get the pod actual request cpu and memory
		if resource, ok := instancePod.Annotations[utils.AnnotationResource]; ok {
			var podResource commonvpa.PodResourceInfo
			err := json.Unmarshal([]byte(resource), &podResource)
			if err != nil {
				log.GetLogger().Errorf("failed to unmarshal instance pod resource by node %s with %s and %s",
					node.Name, traceID, err.Error())
				continue
			}
			copyPod := migratePod{
				pod:    instancePod.DeepCopy(),
				cpu:    podResource.Runtime.CPULimit,
				memory: podResource.Runtime.MemoryLimit,
			}
			migratePodList = append(migratePodList, copyPod)
		} else {
			log.GetLogger().Warnf("failed to get pod %s annotation resource | traceID %s", instancePod.Name, traceID)
			copyPod, err := getInstancePodResource(instancePod)
			if err != nil {
				log.GetLogger().Errorf("failed to get pod %s runtime resource | traceID %s", instancePod.Name, traceID)
				continue
			}
			migratePodList = append(migratePodList, copyPod)
		}
	}

	sort.Slice(migratePodList, func(i, j int) bool {
		if migratePodList[i].cpu < migratePodList[j].cpu {
			return false
		}
		return true
	})

	return migratePodList, nil
}

func getInstancePodResource(instancePod corev1.Pod) (migratePod, error) {
	for _, container := range instancePod.Spec.Containers {
		if container.Name == utils.RuntimeContainerName {
			return migratePod{
				pod:    instancePod.DeepCopy(),
				cpu:    container.Resources.Requests.Cpu().MilliValue(),
				memory: container.Resources.Requests.Memory().Value() >> utils.MiToByteExponent,
			}, nil
		}
	}
	return migratePod{}, errors.New("failed to get pod resource")
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

// vpaMigrateNodeSelector creates vpa migrate node label selector
func vpaMigrateNodeSelector() labels.Selector {
	req, err := labels.NewRequirement(constants.NodeVpaPausePodFaultsLabelKey, selection.DoesNotExist, []string{})
	if err != nil {
		log.GetLogger().Errorf("failed to requirement, err: %s", err.Error())
		return labels.Nothing()
	}
	s := labels.NewSelector()
	// NOTE: selector.Add() is similar to append(), we should use the return value
	return s.Add(*req)
}

func selectMigrateNode(funcSvcID, traceID string) (corev1.Node, error) {
	nodeList, err := utils.GetNodeInformer().Lister().List(vpaMigrateNodeSelector())
	if err != nil {
		log.GetLogger().Errorf("failed to get node list with traceID: %s, error%s", traceID, err.Error())
		return corev1.Node{}, err
	}
	var migrateNodeList = make([]migrateNode, 0, 1)
	for _, node := range nodeList {
		cached := cache.GetNodeInfo(node.Name)
		if !isNodeNormalSchedule(cached, funcSvcID, node.Name, traceID) {
			continue
		}

		migrateNodeInfo, err := computeNodeResource(cached, node.Name, traceID)
		if err != nil {
			log.GetLogger().Errorf("compute node resource with traceID %s error: %s", traceID, err.Error())
			return corev1.Node{}, errors.New("k8s must support metrics server")
		}

		if migrateNodeInfo.score < vpaLowThreshold {
			log.GetLogger().Infof("migrate node in %s | cpuRatio %3f | memRatio %3f | traceID %s", node.Name,
				migrateNodeInfo.cpuRatio, migrateNodeInfo.memRatio, traceID)
			return *node, nil
		}
		if migrateNodeInfo.score > vpaHighThreshold {
			log.GetLogger().Warnf("high pressure node %s | cpuRatio %3f | memRatio %3f | traceID %s", node.Name,
				migrateNodeInfo.cpuRatio, migrateNodeInfo.memRatio, traceID)
			continue
		}
		migrateNodeInfo.node = *node
		migrateNodeList = append(migrateNodeList, migrateNodeInfo)
	}
	if len(migrateNodeList) == 0 {
		return corev1.Node{}, errors.New("no available node for migrate")
	}

	sort.Slice(migrateNodeList, func(i, j int) bool {
		if migrateNodeList[i].score < migrateNodeList[j].score {
			return true
		}
		return false
	})
	chooseNode := migrateNodeList[0]
	log.GetLogger().Infof("migrate node in %s | cpuRatio %3f | memRatio %3f | traceID %s", chooseNode.node.Name,
		chooseNode.cpuRatio, chooseNode.memRatio, traceID)
	return chooseNode.node, nil
}

func isNodeNormalSchedule(cached *cache.NodeInfo, funcSvcID, name, traceID string) bool {
	// skip node that has taints
	if cached == nil || hasTaints(cached) {
		log.GetLogger().Warnf("can't get cache of node %s with traceID: %s", name, traceID)
		return false
	}
	if cached.IsDefaultLabel {
		log.GetLogger().Warnf("can't migrate node label [default=default-NoSchedule] of node %s with "+
			"traceID: %s", name, traceID)
		return false
	}

	// when vpa pod resources are updated, vpa-pod is deleted first and then a new vpa-pod is created.
	// as a result, the vpa-pod status may be unhealthy.
	vpaPod := vpa.NewVpaPausePod()
	pod, err := vpaPod.GetVpaPod(context.Background(), utils.VpaPausePodPrefix+name)
	if (err != nil && !utils.IsPodNotFound(err)) || (pod != nil && pod.Status.Phase == corev1.PodPending) {
		log.GetLogger().Warnf("vpa-pause-pod in node %s is unhealthy with traceID: %s", name, traceID)
		return false
	}

	podScheduleInfo := cache.WorkerCache.GetScheduleLabelNodes(funcSvcID)
	if len(podScheduleInfo.AntiAffinityNodes) > 0 && utils.IsInSlice(name, podScheduleInfo.AntiAffinityNodes) {
		log.GetLogger().Warnf("can't migrate node label with anti-affinity node %s with traceID: %s", name,
			traceID)
		return false
	}

	if len(podScheduleInfo.AffinityNodes) > 0 && !utils.IsInSlice(name, podScheduleInfo.AffinityNodes) {
		log.GetLogger().Warnf("can't migrate func %s with affinity label to node %s with traceID: %s", funcSvcID,
			name, traceID)
		return false
	}
	return true
}

func computeNodeResource(cached *cache.NodeInfo, name, traceID string) (migrateNode, error) {
	allCPU := cached.AllocatableCPU
	reqCPU := cached.RequestCPU
	allMem := cached.AllocatableMemory
	reqMem := cached.RequestMemory
	log.GetLogger().Debugf("get node %s | allCPU %d | reqCPU %d | allMem %d | reqMem %d | traceID %s",
		name, allCPU, reqCPU, allMem, reqMem, traceID)
	if reqCPU <= 0 || reqMem <= 0 {
		log.GetLogger().Errorf("get node %s cpu and mem is zero with traceID: %s, is support metrics server?",
			name, traceID)
		return migrateNode{}, errors.New("k8s must support metrics server")
	}
	cpuRatio := float64(reqCPU) / float64(allCPU)
	memRatio := float64(reqMem) / float64(allMem)
	log.GetLogger().Debugf("curNode %s | cpuRatio %f | memRatio %f | traceID %s", name, cpuRatio, memRatio, traceID)
	migrateNodeInfo := migrateNode{
		cpuRatio: cpuRatio,
		memRatio: memRatio,
		score:    maxRatio(cpuRatio, memRatio),
	}

	return migrateNodeInfo, nil
}

func maxRatio(x, y float64) float64 {
	if x > y {
		return x
	}
	return y
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
