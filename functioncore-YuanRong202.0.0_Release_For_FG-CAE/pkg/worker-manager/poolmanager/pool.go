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

// Package poolmanager manage pool
package poolmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/schedulequeue"
	tools "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
)

var (
	waitPodInterval = 100 * time.Millisecond
	// waitOndemandPodTimeout In the concurrent environment, it takes more than 15 seconds to start an ondemand pool.
	// Set waitOndemandPodTimeout to 30s to ensure that the pool is properly started.
	waitOndemandPodTimeout          = 30 * time.Second
	waitPodTimeout                  = 10 * time.Second
	customPathPrefix                = "custom-"
	weakAntiAffinity                = "weak"
	customContainerPreStopSLEEPTime = os.Getenv("CUSTOM_CONTAINER_PRE_STOP_SLEEP_TIME")
)

// NodePod stores pod in a node
type NodePod struct {
	podsQueue   podsObjQueue
	readyPodNum int
}

// podsObjQueue represents FIFO queue
type podsObjQueue struct {
	items []*podObj
}

// push adds an item to the end of the queue
func (q *podsObjQueue) push(obj *podObj) bool {
	if obj == nil {
		return false
	}
	for _, pod := range q.items {
		if pod.pod.Name == obj.pod.Name {
			return false
		}
	}
	q.items = append(q.items, obj)
	return true
}

// getSpecificPod get specific pod by podName
func (q *podsObjQueue) getSpecificPod(podName string) *podObj {
	if len(q.items) == 0 {
		return nil
	}
	for _, item := range q.items {
		if item.pod.Name == podName {
			return item
		}
	}
	return nil
}

// getReadyPod get ready pod from queue
func (q *podsObjQueue) getReadyPod() *podObj {
	if len(q.items) == 0 {
		return nil
	}
	for _, pod := range q.items {
		if !pod.usedFlag && utils.IsPodReady(pod.pod) {
			return pod
		}
	}
	return nil
}

func (q *podsObjQueue) getAvailablePodList() []*corev1.Pod {
	if len(q.items) == 0 {
		return nil
	}
	var availablePodList []*corev1.Pod
	for _, podItem := range q.items {
		if !podItem.usedFlag && utils.IsPodReady(podItem.pod) {
			availablePodList = append(availablePodList, podItem.pod)
		}
	}
	return availablePodList
}

func (q *podsObjQueue) remove(podName string) *podObj {
	if len(q.items) == 0 {
		return nil
	}
	var result []*podObj
	var removeItem *podObj
	for _, v := range q.items {
		if v != nil && v.pod != nil && v.pod.Name != podName {
			result = append(result, v)
			continue
		}
		if v != nil {
			removeItem = v
		}
	}
	q.items = result
	return removeItem
}

// Pool defines Pool's struct
type Pool struct {
	Deployment      *appsv1.Deployment
	mux             *sync.Mutex
	PodList         map[nodeName]*NodePod
	scalingConfig   *ScalingConfig
	Label           map[string]string
	Annotations     map[string]string
	CustomResources map[string]int64
	OndemandChan    map[string]chan *corev1.Pod

	Name             string
	NameSpace        string
	Standard         standard
	Language         string
	PodNum           int64
	LimitCPU         int64
	RequestCPU       int64
	LimitMemory      int64
	RequestMemory    int64
	EphemeralStorage int64
	WorkerLimitCPU   int64

	LivenessProbePeriodSeconds    int32
	LivenessProbeFailureThreshold int32

	PoolSize      int32
	PoolSizeLimit int32
	NonIsolated   bool
	queue         schedulequeue.Interface

	updateTime time.Time
}

type podObj struct {
	pod      *corev1.Pod
	usedFlag bool
}

// GetReplicas get current replicas
func (p *Pool) GetReplicas() int32 {
	return p.scalingConfig.replicas
}

func (p *Pool) string() string {
	return strings.Join([]string{p.NameSpace, string(p.Standard)}, "/")
}

func (p *Pool) choosePodFromNodes(ctx context.Context, nodes []string, noScheduleNodes []string, timeout time.Duration,
	req common.PodRequest) (*corev1.Pod, error) {
	// choose pod from a node which is chosen randomly from candidate nodes. all these nodes have pods but we dont
	// know if pods on the selected node are available. use PollImmediate to retry if there is no available pod be
	// found temporarily
	traceID := req.TraceID
	enableTMPExpansion := req.EnableTMPExpansion
	funcName := req.FuncName
	log.GetLogger().Debugf("choose pod from nodes %v with traceID: %s", nodes, traceID)
	var pod *corev1.Pod
	err := wait.PollImmediate(waitPodInterval, timeout, func() (done bool, err error) {
		select {
		case <-ctx.Done():
			log.GetLogger().Warnf("choosePodFromNodes for functionService %v canceled with traceID: %s",
				ctx.Value(utils.FunctionServiceIDKey{}), traceID)
			return true, ErrCancelGotPod
		default:
		}

		config, err := config.GetConfig()
		if err != nil || config.BinPackEnable == false {
			if len(nodes) == 0 {
				pod, done, err = p.selectPodFromRandomNodes(enableTMPExpansion, noScheduleNodes)
				return done, err
			}
			updatedNodeList := filterNoScheduleNode(nodes, noScheduleNodes)
			pod, done, err = p.selectPodFromSpecifiedNodes(updatedNodeList, enableTMPExpansion, noScheduleNodes)
		} else {
			// before: we try our best to get a pod in node which send scale up, if not exist, get a pod from random nodes
			// after: select pod all use bin-pack, we try our best to fill a node we expected usually use
			selectPodRule := SelectPodRuleFactory.GetRule(NormalPriorityPackingRule).
				WithEnableTMPExpansion(enableTMPExpansion).WithFuncName(funcName).WithNoScheduleNodes(noScheduleNodes)
			pod, done, err = p.selectPodByRule(selectPodRule)
			SelectPodRuleFactory.PutRuleToPool(NormalPriorityPackingRule, selectPodRule)
		}
		return done, err
	})
	if err != nil {
		log.GetLogger().Warnf("cannot find pod in nodes %s for pool %s with error: %s and traceID: %s",
			nodes, p.Name, err.Error(), traceID)
		return nil, ErrRequestExceedLimit
	}
	if pod == nil {
		log.GetLogger().Errorf("choose pod is nil with traceID: %s", traceID)
		return nil, ErrRequestExceedLimit
	}
	if enableTMPExpansion {
		alarm.GetClient().ClearAlarm(alarm.InsufficientEphemeralStorage)
	}
	return pod, nil
}

func (p *Pool) selectPodFromSpecifiedNodes(specifiedNodes []string,
	enableTMPExpansion bool, noScheduleNodes []string) (*corev1.Pod, bool, error) {
	// if node list is not empty will always choose pod from node list
	copyObj := tools.DeepCopy(specifiedNodes)
	copyNodes, ok := copyObj.([]string)
	if !ok {
		return nil, false, errors.New("deep copy failed")
	}
	var pod *corev1.Pod
	availableNodes := map[string]*cache.NodeInfo{}
	if enableTMPExpansion {
		availableNodes = cache.GetAvailableNodesForTMPExpansion()
	}
	for len(copyNodes) != 0 {
		randomIndex := utils.GetRandomIndex(len(copyNodes))
		// if TMPExpansion is enabled, select the node with sufficient storage
		_, existAvailableNode := availableNodes[copyNodes[randomIndex]]
		if enableTMPExpansion && !existAvailableNode {
			// node is invalid, continue without the node
			copyNodes = append(copyNodes[:randomIndex], copyNodes[randomIndex+1:]...)
			continue
		}

		pod = p.choosePodInNode(copyNodes[randomIndex])
		if pod != nil {
			go cache.ReduceNodeStorage(availableNodes, pod.Spec.NodeName)
			return pod, true, nil
		}
		// cannot find a available pod in this node, continue without this node
		copyNodes = append(copyNodes[:randomIndex], copyNodes[randomIndex+1:]...)
	}
	go cache.ReduceNodeStorage(availableNodes, "")
	// done is false and error is nil will be continue to retry, if tmp expansion is enabled, done is true, don't retry
	return nil, enableTMPExpansion, nil
}

func (p *Pool) selectPodFromRandomNodes(enableTMPExpansion bool, noScheduleNode []string) (*corev1.Pod, bool, error) {
	var (
		pod *corev1.Pod
	)
	if p.PodNum > 0 {
		// if pass empty node list into this function will directly choose pod randomly
		// if TMPExpansion is enabled, select the node with sufficient storage
		pod = p.choosePodRandom(enableTMPExpansion, noScheduleNode)
	}
	if pod != nil {
		return pod, true, nil
	}
	// done is false and error is nil will be continue to retry, if tmp expansion is enabled, done is true, don't retry
	return nil, enableTMPExpansion, nil
}

func (p *Pool) selectPodByRule(selectPodRule SelectPodRule) (*corev1.Pod, bool, error) {
	log.GetLogger().Debugf("packing: start to select pod by rule!, funcName is %s", selectPodRule.GetFuncName())
	var (
		pod *corev1.Pod
	)
	if p.PodNum > 0 {
		pod = selectPodRule.SelectPod(p)
	}
	if pod != nil {
		log.GetLogger().Debugf("packing: succeed to select pod by rule!, funcName is %s",
			selectPodRule.GetFuncName())
		return pod, true, nil
	}
	// done is false and error is nil will be continued to retry, if tmp expansion is enabled, done is true, don't retry
	return nil, selectPodRule.GetEnableTMPExpansion(), nil
}

func (p *Pool) demandPodFromAffinityNode(ctx context.Context,
	req common.PodRequest, timeout time.Duration, availableNodes []string) (*corev1.Pod, error) {
	// demand a pod from a node which is chosen randomly from candidate nodes. if there is no pod can be created on any
	// of the nodes at this moment then return nil and let outer function to call this process later
	log.GetLogger().Infof("demand pod by availableNodes: %v with traceID: %s", availableNodes, req.TraceID)
	var pod *corev1.Pod
	err := wait.PollImmediate(waitPodInterval, timeout, func() (done bool, err error) {
		select {
		case <-ctx.Done():
			log.GetLogger().Errorf("demandPodFromNodes for functionService %v canceled",
				ctx.Value(utils.FunctionServiceIDKey{}))
			return true, ErrCancelGotPod
		default:
		}
		log.GetLogger().Infof("func %s demand pod by availableNodes: %v with traceID: %s", req.FuncName,
			availableNodes, req.TraceID)
		pod = p.getPodInAffinityNode(availableNodes, p.CustomResources)
		if pod != nil {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		log.GetLogger().Errorf("demand pod by node error: %s with traceID: %s", err.Error(), req.TraceID)
		return nil, ErrGetPodOnDemand
	}
	if pod == nil {
		log.GetLogger().Warnf("pod is nil by node %v for standard %s with traceID: %s and timeout: %v",
			availableNodes, p.Standard, req.TraceID, timeout)
		return nil, ErrGetPodOnDemand
	}
	return pod, nil
}

func (p *Pool) demandPodFromNodes(ctx context.Context,
	req common.PodRequest, timeout time.Duration) (*corev1.Pod, error) {
	// demand a pod from a node which is chosen randomly from candidate nodes. if there is no pod can be created on any
	// of the nodes at this moment then return nil and let outer function to call this process later
	log.GetLogger().Infof("demand pod by node label: %v with traceID: %s", req.NodeLabel, req.TraceID)
	var pod *corev1.Pod
	err := wait.PollImmediate(waitPodInterval, timeout, func() (done bool, err error) {
		select {
		case <-ctx.Done():
			log.GetLogger().Errorf("demandPodFromNodes for functionService %v canceled",
				ctx.Value(utils.FunctionServiceIDKey{}))
			return true, ErrCancelGotPod
		default:
		}
		if len(req.NodeLabel) != 0 {
			pod = p.demandPodInNode(req.NodeLabel, p.CustomResources)
			if pod != nil {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		log.GetLogger().Errorf("demand pod by node labels error: %s with traceID: %s", err.Error(), req.TraceID)
		return nil, ErrGetPodOnDemand
	}
	if pod == nil {
		log.GetLogger().Warnf("pod is nil by node label %v for standard %s with traceID: %s and timeout: %v",
			req.NodeLabel, p.Standard, req.TraceID, timeout)
		return nil, ErrGetPodOnDemand
	}
	return pod, nil
}

func (p *Pool) choosePodInNode(node string) *corev1.Pod {
	p.mux.Lock()
	defer p.mux.Unlock()
	nodePod, exist := p.PodList[nodeName(node)]
	if !exist {
		return nil
	}
	if pod := nodePod.podsQueue.getReadyPod(); pod != nil {
		pod.usedFlag = true
		nodePod.readyPodNum--
		p.PodNum--
		return pod.pod
	}
	return nil
}

func getSortedNodeList(podList map[nodeName]*NodePod) []nodeName {
	noTaintsNodeList, hasTaintsNodeList := getTaintsNodeListAndNoTaintsNodeList(podList)
	return append(noTaintsNodeList, hasTaintsNodeList...)
}

func getTaintsNodeListAndNoTaintsNodeList(podList map[nodeName]*NodePod) ([]nodeName, []nodeName) {
	noTaintsNodeList := make([]nodeName, 0, len(podList))
	hasTaintsNodeList := make([]nodeName, 0, len(podList))

	for nodeID := range podList {
		nodeInfo := cache.GetNodeInfo(string(nodeID))
		if nodeInfo == nil {
			continue
		}
		if utils.HasMigratingTaints(nodeInfo.Taints) || utils.HasDsWorkerCrashTaints(nodeInfo.Taints) {
			hasTaintsNodeList = append(hasTaintsNodeList, nodeID)
		} else {
			noTaintsNodeList = append(noTaintsNodeList, nodeID)
		}
	}

	return noTaintsNodeList, hasTaintsNodeList
}

func (p *Pool) choosePodRandom(enableTMPExpansion bool, noScheduleNode []string) *corev1.Pod {
	availableNodes := map[string]*cache.NodeInfo{}
	if enableTMPExpansion {
		availableNodes = cache.GetAvailableNodesForTMPExpansion()
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	// prefer to select pod on no taints node
	nodeList := getSortedNodeList(p.PodList)
	updatedNodeList := filterNoScheduleNode(nodeList, noScheduleNode)
	for _, nodeID := range updatedNodeList {
		nodePod := p.PodList[nodeID]
		if nodePod.readyPodNum == 0 {
			continue
		}
		if pod := nodePod.podsQueue.getReadyPod(); pod != nil {
			_, existAvailableNode := availableNodes[pod.pod.Spec.NodeName]
			podIsValid := !enableTMPExpansion || existAvailableNode
			if podIsValid {
				pod.usedFlag = true
				nodePod.readyPodNum--
				p.PodNum--
				go cache.ReduceNodeStorage(availableNodes, pod.pod.Spec.NodeName)
				return pod.pod
			}
		}
	}
	go cache.ReduceNodeStorage(availableNodes, "")
	return nil
}

func (p *Pool) processSelectedPod(pod *corev1.Pod) {
	// the pool is locked when precessSelectedPod is invoked
	if pod == nil {
		return
	}
	nodePod := p.PodList[nodeName(pod.Spec.NodeName)]
	if podObj := nodePod.podsQueue.getSpecificPod(pod.Name); podObj != nil {
		podObj.usedFlag = true
		nodePod.readyPodNum--
		p.PodNum--
	}
}

func (p *Pool) reclaimUnusedSelectedPod(pod *corev1.Pod, enableTMPExpansion bool) {
	// the pool is locked when reclaimUnusedSelectedPod is invoked
	if pod == nil {
		return
	}
	if enableTMPExpansion {
		nodeInfo := cache.GetNodeInfo(pod.Spec.NodeName)
		if nodeInfo != nil {
			nodeInfo.RemainingCapacityStorageInc()
		}
	}
	nodePod := p.PodList[nodeName(pod.Spec.NodeName)]
	if podObj := nodePod.podsQueue.getSpecificPod(pod.Name); podObj != nil {
		podObj.usedFlag = false
		nodePod.readyPodNum++
		p.PodNum++
	}
}

func (p *Pool) getDemandPodWithLabel(nodeLabels map[string]string) *corev1.Pod {
	podSpec := p.Deployment.Spec.Template.Spec.DeepCopy()
	if len(nodeLabels) != 0 {
		podSpec.NodeSelector = nodeLabels
	}
	meta := p.Deployment.Spec.Template.ObjectMeta.DeepCopy()
	meta.GenerateName = fmt.Sprintf("%s-%s-", p.Deployment.Name, constants.OndemandKey)
	meta.Annotations[constants.OndemandKey] = utils.TrueString
	meta.Labels[constants.TenantIDLabelKey] = p.Label[constants.TenantIDLabelKey]
	meta.OwnerReferences = getThisOwnerReference()
	return &corev1.Pod{
		ObjectMeta: *meta,
		Spec:       *podSpec,
	}
}

func (p *Pool) getDemandPodWithAffinityNodes(availableNodes []string) *corev1.Pod {
	podSpec := p.Deployment.Spec.Template.Spec.DeepCopy()
	opInNodeSelector := generateOpInNodeSelectorRequirement(availableNodes)
	configDemandPodWithSchedulePolicy(podSpec, opInNodeSelector)
	meta := p.Deployment.Spec.Template.ObjectMeta.DeepCopy()
	meta.GenerateName = fmt.Sprintf("%s-%s-", p.Deployment.Name, constants.OndemandKey)
	meta.Annotations[constants.OndemandKey] = utils.TrueString
	meta.Labels[constants.TenantIDLabelKey] = p.Label[constants.TenantIDLabelKey]
	meta.OwnerReferences = getThisOwnerReference()
	return &corev1.Pod{
		ObjectMeta: *meta,
		Spec:       *podSpec,
	}
}

func generateOpInNodeSelectorRequirement(availableNodes []string) []corev1.NodeSelectorRequirement {
	return []corev1.NodeSelectorRequirement{
		{
			Key:      k8sHostNameKey,
			Operator: corev1.NodeSelectorOpIn,
			Values:   availableNodes,
		},
	}
}
func configDemandPodWithSchedulePolicy(podSpec *corev1.PodSpec, nodeSelector []corev1.NodeSelectorRequirement) {
	if podSpec.Affinity == nil {
		podSpec.Affinity = &corev1.Affinity{}
	}
	if podSpec.Affinity.NodeAffinity == nil {
		podSpec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	if podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}
	terms := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for i := range terms {
		// NodeSelectorTerms are ORed, so we should append on-demand node expression for each term
		terms[i].MatchExpressions = append(terms[i].MatchExpressions, nodeSelector...)
	}
	// add demand nodes if no term configured
	if len(terms) == 0 {
		terms = append(terms, corev1.NodeSelectorTerm{
			MatchExpressions: nodeSelector})
	}
	podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = terms
}

func (p *Pool) getPodInAffinityNode(availableNodes []string, customResource map[string]int64) *corev1.Pod {
	if len(availableNodes) == 0 {
		log.GetLogger().Errorf("nodeLabels len is 0")
		return nil
	}
	pod := p.getDemandPodWithAffinityNodes(availableNodes)
	if len(customResource) != 0 {
		configCustomResources(pod, customResource)
	}

	readyPod, err := p.getDemandPodInNode(pod)
	if err != nil {
		log.GetLogger().Errorf("failed to get demand pod in node, err: %s", err.Error())
		return nil
	}
	return readyPod
}

func (p *Pool) getDemandPodInNode(pod *corev1.Pod) (*corev1.Pod, error) {
	if pod == nil {
		return nil, errors.New("pod is nil")
	}
	clientSet := utils.GetKubernetesClient()
	if clientSet == nil {
		log.GetLogger().Errorf("kubernetes client set is nil")
		return nil, errors.New("kubernetes client set is nil")
	}
	curPod, err := clientSet.CoreV1().Pods(p.NameSpace).Create(context.TODO(), pod, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create demand pod by node error: %s", err.Error())
		return nil, err
	}
	strength.GetPodCache().SetState(curPod.Name, "", strength.BeforeSpecialized)
	log.GetLogger().Debugf("succeed to get demandPod, podName %s", curPod.Name)
	readyPod, err := p.waitPodReadyOrTimeout(clientSet, curPod)
	if err != nil {
		log.GetLogger().Errorf("failed to wait ready pod with name: %s, error: %s", curPod.Name, err.Error())
		return nil, err
	}
	return readyPod, nil
}

func (p *Pool) demandPodInNode(nodeLabels []map[string]string, customResource map[string]int64) *corev1.Pod {
	if len(nodeLabels) == 0 {
		log.GetLogger().Errorf("nodeLabels len is 0")
		return nil
	}
	pod := p.getDemandPodWithLabel(nodeLabels[0])
	if len(customResource) != 0 {
		configCustomResources(pod, customResource)
	}

	readyPod, err := p.getDemandPodInNode(pod)
	if err != nil {
		log.GetLogger().Errorf("failed to get demand pod in node, err: %s", err.Error())
		return nil
	}
	return readyPod
}
func (p *Pool) waitPodReadyOrTimeout(clientSet kubernetes.Interface, curPod *corev1.Pod) (*corev1.Pod, error) {
	podChan := make(chan *corev1.Pod)
	p.mux.Lock()
	// curPod name is different after pod created
	p.OndemandChan[curPod.Name] = podChan
	p.mux.Unlock()
	timer := time.NewTimer(waitOndemandPodTimeout)
	defer func() {
		p.mux.Lock()
		delete(p.OndemandChan, curPod.Name)
		p.mux.Unlock()
		podChan = nil
		timer.Stop()
	}()
	select {
	case readyPod, ok := <-podChan:
		if !ok {
			log.GetLogger().Warnf("notify channel is closed")
			return nil, ErrGetPodOnDemand
		}
		log.GetLogger().Infof("receive notification for ondemand pod %s", readyPod.Name)
		return readyPod, nil
	case _, ok := <-timer.C:
		if !ok {
			log.GetLogger().Warnf("timer closed")
		}
		log.GetLogger().Warnf("never receive notification for ondemand pod %s", curPod.Name)
		err := clientSet.CoreV1().Pods(p.NameSpace).Delete(context.TODO(), curPod.Name, metav1.DeleteOptions{})
		if err != nil {
			utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
				Namespace:        p.NameSpace,
				Ctx:              context.TODO(),
				OperationType:    utils.DeleteType,
				PodName:          curPod.Name,
				OperationOptions: metav1.DeleteOptions{},
			})
			log.GetLogger().Errorf("failed to delete ondemand pod %s with error: %s", curPod.Name, err.Error())
		} else {
			log.GetLogger().Infof("succeed to delete pod %s for %s", curPod.Name, utils.GetFullFuncNameByPod(curPod))
		}
		return nil, ErrGetPodOnDemand
	}
}

func (p *Pool) getCandidateAffinityNode(podScheduleInfo cache.PodScheduleInfo) ([]string, []string, bool) {
	candidates := make([]string, 0, utils.DefaultSliceSize)
	var availableNode []string
	var requireDeploy bool
	if podScheduleInfo.TaggedScheduleLabel && len(podScheduleInfo.AffinityNodes) > 0 {
		availableNode = filterNoScheduleNode(podScheduleInfo.AffinityNodes, podScheduleInfo.AntiAffinityNodes)
		requireDeploy = true
		p.mux.Lock()
		for _, node := range availableNode {
			if nodePod, exist := p.PodList[nodeName(node)]; exist && nodePod.readyPodNum > 0 {
				candidates = append(candidates, node)
			}
		}
		p.mux.Unlock()
	}
	return candidates, availableNode, requireDeploy
}

func (p *Pool) getPodFromAffinityNode(ctx context.Context, req common.PodRequest, podScheduleInfo cache.PodScheduleInfo, timeout time.Duration) (*corev1.Pod, error) {
	havePodNodes, availableNode, requireDeploy := p.getCandidateAffinityNode(podScheduleInfo)
	log.GetLogger().Infof("funcName: %s, traceID: %s, podScheduleInfo %v havePodNodes %v availableNode %v, "+
		"requireDeploy %t", req.FuncName, req.TraceID, podScheduleInfo, havePodNodes, availableNode, requireDeploy)
	if len(havePodNodes) != 0 {
		pod, err := p.choosePodFromNodes(ctx, havePodNodes, podScheduleInfo.AntiAffinityNodes, waitPodTimeout, req)
		if err == nil {
			log.GetLogger().Debugf("funcName %s succeed to choosePodFromNodes, traceID: %s podName: %s",
				req.FuncName, req.TraceID, pod.Name)
			return pod, nil
		}
		log.GetLogger().Warnf("failed to find pod on preferable nodes %s %s with traceID: %s",
			havePodNodes, err.Error(), req.TraceID)
	}
	if requireDeploy && len(availableNode) > 0 {
		return p.demandPodFromAffinityNode(ctx, req, timeout, availableNode)
	}
	if requireDeploy && len(availableNode) == 0 {
		log.GetLogger().Errorf("affinity and anti-Affinity conflicts, funcName: %s podScheduleInfo %v",
			req.FuncName, podScheduleInfo)
		return nil, errors.New("affinity and anti-Affinity conflicts")
	}
	return nil, nil
}

func (p *Pool) getPodInternal(ctx context.Context, demandNodes []string,
	req common.PodRequest, reservedAnti string, withPriority bool) (*corev1.Pod, error) {
	log.GetLogger().Infof("got pod from pool %s, the number of pods: %d, force: %t with traceID: %s",
		p.string(), p.PodNum, req.ForceDeploy, req.TraceID)
	log.GetLogger().Debugf("prefer to get pod on demandNodes: %v if one of demandNodes exists in pool %s "+
		"with traceID: %s", demandNodes, p.string(), req.TraceID)
	timeout, cancel := context.WithTimeout(ctx, waitPodTimeout)
	defer cancel()
	priorityCost := time.Duration(0)
	if withPriority && !req.IsMigrating {
		now := time.Now()
		item := p.queue.Add(&req)
		if err := item.Wait(timeout); err != nil {
			log.GetLogger().Warnf("wait schedule failed, traceID: %s, err: %s", req.TraceID, err.Error())
			return nil, ErrRequestExceedLimit
		}
		// NOTE: defer queue.Done should after error check
		// see Test_priorityScheduleQueue_ScheduleWithPriority_Atomic
		defer p.queue.Done(item)
		if priorityCost = time.Since(now); priorityCost >= waitPodTimeout {
			log.GetLogger().Errorf("wait schedule timeout, traceID: %s", req.TraceID)
			return nil, ErrRequestExceedLimit
		}
	}
	podScheduleInfo := cache.WorkerCache.GetScheduleLabelNodes(req.FunSvcID)
	log.GetLogger().Debugf("get schedule label nodes, FunSvcID %s, podScheduleInfo %v", req.FunSvcID, podScheduleInfo)
	affinityPod, err := p.getPodFromAffinityNode(ctx, req, podScheduleInfo, waitOndemandPodTimeout-priorityCost)
	if affinityPod != nil && err == nil {
		return affinityPod, nil
	}
	if affinityPod == nil && err != nil {
		log.GetLogger().Warnf("funcName %s failed to get pod from affinity node %v",
			req.FuncName, podScheduleInfo.AffinityNodes)
		return affinityPod, err
	}
	candidates := p.findCandidateNode(demandNodes)
	// try to get pod from candidates with no wait, if cannot get pod immediately then check if it is force deploy
	// scenario, if it is then demand a pod from demanded nodes otherwise start random choosing
	if len(candidates) != 0 {
		log.GetLogger().Debugf("preferable nodes are %v with traceID: %s", candidates, req.TraceID)
		pod, err := p.choosePodFromNodes(ctx, candidates, podScheduleInfo.AntiAffinityNodes, waitPodTimeout, req)
		if err == nil {
			return pod, nil
		}
		log.GetLogger().Warnf("failed to find pod on preferable nodes %s %s with traceID: %s",
			candidates, err.Error(), req.TraceID)
	}
	if req.ForceDeploy {
		return p.demandPodFromNodes(ctx, req, waitOndemandPodTimeout-priorityCost)
	}
	if req.Applier == constants.WorkerManagerApplier && reservedAnti == weakAntiAffinity {
		p.mux.Lock()
		nodeList := make([]string, 0, len(p.PodList))
		for nodeName := range p.PodList {
			nodeList = append(nodeList, string(nodeName))
		}
		p.mux.Unlock()
		reservedInstanceNode := cache.WorkerCache.GetReservedInsNodeDis(req.FunSvcID, nodeList, req.TraceID)
		log.GetLogger().Infof("start to choose pod from antiAffinity"+
			" node : %v, funSvcID: %s, traceID: %s", reservedInstanceNode, req.FunSvcID, req.TraceID)
		pod, err := p.choosePodFromNodes(ctx, reservedInstanceNode, podScheduleInfo.AntiAffinityNodes,
			waitPodTimeout-priorityCost, req)
		if len(reservedInstanceNode) != 0 && (pod == nil || err != nil) {
			_ = cache.WorkerCache.DeleteReservedInsNodeDis(req.FunSvcID, reservedInstanceNode[0])
		}
		return pod, err
	}
	return p.choosePodFromNodes(ctx, []string{}, podScheduleInfo.AntiAffinityNodes, waitPodTimeout-priorityCost, req)
}

func (p *Pool) getPodInternalWithCustomResource(customResource map[string]int64) (*corev1.Pod, error) {
	// get pod with custom resource doesn't support designating node
	var pod *corev1.Pod
	if len(p.CustomResources) != 0 {
		log.GetLogger().Debugf("choose pod wih custom resource %v from pool %s", customResource, p.Standard)
		pod = p.choosePodRandom(false, []string{})
		if pod != nil {
			return pod, nil
		}
	}
	log.GetLogger().Debugf("demand pod wih custom resource %v from pool %s", customResource, p.Standard)
	pod = p.demandPodInNode([]map[string]string{}, customResource)
	if pod != nil {
		return pod, nil
	}
	return nil, ErrRequestExceedLimit
}

func (p *Pool) findCandidateNode(demandNodes []string) []string {
	p.mux.Lock()
	candidates := make([]string, 0, utils.DefaultSliceSize)
	for _, node := range demandNodes {
		if nodePod, exist := p.PodList[nodeName(node)]; exist && nodePod.readyPodNum > 0 {
			candidates = append(candidates, node)
		}
	}
	p.mux.Unlock()
	return candidates
}

// NotifyRetryScale will be set by handler
var NotifyRetryScale func(string)

func (p *Pool) addPodToPool(pod *corev1.Pod) {
	p.mux.Lock()
	defer p.mux.Unlock()
	if isOndemand, ok := pod.Annotations[constants.OndemandKey]; ok && isOndemand == utils.TrueString {
		log.GetLogger().Infof("check ondemand pod status is ready or not: %s", pod.Name)
		if notifyChan, exist := p.OndemandChan[pod.Name]; exist && notifyChan != nil {
			log.GetLogger().Infof("ondemand pod %s is ready, notifying pool %s", pod.Name, p.Name)
			select {
			case notifyChan <- pod:
			default:
				log.GetLogger().Warnf("failed to write channel for pod %s, already notified?", pod.Name)
			}
		}
		return
	}
	nodePod, ok := p.PodList[nodeName(pod.Spec.NodeName)]
	if !ok || nodePod == nil {
		nodePod = &NodePod{
			readyPodNum: 0,
		}
		p.PodList[nodeName(pod.Spec.NodeName)] = nodePod
	}
	obj := &podObj{pod: pod, usedFlag: false}
	if nodePod.podsQueue.push(obj) {
		nodePod.readyPodNum++
		p.PodNum++
		if NotifyRetryScale != nil {
			NotifyRetryScale(string(p.Standard))
		}
	}
	p.updateTime = time.Now()
	log.GetLogger().Infof("added pod(%s) to pool(%s) in node %s, and the number of pods was %d",
		pod.Name, p.string(), pod.Spec.NodeName, p.PodNum)
}

func (p *Pool) deletePodFromPool(pod *corev1.Pod) {
	p.mux.Lock()
	defer p.mux.Unlock()
	if isOndemand, ok := pod.Annotations[constants.OndemandKey]; ok && isOndemand == utils.TrueString {
		log.GetLogger().Debugf("no need to delete ondemand pod from pool")
		return
	}
	nodePod, ok := p.PodList[nodeName(pod.Spec.NodeName)]
	if !ok {
		log.GetLogger().Debugf("deletePodFromPool pool %s doesn't exist in node %s",
			p.string(), pod.Spec.NodeName)
		return
	}

	if item := nodePod.podsQueue.remove(pod.Name); item != nil {
		// if usedFlag is false then this pod is not selected yet, decrease readyPodNum and podNum
		if !item.usedFlag {
			nodePod.readyPodNum--
			p.PodNum--
		}
		log.GetLogger().Infof("delete pod(%s) from pool(%s) in node %s, the number of pods is %d",
			pod.Name, p.string(), pod.Spec.NodeName, p.PodNum)
		p.updateTime = time.Now()
	}
	if len(p.PodList[nodeName(pod.Spec.NodeName)].podsQueue.items) == 0 {
		delete(p.PodList, nodeName(pod.Spec.NodeName))
	}
}

func (p *Pool) getHangingTime() time.Duration {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.PoolSize == 0 || p.PodNum > 0 {
		return 0
	}
	return time.Since(p.updateTime)
}

func (p *Pool) resetHangingTime() {
	p.mux.Lock()
	defer p.mux.Unlock()
	p.updateTime = time.Now()
}

func configCustomContainerLiftCycle(pod *corev1.Pod) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name != utils.RuntimeContainerName {
			continue
		}
		if pod.Spec.Containers[i].Lifecycle == nil {
			pod.Spec.Containers[i].Lifecycle = &corev1.Lifecycle{}
		}
		pod.Spec.Containers[i].Lifecycle.PreStop = &corev1.LifecycleHandler{}
		pod.Spec.Containers[i].Lifecycle.PreStop.HTTPGet = &corev1.HTTPGetAction{
			// in k8s version below 1.26, HTTPGetAction is hard-coded as fmt.Sprintf("http://%s/%s", host+port, path)
			// therefore, here we trim '/' prefix in path, and always use HTTP as scheme
			Path: strings.TrimPrefix(constants.PreStopPath, "/"),
			Port: intstr.IntOrString{
				Type:   intstr.Int,
				IntVal: workerAdminPort + 1,
			},
			Scheme: corev1.URISchemeHTTP,
		}
	}
}

func configCustomResources(pod *corev1.Pod, customResource map[string]int64) {
	conf, err := config.GetConfig()
	if err != nil {
		return
	}
	customResourcePaths := conf.PoolMgrConfig.VolumeConfig.CustomResourceMountPaths

	for i, container := range pod.Spec.Containers {
		if container.Name != utils.RuntimeContainerName {
			continue
		}
		for key, value := range customResource {
			container.Resources.Requests[corev1.ResourceName(key)] = resource.MustParse(fmt.Sprintf("%d",
				value))
			container.Resources.Limits[corev1.ResourceName(key)] = resource.MustParse(fmt.Sprintf("%d",
				value))
		}
		for index := 0; index < len(customResourcePaths); index++ {
			volume := corev1.VolumeMount{Name: customPathPrefix + customResourcePaths[index].Name,
				MountPath: customResourcePaths[index].ContainerPath}
			container.VolumeMounts = append(container.VolumeMounts, volume)
			pod.Spec.Containers[i] = container
		}
	}
	for index := 0; index < len(customResourcePaths); index++ {
		hostPath := &corev1.HostPathVolumeSource{Path: customResourcePaths[index].HostPath}
		volumeSource := corev1.VolumeSource{HostPath: hostPath}
		podVolume := corev1.Volume{Name: customPathPrefix + customResourcePaths[index].Name,
			VolumeSource: volumeSource}
		pod.Spec.Volumes = append(pod.Spec.Volumes, podVolume)
	}
}

type nodeNameString interface {
	~string
}

func filterNoScheduleNode[T, M nodeNameString](nodeList []T, noScheduleNode []M) []T {
	excludeMap := make(map[T]bool, len(noScheduleNode))
	for _, elem := range noScheduleNode {
		excludeMap[T(elem)] = true
	}

	var scheduleNode []T
	for _, elem := range nodeList {
		if !excludeMap[elem] {
			scheduleNode = append(scheduleNode, elem)
		}
	}
	return scheduleNode
}
