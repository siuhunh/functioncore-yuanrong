/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package vpa

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/wait"
	informer "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

const (
	verticalPausePodStandard = "pause-pod"
	verticalPauseImageName   = "pause"

	defaultPausePodTimeout = 1 * time.Minute
	checkPausePodInterval  = 30 * time.Second
	goroutineInterval      = 50 * time.Millisecond
	retryCountLimit        = 3
	retrySleepTime         = 1 * time.Second

	envVpaThresholdCPU           = "VPA_THRESHOLD_CPU"
	defaultPausePodCPU     int64 = 100
	defaultPausePodMem     int64 = 128
	defaultUpdateThreshold int64 = 1000
)

var (
	// ErrCreatePodPausePod represents error node creat pause pod exceed limited
	ErrCreatePodPausePod = errors.New("create pause pod error")
	// ErrUnScheduledForPausePod is the node role unscheduled
	ErrUnScheduledForPausePod = errors.New("the node is unscheduled for create pause pod")
	vpaPausePodResourceMap    sync.Map
)

// VerticalPod provide the vpa pod crud interface
type VerticalPod interface {
	AddVpaPod(ctx context.Context, nodeName string, req *corev1.ResourceRequirements) error
	AddVpaPodFull(ctx context.Context) error
	GetVpaPod(ctx context.Context, podName string) (*corev1.Pod, error)
	GetVpaPodList(ctx context.Context) ([]*corev1.Pod, error)
	DeleteVpaPod(ctx context.Context, pod *corev1.Pod) error
	UpdateVpaPodImage(ctx context.Context, pod *corev1.Pod, image string) error
	AddLabelNodeVpaPausePodFaults(ctx context.Context, nodeName string, insufficient bool) error
	DelLabelNodeVpaPausePodFaults(ctx context.Context, nodeName string) error
}

// Option is the user custom parameter
type Option func(v *VerticalPausePod)

// WithPodReadyTimeout is the vpa pod ready timeout
func WithPodReadyTimeout(t time.Duration) Option {
	return func(v *VerticalPausePod) {
		v.waitReadyTimeout = t
	}
}

// GetVpaThresholdCPU -
func GetVpaThresholdCPU() int64 {
	vpaThresholdCPUStr := os.Getenv(envVpaThresholdCPU)
	if vpaThresholdCPUStr == "" {
		return defaultUpdateThreshold
	}
	vpaThresholdCPU, err := strconv.Atoi(vpaThresholdCPUStr)
	if err != nil {
		return defaultUpdateThreshold
	}
	return int64(vpaThresholdCPU)
}

// VerticalPausePod is the vpa pod struct
type VerticalPausePod struct {
	nameSpace              string
	kubeClient             kubernetes.Interface
	waitReadyTimeout       time.Duration
	checkPodStatusInterval time.Duration
	informer               informer.PodInformer
	label                  labels.Selector
}

// NewVpaPausePod is the new vpa pod interface
func NewVpaPausePod(opts ...Option) VerticalPod {
	v := &VerticalPausePod{
		nameSpace:              utils.FunctionNameSpace(),
		kubeClient:             utils.GetKubernetesClient(),
		waitReadyTimeout:       defaultPausePodTimeout,
		checkPodStatusInterval: checkPausePodInterval,
		informer:               utils.GetPodInformer(),
		label:                  NewVpaPodSelector(),
	}

	for _, opt := range opts {
		opt(v)
	}
	return v
}

// NewVpaPodSelector creates instance pod's label selector
func NewVpaPodSelector() labels.Selector {
	req, err := labels.NewRequirement("app", selection.Equals, []string{"vpa-pause-pod"})
	if err != nil {
		log.GetLogger().Errorf("failed to new vpa pod requirement, err: %s", err.Error())
		return labels.Nothing()
	}
	s := labels.NewSelector()
	// NOTE: selector.Add() is similar to append(), we should use the return value
	return s.Add(*req)
}

// GetVpaPod provide pod info by pod name
func (v *VerticalPausePod) GetVpaPod(ctx context.Context, podName string) (*corev1.Pod, error) {
	nodeLister := v.informer.Lister()
	pausePod, err := nodeLister.Pods(v.nameSpace).Get(podName)
	if err != nil {
		log.GetLogger().Errorf("failed to get pod %s in namespace %s, error %s", podName, v.nameSpace,
			err.Error())
		return nil, err
	}
	return pausePod, nil
}

// GetVpaPodList provide all the vap pod
func (v *VerticalPausePod) GetVpaPodList(ctx context.Context) ([]*corev1.Pod, error) {
	nodeLister := v.informer.Lister()
	pausePodList, err := nodeLister.List(v.label)
	if err != nil {
		log.GetLogger().Errorf("failed to get pod by label app=vpa-pause-pod, error %s", err.Error())
		return nil, err
	}
	return pausePodList, nil
}

// UpdateVpaPodImage update the vpa pod resources
func (v *VerticalPausePod) UpdateVpaPodImage(ctx context.Context, pod *corev1.Pod, image string) error {
	pausePod, err := v.informer.Lister().Pods(v.nameSpace).Get(pod.Name)
	if err != nil {
		log.GetLogger().Errorf("failed to get pod %s in namespace %s, error %s", pod.Name, v.nameSpace,
			err.Error())
		return err
	}
	pausePodCopy := pausePod.DeepCopy()
	for i := range pausePodCopy.Spec.Containers {
		if pausePodCopy.Spec.Containers[i].Name == verticalPausePodStandard {
			pausePodCopy.Spec.Containers[i].Image = image
		}
	}
	_, err = v.kubeClient.CoreV1().Pods(v.nameSpace).Update(ctx, pausePodCopy, metav1.UpdateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to update pod with name: %s, error: %s", pod.Name, err.Error())
		return err
	}
	err = v.waitPodReady(ctx, pausePod)
	if err != nil {
		log.GetLogger().Errorf("failed to wait ready pod with name: %s, error: %s", pod.Name, err.Error())
		return err
	}
	return nil
}

// AddVpaPod pull up a new vpa pod in the new node
func (v *VerticalPausePod) AddVpaPod(ctx context.Context, nodeName string, req *corev1.ResourceRequirements) error {
	if !utils.IsVpaFeatureEnabled() {
		log.GetLogger().Info("the vpa feature is not open")
		return nil
	}

	nodeInfo, err := v.kubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		log.GetLogger().Errorf("get k8s node failed by: %s", err.Error())
		return err
	}
	if nodeInfo.Labels == nil {
		return ErrUnScheduledForPausePod
	}
	if utils.HasMigratingTaints(nodeInfo.Spec.Taints) {
		return ErrUnScheduledForPausePod
	}

	value, ok := nodeInfo.Labels[constants.NodeRoleLabelKey]
	if !ok || !utils.IsInSlice(value, constants.GetValidNodeRoleValues()) {
		log.GetLogger().Warnf("node %s is unscheduled", nodeInfo.Name)
		return ErrUnScheduledForPausePod
	}

	podName := utils.VpaPausePodPrefix + nodeInfo.Name
	_, err = v.addVpaPodInternal(ctx, podName, nodeInfo.Name, req)
	if err != nil {
		log.GetLogger().Errorf("add new node %s with a vpa pod failed by error: %s", nodeName, err.Error())
		return err
	}
	return nil
}

// AddVpaPodFull pulls up the vpa pod in the all node
func (v *VerticalPausePod) AddVpaPodFull(ctx context.Context) error {
	if !utils.IsVpaFeatureEnabled() {
		log.GetLogger().Info("the vpa feature is not open")
		return nil
	}

	var (
		errCh   = make(chan error, 1)
		onceErr = new(sync.Once)
	)

	nodesList, err := v.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.GetLogger().Errorf("get k8s node failed by: %s", err.Error())
		return err
	}

	var wg sync.WaitGroup
	for _, node := range nodesList.Items {
		if node.Labels == nil {
			continue
		}
		value, ok := node.Labels[constants.NodeRoleLabelKey]
		if !ok || !utils.IsInSlice(value, constants.GetValidNodeRoleValuesWithGPU()) {
			log.GetLogger().Warnf("node %s is unscheduled", node.Name)
			continue
		}

		podName := utils.VpaPausePodPrefix + node.Name
		nodeName := node.Name
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, e := v.addVpaPodInternal(ctx, podName, nodeName, nil)
			if e != nil {
				// err info is transferred only once to avoid err chan blocking
				onceErr.Do(func() {
					errCh <- e
				})
			}
		}()
	}

	wg.Wait()
	select {
	case err := <-errCh:
		log.GetLogger().Errorf("add all node with a vpa pod failed by error: %s", err.Error())
		return err
	default:
		log.GetLogger().Info("add all node with a vpa pod success")
		return nil
	}
}

// DeleteVpaPod delete vpa pod by pod name
func (v *VerticalPausePod) DeleteVpaPod(ctx context.Context, pod *corev1.Pod) error {
	gracePeriodSeconds := int64(0)
	propagationPolicy := metav1.DeletePropagationForeground
	option := metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &propagationPolicy}
	err := v.kubeClient.CoreV1().Pods(v.nameSpace).Delete(ctx, pod.Name, option)
	if err != nil {
		log.GetLogger().Warnf("failed to force delete pod: %s", pod.Name)
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        v.nameSpace,
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          pod.Name,
			OperationOptions: option,
		})
		return err
	}
	log.GetLogger().Infof("succeed to force delete pod %s", pod.Name)
	return nil
}

// AddLabelNodeVpaPausePodFaults -
func (v *VerticalPausePod) AddLabelNodeVpaPausePodFaults(ctx context.Context,
	nodeName string, insufficient bool) error {
	log.GetLogger().Infof("start to add label %s to node %s", constants.NodeVpaPausePodFaultsLabelKey, nodeName)
	nodeCache := cache.GetK8sNodeCacheByDeepCopy(nodeName)
	if nodeCache == nil {
		log.GetLogger().Warnf("failed to get node: %s from nodeCache , nodeCache is nil", nodeName)
		return nil
	}
	value := "default"
	if insufficient {
		value = constants.NodeVpaPausePodResourceInsufficientLabelValue
	}
	if lastValue, ok := nodeCache.Labels[constants.NodeVpaPausePodFaultsLabelKey]; ok && lastValue == value {
		return nil
	}
	nodeCache.Labels[constants.NodeVpaPausePodFaultsLabelKey] = value
	_, err := v.kubeClient.CoreV1().Nodes().Update(ctx, nodeCache, metav1.UpdateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to add label %s to node %s, err %s",
			constants.NodeVpaPausePodFaultsLabelKey, nodeName, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to add label %s to node %s", constants.NodeVpaPausePodFaultsLabelKey, nodeName)
	return nil
}

// DelLabelNodeVpaPausePodFaults -
func (v *VerticalPausePod) DelLabelNodeVpaPausePodFaults(ctx context.Context, nodeName string) error {
	nodeCache := cache.GetK8sNodeCacheByDeepCopy(nodeName)
	if nodeCache == nil {
		log.GetLogger().Warnf("failed to get node: %s from nodeCache , nodeCache is nil", nodeName)
		return nil
	}
	if _, ok := nodeCache.Labels[constants.NodeVpaPausePodFaultsLabelKey]; !ok {
		return nil
	}
	delete(nodeCache.Labels, constants.NodeVpaPausePodFaultsLabelKey)
	_, err := v.kubeClient.CoreV1().Nodes().Update(ctx, nodeCache, metav1.UpdateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to delete label %s to node %s, err %s",
			constants.NodeVpaPausePodFaultsLabelKey, nodeName, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to delete label %s to node %s", constants.NodeVpaPausePodFaultsLabelKey, nodeName)
	return nil
}

// addVpaPodInternal start a pause pod on single node
func (v *VerticalPausePod) addVpaPodInternal(ctx context.Context, podName string, nodeName string,
	req *corev1.ResourceRequirements) (
	*corev1.Pod, error) {
	pod := v.getPodSpec(podName, nodeName, req)
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, v.waitReadyTimeout)
	defer cancel()
	// avoid create pause-pod repeatedly when restarting worker-manager
	_, err := v.informer.Lister().Pods(v.nameSpace).Get(podName)
	if err == nil {
		log.GetLogger().Warnf("the vpa pause pod %s already existed", podName)
		return nil, err
	}
	if !utils.IsPodNotFound(err) {
		log.GetLogger().Errorf("failed to get pod by name: %s, error: %s", podName, err.Error())
		return nil, err
	}
	log.GetLogger().Infof("begin to create pause pod: %s", podName)
	curPod, err := v.kubeClient.CoreV1().Pods(v.nameSpace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create pod by name: %s, error: %s", podName, err.Error())
		return nil, err
	}

	log.GetLogger().Infof("wait for pause pod to become ready, podName: %s", podName)
	err = v.waitPodReady(ctx, curPod)
	if err != nil {
		log.GetLogger().Errorf("failed to wait ready pod with name: %s, error: %s", curPod.Name, err.Error())
		return nil, err
	}
	log.GetLogger().Infof("create pause pod success, podName: %s, used time: %s", podName,
		time.Since(start).String())
	return curPod, nil
}

func (v *VerticalPausePod) getPodSpec(podName string, nodeName string, req *corev1.ResourceRequirements) *corev1.Pod {
	var (
		pausePodSpec = corev1.PodSpec{}
		meta         = metav1.ObjectMeta{}
	)
	pauseImage := os.Getenv("PAUSE_POD_IMAGE")
	if pauseImage == "" {
		pauseImage = verticalPauseImageName
	}
	log.GetLogger().Infof("getPodSpec req %v", req)
	var requestCPU, requestMemory, limitCPU, limitMemory int64
	initPausePodCPU := getInitPauseSpecCPU()
	initPausePodMemory := getInitPauseSpecMemory()
	if req != nil && req.Requests != nil {
		requestCPU = req.Requests.Cpu().MilliValue() + initPausePodCPU
		requestMemory = req.Requests.Memory().Value()>>utils.MiToByteExponent + initPausePodMemory
	} else {
		requestCPU = initPausePodCPU
		requestMemory = initPausePodMemory
	}
	if req != nil && req.Limits != nil {
		limitCPU = req.Limits.Cpu().MilliValue() + initPausePodCPU
		limitMemory = req.Limits.Memory().Value()>>utils.MiToByteExponent + initPausePodMemory
	} else {
		limitCPU = initPausePodCPU
		limitMemory = initPausePodMemory
	}
	pausePodRequest := map[corev1.ResourceName]resource.Quantity{
		"cpu":    resource.MustParse(fmt.Sprintf("%dm", requestCPU)),
		"memory": resource.MustParse(fmt.Sprintf("%dMi", requestMemory)),
	}
	pausePodLimit := map[corev1.ResourceName]resource.Quantity{
		"cpu":    resource.MustParse(fmt.Sprintf("%dm", limitCPU)),
		"memory": resource.MustParse(fmt.Sprintf("%dMi", limitMemory)),
	}
	pausePodNodes := make([]string, 0, 1)
	pausePodNodes = append(pausePodNodes, nodeName)
	pausePodContainer := corev1.Container{
		Name:  verticalPausePodStandard,
		Image: pauseImage,
		Resources: corev1.ResourceRequirements{
			Requests: pausePodRequest,
			Limits:   pausePodLimit,
		},
	}
	pausePodSpec.Containers = append(pausePodSpec.Containers, pausePodContainer)
	meta.Name = podName
	meta.Labels = map[string]string{
		utils.StandardLabelKey: utils.VpaPausePodStandard,
		"app":                  utils.VpaPausePodStandard,
	}
	pod := &corev1.Pod{
		ObjectMeta: meta,
		Spec:       pausePodSpec,
	}
	if os.Getenv(constants.PodNamespaceEnvKey) != constants.TrustworthinessNameSpace {
		ownerReference := utils.DescribeOwnerReferences()
		pod.OwnerReferences = append(pod.OwnerReferences, ownerReference)
	}
	var nodeSelector []corev1.NodeSelectorRequirement
	nodeSelectorTerms := utils.GetNodeSelectorTerm(pausePodNodes)
	nodeSelector = append(nodeSelector, corev1.NodeSelectorRequirement{
		Key:      constants.NodeRoleLabelKey,
		Operator: corev1.NodeSelectorOpIn,
		Values:   constants.GetValidNodeRoleValues(),
	})
	for i := range nodeSelectorTerms {
		// NodeSelectorTerms are ORed, so we should append on-demand node expression for each term
		nodeSelectorTerms[i].MatchExpressions = append(nodeSelectorTerms[i].MatchExpressions, nodeSelector...)
	}

	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: nodeSelectorTerms,
				},
			},
		}
	}
	return pod
}

func getInitPauseSpecCPU() int64 {
	initPauseSpec := config.GetInitVpaPodSpec()
	if initPauseSpec.CPU > 0 {
		return initPauseSpec.CPU
	}
	return defaultPausePodCPU
}

func getInitPauseSpecMemory() int64 {
	initPauseSpec := config.GetInitVpaPodSpec()
	if initPauseSpec.Memory > 0 {
		return initPauseSpec.Memory
	}
	return defaultPausePodMem
}

func (v *VerticalPausePod) waitPodReady(ctx context.Context, pod *corev1.Pod) error {
	stopCh := make(chan struct{}, 1)
	podReady := make(chan bool, 1)
	defer close(stopCh)

	go func() {
		err := wait.PollImmediateUntil(v.checkPodStatusInterval, func() (bool, error) {
			pausePod, err := v.informer.Lister().Pods(v.nameSpace).Get(pod.Name)
			if err != nil {
				if utils.IsPodNotFound(err) {
					log.GetLogger().Warnf("pod %s not found", pod.Name)
					return false, nil
				}
				log.GetLogger().Errorf("get pod %s error: %s", pod.Name, err.Error())
				return false, err
			}
			if pausePod.Status.Phase == corev1.PodRunning {
				log.GetLogger().Infof("pod %s is running", pod.Name)
				podReady <- true
				return true, nil
			}

			log.GetLogger().Debugf("pod %s is not running yet", pod.Name)
			return false, nil
		}, stopCh)
		if err != nil {
			log.GetLogger().Errorf("pod %s is still not running, error: %s", pod.Name, err.Error())
		}
	}()

	select {
	case <-podReady:
		log.GetLogger().Infof("receive pause pod %s ready notification", pod.Name)
		return nil
	case _, ok := <-time.After(v.waitReadyTimeout):
		if !ok {
			log.GetLogger().Warnf("timer closed")
		}
		log.GetLogger().Warnf("never receive pause pod %s notification", pod.Name)
		return ErrCreatePodPausePod
	}
}

// InitPausePod -
func InitPausePod(stopCh <-chan struct{}) {
	if !utils.IsVpaFeatureEnabled() {
		log.GetLogger().Warn("vpa feature is not enable")
		return
	}
	if err := updatePausePodImage(); err != nil {
		log.GetLogger().Warnf("failed to update update vpa pause pod image, err %s", err.Error())
	}
	alarm.GetClient().RegisterAlarmType(alarm.VpaPausePod)
	go periodicalCheckVpaPausePodStatus(stopCh)
}

func updatePausePodImage() error {
	pausePodMgmt := NewVpaPausePod()
	pods, err := pausePodMgmt.GetVpaPodList(context.Background())
	if err != nil {
		log.GetLogger().Errorf("failed to get vpa pod list, err %s", err.Error())
		return err
	}
	if len(pods) == 0 {
		log.GetLogger().Warn("there is no vpa pause pod")
		return errors.New("there is no vpa pause pod")
	}
	var wg sync.WaitGroup
	for _, pod := range pods {
		pausePodImage := getPausePodImage()
		if pod.Spec.Containers[0].Image == pausePodImage {
			continue
		}
		wg.Add(1)
		pausePod := pod
		// Prevents instantaneous concurrent update of vpa pause pod images
		// from exerting too much pressure on the cluster.
		time.Sleep(goroutineInterval)
		go func() {
			defer wg.Done()
			log.GetLogger().Infof("the vpa pause pod %s image changes from %s to %s",
				pausePod.Name, pausePod.Spec.Containers[0].Image, pausePodImage)
			if err := pausePodMgmt.UpdateVpaPodImage(context.Background(), pausePod, pausePodImage); err != nil {
				log.GetLogger().Warnf("failed to update vpa pause pod %s images, err %s",
					pausePod.Name, err.Error())
			}
		}()
	}
	wg.Wait()
	return nil
}

func periodicalCheckVpaPausePodStatus(stopCh <-chan struct{}) {
	ticker := time.NewTicker(checkPausePodInterval)
	for {
		select {
		case <-stopCh:
			log.GetLogger().Infof("graceful exit")
			ticker.Stop()
			return
		case <-ticker.C:
			if err := checkPausePodStatus(); err != nil {
				log.GetLogger().Warnf("failed to check vpa pause pod status, err %s", err.Error())
			}
		}
	}
}

func checkPausePodStatus() error {
	pausePodMgmt := NewVpaPausePod()
	pods, err := pausePodMgmt.GetVpaPodList(context.Background())
	if err != nil {
		log.GetLogger().Warnf("failed to list vpa pause pod, err %s", err.Error())
		alarm.GetClient().ReportAlarm(alarm.VpaPausePod,
			fmt.Sprintf("error msg=pause pod list failed, err: %s", err.Error()))
		return err
	}
	notRunningPausePodNum := 0
	if len(pods) == 0 {
		return errors.New("there is no pause pod")
	}
	var wg sync.WaitGroup
	for _, pod := range pods {
		wg.Add(1)
		pausePod := pod
		go func() {
			defer wg.Done()
			if pausePod.Status.Phase == "Running" && checkPodContainerStatusesAllRunning(pausePod) {
				nodeName := utils.GetVpaPausePodNodeName(pausePod)
				if err := pausePodMgmt.DelLabelNodeVpaPausePodFaults(context.Background(), nodeName); err != nil {
					log.GetLogger().Warnf("failed to del pause pausePod fault label of node %s, err %s",
						nodeName, err.Error())
				}
				return
			}
			log.GetLogger().Warnf("the vpa pause pausePod %s is not running", pausePod.Name)
			if !reCheckPausePodStatusIsRunning(pausePodMgmt, pausePod.Name) {
				log.GetLogger().Warnf("recheck pause pausePod %s status is not running", pausePod.Name)
				nodeName := utils.GetVpaPausePodNodeName(pausePod)
				isInsufficient := checkPausePodIsInsufficient(pausePodMgmt, pausePod.Name)
				if err := pausePodMgmt.AddLabelNodeVpaPausePodFaults(context.Background(), nodeName, isInsufficient); err != nil {
					log.GetLogger().Warnf("failed to add pause pausePod fault label to node %s, err %s",
						nodeName, err.Error())
				}
				notRunningPausePodNum++
			}
		}()
		time.Sleep(goroutineInterval)
	}
	wg.Wait()
	if notRunningPausePodNum > 0 {
		errMsg := fmt.Sprintf("error msg=there is %d pause pod is not running", notRunningPausePodNum)
		alarm.GetClient().ReportAlarm(alarm.VpaPausePod, errMsg)
		return errors.New(errMsg)
	}
	alarm.GetClient().ClearAlarm(alarm.VpaPausePod)
	return nil
}

func checkPausePodIsInsufficient(mgmt VerticalPod, podName string) bool {
	pod, err := mgmt.GetVpaPod(context.Background(), podName)
	if err != nil {
		log.GetLogger().Warnf("failed to get vpa pause pod %s status, err: %s", podName, err.Error())
		return false
	}
	if pod.Status.Phase == "Pending" && checkPodContainerIsInsufficient(pod) {
		return true
	}
	return false
}

func checkPodContainerIsInsufficient(pod *corev1.Pod) bool {
	insufficient := false
	for i := range pod.Status.Conditions {
		if strings.Contains(pod.Status.Conditions[i].Message, "Insufficient") {
			insufficient = true
		}
	}
	return insufficient
}

func checkPodContainerStatusesAllRunning(pod *corev1.Pod) bool {
	allRunning := true
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].State.Running == nil {
			allRunning = false
		}
	}
	return allRunning
}

func reCheckPausePodStatusIsRunning(mgmt VerticalPod, podName string) bool {
	for retryCount := 1; retryCount <= retryCountLimit; retryCount++ {
		time.Sleep(time.Duration(retryCount) * retrySleepTime)
		pod, err := mgmt.GetVpaPod(context.Background(), podName)
		if err != nil {
			log.GetLogger().Warnf("failed to get vpa pause pod %s status, err: %s", podName, err.Error())
			continue
		}
		if pod.Status.Phase == "Running" && checkPodContainerStatusesAllRunning(pod) {
			return true
		}
		log.GetLogger().Warnf("%d recheck vpa pause pod %s status is still not running", retryCount, podName)
	}
	return false
}

func getPausePodImage() string {
	pauseImage := os.Getenv("PAUSE_POD_IMAGE")
	if pauseImage == "" {
		pauseImage = verticalPauseImageName
	}
	return pauseImage
}

// SetPausePodResourceRequirements -
func SetPausePodResourceRequirements(podName string, req *corev1.ResourceRequirements) {
	vpaPausePodResourceMap.Store(podName, req)
}

// RemovePausePodResourceRequirements -
func RemovePausePodResourceRequirements(podName string) {
	vpaPausePodResourceMap.Delete(podName)
}

// GetPausePodResourceRequirements -
func GetPausePodResourceRequirements(podName string) *corev1.ResourceRequirements {
	value, ok := vpaPausePodResourceMap.Load(podName)
	if !ok {
		return nil
	}
	req, ok := value.(*corev1.ResourceRequirements)
	if !ok {
		return nil
	}
	return req
}
