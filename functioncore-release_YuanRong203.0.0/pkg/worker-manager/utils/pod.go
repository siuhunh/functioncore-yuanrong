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

// Package utils provide CreatePod, DeletePod Function
package utils

import (
	"context"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker-manager/strength"
)

const (
	// DefaultWaitTimeout is the default time for waiting timeout, the default is 60s
	DefaultWaitTimeout      int64 = 60
	k8sHostNameKey                = "kubernetes.io/hostname"
	stuckTerminatingTimeout       = 10 * time.Minute
)

var waitMap sync.Map

// CreatePodTemplate is the template for creating pod
type CreatePodTemplate struct {
	// TraceID used to view related log information
	// require
	TraceID string
	// Timeout for wait pod ready
	WaitReadyTimeout int64
	// Spec is a description of a pod
	// require
	Spec *corev1.PodSpec
	// ObjectMeta is metadata that all persisted resources must have, which includes all objects
	// require
	ObjectMeta *metav1.ObjectMeta
}

// CreatePod create pod based on Pod template
func CreatePod(ctx context.Context, template *CreatePodTemplate) (*corev1.Pod, error) {
	pod := &corev1.Pod{
		ObjectMeta: *template.ObjectMeta,
		Spec:       *template.Spec,
	}
	start := time.Now()
	waiter := NewPodWaiter()
	waitMap.Store(pod.ObjectMeta.Name, waiter)
	waitReadyTimeout := DefaultWaitTimeout
	if template.WaitReadyTimeout > 0 {
		waitReadyTimeout = template.WaitReadyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(waitReadyTimeout)*time.Second)
	defer func() {
		waitMap.Delete(pod.ObjectMeta.Name)
		cancel()
	}()
	curPod, err := GetKubernetesClient().CoreV1().Pods(FunctionNameSpace()).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create pod: %s for %s traceID %s", err.Error(), pod.ObjectMeta.Name,
			template.TraceID)
		return nil, err
	}
	strength.GetPodCache().SetState(curPod.Name, "", strength.BeforeSpecialized)
	log.GetLogger().Infof("wait for %s pod to become ready, traceID: %s", pod.ObjectMeta.Name, template.TraceID)

	select {
	case <-ctx.Done():
		log.GetLogger().Errorf("create pod timeout for %ds, traceID: %s", waitReadyTimeout, template.TraceID)
		err := DeletePod(pod)
		if err != nil {
			log.GetLogger().Errorf("failed to delete pod %s with error %s, traceID: %s", pod.Name, err.Error(),
				template.TraceID)
		}
		return nil, snerror.NewSNError(snerror.GettingPodErrorCode, snerror.ErrText(snerror.GettingPodErrorCode))
	case status := <-waiter.WaitStatus():
		log.GetLogger().Infof("receive notification from %s pod , traceID: %s", curPod.Name, template.TraceID)
		if err := status.Err; err != nil {
			// The pod has entered a non-recoverable error state.
			log.GetLogger().Errorf("failed to wait container pod %s: %s, traceID: %s", curPod.Name, err.Error(),
				template.TraceID)
			delErr := DeletePod(pod)
			if delErr != nil {
				log.GetLogger().Errorf("failed to delete pod %s with error %s, traceID: %s", pod.Name,
					delErr.Error(), template.TraceID)
			}
			return nil, snerror.NewSNError(snerror.GettingPodErrorCode, err.Error())
		}
		log.GetLogger().Infof("%s pod is ready in node %s, traceID: %s, cost: %s",
			curPod.Name, curPod.Spec.NodeName, template.TraceID, time.Since(start))
		return status.Pod, nil
	}
}

// NotifyPodReadyStatus notifies pod's ready status
func NotifyPodReadyStatus(pod *corev1.Pod) {
	if !IsPodReady(pod) {
		log.GetLogger().Warnf("pod %s isn't ready", pod.Name)
		return
	}
	podWaiter, exist := waitMap.Load(pod.Name)
	if !exist {
		log.GetLogger().Warnf("the %s pod was not found in status waiter map, it may have been notified",
			pod.Name)
		return
	}
	waiter, ok := podWaiter.(*PodWaiter)
	if !ok || waiter == nil {
		log.GetLogger().Errorf("failed to assert PodWaiter for pod: %s", pod.Name)
		return
	}
	waiter.NotifyStatus(PodStatus{Pod: pod, Err: nil})
}

// DeletePod delete pod
func DeletePod(pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}
	err := GetKubernetesClient().CoreV1().Pods(FunctionNameSpace()).Delete(context.TODO(), pod.Name,
		metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		RetryPodOperationList.AddPodOperation(&PodOperationEle{
			Namespace:        FunctionNameSpace(),
			Ctx:              context.TODO(),
			OperationType:    DeleteType,
			PodName:          pod.Name,
			OperationOptions: metav1.DeleteOptions{},
		})
		log.GetLogger().Errorf("failed to delete pod %s with error %s", pod.Name, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to delete pod %s for %s", pod.Name, GetFullFuncNameByPod(pod))
	return nil
}

// ForciblyDeletePod forcibly delete a pod
func ForciblyDeletePod(pod *corev1.Pod) error {
	if pod == nil {
		return nil
	}
	deletionPolicy := metav1.DeletePropagationForeground
	gracePeriodSeconds := int64(0)
	option := metav1.DeleteOptions{PropagationPolicy: &deletionPolicy, GracePeriodSeconds: &gracePeriodSeconds}
	err := GetKubernetesClient().CoreV1().Pods(FunctionNameSpace()).Delete(context.TODO(), pod.Name, option)
	if err != nil && !errors.IsNotFound(err) {
		RetryPodOperationList.AddPodOperation(&PodOperationEle{
			Namespace:        FunctionNameSpace(),
			Ctx:              context.TODO(),
			OperationType:    DeleteType,
			PodName:          pod.Name,
			OperationOptions: option,
		})
		log.GetLogger().Errorf("failed to force delete pod %s with error %s", pod.Name, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to force delete pod %s for %s", pod.Name, GetFullFuncNameByPod(pod))
	return nil
}

// IsPodStuckTerminating checks if pod stuck in terminating state for a long time (10min)
func IsPodStuckTerminating(pod *corev1.Pod) bool {
	if len(pod.Status.InitContainerStatuses)+len(pod.Status.ContainerStatuses) == 0 {
		return false
	}

	allRunning, restartCount, lastTime := true, int32(0), time.Time{}

	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].State.Terminated == nil {
			return false
		}
		if pod.Status.InitContainerStatuses[i].State.Terminated.FinishedAt.Time.IsZero() {
			return false
		}
		if lastTime.Before(pod.Status.InitContainerStatuses[i].State.Terminated.FinishedAt.Time) {
			lastTime = pod.Status.InitContainerStatuses[i].State.Terminated.FinishedAt.Time
		}
	}

	for i := range pod.Status.ContainerStatuses {
		containerStatus := pod.Status.ContainerStatuses[i]
		restartCount += containerStatus.RestartCount

		terminated := false
		if containerStatus.State.Running == nil {
			allRunning = false
		}
		if containerStatus.State.Terminated != nil && !containerStatus.State.Terminated.FinishedAt.Time.IsZero() {
			terminated = true
			if lastTime.Before(containerStatus.State.Terminated.FinishedAt.Time) {
				lastTime = containerStatus.State.Terminated.FinishedAt.Time
			}

		} else if containerStatus.LastTerminationState.Terminated != nil &&
			!containerStatus.LastTerminationState.Terminated.FinishedAt.Time.IsZero() {
			terminated = true
			if lastTime.Before(containerStatus.LastTerminationState.Terminated.FinishedAt.Time) {
				lastTime = containerStatus.LastTerminationState.Terminated.FinishedAt.Time
			}
		}
		if !terminated {
			return false
		}
	}
	// skip pod that all containers are running (maybe all containers restarted successfully)
	if allRunning {
		return false
	}

	if restartCount > 0 {
		return true
	}

	if lastTime.Add(stuckTerminatingTimeout).After(time.Now()) {
		return false
	}
	return true
}

// IsPodNotFound check if it is pod not found
func IsPodNotFound(err error) bool {
	return err != nil && (k8serror.IsNotFound(err) || strings.Contains(err.Error(), "not found"))
}

// GetTmpExpansionNodeSelectorTerm get demandNodes nodeSelector and node label selector
func GetTmpExpansionNodeSelectorTerm(demandNodes []string) []corev1.NodeSelectorTerm {
	var nodeSelectorTerms []corev1.NodeSelectorTerm
	if len(demandNodes) != 0 {
		nodeSelectorTerm := corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      k8sHostNameKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   demandNodes,
			}, {
				Key:      "node-role",
				Operator: corev1.NodeSelectorOpIn,
				Values:   constants.GetValidNodeRoleValuesWithGPU(),
			}},
		}
		nodeSelectorTerms = append(nodeSelectorTerms, nodeSelectorTerm)
	}
	return nodeSelectorTerms
}

// GetNodeSelectorTerm get demandNodes nodeSelector
func GetNodeSelectorTerm(demandNodes []string) []corev1.NodeSelectorTerm {
	var nodeSelectorTerms []corev1.NodeSelectorTerm
	if len(demandNodes) != 0 {
		nodeSelectorTerm := corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      k8sHostNameKey,
				Operator: corev1.NodeSelectorOpIn,
				Values:   demandNodes,
			}},
		}
		nodeSelectorTerms = append(nodeSelectorTerms, nodeSelectorTerm)
	}
	return nodeSelectorTerms
}
