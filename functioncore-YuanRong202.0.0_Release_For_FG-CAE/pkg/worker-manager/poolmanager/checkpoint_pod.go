/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/snerror"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
)

const defaultWaitPodReadyTimeout = 60 * time.Second

type checkpointPodMGR struct {
	nameSpace        string
	kubeClient       kubernetes.Interface
	config           config.PoolMgrConfig
	waitReadyTimeout time.Duration
	waitMap          sync.Map // key: pod.Name, value: *podWaiter
}

func newCheckpointPodMGR(kubeClient kubernetes.Interface, cfg config.PoolMgrConfig) *checkpointPodMGR {
	waitReadyTimeout := defaultWaitPodReadyTimeout
	if cfg.CheckpointPodConfig.WaitReadyTimeout > 0 {
		waitReadyTimeout = time.Duration(cfg.CheckpointPodConfig.WaitReadyTimeout) * time.Second
	}
	return &checkpointPodMGR{
		nameSpace:        utils.FunctionNameSpace(),
		kubeClient:       kubeClient,
		config:           cfg,
		waitReadyTimeout: waitReadyTimeout,
	}
}

func (c *checkpointPodMGR) start() {
	podList, err := c.kubeClient.CoreV1().Pods(utils.FunctionNameSpace()).List(context.TODO(),
		metav1.ListOptions{LabelSelector: fmt.Sprintf("%s=%s", utils.StandardLabelKey, utils.CheckpointPodStandard)})
	if err != nil {
		log.GetLogger().Warnf("failed to list all checkpoint pod with error: %s", err.Error())
		return
	}
	// clear previous checkpoint pods
	for _, pod := range podList.Items {
		c.deletePod(&pod)
	}
}

func (c *checkpointPodMGR) filter(req common.PodRequest) bool {
	if !snapshotmanager.GetSnapshotManagerMode() {
		// WorkerManager Mode: create snapshots for the first time
		return req.SnapshotDeployment != nil &&
			req.SnapshotDeployment.BucketID != "" && req.SnapshotDeployment.SnapshotSize == 0
	}

	// SnapshotManager Mode: update the existing snapshots
	return req.SnapshotDeployment != nil && req.SnapshotDeployment.BucketID != ""
}

func (c *checkpointPodMGR) getPodSpec(podName string, pool *Pool) *corev1.Pod {
	podSpec := pool.Deployment.Spec.Template.Spec.DeepCopy()
	runtimeImageTag := c.config.PoolContainerTags.RuntimeImageTag
	workerImageTag := c.config.PoolContainerTags.WorkerImageTag
	workerInitImageTag := c.config.PoolContainerTags.WorkerInitImageTag
	for i := range podSpec.Containers {
		podSpec.Containers[i].Env = append(podSpec.Containers[i].Env, corev1.EnvVar{
			Name:  "RUNNING_MODE",
			Value: "CHECKPOINT",
		})
		if podSpec.Containers[i].Name == utils.RuntimeContainerName {
			podSpec.Containers[i].Image = runtimeImageTag
			podSpec.Containers[i].Env = append(podSpec.Containers[i].Env, corev1.EnvVar{
				Name:  constants.CheckpointRestoreOpts,
				Value: c.config.CheckpointPodConfig.CRIUCheckpointOpts,
			})
		} else if podSpec.Containers[i].Name == utils.WorkerContainerName {
			podSpec.Containers[i].Image = workerImageTag
		} else if podSpec.Containers[i].Name == utils.WorkerInitContainerName {
			podSpec.Containers[i].Image = workerInitImageTag
		}
	}

	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("ReCheckpoint | checkpoint runtimeImageTag: %s, workerImageTag: %s, workerInitImageTag: %s",
			runtimeImageTag, workerImageTag, workerInitImageTag)
	}

	meta := pool.Deployment.Spec.Template.ObjectMeta.DeepCopy()
	meta.Name = podName
	meta.Labels[utils.StandardLabelKey] = utils.CheckpointPodStandard
	return &corev1.Pod{
		ObjectMeta: *meta,
		Spec:       *podSpec,
	}
}

func (c *checkpointPodMGR) createPod(ctx context.Context, pool *Pool,
	req common.PodRequest) (*corev1.Pod, error) {
	podName := strings.ToLower(fmt.Sprintf("checkpoint-%d-%d-%s", req.CPU, req.Mem, commonutils.UniqueID()))
	pod := c.getPodSpec(podName, pool)

	start := time.Now()
	waiter := utils.NewPodWaiter()
	c.waitMap.Store(podName, waiter)
	ctx, cancel := context.WithTimeout(ctx, c.waitReadyTimeout)

	defer func() {
		c.waitMap.Delete(podName)
		cancel()
	}()

	curPod, err := c.kubeClient.CoreV1().Pods(c.nameSpace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create pod: %s for %s", err.Error(), podName)
		return nil, err
	}
	strength.GetPodCache().SetState(curPod.Name, req.FunSvcID, strength.BeforeSpecialized)
	log.GetLogger().Infof("wait for checkpoint pod to become ready, podName: %s, traceID: %s",
		podName, req.TraceID)

	select {
	case <-ctx.Done():
		c.deletePod(pod)
		return nil, snerror.NewSNError(snerror.GettingPodErrorCode, snerror.ErrText(snerror.GettingPodErrorCode))
	case status := <-waiter.WaitStatus():
		log.GetLogger().Infof("receive notification for container pod %s", curPod.Name)
		if err := status.Err; err != nil {
			// The pod has entered a non-recoverable error state.
			log.GetLogger().Errorf("failed to wait container pod %s: %s", curPod.Name, err.Error())
			c.deletePod(pod)
			return nil, snerror.NewSNError(snerror.GettingPodErrorCode, err.Error())
		}
		log.GetLogger().Infof("checkpoint container pod is ready, podName: %s, traceID: %s, cost: %s", curPod.Name,
			req.TraceID, time.Since(start))
		return status.Pod, nil
	}
}

func (c *checkpointPodMGR) deletePod(pod *corev1.Pod) {
	gracePeriodSeconds := int64(0)
	propagationPolicy := metav1.DeletePropagationForeground
	option := metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &propagationPolicy}
	err := c.kubeClient.CoreV1().Pods(c.nameSpace).Delete(context.TODO(), pod.Name, option)
	if err != nil {
		log.GetLogger().Warnf("failed to force delete pod: %s", pod.Name)
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        c.nameSpace,
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          pod.Name,
			OperationOptions: option,
		})
		return
	}
	log.GetLogger().Infof("succeed to force delete pod %s for %s", pod.Name, utils.GetFullFuncNameByPod(pod))
}

func (c *checkpointPodMGR) updatePod(pod *corev1.Pod) {
	if !utils.IsPodReady(pod) {
		log.GetLogger().Warnf("onAdd pod %s isn't ready", pod.Name)
		return
	}

	waiterIf, exist := c.waitMap.Load(pod.Name)
	if !exist {
		log.GetLogger().Warnf("unknown pod %s, already notified?", pod.Name)
		return
	}
	waiter, ok := waiterIf.(*utils.PodWaiter)
	if !ok || waiter == nil {
		log.GetLogger().Errorf("failed to assert for pod: %s", pod.Name)
		return
	}
	waiter.NotifyStatus(utils.PodStatus{Pod: pod, Err: nil})
}
