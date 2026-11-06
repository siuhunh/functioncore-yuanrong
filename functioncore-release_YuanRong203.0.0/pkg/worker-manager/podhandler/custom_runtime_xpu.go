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

// Package podhandler -
package podhandler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
)

const (
	customRuntime                 = "custom-runtime"
	customRuntimeGpuPodStandard   = "custom-runtime-gpu-pod"
	customRuntimeGpuPodNamePrefix = "custom.runtime.gpu"
	defaultWaitReadyTimeout       = 60
)

type customRuntimeXpuPodHandler struct {
	nameSpace  string
	kubeClient kubernetes.Interface
	config     config.PoolMgrConfig
	waitMap    sync.Map // key: pod.Name, value: *PodWaiter
}

func createCustomRuntimeXpuPodHandler(kubeClient kubernetes.Interface, cfg config.PoolMgrConfig) PodHandler {
	if cfg.GPUPodConfig.WaitReadyTimeout <= 0 {
		cfg.GPUPodConfig.WaitReadyTimeout = defaultWaitReadyTimeout
	}
	return &customRuntimeXpuPodHandler{
		nameSpace:  utils.FunctionNameSpace(),
		kubeClient: kubeClient,
		config:     cfg,
	}
}

// FilterByPodCreateReq -
func (c *customRuntimeXpuPodHandler) FilterByPodCreateReq(req *PodCreateReq) bool {
	if req.Spec.Configuration.Runtime.Language != customRuntime {
		return false
	}
	if req.PodReq.GpuMemory <= 0 {
		return false
	}
	return true
}

// FilterByPodName -
func (c *customRuntimeXpuPodHandler) FilterByPodName(podName string) bool {
	return strings.Contains(podName, customRuntimeGpuPodNamePrefix)
}

// CreatePod -
func (c *customRuntimeXpuPodHandler) CreatePod(req *PodCreateReq, template *PodTemplate) (*corev1.Pod, error) {
	gpuMem := req.PodReq.GpuMemory
	podName := strings.ToLower(fmt.Sprintf("%s-%d-%d-%d-%s", customRuntimeGpuPodNamePrefix,
		req.PodReq.CPU, req.PodReq.Mem, gpuMem, commonutils.UniqueID()))
	gpuType := req.PodReq.GpuType
	gpuMemoryCapacity := c.config.GPUPodConfig.GPUMemoryCapacity[gpuType]
	if gpuMemoryCapacity <= 0 {
		gpuMemoryCapacity = int64(utils.DefaultGPUMemoryCapacity)
	}
	utils.ConfigureResource(req.PodReq.CPU, req.PodReq.Mem, template.Spec, req.Spec.Configuration.BusinessType)
	pod := c.getPodSpec(podName, template, gpuMem, gpuType)
	log.GetLogger().Infof("create custom runtime gpu pod, funcName: %s, traceID: %s, pod name: %s, gpu memory is: %d, "+
		"gpu type is %s", req.Spec.Configuration.FunctionName, req.PodReq.TraceID, podName, gpuMem, gpuType)
	ctx, cancel := context.WithTimeout(req.Ctx, time.Duration(c.config.GPUPodConfig.WaitReadyTimeout)*time.Second)
	defer cancel()
	req.Ctx = ctx
	cnt := 0
	for {
		readyPod, err := c.createPodInternal(req, pod)
		if err == nil || !utils.IsXGPUStartError(err) {
			return readyPod, err
		}
		cnt++
		pod.Name = strings.ToLower(fmt.Sprintf("%s-%d-%d-%d-%s", customRuntimeGpuPodNamePrefix,
			req.PodReq.CPU, req.PodReq.Mem, gpuMem, commonutils.UniqueID()))
		time.Sleep(time.Second)
		log.GetLogger().Warnf("retry to create pod with start error, retry count %d, traceID %s", cnt,
			req.PodReq.TraceID)
	}
}

func (c *customRuntimeXpuPodHandler) createPodInternal(req *PodCreateReq, pod *corev1.Pod) (*corev1.Pod, error) {
	start := time.Now()
	waiter := utils.NewPodWaiter()
	c.waitMap.Store(pod.Name, waiter)

	defer func() {
		c.waitMap.Delete(pod.Name)
	}()
	curPod, err := c.kubeClient.CoreV1().Pods(c.nameSpace).Create(req.Ctx, pod, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create pod: %s for %s traceID %s",
			err.Error(), pod.Name, req.PodReq.TraceID)
		return nil, err
	}
	strength.GetPodCache().SetState(curPod.Name, "", strength.BeforeSpecialized)
	log.GetLogger().Infof("wait for gpu custom runtime pod to become ready, podName: %s, funcName: %s, traceID: %s",
		pod.Name, req.Spec.Configuration.FunctionName, req.PodReq.TraceID)

	select {
	case <-req.Ctx.Done():
		log.GetLogger().Errorf("create pod timeout for %ds, traceID: %s", c.config.GPUPodConfig.WaitReadyTimeout,
			req.PodReq.TraceID)
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
		log.GetLogger().Infof("custom runtime gpu container pod is ready, podName: %s, traceID: %s, cost: %s",
			curPod.Name, req.PodReq.TraceID, time.Since(start))
		return status.Pod, nil
	}
}

func (c *customRuntimeXpuPodHandler) getPodSpec(podName string, template *PodTemplate,
	gpuMem int64, gpuType string) *corev1.Pod {
	podSpec := template.Spec
	var nodeAffinityValue []string
	nodeAffinityValue = append(nodeAffinityValue, utils.GpuNodeAffinityValue)
	podSpec.Affinity.NodeAffinity = utils.DescribeGPUNodeAffinity(c.config.PoolList.NodeAffinity.NodeAffinityKey,
		nodeAffinityValue, gpuType)
	for _, container := range podSpec.Containers {
		if container.Name == utils.RuntimeContainerName {
			gpuMemoryCapacity := c.config.GPUPodConfig.GPUMemoryCapacity[gpuType]
			if gpuMemoryCapacity <= 0 {
				gpuMemoryCapacity = int64(utils.DefaultGPUMemoryCapacity)
			}
			utils.ConfigGPUResource(container.Resources, gpuMem, gpuMemoryCapacity)
			break
		}
	}

	podSpec.Tolerations = append(podSpec.Tolerations, corev1.Toleration{
		Key:      "node-type",
		Operator: corev1.TolerationOpEqual,
		Value:    "gpu",
		Effect:   corev1.TaintEffectNoSchedule,
	})
	podSpec.RestartPolicy = "Never"
	meta := template.Meta
	meta.Name = podName
	meta.Labels[utils.StandardLabelKey] = customRuntimeGpuPodStandard
	return &corev1.Pod{
		ObjectMeta: *meta,
		Spec:       *podSpec,
	}
}

// UpdatePod -
func (c *customRuntimeXpuPodHandler) UpdatePod(pod *corev1.Pod) {
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

	if utils.IsPodReady(pod) {
		waiter.NotifyStatus(utils.PodStatus{Pod: pod, Err: nil})
		return
	}
	log.GetLogger().Warnf("onAdd pod %s isn't ready", pod.Name)

	for _, status := range pod.Status.ContainerStatuses {
		if status.Name != utils.RuntimeContainerName {
			continue
		}
		if status.State.Terminated != nil && status.State.Terminated.Reason == "StartError" {
			waiter.NotifyStatus(utils.PodStatus{Pod: pod, Err: snerror.New(snerror.GettingPodErrorCode,
				fmt.Sprintf("%s: %s", status.State.Terminated.Reason, status.State.Terminated.Message))})
			return
		}
	}
}

func (c *customRuntimeXpuPodHandler) deletePod(pod *corev1.Pod) {
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
