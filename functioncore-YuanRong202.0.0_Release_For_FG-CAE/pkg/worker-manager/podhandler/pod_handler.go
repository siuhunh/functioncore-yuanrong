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

package podhandler

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

// PodType -
type PodType int

const (
	// CustomRuntimeXpu -
	CustomRuntimeXpu PodType = iota
)

// PodCreateReq -
type PodCreateReq struct {
	Spec        *faasfunction.FunctionServiceSpec
	PodReq      *common.PodRequest
	Ctx         context.Context
	DemandNodes []string
}

// PodTemplate -
type PodTemplate struct {
	Spec *corev1.PodSpec
	Meta *metav1.ObjectMeta
}

// PodHandler -
type PodHandler interface {
	FilterByPodCreateReq(req *PodCreateReq) bool
	FilterByPodName(podName string) bool
	CreatePod(req *PodCreateReq, template *PodTemplate) (*corev1.Pod, error)
	UpdatePod(pod *corev1.Pod)
}

// CreatePodHandler -
func CreatePodHandler(podType PodType, kubeClient kubernetes.Interface, cfg config.PoolMgrConfig) PodHandler {
	switch podType {
	case CustomRuntimeXpu:
		return createCustomRuntimeXpuPodHandler(kubeClient, cfg)
	default:
	}
	return nil
}
