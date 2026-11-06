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

// Package podtemplate  creates pod templates
package podtemplate

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/podhandler"
	"functioncore/pkg/worker-manager/utils"
)

const (
	// TmpExpansionPodStandard is tmp expansion pod's standard label value
	TmpExpansionPodStandard = "tmpExpansion"

	// a single pod with tmp storage extension enabled consumes 10 GB
	podEphemeralStorage     = "10G"
	defaultWaitReadyTimeout = 60
)

// CreateTmpExpansionPodTemplate crate a expansion Pod template
func CreateTmpExpansionPodTemplate(podSpec *corev1.PodSpec, objectMeta *metav1.ObjectMeta,
	req *podhandler.PodCreateReq, demandNodes []string) *utils.CreatePodTemplate {
	if podSpec == nil || objectMeta == nil {
		return nil
	}
	podSpec.RestartPolicy = corev1.RestartPolicyNever
	name := fmt.Sprintf("pool-%d-%d", req.PodReq.CPU, req.PodReq.Mem)
	podName := strings.ToLower(fmt.Sprintf("%s-%s-%s", name, TmpExpansionPodStandard, commonutils.ShortUUID()))
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == utils.RuntimeContainerName {
			// change the runtime container's request resource ephemeralStorage to 10g
			podSpec.Containers[i].Resources.Requests[corev1.ResourceEphemeralStorage] = resource.MustParse(
				podEphemeralStorage)
			break
		}
	}
	utils.ConfigureResource(req.PodReq.CPU, req.PodReq.Mem, podSpec, req.Spec.Configuration.BusinessType)
	if len(demandNodes) != 0 {
		// the pod is scheduled to a specific node through nodeSelector
		podSpec.Affinity.NodeAffinity = &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: utils.GetTmpExpansionNodeSelectorTerm(demandNodes),
			},
		}
	}

	objectMeta.Name = podName
	objectMeta.Namespace = utils.FunctionNameSpace()
	// add relevant annotation and label information to the newly created pod
	objectMeta.Labels[utils.LabelTMPExpansion] = utils.TrueString
	// In order to ensure that the pod is created successfully, but the specialized labeling fails,
	// the deletion of the pod event can restore the 10g storage
	objectMeta.Labels[utils.IsPoolPodLabelKey] = utils.FalseString

	return &utils.CreatePodTemplate{
		TraceID:          req.PodReq.TraceID,
		WaitReadyTimeout: defaultWaitReadyTimeout,
		Spec:             podSpec,
		ObjectMeta:       objectMeta,
	}
}
