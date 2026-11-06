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

package podtemplate

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/worker-manager/utils"
)

func TestCreateTmpExpansionPodTemplate(t *testing.T) {
	convey.Convey("Given a PodRequest and a demandNodes list", t, func() {
		podName := "pool22-300-128-fusion-tmpExpansion-mock-id"
		traceID := "mock-trace-id"

		demandNodes := []string{"mock-node-1", "mock-node-2", "mock-node-3"}
		convey.Convey("case01: When PodSpec is nil, the returned CreatePodTemplate is nil", func() {
			podTemplate := CreateTmpExpansionPodTemplate(nil, &metav1.ObjectMeta{}, podName, traceID,
				demandNodes)
			convey.So(podTemplate, convey.ShouldBeNil)
		})
		convey.Convey("case02: When ObjectMeta is nil, the returned CreatePodTemplate is nil", func() {
			podTemplate := CreateTmpExpansionPodTemplate(&corev1.PodSpec{}, nil, podName, traceID,
				demandNodes)
			convey.So(podTemplate, convey.ShouldBeNil)
		})
		convey.Convey("case03: When PodSpec and ObjectMeta is not nil, the returned CreatePodTemplate is not nil",
			func() {
				spec := &corev1.PodSpec{
					Affinity: &corev1.Affinity{NodeAffinity: nil},
					Containers: []corev1.Container{
						{
							Name:  utils.RuntimeContainerName,
							Image: "mock-runtime-image",
							Resources: corev1.ResourceRequirements{
								Limits:   make(map[corev1.ResourceName]resource.Quantity, 0),
								Requests: make(map[corev1.ResourceName]resource.Quantity, 0),
							},
						},
						{
							Name:  utils.WorkerContainerName,
							Image: "mock-worker-image",
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  utils.WorkerInitContainerName,
							Image: "init-image",
						},
					},
				}
				meta := &metav1.ObjectMeta{
					Name:      "mock-node-name",
					Namespace: "mock-namespace",
					Annotations: map[string]string{
						"mock-annotation-key": "mock-annotation-value",
					},
					Labels: map[string]string{
						"mock-key": "mock-value",
					},
				}
				podTemplate := CreateTmpExpansionPodTemplate(spec, meta, podName, traceID, demandNodes)
				convey.So(podTemplate, convey.ShouldNotBeNil)
			})
	})
}

func Test_configNodeSelectorTerm(t *testing.T) {
	convey.Convey("case01: Given an empty demandNodes list, the length of returned NodeSelectorTerm is 0 ",
		t, func() {
			emptyDemandNodes := make([]string, 0)
			nodeSelectorTerm := getNodeSelectorTerm(emptyDemandNodes)
			convey.So(len(nodeSelectorTerm), convey.ShouldBeZeroValue)
		})

	convey.Convey("case02: Given a demandNodes list, the length of returned NodeSelectorTerm is 1, "+
		"MatchExpressions len is 2 ",
		t, func() {
			emptyDemandNodes := []string{"mock-node-1", "mock-node-2", "mock-node-3"}
			nodeSelectorTerm := getNodeSelectorTerm(emptyDemandNodes)
			convey.So(len(nodeSelectorTerm), convey.ShouldEqual, 1)
			convey.So(len(nodeSelectorTerm[0].MatchExpressions), convey.ShouldEqual, 2)
		})
}
