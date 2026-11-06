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

package poolmanager

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

type resourceRequirementsBuilder struct {
	data corev1.ResourceRequirements
}

func newResourceRequirementsBuilder() *resourceRequirementsBuilder {
	return &resourceRequirementsBuilder{
		data: corev1.ResourceRequirements{
			Requests: make(corev1.ResourceList),
			Limits:   make(corev1.ResourceList),
		},
	}
}

func (b *resourceRequirementsBuilder) setCPU(request, limit int64) {
	b.data.Requests["cpu"] = resource.MustParse(fmt.Sprintf("%dm", request))
	b.data.Limits["cpu"] = resource.MustParse(fmt.Sprintf("%dm", limit))
}

func (b *resourceRequirementsBuilder) setMemory(request, limit int64) {
	b.data.Requests["memory"] = resource.MustParse(fmt.Sprintf("%dMi", request))
	b.data.Limits["memory"] = resource.MustParse(fmt.Sprintf("%dMi", limit))
}

func (b *resourceRequirementsBuilder) setEphemeralStorage(request, limit int64) {
	b.data.Requests["ephemeral-storage"] = resource.MustParse(fmt.Sprintf("%dMi", request))
	b.data.Limits["ephemeral-storage"] = resource.MustParse(fmt.Sprintf("%dMi", limit))
}

func (b *resourceRequirementsBuilder) setCustomResource(customResource map[string]int64) {
	for key, value := range customResource {
		b.data.Requests[corev1.ResourceName(key)] = resource.MustParse(fmt.Sprintf("%d", value))
		b.data.Limits[corev1.ResourceName(key)] = resource.MustParse(fmt.Sprintf("%d", value))
	}
}

func (b *resourceRequirementsBuilder) build() corev1.ResourceRequirements {
	return b.data
}
