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

package utils

import (
	"context"
	"reflect"

	"github.com/agiledragon/gomonkey/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// GetListPodsPatch -
func GetListPodsPatch(podList *corev1.PodList, mockErr error) PatchSlice {
	return PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
			func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
				return &typecorev1.CoreV1Client{}
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&typecorev1.CoreV1Client{}), "Pods",
			func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
				return NewK8sPodImp(namespace)
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&K8sPodImp{}), "List", func(_ *K8sPodImp,
			ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
			return podList, mockErr
		}),
	}
}
