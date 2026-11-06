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

// Package utils init pod informer and node informer
package utils

import (
	"time"

	"k8s.io/client-go/informers"
	batchv1 "k8s.io/client-go/informers/batch/v1"
	corev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/logger/log"
)

var (
	podInformer    corev1.PodInformer
	nodeInformer   corev1.NodeInformer
	secretInformer corev1.SecretInformer
	jobInformer    batchv1.JobInformer
)

// InitInformer init informer
func InitInformer(stopCh <-chan struct{}) {
	start := time.Now()
	kubeClient := GetKubernetesClient()
	if kubeClient == nil {
		log.GetLogger().Errorf("kubernetes client set is nil")
		return
	}
	podInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	nodeInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	factory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 0,
		informers.WithNamespace(FunctionNameSpace()))
	// init informer
	podInformer = podInformerFactory.Core().V1().Pods()
	nodeInformer = nodeInformerFactory.Core().V1().Nodes()
	secretInformer = factory.Core().V1().Secrets()
	jobInformer = factory.Batch().V1().Jobs()
	go podInformer.Informer().Run(stopCh)
	go nodeInformer.Informer().Run(stopCh)
	go secretInformer.Informer().Run(stopCh)
	go jobInformer.Informer().Run(stopCh)
	if ok := cache.WaitForCacheSync(stopCh, podInformer.Informer().HasSynced); !ok {
		return
	}
	if ok := cache.WaitForCacheSync(stopCh, nodeInformer.Informer().HasSynced); !ok {
		return
	}
	if ok := cache.WaitForCacheSync(stopCh, secretInformer.Informer().HasSynced); !ok {
		return
	}
	if ok := cache.WaitForCacheSync(stopCh, jobInformer.Informer().HasSynced); !ok {
		return
	}
	log.GetLogger().Infof("finished to init informer and sync cache cost: %v", time.Now().Sub(start))
}

// GetPodInformer get pod informer
func GetPodInformer() corev1.PodInformer {
	return podInformer
}

// GetNodeInformer get node informer
func GetNodeInformer() corev1.NodeInformer {
	return nodeInformer
}

// GetSecretInformer get secret informer
func GetSecretInformer() corev1.SecretInformer {
	return secretInformer
}

// GetJobInformer get job informer
func GetJobInformer() batchv1.JobInformer {
	return jobInformer
}
