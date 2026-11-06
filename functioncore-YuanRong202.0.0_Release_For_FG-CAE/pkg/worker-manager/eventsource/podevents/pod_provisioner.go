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

// Package podevents -
package podevents

import (
	corev1 "k8s.io/api/core/v1"
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

// Provisioner define k8s event provision
type Provisioner struct {
	// Namespace is the kubernetes namespace to watch for events from.
	Namespace string
	// SinkURI is the URI messages will be forwarded on to.
	SinkQueue workqueue.RateLimitingInterface
	// FunctionInformer is pod watcher client
	PodInformer informercorev1.PodInformer
	// SourceType : for this is kubernetes
	SourceType string

	Kind string
}

// NewProvisioner New Provisioner
func NewProvisioner(sourcetype string, kind string, queue workqueue.RateLimitingInterface,
	podInformer informercorev1.PodInformer) *Provisioner {
	imp := &Provisioner{
		Namespace:   utils.FunctionNameSpace(),
		SinkQueue:   queue,
		PodInformer: podInformer,
		SourceType:  sourcetype,
		Kind:        kind,
	}
	return imp
}

// Start Run starts the kubernetes receive adapter and sends events produced in the
// configured namespace and send them to the sink uri provided.
func (p *Provisioner) Start(stopCh <-chan struct{}) error {
	p.PodInformer.Informer().AddEventHandler(p.generateFilteringResourceEventHandler())
	log.GetLogger().Debugf("starting pod informer...")
	return nil
}

func (p *Provisioner) generateFilteringResourceEventHandler() cache.FilteringResourceEventHandler {
	return cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return false
			}

			if utils.IsWorkerPod(pod) {
				return true
			}

			if utils.IsDsWorker(pod) {
				log.GetLogger().Debugf("Pod ds-worker event, pod is %s", pod.Spec.NodeName)
				return true
			}
			return false
		},
		Handler: p.generateResourceEventHandler(),
	}
}

func (p *Provisioner) generateResourceEventHandler() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				// ignore obj and let SinkQueue to use "key" to eliminate redundant add event
				eventItem := p.generateEvents(key, eventsource.Added, nil)
				p.SinkQueue.Add(eventItem)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				// ignore obj and let SinkQueue to use "key" to eliminate redundant update event
				eventItem := p.generateEvents(key, eventsource.Updated, nil)
				p.SinkQueue.Add(eventItem)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				eventItem := p.generateEvents(key, eventsource.Deleted, obj)
				p.SinkQueue.Add(eventItem)
			}
		},
	}
}

func (p *Provisioner) generateEvents(key string, sourcetype string, value interface{}) eventsource.Event {
	return eventsource.Event{
		SourceType: p.SourceType,
		EventType:  sourcetype,
		Kind:       p.Kind,
		KeyRef:     key,
		Object:     value,
	}
}
