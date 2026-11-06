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

// Package nodeevents -
package nodeevents

import (
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/eventsource"
)

// Provisioner define k8s event provision
type Provisioner struct {
	// SinkURI is the URI messages will be forwarded on to.
	SinkQueue workqueue.RateLimitingInterface
	// NodeInformer is pod watcher client
	NodeInformer informercorev1.NodeInformer
	// SourceType : for this is kubernetes
	SourceType string

	Kind string
}

// NewProvisioner New Provisioner
func NewProvisioner(sourcetype string, kind string, queue workqueue.RateLimitingInterface,
	nodeInformer informercorev1.NodeInformer) *Provisioner {
	imp := &Provisioner{
		SinkQueue:    queue,
		NodeInformer: nodeInformer,
		SourceType:   sourcetype,
		Kind:         kind,
	}
	return imp
}

// Start Run starts the kubernetes receive adapter and sends events produced in the
// configured namespace and send them to the sink uri provided.
func (p *Provisioner) Start(stopCh <-chan struct{}) error {
	p.NodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    p.addFunc,
		UpdateFunc: p.updateFunc,
		DeleteFunc: p.deleteFunc,
	})
	log.GetLogger().Debugf("starting node informer...")
	return nil
}

func (p *Provisioner) deleteFunc(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		eventItem := p.generateEvents(key, eventsource.Deleted, obj)
		p.SinkQueue.Add(eventItem)
	}
}

func (p *Provisioner) updateFunc(oldObj, newObj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(newObj)
	if err == nil {
		eventItem := p.generateEvents(key, eventsource.Updated, newObj)
		p.SinkQueue.Add(eventItem)
	}
}

func (p *Provisioner) addFunc(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err == nil {
		eventItem := p.generateEvents(key, eventsource.Added, obj)
		p.SinkQueue.Add(eventItem)
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
