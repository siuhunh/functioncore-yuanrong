/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package etcdevents handle etcd
package etcdevents

import (
	"context"
	"strings"
	"time"

	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
)

// Provisioner define etcd event provision
type Provisioner struct {
	EtcdClient *etcd3.EtcdClient

	// Path is the etcd path to watch for events from.
	Path string

	// SinkQueue is the URI messages will be forwarded on to.
	SinkQueue workqueue.RateLimitingInterface

	// SourceType : for this is kubernetes
	SourceType string

	Kind string

	// CellEnable : Check whether the interception information needs to adapt to the cell architecture
	CellEnable bool
}

// NewProvisioner New Provisioner
func NewProvisioner(sourcetype string, kind string, path string,
	queue workqueue.RateLimitingInterface, cellEnable bool) *Provisioner {
	var etcdClient *etcd3.EtcdClient
	etcdClient = etcd3.GetCAEMetaEtcdClient()
	imp := &Provisioner{
		Path:       path,
		SinkQueue:  queue,
		SourceType: sourcetype,
		Kind:       kind,
		EtcdClient: etcdClient,
		CellEnable: cellEnable,
	}
	return imp
}

func (a *Provisioner) createWatcher() *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(utils.KeepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if a.EtcdClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, a.CellEnable)
			watchChan := a.EtcdClient.Watch(ctx, a.Path, 0, true)
			ticker.Stop()
			return watchChan
		}
	}
}

// Start Run starts the kubernetes receive adapter and sends events produced in the
// configured namespace and send them to the sink uri provided.
func (a *Provisioner) Start(stopCh <-chan struct{}) error {
	go a.StartWatch(stopCh)

	return nil
}

// StartWatch Start watch Channel
func (a *Provisioner) StartWatch(stop <-chan struct{}) {
	if stop == nil {
		log.GetLogger().Errorf("start watch stop channel is nil")
		return
	}
	watchChan := a.createWatcher()
	if watchChan == nil {
		log.GetLogger().Errorf("start watchChan is nil")
		return
	}
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("ResultChan may closed")
				watchChan.Stop()
				watchChan = a.createWatcher()
				continue
			}
			a.processWatchEvent(event)
		case _, ok := <-stop:
			if !ok {
				log.GetLogger().Errorf("start watch stop channel may be closed")
				return
			}
			log.GetLogger().Warnf("start watch received signal to quit")
			return
		}
	}
}

func (a *Provisioner) processWatchEvent(event *etcd3.Event) {
	if !a.isNeedWatchPath(event.Key) {
		log.GetLogger().Infof("skipped the event that was not a repo meta event "+
			"or instance event or worker-manager instance event or cae available cluster event, key: %s", event.Key)
		return
	}
	a.processEvent(event)
}

func (a *Provisioner) processEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		eventItem := a.generateEvents(event.Key, eventsource.Added, event)
		a.SinkQueue.Add(eventItem)
		log.GetLogger().Infof("processEvent etcd add event, key: %s", etcdkey.AnonymizeTenantCommonEtcdKey(event.Key))
	case etcd3.DELETE:
		eventItem := a.generateEvents(event.Key, eventsource.Deleted, event)
		a.SinkQueue.Add(eventItem)
		log.GetLogger().Infof("processEvent etcd delete event, key: %s", etcdkey.AnonymizeTenantCommonEtcdKey(event.Key))
	case etcd3.ERROR:
		log.GetLogger().Errorf("processEvent receive etcd error event, err: %s", string(event.Value))
	default:
		log.GetLogger().Warnf("processEvent etcd unknown event")
	}
}

func (a *Provisioner) isNeedWatchPath(path string) bool {
	if strings.HasPrefix(path, utils.InstanceMetadataPrefix) ||
		strings.HasPrefix(path, utils.WorkerManagerInstancePrefix) ||
		strings.HasPrefix(path, utils.FunctionMetadataPrefix) ||
		strings.HasPrefix(path, utils.CaeAvailableClusterPrefix) {
		return true
	}
	return false
}

func (a *Provisioner) generateEvents(path string, sourcetype string, value interface{}) eventsource.Event {
	return eventsource.Event{
		Kind:       a.Kind,
		Object:     value,
		SourceType: a.SourceType,
		EventType:  sourcetype,
		StartTime:  time.Now(),
		KeyRef:     path,
	}
}
