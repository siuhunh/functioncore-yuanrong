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

// Package etcdevents handle etcd
package etcdevents

import (
	"context"
	"strings"
	"time"

	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
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

	// CellEnable -
	CellEnable bool
}

const keepConnAliveTTL = 3 * time.Second

// NewProvisioner New Provisioner
func NewProvisioner(sourcetype string, kind string, path string, queue workqueue.RateLimitingInterface) *Provisioner {
	var etcdClient *etcd3.EtcdClient
	var cellEnable bool
	if path == utils.WorkerInstancePrefix {
		etcdClient = etcd3.GetRouterEtcdClient()
	} else if path == utils.FunctionMetadataPrefix {
		etcdClient = etcd3.GetMetaEtcdClient()
		cellEnable = true
	} else if path == utils.WorkerManagerInstancePrefix {
		etcdClient = etcd3.GetCAEMetaEtcdClient()
	} else {
		etcdClient = etcd3.GetStateEtcdClient()
	}

	imp := &Provisioner{
		SourceType: sourcetype,
		Kind:       kind,
		EtcdClient: etcdClient,
		Path:       path,
		SinkQueue:  queue,
		CellEnable: cellEnable,
	}
	return imp
}

func (a *Provisioner) createWatcher() *etcd3.EtcdWatchChan {
	watcherTicker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-watcherTicker.C:
			if a.EtcdClient == nil {
				log.GetLogger().Warnf("etcd client is nil, skip watch path: %s", a.Path)
				return nil
			}
			if a.EtcdClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, a.CellEnable)
			watchChan := a.EtcdClient.Watch(ctx, a.Path, 0, true)
			watcherTicker.Stop()

			return watchChan
		}
	}
}

// Start Run starts the kubernetes receive adapter
// and sends events produced in the
// configured namespace and send them to the sink uri provided.
func (a *Provisioner) Start(stopCh <-chan struct{}) error {
	go a.StartWatch(stopCh)
	return nil
}

// StartWatch -
func (a *Provisioner) StartWatch(stop <-chan struct{}) {
	if stop == nil {
		log.GetLogger().Errorf("stop channel is nil, so start watch error")
		return
	}
	watchChan := a.createWatcher()
	if watchChan == nil {
		return
	}
	for {
		select {
		case watchEvent, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("it's going to reconnect as ResultChan may closed")
				watchChan.Stop()
				watchChan = a.createWatcher()
				continue
			}
			a.processWatchEvent(watchEvent)
		case _, ok := <-stop:
			if !ok {
				log.GetLogger().Errorf("stop channel may be closed")
				return
			}
			log.GetLogger().Warnf("received signal to quit")
			return
		}
	}
}

func (a *Provisioner) processWatchEvent(event *etcd3.Event) {
	if !a.isNeedWatchPath(event.Key) {
		log.GetLogger().Infof("skipped the event of not a repo meta event "+
			"or instance event or bridgemanager cache or client cache event, key: %s", event.Key)
		return
	}
	a.processEvent(event)
}

func (a *Provisioner) processEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		eventItem := a.generateEvents(event.Key, eventsource.Added, event)
		a.SinkQueue.Add(eventItem)
		log.GetLogger().Infof("etcdAdd | %s", etcdkey.AnonymizeTenantCommonEtcdKey(event.Key))
	case etcd3.DELETE:
		eventItem := a.generateEvents(event.Key, eventsource.Deleted, event)
		a.SinkQueue.Add(eventItem)
		log.GetLogger().Infof("etcdDel | %s", etcdkey.AnonymizeTenantCommonEtcdKey(event.Key))
	case etcd3.ERROR:
		log.GetLogger().Errorf("receive etcd error event, err: %s", string(event.Value))
	default:
		log.GetLogger().Warnf("etcd unknown event")
	}
}

func (a *Provisioner) isNeedWatchPath(path string) bool {
	if strings.HasPrefix(path, utils.FunctionMetadataPrefix) ||
		strings.HasPrefix(path, utils.WorkerInstancePrefix) ||
		strings.HasPrefix(path, utils.BridgeCachePrefix) ||
		strings.HasPrefix(path, utils.StateWorkersPrefix) ||
		strings.HasPrefix(path, utils.ClientCachePrefix) ||
		strings.HasPrefix(path, utils.WorkerManagerInstancePrefix) {
		return true
	}
	return false
}

func (a *Provisioner) generateEvents(path string, sourcetype string, value interface{}) eventsource.Event {
	return eventsource.Event{
		SourceType: a.SourceType,
		EventType:  sourcetype,
		Kind:       a.Kind,
		KeyRef:     path,
		Object:     value,
		StartTime:  time.Now(),
	}
}
