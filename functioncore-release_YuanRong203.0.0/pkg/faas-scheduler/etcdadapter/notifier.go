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

// Package etcdadapter contains etcd interaction
package etcdadapter

import (
	"context"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

// Notifier defines notifier
type Notifier interface {
	AddObservers(observer Observer)
	StartNotify(observer Observer)
}

// EtcdNotifier is the notifier watching etcd event
type EtcdNotifier struct {
	etcdClient       *etcd3.EtcdClient
	routerETCDClient *etcd3.EtcdClient
	stopCh           <-chan struct{}
	rev              int64
}

const keepConnAliveTTL = 3 * time.Second

// NewEtcdNotifier creates EtcdNotifier
func NewEtcdNotifier(stopCh <-chan struct{}) (Notifier, error) {
	return &EtcdNotifier{
		etcdClient:       etcd3.GetMetaEtcdClient(),
		routerETCDClient: etcd3.GetRouterEtcdClient(),
		stopCh:           stopCh,
	}, nil
}

// AddObservers add observer to notifier
func (en *EtcdNotifier) AddObservers(observer Observer) {
	go (&EtcdNotifier{
		etcdClient:       en.etcdClient,
		routerETCDClient: en.routerETCDClient,
		stopCh:           en.stopCh,
	}).StartNotify(observer)
}

// circulateWatch is used to watch etcd
func (en *EtcdNotifier) circulateWatch(etcdWatchChan **etcd3.EtcdWatchChan, observer Observer,
	selection etcd3.EtcdType, watchPath string) bool {
	select {
	case event, ok := <-(*etcdWatchChan).ResultChan:
		if !ok {
			log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect %s from rev %d",
				selection, en.rev)
			etcdClient := etcd3.GetETCDClient(selection)
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, en.isCellEnable(observer))
			*etcdWatchChan = etcdClient.Watch(ctx, watchPath, en.rev, true)
			return false
		}
		if event.Rev > en.rev {
			en.rev = event.Rev
		}
		switch event.Type {
		case etcd3.PUT:
			observer.HandleUpdate(event)
		case etcd3.SYNCED:
			observer.HandleSync(event)
		case etcd3.DELETE:
			observer.HandleDelete(event)
		case etcd3.ERROR:
			log.GetLogger().Errorf("%s etcd event error of %s: %s", selection, watchPath, string(event.Value))
			if strings.Contains(string(event.Value), "compacted") {
				log.GetLogger().Warnf("revision has compacted, reset rev to 0")
				en.rev = 0
			}
		default:
			log.GetLogger().Errorf("unsupport event type: %v", event.Type)
			return false
		}
	case _, ok := <-en.stopCh:
		if !ok {
			log.GetLogger().Warnf("receive termination signal to shutdown")
		}
		return true
	}
	return false
}

// StartNotify starts notify observer
func (en *EtcdNotifier) StartNotify(observer Observer) {
	selection := observer.Selection()
	watchObject := observer.Object()
	watchPath, ok := watchObject.(string)
	if !ok {
		log.GetLogger().Errorf("failed to decode object %v to etcd watch path", watchObject)
		return
	}

	etcdClient := etcd3.GetETCDClient(selection)
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if etcdClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, en.isCellEnable(observer))
			etcdWatchChan := etcdClient.Watch(ctx, watchPath, 0, true)
			go en.runWatcher(etcdWatchChan, observer, selection, watchPath)
			ticker.Stop()
			return
		}
	}
}

func (en *EtcdNotifier) runWatcher(etcdWatchChan *etcd3.EtcdWatchChan, observer Observer,
	selection etcd3.EtcdType, watchPath string) {
	for {
		flag := en.circulateWatch(&etcdWatchChan, observer, selection, watchPath)
		if flag {
			return
		}
	}
}

func (en *EtcdNotifier) isCellEnable(observer Observer) bool {
	if observer.Selection() == etcd3.Meta || observer.Selection() == etcd3.CAEMeta {
		return true
	}
	return false
}
