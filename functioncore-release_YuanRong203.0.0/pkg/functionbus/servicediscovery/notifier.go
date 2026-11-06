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

// Package servicediscovery is used for service discovery
package servicediscovery

import (
	"context"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/config"
)

// EtcdWatchChannels etcd watch channels
var (
	EtcdWatchChannels      = make([]*etcd3.EtcdWatchChan, 0)
	EtcdWatchChannelsMutex sync.RWMutex
)

const keepConnAliveTTL = 3 * time.Second

// Notifier defines notifier
type Notifier interface {
	AddObservers(observers ...Observer)
	StartNotify(observer Observer)
}

// EtcdNotifier is the notifier watching etcd event
type EtcdNotifier struct {
	etcdClient       *etcd3.EtcdClient
	routerETCDClient *etcd3.EtcdClient
	stopCh           <-chan struct{}
	rev              int64
}

// NewEtcdNotifier creates EtcdNotifier
func NewEtcdNotifier(stopCh <-chan struct{}) (Notifier, error) {
	return &EtcdNotifier{
		etcdClient:       etcd3.GetMetaEtcdClient(),
		routerETCDClient: etcd3.GetRouterEtcdClient(),
		stopCh:           stopCh,
	}, nil
}

// AddObservers add observer to notifier
func (en *EtcdNotifier) AddObservers(observers ...Observer) {
	for _, observer := range observers {
		go (&EtcdNotifier{
			etcdClient:       en.etcdClient,
			routerETCDClient: en.routerETCDClient,
			stopCh:           en.stopCh,
		}).StartNotify(observer)
	}
}

// circulateWatch is used to watch etcd
func (en *EtcdNotifier) circulateWatch(etcdwatchChan **etcd3.EtcdWatchChan, observer Observer,
	selection etcd3.EtcdType, watchPath string) bool {
	select {
	case event, ok := <-(*etcdwatchChan).ResultChan:
		if !ok {
			log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect from rev %d"+
				" of path %s of %s etcd", en.rev, watchPath, selection)
			etcdClient := etcd3.GetETCDClient(selection)
			if etcdClient.Client == nil {
				log.GetLogger().Warnf("etcd client is nil, etcdType: %s", selection.String())
				return false
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, en.isCellEnable(selection))
			*etcdwatchChan = etcdClient.Watch(ctx, watchPath, en.rev, true)
			return false
		}
		if event.Rev > en.rev {
			en.rev = event.Rev
		}
		switch event.Type {
		case etcd3.DELETE:
			observer.HandleDelete(event)
		case etcd3.PUT:
			observer.HandleUpdate(event)
		case etcd3.SYNCED:
			observer.HandleSync(event)
		case etcd3.ERROR:
			log.GetLogger().Errorf("%s etcd event has error of %s: %s", selection, watchPath, string(event.Value))
			if strings.Contains(string(event.Value), "compacted") {
				log.GetLogger().Warnf("revision has been compacted, reset rev to 0")
				en.rev = 0
			}
		default:
			log.GetLogger().Errorf("unsupported event type: %v", event.Type)
			return false
		}
	case _, ok := <-en.stopCh:
		if !ok {
			log.GetLogger().Warnf("received termination signal to shutdown")
		}
		return true
	}
	return false
}

// StartNotify starts notify observer
func (en *EtcdNotifier) StartNotify(observer Observer) {
	watchObject := observer.Object()
	watchPath, ok := watchObject.(string)
	selection := observer.Selection()
	if !ok {
		log.GetLogger().Errorf("failed to decode object %v to etcd path", watchObject)
		return
	}
	var ctx context.Context
	if etcdObserver, ok := observer.(*EtcdObserver); ok && etcdObserver.EnableEtcdCache {
		cacheConfig := &etcd3.EtcdCacheConfig{
			EnableEtcdCache:        config.Conf.EnableMetaEtcdCache,
			UpdateCacheFilePeriod:  config.Conf.UpdateCacheFilePeriod,
			FlushCacheThreshold:    config.Conf.FlushCacheThreshold,
			EtcdDiffSyncTimeout:    config.Conf.EtcdDiffSyncTimeout,
			CacheUpdateBufSize:     config.Conf.CacheUpdateBufSize,
			CacheFilePartitionSize: config.Conf.CacheFilePartitionSize,
		}
		ctx = context.WithValue(context.Background(), etcd3.EtcdCacheConfigKey, cacheConfig)
	} else {
		ctx = context.Background()
	}
	etcdClient := etcd3.GetETCDClient(selection)
	etcdWatchChan := en.getWatchChan(ctx, etcdClient, watchPath, selection)
	EtcdWatchChannelsMutex.Lock()
	EtcdWatchChannels = append(EtcdWatchChannels, etcdWatchChan)
	EtcdWatchChannelsMutex.Unlock()
	for {
		flag := en.circulateWatch(&etcdWatchChan, observer, selection, watchPath)
		if flag {
			return
		}
	}
}

func (en *EtcdNotifier) getWatchChan(ctx context.Context, etcdClient *etcd3.EtcdClient,
	watchPath string, selection etcd3.EtcdType) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if etcdClient == nil {
				log.GetLogger().Warnf("etcd client is nil, etcdType:%s", selection.String())
				continue
			}
			if etcdClient.Client == nil &&
				(selection == etcd3.Meta || selection == etcd3.CAEMeta || selection ==
					etcd3.CAERouter || selection == etcd3.State) {
				log.GetLogger().Warnf("failed to init %s etcd client", selection)
				continue
			}
			log.GetLogger().Infof("success to init etcd client, start to watch %s", watchPath)
			cellCtx := context.WithValue(ctx, etcd3.CellEnable, en.isCellEnable(selection))
			etcdWatchChan := etcdClient.Watch(cellCtx, watchPath, 0, true)
			ticker.Stop()
			return etcdWatchChan
		}
	}
}

func (en *EtcdNotifier) isCellEnable(selection etcd3.EtcdType) bool {
	if selection == etcd3.Meta || selection == etcd3.CAEMeta {
		return true
	}
	return false
}
