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

// Package reactor deal with function service, instance and repo
package reactor

import (
	"errors"
	"time"

	"golang.org/x/time/rate"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	etcdevs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource/etcdevents"
	evh "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler/bridgeinstancehandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler/clientinfohandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler/clientinstancehandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler/functionrepohandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler/stateinfohandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler/stateworkershandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

const (
	maxRetries  = 12
	rateLimit   = 1000
	tokensBurst = 2000
	baseDelay   = 5
	maxDelay    = 1000
	metaType    = "Etcd"
)

var (
	workerNum = 100
)

// APIClients define the API client event driver used
type APIClients struct {
	MetadataEtcdClient  *etcd3.EtcdClient
	RoutedataEtcdClient *etcd3.EtcdClient
}

// Reactor define event drive design
type Reactor struct {
	APIClients      *APIClients
	EventSources    map[string]evs.EventProvisioner
	EventHandlerMap map[string]evh.Handler
	queue           workqueue.RateLimitingInterface
}

func setWorkerNum(workerNums int) {
	workerNum = workerNums
}

// NewEventDrive returns a new EventDrive
func NewEventDrive() *Reactor {
	apiClients := &APIClients{
		MetadataEtcdClient:  etcd3.GetMetaEtcdClient(),
		RoutedataEtcdClient: etcd3.GetRouterEtcdClient(),
	}
	return makeReactor(apiClients)
}

// makeReactor returns a reactor
func makeReactor(apiClients *APIClients) *Reactor {
	queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))
	reactor := &Reactor{
		EventSources:    map[string]evs.EventProvisioner{},
		queue:           queue,
		EventHandlerMap: map[string]evh.Handler{},
		APIClients:      apiClients,
	}
	// register etcd handler
	reactor.makeFunctionRepoReactor()
	reactor.makeClientInstanceReactor()
	reactor.makeBridgeInstanceReactor()
	reactor.makeStateInfoReactor()
	reactor.makeClientInfoReactor()
	reactor.makeStateWorkersReactor()
	return reactor
}

func (reactor *Reactor) makeFunctionRepoReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		metaType, utils.FunctionRepoEventKind, utils.FunctionMetadataPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.FunctionRepoEventKind)
	}
	functionRepoEventHandler := prepareFunctionRepoEventHandler(reactor.APIClients)
	if functionRepoEventHandler != nil {
		reactor.registerEventHandler(functionRepoEventHandler, utils.FunctionRepoEventKind)
	}
}

func (reactor *Reactor) makeClientInstanceReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		metaType, utils.ClientInstanceEventKind, utils.WorkerInstancePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.ClientInstanceEventKind)
	}
	clientInstanceEventHandler := prepareClientInstanceEventHandler(reactor.APIClients)
	if clientInstanceEventHandler != nil {
		reactor.registerEventHandler(clientInstanceEventHandler, utils.ClientInstanceEventKind)
	}
}

func (reactor *Reactor) makeBridgeInstanceReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		metaType, utils.BridgeInstanceEventKind, utils.WorkerInstancePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.BridgeInstanceEventKind)
	}
	bridgeInstanceEventHandler := prepareBridgeInstanceEventHandler(reactor.APIClients)
	if bridgeInstanceEventHandler != nil {
		reactor.registerEventHandler(bridgeInstanceEventHandler, utils.BridgeInstanceEventKind)
	}
}

func (reactor *Reactor) makeStateInfoReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		metaType, utils.StateInfoEventKind, utils.BridgeCachePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.StateInfoEventKind)
	}
	stateInfoEventHandler := prepareStateInfoEventHandler(reactor.APIClients)
	if stateInfoEventHandler != nil {
		reactor.registerEventHandler(stateInfoEventHandler, utils.StateInfoEventKind)
	}
}

func (reactor *Reactor) makeClientInfoReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		metaType, utils.ClientInfoEventKind, utils.ClientCachePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.ClientInfoEventKind)
	}
	clientInfoEventHandler := prepareClientInfoEventHandler(reactor.APIClients)
	if clientInfoEventHandler != nil {
		reactor.registerEventHandler(clientInfoEventHandler, utils.ClientInfoEventKind)
	}
}

func (reactor *Reactor) makeStateWorkersReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		metaType, utils.StateWorkersEventKind, utils.StateWorkersPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.StateWorkersEventKind)
	}
	stateWorkersEventHandler := prepareStateWorkersEventHandler(reactor.APIClients)
	if stateWorkersEventHandler != nil {
		reactor.registerEventHandler(stateWorkersEventHandler, utils.StateWorkersEventKind)
	}
}

func (reactor *Reactor) registerEventSource(adapter evs.EventProvisioner, kind string) {
	reactor.EventSources[kind] = adapter
}

func prepareEtcdEventSource(sourcetype string, kind string, path string,
	queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
	provisoner := etcdevs.NewProvisioner(sourcetype, kind, path, queue)
	return provisoner
}

func (reactor *Reactor) registerEventHandler(handler evh.Handler, kind string) {
	reactor.EventHandlerMap[kind] = handler
}

func prepareFunctionRepoEventHandler(apiClients *APIClients) *functionrepohandler.FunctionRepoHandler {
	handler := functionrepohandler.NewEventHandler(apiClients.MetadataEtcdClient)
	return handler
}

func prepareClientInstanceEventHandler(apiClients *APIClients) *clientinstancehandler.ClientInstanceHandler {
	handler := clientinstancehandler.NewEventHandler(apiClients.RoutedataEtcdClient)
	return handler
}

func prepareBridgeInstanceEventHandler(apiClients *APIClients) *bridgeinstancehandler.BridgeInstanceHandler {
	handler := bridgeinstancehandler.NewEventHandler(apiClients.RoutedataEtcdClient)
	return handler
}

func prepareStateInfoEventHandler(apiClients *APIClients) *stateinfohandler.StateInfoHandler {
	handler := stateinfohandler.NewEventHandler(apiClients.MetadataEtcdClient)
	return handler
}

func prepareClientInfoEventHandler(apiClients *APIClients) *clientinfohandler.ClientInfoHandler {
	handler := clientinfohandler.NewEventHandler(apiClients.MetadataEtcdClient)
	return handler
}

func prepareStateWorkersEventHandler(apiClients *APIClients) *stateworkershandler.StateWorkerHandler {
	handler := stateworkershandler.NewEventHandler(apiClients.MetadataEtcdClient)
	return handler
}

func (reactor *Reactor) runWorker() {
	for reactor.processNextItem() {
		// continue looping
	}
}

// Run start stop Channel
func (reactor *Reactor) Run(stopCh <-chan struct{}) error {
	if stopCh == nil {
		log.GetLogger().Errorf("[bridgeplugin] stopCh is nil")
		return errors.New("[bridgeplugin] stopCh is nil")
	}
	go func(stopCh <-chan struct{}) {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("[bridgeplugin] stop channel is closed")
			}
			log.GetLogger().Warnf("[bridgeplugin] closing queue of reactor")
			reactor.queue.ShutDown()
		}
	}(stopCh)
	for eventSource, adapter := range reactor.EventSources {
		err := adapter.Start(stopCh)
		if err != nil {
			log.GetLogger().Warnf("[bridgeplugin] failed to start adapter for eventSource %s", eventSource)
		}
	}
	// Run forever
	for i := 0; i < workerNum; i++ {
		go wait.Until(reactor.runWorker, time.Second, stopCh)
	}
	log.GetLogger().Infof("[bridgeplugin] function meta reactor's workers coroutine are running")
	return nil
}

func (reactor *Reactor) processNextItem() bool {
	event, quit := reactor.queue.Get()
	if quit {
		return false
	}
	defer reactor.queue.Done(event)
	ev, ok := event.(evs.Event)
	if !ok {
		return false
	}
	if err := reactor.processItem(ev); err == nil {
		// No error, reset the ratelimit counters
		reactor.queue.Forget(event)
	} else if reactor.queue.NumRequeues(event) < maxRetries {
		reactor.queue.AddRateLimited(event)
	} else {
		// err != nil and too many retries
		log.GetLogger().Errorf("[bridgeplugin] gave up process %s with error %s", ev.SourceType, err.Error())
		reactor.queue.Forget(event)
		utilruntime.HandleError(err)
	}
	return true
}

func (reactor *Reactor) processItem(event evs.Event) error {
	startTime := time.Now()
	handler, ok := reactor.EventHandlerMap[event.Kind]
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] unknown event kind: %s with event type: %s and key: %s",
			event.Kind, event.EventType, event.KeyRef)
		return errors.New("unKnowKind event")
	}
	err := handler.HandleEvent(event)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to process event kind: %s with event type: %s "+
			"and key: %s, error %s", event.Kind, event.EventType, event.KeyRef, err.Error())
		return err
	}
	log.GetLogger().Infof("processChange | %s | %s | %s | %s", event.Kind, event.EventType, event.KeyRef,
		time.Since(startTime))
	return nil
}
