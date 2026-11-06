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

// Package reactor -
package reactor

import (
	"errors"
	"time"

	"golang.org/x/time/rate"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/eventsource/etcdevents"
	"functioncore/pkg/autoscaler/reactor/eventhandler"
	"functioncore/pkg/autoscaler/reactor/eventhandler/functionrepohandler"
	"functioncore/pkg/autoscaler/reactor/eventhandler/workermanagereventhandler"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/opqueue"
)

const (
	maxRetries  = 12
	rateLimit   = 1000
	tokensBurst = 2000
	baseDelay   = 5
	maxDelay    = 1000
	metaType    = "ETCD"
)

// Reactor define event drive design
type Reactor struct {
	// MetaEtcdClient- metadata etcd client
	MetaEtcdClient   *etcd3.EtcdClient
	RouterETCDClient *etcd3.EtcdClient
	EventSources     map[string]eventsource.EventProvisioner
	EventHandlerMap  map[string]eventhandler.Handler
	queue            workqueue.RateLimitingInterface
	Name             string
	Pattern          string
	opq              opqueue.OperationQueue
}

var (
	// default thread number to handle reactor event
	workerNum = 100
)

func (reactor *Reactor) makeFunctionRepoReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		"Etcd", utils.FunctionRepoEventKind, utils.FunctionMetadataPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.FunctionRepoEventKind)
	}
	functionRepoEventHandler := prepareFunctionRepoEventHandler(reactor.MetaEtcdClient)
	if functionRepoEventHandler != nil {
		reactor.registerEventHandler(functionRepoEventHandler, utils.FunctionRepoEventKind)
	}
}

func (reactor *Reactor) makeWorkerManagerInstanceReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		"Etcd", utils.WorkManagerEventKind, utils.WorkerManagerInstancePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.WorkManagerEventKind)
	}
	workerManagerInsEventHandler := prepareWorkerManagerInsEventHandler(reactor.MetaEtcdClient)
	if workerManagerInsEventHandler != nil {
		reactor.registerEventHandler(workerManagerInsEventHandler, utils.WorkManagerEventKind)
	}
}

func (reactor *Reactor) registerEventHandler(handler eventhandler.Handler, kind string) {
	reactor.EventHandlerMap[kind] = handler
}

func (reactor *Reactor) registerEventSource(adapter eventsource.EventProvisioner, kind string) {
	reactor.EventSources[kind] = adapter
}

func prepareEtcdEventSource(sourcetype string, kind string, namespace string,
	queue workqueue.RateLimitingInterface) *etcdevents.Provisioner {
	provisoner := etcdevents.NewProvisioner(sourcetype, kind, namespace, queue)
	return provisoner
}

func prepareFunctionRepoEventHandler(etcdctl *etcd3.EtcdClient) *functionrepohandler.FunctionRepoHandler {
	handler := functionrepohandler.NewEventHandler(etcdctl)
	return handler
}

func prepareWorkerManagerInsEventHandler(etcdctl *etcd3.EtcdClient) *workermanagereventhandler.WorkerManagerHandler {
	return workermanagereventhandler.NewEventHandler(etcdctl)
}

func initEventDrive() (*config.APIClients, error) {
	etcdClients := &config.EtcdClients{
		MetadataEtcdClient: etcd3.GetMetaEtcdClient(),
		RouterETCDClient:   etcd3.GetCaeRouterEtcdClient(),
	}

	apiClients := &config.APIClients{
		EtcdClients: etcdClients,
	}
	return apiClients, nil
}

// NewEventDrive -
func NewEventDrive() (*Reactor, error) {
	apiClients, err := initEventDrive()
	if err != nil {
		return nil, err
	}
	var metaReactor *Reactor
	metaReactor, err = makeReactor(apiClients)
	return metaReactor, err
}

func setWorkerNum(workerNums int) {
	workerNum = workerNums
}

// makeReactor returns a reactor
func makeReactor(apiClients *config.APIClients) (*Reactor, error) {
	queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))
	reactor := &Reactor{
		EventSources:     map[string]eventsource.EventProvisioner{},
		queue:            queue,
		EventHandlerMap:  map[string]eventhandler.Handler{},
		MetaEtcdClient:   apiClients.EtcdClients.MetadataEtcdClient,
		RouterETCDClient: apiClients.EtcdClients.RouterETCDClient,
	}

	reactor.makeFunctionRepoReactor()
	reactor.makeInstanceMetadataReactor()
	// flow state change, autoScale need know
	reactor.makeWorkerManagerInstanceReactor()
	return reactor, nil
}

// Run start stop Channel
func (reactor *Reactor) Run(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	go func(stopCh <-chan struct{}) {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed")
			}
			log.GetLogger().Warnf("closing queue of reactor")
			reactor.queue.ShutDown()
		}
	}(stopCh)
	for eventSource, adapter := range reactor.EventSources {
		err := adapter.Start(stopCh)
		if err != nil {
			log.GetLogger().Warnf("failed to start adapter for eventSource %s", eventSource)
		}
	}

	log.GetLogger().Infof("start normal reactor workers %s", reactor.Pattern)
	for i := 0; i < workerNum; i++ {
		go wait.Until(reactor.runWorker, time.Second, stopCh)
	}

	log.GetLogger().Infof("%s reactor's workers coroutine are running", reactor.Pattern)
}

func (reactor *Reactor) runWorker() {
	for reactor.processNextItem() {
		// continue looping
	}
}

func (reactor *Reactor) processNextItem() bool {
	event, quit := reactor.queue.Get()
	if quit {
		return false
	}
	defer reactor.queue.Done(event)
	ev, ok := event.(eventsource.Event)
	if !ok {
		return false
	}
	if err := reactor.processItem(ev); err == nil {
		// No error, reset the ratelimit counters
		reactor.queue.Forget(event)
	} else if err == utils.ErrRouterETCDOperation {
		reactor.queue.AddRateLimited(event)
	} else if reactor.queue.NumRequeues(event) < maxRetries {
		reactor.queue.AddRateLimited(event)
	} else {
		// err != nil and too many retries
		log.GetLogger().Errorf("gave up process %s with error %s", ev.SourceType, err.Error())
		reactor.queue.Forget(event)
		utilruntime.HandleError(err)
	}
	return true
}

func (reactor *Reactor) processItem(event eventsource.Event) error {
	startTime := time.Now()
	handler, ok := reactor.EventHandlerMap[event.Kind]
	if !ok {
		log.GetLogger().Errorf("unknown event kind: %s with event type: %s and key: %s",
			event.Kind, event.EventType, event.KeyRef)
		return errors.New("unKnowKind event")
	}
	err := handler.HandleEvent(event)
	if err != nil {
		log.GetLogger().Errorf("failed to process event kind: %s with event type: %s and key: %s, error %s",
			event.Kind, event.EventType, event.KeyRef, err.Error())
		return err
	}
	log.GetLogger().Infof("processing change of event kind: %s with event type: %s and key: %s cost time: %s",
		event.Kind, event.EventType, event.KeyRef, time.Since(startTime))
	return nil
}

func (reactor *Reactor) makeInstanceMetadataReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		"Etcd", utils.InstanceMetadataEventKind, utils.InstanceMetadataPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.InstanceMetadataEventKind)
	}
	functionRepoEventHandler := prepareFunctionRepoEventHandler(reactor.MetaEtcdClient)
	if functionRepoEventHandler != nil {
		reactor.registerEventHandler(functionRepoEventHandler, utils.InstanceMetadataEventKind)
	}
}
