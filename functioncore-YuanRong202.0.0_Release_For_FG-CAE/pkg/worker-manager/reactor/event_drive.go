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

// Package reactor deal with function service, instance and repo
package reactor

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/time/rate"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/opqueue"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	etcdevs "functioncore/pkg/worker-manager/eventsource/etcdevents"
	"functioncore/pkg/worker-manager/eventsource/nodeevents"
	"functioncore/pkg/worker-manager/eventsource/podevents"
	"functioncore/pkg/worker-manager/podfetcher"
	evh "functioncore/pkg/worker-manager/reactor/eventhandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/apighandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/clusterhandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/functioninstancehandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/functionrepohandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/networkhandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/nodehandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/predictionhandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/staterouterhandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/tenantmetadatahandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/tenantvpchandler"
	"functioncore/pkg/worker-manager/utils"
)

const (
	maxRetries       = 12
	rateLimit        = 1000
	tokensBurst      = 2000
	baseDelay        = 5
	maxDelay         = 1000
	k8sType          = "k8s"
	metaType         = "ETCD"
	snapshotMetaType = "snapshot"
)

var (
	workerNum             = 100
	operationQueueLength  = 500
	disableOperationQueue = parseDisableOperationQueue()
)

func parseDisableOperationQueue() bool {
	return os.Getenv("DISABLE_OPERATION_QUEUE") == "true"
}

// Reactor define event drive design
type Reactor struct {
	// EtcdClient- metadata etcd client
	EtcdClient       *etcd3.EtcdClient
	RouterETCDClient *etcd3.EtcdClient
	EventSources     map[string]evs.EventProvisioner
	EventHandlerMap  map[string]evh.Handler
	Name             string
	Pattern          string
	queue            workqueue.RateLimitingInterface
	PodInformer      informercorev1.PodInformer
	NodeInformer     informercorev1.NodeInformer
	opq              opqueue.OperationQueue
}

// Info define name and pattern
type Info struct {
	name    string
	pattern string
}

func setWorkerNum(workerNums int) {
	workerNum = workerNums
}

// makeReactor returns a reactor
func makeReactor(info Info, apiClients *utils.APIClients,
	podFetcher *podfetcher.PodFetcher, stopCh <-chan struct{}) (*Reactor, error) {
	queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))
	reactor := &Reactor{
		Name:             info.name,
		Pattern:          info.pattern,
		EventSources:     map[string]evs.EventProvisioner{},
		queue:            queue,
		EventHandlerMap:  map[string]evh.Handler{},
		EtcdClient:       apiClients.EtcdClients.MetadataEtcdClient,
		RouterETCDClient: apiClients.EtcdClients.RouterETCDClient,
		PodInformer:      utils.GetPodInformer(),
		NodeInformer:     utils.GetNodeInformer(),
	}

	if info.pattern == k8sType {
		// register k8s handler
		reactor.makeFunctionInstanceReactor(podFetcher)
		reactor.makeNodeReactor(stopCh)
	} else if info.pattern == metaType {
		// register etcd handler
		reactor.makeFunctionRepoReactor()
		reactor.makeInstanceMetadataReactor()
		reactor.makeTenantMetadataReactor()
		reactor.makePredictionReactor()
		reactor.makeAPIGReactor()
		reactor.makePublicNetworkReactor()
		reactor.makeClusterReactor()
		reactor.makeTenantVpcRouterReactor()
		reactor.makeStateRouterReactor()
	} else if info.pattern == snapshotMetaType {
		reactor.makeFunctionRepoReactor()
	}
	return reactor, nil
}

func (reactor *Reactor) makePredictionReactor() {
	clusterID := os.Getenv("CLUSTER_ID")
	predictionPrefix := fmt.Sprintf("%s/%s", utils.PredictionPrefix, clusterID)
	etcdEventProvisioner := etcdevs.NewProvisioner(etcd3.Meta, utils.PredictionKind, predictionPrefix,
		reactor.queue)
	reactor.registerEventSource(etcdEventProvisioner, utils.PredictionKind)
	predictionHandler := predictionhandler.NewEventHandler()
	if predictionHandler != nil {
		reactor.registerEventHandler(predictionHandler, utils.PredictionKind)
	}
}

func (reactor *Reactor) makeInstanceMetadataReactor() {
	clusterID := os.Getenv("CLUSTER_ID")
	log.GetLogger().Infof("The clusterID of the current cluster is: %s", clusterID)
	instancePrefix := fmt.Sprintf("%s/%s", utils.InstanceMetadataPrefix, clusterID)
	etcdEventProvisioner := prepareEtcdEventSource(
		etcd3.Meta, utils.InstanceMetadataEventKind, instancePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.InstanceMetadataEventKind)
	}
	functionRepoEventHandler := prepareFunctionRepoEventHandler(reactor.EtcdClient)
	if functionRepoEventHandler != nil {
		reactor.registerEventHandler(functionRepoEventHandler, utils.InstanceMetadataEventKind)
	}
	cfg, err := config.GetConfig()
	if err != nil || cfg.CAEMetaEtcd == nil {
		return
	}
	caeMetaProvisioner := prepareEtcdEventSource(
		etcd3.CAEMeta, utils.InstanceMetadataEventKind, instancePrefix, reactor.queue)
	if caeMetaProvisioner != nil {
		reactor.registerEventSource(caeMetaProvisioner, utils.InstanceMetadataEventKind+"_CAE")
	}
}

func (reactor *Reactor) makeFunctionInstanceReactor(podFetcher *podfetcher.PodFetcher) {
	podEventProvisioner := preparePodEventSource("Kubernetes", utils.PodEventKind,
		reactor.queue, reactor.PodInformer)
	if podEventProvisioner != nil {
		reactor.registerEventSource(podEventProvisioner, utils.PodEventKind)
	}

	functionInstanceEventHandler := prepareFunctionInstanceEventHandler(
		reactor.EtcdClient, podFetcher, reactor.PodInformer.Lister())
	if functionInstanceEventHandler != nil {
		reactor.registerEventHandler(functionInstanceEventHandler, utils.PodEventKind)
	}
}

func (reactor *Reactor) makeFunctionRepoReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		etcd3.Meta, utils.FunctionRepoEventKind, utils.FunctionMetadataPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.FunctionRepoEventKind)
	}
	functionRepoEventHandler := prepareFunctionRepoEventHandler(reactor.EtcdClient)
	if functionRepoEventHandler != nil {
		reactor.registerEventHandler(functionRepoEventHandler, utils.FunctionRepoEventKind)
	}
	cfg, err := config.GetConfig()
	if err != nil || cfg.CAEMetaEtcd == nil {
		return
	}
	caeMetaProvisioner := prepareEtcdEventSource(
		etcd3.CAEMeta, utils.FunctionRepoEventKind, utils.FunctionMetadataPrefix, reactor.queue)
	if caeMetaProvisioner != nil {
		reactor.registerEventSource(caeMetaProvisioner, utils.FunctionRepoEventKind+"_CAE")
	}
}

func (reactor *Reactor) makeTenantVpcRouterReactor() {
	cfg, err := config.GetConfig()
	if err != nil || cfg.CAEMetaEtcd == nil {
		log.GetLogger().Warnf("no cae meta etcd configured, skip watch tenant vpc router")
		return
	}
	provisioner := prepareEtcdEventSource(etcd3.CAEMeta, utils.TenantVPCRouterKind, utils.TenantVPCRouterPrefix,
		reactor.queue)
	if provisioner != nil {
		reactor.registerEventSource(provisioner, utils.TenantVPCRouterKind)
	}
	handler := tenantvpchandler.NewEventHandler()
	if handler != nil {
		reactor.registerEventHandler(handler, utils.TenantVPCRouterKind)
	}
}

func (reactor *Reactor) makeStateRouterReactor() {
	provisioner := prepareEtcdEventSource(etcd3.State, utils.StateRouterKind, utils.StateWorkerKey,
		reactor.queue)
	if provisioner != nil {
		reactor.registerEventSource(provisioner, utils.StateRouterKind)
	}
	handler := staterouterhandler.NewEventHandler()
	if handler != nil {
		reactor.registerEventHandler(handler, utils.StateRouterKind)
	}
}

func (reactor *Reactor) makeNodeReactor(stopCh <-chan struct{}) {
	nodeEventProvisioner := prepareNodeEventSource("kubernetes", utils.NodeEventKind, reactor.queue,
		reactor.NodeInformer)
	if nodeEventProvisioner != nil {
		reactor.registerEventSource(nodeEventProvisioner, utils.NodeEventKind)
	}

	nodeEventHandler := prepareNodeEventHandler(reactor.RouterETCDClient, stopCh)
	if nodeEventHandler != nil {
		reactor.registerEventHandler(nodeEventHandler, utils.NodeEventKind)
	}
}

func (reactor *Reactor) makeAPIGReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		etcd3.Meta, utils.APIGEventKind, utils.APIGMetadataPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.APIGEventKind)
	}
	apigEventHandler := prepareAPIGEventHandler(reactor.EtcdClient)
	if apigEventHandler != nil {
		reactor.registerEventHandler(apigEventHandler, utils.APIGEventKind)
	}
}

func (reactor *Reactor) makePublicNetworkReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		etcd3.Meta, utils.PublicNetworkEventKind, utils.PublicNetworkPrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.PublicNetworkEventKind)
	}
	publicNetworkEventHandler := preparePublicNetworkEventHandler(reactor.EtcdClient)
	if publicNetworkEventHandler != nil {
		reactor.registerEventHandler(publicNetworkEventHandler, utils.PublicNetworkEventKind)
	}
}

func (reactor *Reactor) makeTenantMetadataReactor() {
	clusterID := os.Getenv("CLUSTER_ID")
	log.GetLogger().Infof("The clusterID of the current cluster is: %s", clusterID)
	instancePrefix := fmt.Sprintf("%s/%s", utils.TenantMetadataPrefix, clusterID)
	etcdEventProvisioner := prepareEtcdEventSource(
		etcd3.Meta, utils.TenantMetadataEventKind, instancePrefix, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.TenantMetadataEventKind)
	}
	tenantMetadataEventHandler := prepareTenantMetadataEventHandler(reactor.EtcdClient)
	if tenantMetadataEventHandler != nil {
		reactor.registerEventHandler(tenantMetadataEventHandler, utils.TenantMetadataEventKind)
	}
}

func (reactor *Reactor) makeClusterReactor() {
	etcdEventProvisioner := prepareEtcdEventSource(
		etcd3.Meta, utils.ClusterEventKind, utils.ClusterMetadataKey, reactor.queue)
	if etcdEventProvisioner != nil {
		reactor.registerEventSource(etcdEventProvisioner, utils.ClusterEventKind)
	}
	clusterEventHandler := prepareClusterEventHandler(reactor.EtcdClient)
	if clusterEventHandler != nil {
		reactor.registerEventHandler(clusterEventHandler, utils.ClusterEventKind)
	}
}

// NewEventDrive returns a new EventDrive
func NewEventDrive(name string, kubeClient kubernetes.Interface, stopCh <-chan struct{}) (*Reactor, *Reactor, error) {
	podFetcher, apiClients, err := initEventDrive(kubeClient)
	if err != nil {
		return nil, nil, err
	}

	reactorInfo := Info{
		name:    name,
		pattern: k8sType,
	}
	k8sReactor, err := makeReactor(reactorInfo, apiClients, podFetcher, stopCh)
	if err != nil {
		return nil, nil, err
	}
	var metaReactor *Reactor
	if name == "faas" {
		reactorInfo.pattern = metaType
		metaReactor, err = makeReactor(reactorInfo, apiClients, podFetcher, stopCh)
	} else if name == "snapshot" {
		reactorInfo.pattern = snapshotMetaType
		metaReactor, err = makeReactor(reactorInfo, apiClients, podFetcher, stopCh)
	}

	if err != nil {
		return nil, nil, err
	}
	return k8sReactor, metaReactor, nil
}

func initEventDrive(kubeClient kubernetes.Interface) (*podfetcher.PodFetcher, *utils.APIClients, error) {
	etcdClients := &utils.EtcdClients{
		MetadataEtcdClient: etcd3.GetMetaEtcdClient(),
		RouterETCDClient:   etcd3.GetRouterEtcdClient(),
	}
	podFetcher := podfetcher.GetPodFetcher()
	if podFetcher == nil {
		log.GetLogger().Errorf("get pod fetcher failed")
		return nil, nil, errors.New("get pod fetcher failed")
	}

	apiClients := &utils.APIClients{
		EtcdClients: etcdClients,
		KubeClient:  kubeClient,
	}
	return podFetcher, apiClients, nil
}

func (reactor *Reactor) registerEventSource(adapter evs.EventProvisioner, kind string) {
	reactor.EventSources[kind] = adapter
}

func preparePodEventSource(sourcetype string, kind string, queue workqueue.RateLimitingInterface,
	podInformer informercorev1.PodInformer) *podevents.Provisioner {
	provisoner := podevents.NewProvisioner(sourcetype, kind, queue, podInformer)
	return provisoner
}

func prepareEtcdEventSource(sourcetype etcd3.EtcdType, kind string, namespace string,
	queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
	provisoner := etcdevs.NewProvisioner(sourcetype, kind, namespace, queue)
	return provisoner
}

func prepareNodeEventSource(sourcetype string, kind string, queue workqueue.RateLimitingInterface,
	nodeInfomer informercorev1.NodeInformer) *nodeevents.Provisioner {
	provisioner := nodeevents.NewProvisioner(sourcetype, kind, queue, nodeInfomer)
	return provisioner
}

func (reactor *Reactor) registerEventHandler(handler evh.Handler, kind string) {
	reactor.EventHandlerMap[kind] = handler
}

func prepareFunctionInstanceEventHandler(etcdctl *etcd3.EtcdClient,
	podFetcher *podfetcher.PodFetcher, podLister v1.PodLister) *functioninstancehandler.FunctionInstanceHandler {
	handler := functioninstancehandler.NewEventHandler(etcdctl, podFetcher, podLister)
	return handler
}

func prepareFunctionRepoEventHandler(etcdctl *etcd3.EtcdClient) *functionrepohandler.FunctionRepoHandler {
	handler := functionrepohandler.NewEventHandler(etcdctl)
	return handler
}

func prepareNodeEventHandler(etcdctl *etcd3.EtcdClient, stopCh <-chan struct{}) *nodehandler.NodeHandler {
	handler := nodehandler.NewEventHandler(etcdctl, stopCh)
	return handler
}

func prepareAPIGEventHandler(etcdctl *etcd3.EtcdClient) *apighandler.RouteHandler {
	handler := apighandler.NewEventHandler(etcdctl)
	return handler
}

func preparePublicNetworkEventHandler(etcdctl *etcd3.EtcdClient) *networkhandler.PublicNetworkHandler {
	handler := networkhandler.NewEventHandler(etcdctl)
	return handler
}

func prepareTenantMetadataEventHandler(etcdctl *etcd3.EtcdClient) *tenantmetadatahandler.TenantMetadataHandler {
	handler := tenantmetadatahandler.NewEventHandler(etcdctl)
	return handler
}

func (reactor *Reactor) runWorker() {
	for reactor.processNextItem() {
		// continue looping
	}
}

func prepareClusterEventHandler(etcdctl *etcd3.EtcdClient) *clusterhandler.ClusterMetadataHandler {
	handler := clusterhandler.NewEventHandler(etcdctl)
	return handler
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

	if !disableOperationQueue {
		log.GetLogger().Infof("start operation queue %s", reactor.Pattern)
		reactor.opq = opqueue.NewOperationQueue(operationQueueLength)
		go reactor.runOperationQueue()
	} else {
		log.GetLogger().Infof("start normal reactor workers %s", reactor.Pattern)
		for i := 0; i < workerNum; i++ {
			go wait.Until(reactor.runWorker, time.Second, stopCh)
		}
	}

	log.GetLogger().Infof("%s reactor's workers coroutine are running", reactor.Pattern)
}

func (reactor *Reactor) runOperationQueue() {
	for reactor.processNextOperation() {

	}
	log.GetLogger().Warnf("process op queue exit")
}

func (reactor *Reactor) processNextOperation() bool {
	event, quit := reactor.queue.Get()
	if quit {
		return false
	}
	ev, ok := event.(evs.Event)
	if !ok {
		reactor.queue.Done(event)
		return false
	}
	reactor.opq.Push(opqueue.Operation{
		Key: ev.KeyRef,
		Run: func() {
			defer reactor.queue.Done(event)
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
		},
	})
	return true
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

func (reactor *Reactor) processItem(event evs.Event) error {
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
	log.GetLogger().Infof("processChangeEvent | %s | %s | %s | %s", event.Kind, event.EventType, event.KeyRef,
		time.Since(startTime))
	return nil
}
