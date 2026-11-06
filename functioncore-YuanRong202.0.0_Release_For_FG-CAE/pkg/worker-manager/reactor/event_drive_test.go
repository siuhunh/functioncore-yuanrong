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

package reactor

import (
	"errors"
	"functioncore/pkg/common/opqueue"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"golang.org/x/time/rate"
	corev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	etcdevs "functioncore/pkg/worker-manager/eventsource/etcdevents"
	"functioncore/pkg/worker-manager/podfetcher"
	evh "functioncore/pkg/worker-manager/reactor/eventhandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/apighandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/functioninstancehandler"
	"functioncore/pkg/worker-manager/reactor/eventhandler/functionrepohandler"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

var (
	queue = workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))

	reactor = Reactor{
		EventSources:    map[string]evs.EventProvisioner{},
		queue:           queue,
		EventHandlerMap: map[string]evh.Handler{},
	}

	rspChan = make(chan int, 1)
)

func initMockReactor() {
	queue = workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))

	reactor = Reactor{
		EventSources:    map[string]evs.EventProvisioner{},
		queue:           queue,
		EventHandlerMap: map[string]evh.Handler{},
	}
}

type adapter struct{}

func (*adapter) Start(_ <-chan struct{}) error {
	event := evs.Event{
		Kind: utils.NodeEventKind,
	}
	queue.Add(event)
	return nil
}

type handler struct{}

func (*handler) HandleEvent(_ evs.Event) error {
	rspChan <- 1
	return nil
}

func TestRun(t *testing.T) {
	convey.Convey("test_reactor_worker", t, func() {
		initMockReactor()
		disableOperationQueue = true
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.NodeEventKind)
		reactor.registerEventSource(&adapter{}, utils.NodeEventKind)
		setWorkerNum(0)
		reactor.Run(stopCh)

		reactor.processNextItem()
		rsp := <-rspChan
		convey.So(rsp, convey.ShouldEqual, 1)
		close(stopCh)
		close(rspChan)
	})
	convey.Convey("test_reactor_operation_queue", t, func() {
		initMockReactor()
		rspChan = make(chan int, 1)
		disableOperationQueue = false
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.NodeEventKind)
		reactor.registerEventSource(&adapter{}, utils.NodeEventKind)
		reactor.Run(stopCh)

		rsp := <-rspChan
		convey.So(rsp, convey.ShouldEqual, 1)
		close(stopCh)
		close(rspChan)
	})
}

func TestProcessNextItem(t *testing.T) {
	convey.Convey("test_processNextItem_ErrRouterETCDOperation", t, func() {
		initMockReactor()
		disableOperationQueue = true
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.NodeEventKind)
		reactor.registerEventSource(&adapter{}, utils.NodeEventKind)
		setWorkerNum(0)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*Reactor).processItem, func(_ *Reactor, event evs.Event) error {
				return utils.ErrRouterETCDOperation
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		reactor.Run(stopCh)
		reactor.processNextItem()
		close(stopCh)
	})

	convey.Convey("test_processNextItem_error", t, func() {
		initMockReactor()
		disableOperationQueue = true
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.NodeEventKind)
		reactor.registerEventSource(&adapter{}, utils.NodeEventKind)
		setWorkerNum(0)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*Reactor).processItem, func(_ *Reactor, event evs.Event) error {
				return errors.New("etstds")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		reactor.Run(stopCh)
		reactor.processNextItem()
		close(stopCh)
	})
}

func TestReactor_makeInstanceMetadataReactor(t *testing.T) {
	called := false
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				EventHandlerMap: make(map[string]evh.Handler),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(prepareEtcdEventSource, func(t etcd3.EtcdType, kind string, namespace string,
						queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
						called = true
						return &etcdevs.Provisioner{}
					}),
					gomonkey.ApplyFunc(prepareFunctionRepoEventHandler,
						func(etcdctl *etcd3.EtcdClient) *functionrepohandler.FunctionRepoHandler {
							return &functionrepohandler.FunctionRepoHandler{}
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			called = false
			patches := tt.patchesFunc()
			reactor.makeInstanceMetadataReactor()
			if called != tt.calledWant {
				t.Errorf("makeInstanceMetadataReactor error: called %v, wang %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func TestNewEventDrive(t *testing.T) {
	type args struct {
		name       string
		kubeClient kubernetes.Interface
		stopCh     <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		// test GetPodFetcher return nil
		{
			"case1",
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		// test GetPodFetcher return nil
		{
			"case2",
			args{
				name: "faas",
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initEventDrive, func(kubeClient kubernetes.Interface) (*podfetcher.PodFetcher,
						*utils.APIClients, error) {
						return &podfetcher.PodFetcher{}, &utils.APIClients{}, nil
					}),

					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{}
					}),
					gomonkey.ApplyFunc(makeReactor, func(info Info, apiClients *utils.APIClients,
						podFetcher *podfetcher.PodFetcher, stopCh <-chan struct{}) (*Reactor, error) {
						return &Reactor{}, nil
					}),
				})
				return patches
			},
			false,
		},

		// test  return nil
		{
			"case3",
			args{
				name: "snapshot",
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(initEventDrive, func(kubeClient kubernetes.Interface) (*podfetcher.PodFetcher,
						*utils.APIClients, error) {
						return &podfetcher.PodFetcher{}, &utils.APIClients{}, nil
					}),

					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{}
					}),
					gomonkey.ApplyFunc(makeReactor, func(info Info, apiClients *utils.APIClients,
						podFetcher *podfetcher.PodFetcher, stopCh <-chan struct{}) (*Reactor, error) {
						return &Reactor{}, nil
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, _, err := NewEventDrive(tt.args.name, tt.args.kubeClient, tt.args.stopCh)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewEventDrive() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
			time.Sleep(time.Millisecond)
		})
	}
}

func Test_makeReactor(t *testing.T) {
	patches := gomonkey.ApplyFunc(utils.GetPodInformer, func() corev1.PodInformer {
		return mockUtils.InformerFakeFactory.Core().V1().Pods()
	})
	patches.ApplyFunc(functioninstancehandler.NewEventHandler,
		func(etcdctl *etcd3.EtcdClient, podFetcher *podfetcher.PodFetcher,
			podLister listercorev1.PodLister) *functioninstancehandler.FunctionInstanceHandler {
			return &functioninstancehandler.FunctionInstanceHandler{}
		})
	patches.ApplyFunc(functionrepohandler.NewEventHandler,
		func(etcdctl *etcd3.EtcdClient) *functionrepohandler.FunctionRepoHandler {
			return &functionrepohandler.FunctionRepoHandler{}
		})
	defer patches.Reset()
	type args struct {
		info       Info
		apiClients *utils.APIClients
		podFetcher *podfetcher.PodFetcher
		stopCh     <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			k8sType,
			args{
				info: Info{pattern: k8sType},
				apiClients: &utils.APIClients{
					EtcdClients: &utils.EtcdClients{
						MetadataEtcdClient: &etcd3.EtcdClient{},
						RouterETCDClient:   &etcd3.EtcdClient{},
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*Reactor).makeFunctionInstanceReactor, func(_ *Reactor,
						podFetcher *podfetcher.PodFetcher) {
					}),
					gomonkey.ApplyFunc((*Reactor).makeNodeReactor, func(_ *Reactor,
						stopCh <-chan struct{}) {
					}),
				})
				return patches
			},
			false,
		},
		{
			name: metaType,
			args: args{
				info: Info{pattern: metaType},
				apiClients: &utils.APIClients{
					EtcdClients: &utils.EtcdClients{
						MetadataEtcdClient: &etcd3.EtcdClient{},
						RouterETCDClient:   &etcd3.EtcdClient{},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*Reactor).makeFunctionRepoReactor, func(_ *Reactor) {
					}),
					gomonkey.ApplyFunc((*Reactor).makeInstanceMetadataReactor, func(_ *Reactor) {
					}),
					gomonkey.ApplyFunc((*Reactor).makeTenantMetadataReactor, func(_ *Reactor) {
					}),
					gomonkey.ApplyFunc((*Reactor).makePredictionReactor, func(_ *Reactor) {
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: snapshotMetaType,
			args: args{
				info: Info{pattern: snapshotMetaType},
				apiClients: &utils.APIClients{
					EtcdClients: &utils.EtcdClients{
						MetadataEtcdClient: &etcd3.EtcdClient{},
						RouterETCDClient:   &etcd3.EtcdClient{},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*Reactor).makeFunctionRepoReactor, func(_ *Reactor) {
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := makeReactor(tt.args.info, tt.args.apiClients, tt.args.podFetcher, tt.args.stopCh)
			if (err != nil) != tt.wantErr {
				t.Errorf("makeReactor() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestReactor_makeFunctionRepoReactor(t *testing.T) {
	called := false
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		wantCalled  bool
	}{
		{
			"case1",
			fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				EventHandlerMap: make(map[string]evh.Handler),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(prepareFunctionRepoEventHandler,
						func(etcdctl *etcd3.EtcdClient) *functionrepohandler.FunctionRepoHandler {
							called = true
							return &functionrepohandler.FunctionRepoHandler{}
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			called = false
			patches := tt.patchesFunc()
			reactor.makeFunctionRepoReactor()
			if called != tt.wantCalled {
				t.Errorf("makeReactor() called = %v, wantCalled %v", called, tt.wantCalled)
			}
			patches.ResetAll()
		})
	}
}

func TestReactor_makeAPIGReactor(t *testing.T) {
	called := false
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		wantCalled  bool
	}{
		{
			"case1",
			fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				EventHandlerMap: make(map[string]evh.Handler),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(prepareAPIGEventHandler,
						func(etcdctl *etcd3.EtcdClient) *apighandler.RouteHandler {
							called = true
							return &apighandler.RouteHandler{}
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			called = false
			patches := tt.patchesFunc()
			reactor.makeAPIGReactor()
			if called != tt.wantCalled {
				t.Errorf("makeReactor() called = %v, wantCalled %v", called, tt.wantCalled)
			}
			patches.ResetAll()
		})
	}
}

func TestReactor_makeFunctionInstanceReactor(t *testing.T) {
	called := false
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	type args struct {
		podFetcher *podfetcher.PodFetcher
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		callWant    bool
	}{
		{
			"case1",
			fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				PodInformer:     mockUtils.InformerFakeFactory.Core().V1().Pods(),
				EventHandlerMap: make(map[string]evh.Handler),
			},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(functioninstancehandler.NewEventHandler,
						func(etcdctl *etcd3.EtcdClient, podFetcher *podfetcher.PodFetcher,
							podLister listercorev1.PodLister) *functioninstancehandler.FunctionInstanceHandler {
							called = true
							return &functioninstancehandler.FunctionInstanceHandler{}
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			called = false
			patches := tt.patchesFunc()
			reactor.makeFunctionInstanceReactor(tt.args.podFetcher)
			if called != tt.callWant {
				t.Errorf("makeReactor() called = %v, wantCalled %v", called, tt.callWant)
			}
			patches.ResetAll()
		})
	}
}

func TestReactor_makeNodeReactor(t *testing.T) {
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"case1",
			fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				PodInformer:     mockUtils.InformerFakeFactory.Core().V1().Pods(),
				EventHandlerMap: make(map[string]evh.Handler),
			},
			args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			reactor.makeNodeReactor(tt.args.stopCh)
		})
	}
}

func Test_prepareFunctionInstanceEventHandler(t *testing.T) {
	type args struct {
		etcdctl    *etcd3.EtcdClient
		podFetcher *podfetcher.PodFetcher
		podLister  listercorev1.PodLister
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(functioninstancehandler.NewEventHandler,
						func(etcdctl *etcd3.EtcdClient, podFetcher *podfetcher.PodFetcher,
							podLister listercorev1.PodLister) *functioninstancehandler.FunctionInstanceHandler {
							return &functioninstancehandler.FunctionInstanceHandler{}
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := prepareFunctionInstanceEventHandler(tt.args.etcdctl, tt.args.podFetcher, tt.args.podLister); (got != nil) != tt.want {
				t.Errorf("prepareFunctionInstanceEventHandler() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestReactor_makeTenantMetadataReactor(t *testing.T) {
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		Pattern          string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				EventHandlerMap: make(map[string]evh.Handler),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				Pattern:          tt.fields.Pattern,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			reactor.makeTenantMetadataReactor()
		})
	}
}

func TestReactor_makePredictionReactor(t *testing.T) {
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		Pattern          string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01 normal scenario",
			fields: fields{
				EventSources:    make(map[string]evs.EventProvisioner),
				EventHandlerMap: make(map[string]evh.Handler),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{}
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			reactor := &Reactor{
				EtcdClient:       tt.fields.EtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				Name:             tt.fields.Name,
				Pattern:          tt.fields.Pattern,
				queue:            tt.fields.queue,
				PodInformer:      tt.fields.PodInformer,
				NodeInformer:     tt.fields.NodeInformer,
			}
			reactor.makePredictionReactor()
		})
	}
}

func Test_makeTenantVpcRouterReactor(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test1 GetConfig is nil", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("nil")
					}),
				})
				return patches
			},
		},
		{
			"test2 prepareEtcdEventSource is nil", false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("nil")
					}),
					gomonkey.ApplyFunc(prepareEtcdEventSource, func(sourcetype etcd3.EtcdType, kind string, namespace string,
						queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
						return nil
					}),
					gomonkey.ApplyFunc((*Reactor).registerEventHandler, func(_ *Reactor, handler evh.Handler, kind string) {}),
				})
				return patches
			},
		},
		{
			"test2 prepareEtcdEventSource is not nil", false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("nil")
					}),
					gomonkey.ApplyFunc(prepareEtcdEventSource, func(sourcetype etcd3.EtcdType, kind string, namespace string,
						queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
						return &etcdevs.Provisioner{}
					}),
					gomonkey.ApplyFunc((*Reactor).registerEventSource, func(_ *Reactor, adapter evs.EventProvisioner, kind string) {}),
					gomonkey.ApplyFunc((*Reactor).registerEventHandler, func(_ *Reactor, handler evh.Handler, kind string) {}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			reactor.makeTenantVpcRouterReactor()
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestReactor_makeStateRouterReactor(t *testing.T) {
	type fields struct {
		EtcdClient       *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]evs.EventProvisioner
		EventHandlerMap  map[string]evh.Handler
		Name             string
		Pattern          string
		queue            workqueue.RateLimitingInterface
		PodInformer      corev1.PodInformer
		NodeInformer     corev1.NodeInformer
		opq              opqueue.OperationQueue
	}
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test1 prepareEtcdEventSource is nil", false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(prepareEtcdEventSource, func(sourcetype etcd3.EtcdType, kind string, namespace string,
						queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
						return nil
					}),
					gomonkey.ApplyFunc((*Reactor).registerEventHandler, func(_ *Reactor, handler evh.Handler, kind string) {}),
				})
				return patches
			},
		},
		{
			"test2 prepareEtcdEventSource is not nil", false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(prepareEtcdEventSource, func(sourcetype etcd3.EtcdType, kind string, namespace string,
						queue workqueue.RateLimitingInterface) *etcdevs.Provisioner {
						return &etcdevs.Provisioner{}
					}),
					gomonkey.ApplyFunc((*Reactor).registerEventSource, func(_ *Reactor, adapter evs.EventProvisioner, kind string) {}),
					gomonkey.ApplyFunc((*Reactor).registerEventHandler, func(_ *Reactor, handler evh.Handler, kind string) {}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			reactor.makeStateRouterReactor()
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}
