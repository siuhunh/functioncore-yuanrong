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
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/reactor/eventhandler"
	"functioncore/pkg/autoscaler/reactor/eventhandler/functionrepohandler"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/opqueue"
	mockUtils "functioncore/test/utils"
)

func TestReactor_makeFunctionRepoReactor(t *testing.T) {
	type fields struct {
		MetaEtcdClient   *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]eventsource.EventProvisioner
		EventHandlerMap  map[string]eventhandler.Handler
		queue            workqueue.RateLimitingInterface
		Name             string
		Pattern          string
		opq              opqueue.OperationQueue
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		wantCalled  bool
	}{
		// TODO: Add test cases.
		{
			"case1",
			fields{
				EventSources:    make(map[string]eventsource.EventProvisioner),
				EventHandlerMap: make(map[string]eventhandler.Handler),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
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
				MetaEtcdClient:   tt.fields.MetaEtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				queue:            tt.fields.queue,
				Name:             tt.fields.Name,
				Pattern:          tt.fields.Pattern,
				opq:              tt.fields.opq,
			}
			patches := tt.patchesFunc()
			reactor.makeFunctionRepoReactor()
			patches.ResetAll()
		})
	}
}

func TestReactor_makeReactor(t *testing.T) {
	type fields struct {
		MetaEtcdClient   *etcd3.EtcdClient
		RouterETCDClient *etcd3.EtcdClient
		EventSources     map[string]eventsource.EventProvisioner
		EventHandlerMap  map[string]eventhandler.Handler
		queue            workqueue.RateLimitingInterface
		Name             string
		Pattern          string
		opq              opqueue.OperationQueue
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			"case1",
			fields{
				EventSources:    make(map[string]eventsource.EventProvisioner),
				EventHandlerMap: make(map[string]eventhandler.Handler),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reactor := Reactor{
				MetaEtcdClient:   tt.fields.MetaEtcdClient,
				RouterETCDClient: tt.fields.RouterETCDClient,
				EventSources:     tt.fields.EventSources,
				EventHandlerMap:  tt.fields.EventHandlerMap,
				queue:            tt.fields.queue,
				Name:             tt.fields.Name,
				Pattern:          tt.fields.Pattern,
				opq:              tt.fields.opq,
			}
			reactor.makeFunctionRepoReactor()
			reactor.makeInstanceMetadataReactor()
			reactor.makeWorkerManagerInstanceReactor()
		})
	}
}

func TestNewEventDrive(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			"case 1",
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewEventDrive()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewEventDrive() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_makeReactor(t *testing.T) {
	type args struct {
		apiClients *config.APIClients
	}
	apiClients := &config.APIClients{
		EtcdClients: &config.EtcdClients{},
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"case 1",
			args{
				apiClients: apiClients,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := makeReactor(tt.args.apiClients)
			if (err != nil) != tt.wantErr {
				t.Errorf("makeReactor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

var (
	queue = workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))

	reactor = &Reactor{
		EventSources:     map[string]eventsource.EventProvisioner{},
		queue:            queue,
		EventHandlerMap:  map[string]eventhandler.Handler{},
		MetaEtcdClient:   &etcd3.EtcdClient{},
		RouterETCDClient: &etcd3.EtcdClient{},
	}

	rspChan = make(chan int, 1)
)

type handler struct{}

func (*handler) HandleEvent(_ eventsource.Event) error {
	rspChan <- 1
	return nil
}

type adapter struct{}

func (*adapter) Start(_ <-chan struct{}) error {
	event := eventsource.Event{
		Kind: utils.FunctionRepoEventKind,
	}
	queue.Add(event)
	return nil
}

func TestRun(t *testing.T) {
	Convey("TestRun", t, func() {
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.FunctionRepoEventKind)
		reactor.registerEventSource(&adapter{}, utils.FunctionRepoEventKind)
		setWorkerNum(1)
		reactor.Run(stopCh)
		reactor.processNextItem()
		rsp := <-rspChan
		So(rsp, ShouldEqual, 1)
		close(stopCh)
		close(rspChan)
	})
}

func TestProcessNextItem(t *testing.T) {
	Convey("test_processNextItem_ErrRouterETCDOperation", t, func() {
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.FunctionRepoEventKind)
		reactor.registerEventSource(&adapter{}, utils.FunctionRepoEventKind)
		setWorkerNum(0)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*Reactor).processItem, func(_ *Reactor, event eventsource.Event) error {
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

	Convey("test_processNextItem_error", t, func() {
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, utils.FunctionRepoEventKind)
		reactor.registerEventSource(&adapter{}, utils.FunctionRepoEventKind)
		setWorkerNum(0)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*Reactor).processItem, func(_ *Reactor, event eventsource.Event) error {
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
