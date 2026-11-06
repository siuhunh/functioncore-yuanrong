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

package etcdadapter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	testInstanceKey = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
)

var (
	testFuncMeta = &types.FunctionMetaInfo{
		FuncMetaData: types.FuncMetaData{
			FunctionVersionURN: "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-mytest-functionb:$latest",
		},
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
	}

	testWorkerInstance = &types.WorkerInstance{
		InstanceID: testInstanceKey,
		Applier:    "worker-manager",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		CPU:        300,
		Memory:     128,
	}

	funcMetaEventChan = make(chan int, 1)
	funcInsEventChan  = make(chan int, 1)
)

type mockFuncMetaHandler struct {
}

func (handler *mockFuncMetaHandler) UpdateFuncMeta(funcKey string, info *types.FunctionMetaInfo) {
	funcMetaEventChan <- 1
}

func (handler *mockFuncMetaHandler) DeleteFuncMeta(funcKey string, source etcd3.EtcdType) {
	funcMetaEventChan <- 2
}

func TestStartNotifyWithFuncMetaEvent(t *testing.T) {
	convey.Convey("TestStartNotifyWithFuncMetaEvent", t, func() {
		wc := &etcd3.EtcdWatchChan{
			ResultChan: make(chan *etcd3.Event, 1),
		}

		patch := gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})

		patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(client *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			return wc
		})
		patch.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			ch := make(chan time.Time, 1)
			ch <- time.Time{}
			return &time.Ticker{C: ch}
		})

		defer func() {
			patch.Reset()
		}()

		stopCh := make(chan struct{}, 1)
		notifier, err := NewEtcdNotifier(stopCh)
		convey.So(err, convey.ShouldBeNil)
		notifier.AddObservers(CreateFuncMetaObserver(&mockFuncMetaHandler{}))

		value, _ := json.Marshal(testFuncMeta)
		event := &etcd3.Event{
			Key:   "/sn/functions/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/0-mytest-functionb/version/$latest",
			Type:  etcd3.PUT,
			Value: value,
		}
		wc.ResultChan <- event
		res, _ := <-funcMetaEventChan
		convey.So(res, convey.ShouldEqual, 1)

		event = &etcd3.Event{
			Key:       "/sn/functions/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/0-mytest-functionb/version/$latest",
			Type:      etcd3.DELETE,
			PrevValue: value,
		}
		wc.ResultChan <- event
		res, _ = <-funcMetaEventChan
		convey.So(res, convey.ShouldEqual, 2)
	})
}

type mockFuncInsHandler struct {
}

func (handler *mockFuncInsHandler) AddInstance(funcKeyWithRes string, info *types.WorkerInstance) {
	funcInsEventChan <- 1
}

func (handler *mockFuncInsHandler) DeleteInstance(funcKeyWithRes string, info *types.WorkerInstance) {
	funcInsEventChan <- 2
}

func TestStartNotifyWithFuncInsEvent(t *testing.T) {
	convey.Convey("TestStartNotifyWithFuncMetaEvent", t, func() {
		wc := &etcd3.EtcdWatchChan{
			ResultChan: make(chan *etcd3.Event, 1),
		}

		patch := gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})

		patch.ApplyFunc((*etcd3.EtcdClient).Watch, func(client *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			return wc
		})
		patch.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			ch := make(chan time.Time, 1)
			ch <- time.Time{}
			return &time.Ticker{C: ch}
		})

		defer func() {
			patch.Reset()
		}()

		stopCh := make(chan struct{}, 1)
		notifier, err := NewEtcdNotifier(stopCh)
		convey.So(err, convey.ShouldBeNil)
		notifier.AddObservers(CreateFuncInsObserver(&mockFuncInsHandler{}))

		value, _ := json.Marshal(testWorkerInstance)
		event := &etcd3.Event{
			Key: "/sn/workers/business/yrk/tenant/i1fe5394/function/app-storage-list/version/$latest/" +
				"defaultaz/default-#-yhagi1fe-app-storage-list-l.s.t-980699502",
			Type:  etcd3.PUT,
			Value: value,
		}
		wc.ResultChan <- event
		res, _ := <-funcInsEventChan
		convey.So(res, convey.ShouldEqual, 1)

		event = &etcd3.Event{
			Key: "/sn/workers/business/yrk/tenant/i1fe5394/function/app-storage-list/version/$latest/" +
				"defaultaz/default-#-yhagi1fe-app-storage-list-l.s.t-980699502",
			Type:      etcd3.DELETE,
			PrevValue: value,
		}
		wc.ResultChan <- event
		res, _ = <-funcInsEventChan
		convey.So(res, convey.ShouldEqual, 2)

		close(wc.ResultChan)
		close(stopCh)
		time.Sleep(100 * time.Millisecond)
	})
}
