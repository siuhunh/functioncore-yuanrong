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

package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/functionbus/common"
)

var (
	lock              sync.Mutex
	notifierValueMock = ""
)

func TestAddMultiObservers(t *testing.T) {
	wc1 := &etcd3.EtcdWatchChan{
		ResultChan: make(chan *etcd3.Event, 1),
	}
	wc2 := &etcd3.EtcdWatchChan{
		ResultChan: make(chan *etcd3.Event, 1),
	}
	data := "{\"localAuth\":{\"aKey\":\"ak\",\"sKey\":\"sk\"}}"
	err := json.Unmarshal([]byte(data), common.FConfig)
	if err != nil {
		fmt.Println(err)
		return
	}

	watchedRev := int64(0)

	patches := [...]*Patches{
		ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
			switch t {
			case etcd3.Meta:
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			default:
				return nil
			}
		}),
		ApplyFunc((*etcd3.EtcdClient).Watch, func(client *etcd3.EtcdClient, ctx context.Context,
			watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			atomic.StoreInt64(&watchedRev, rev)
			if watchPath == common.WatchPathPrefixWithWorkers {
				select {
				case <-wc1.ResultChan:
					wc1.ResultChan = make(chan *etcd3.Event, 1)
				default:
				}
				return wc1
			}
			return wc2
		}),
		ApplyFunc(os.Exit, func(code int) {
			return
		}),
		ApplyFunc(concurrency.NewSession,
			func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
				return nil, nil
			}),
		ApplyFunc((*alarm.Client).ClearAlarm,
			func(a *alarm.Client, almType alarm.Type) { return }),
		ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
			ch := make(chan time.Time, 1)
			close(ch)
			return &time.Ticker{C: ch}
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	workerObserver := &EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.WatchPathPrefixWithWorkers,
		Filter:        pathFilter,
		UpdateHandler: handleUpdate,
		DeleteHandler: handleDelete,
		SyncHandler:   handlerSync,
	}
	stateObserver := &EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     common.WatchPathPrefixWithStateWorkers,
		Filter:        pathFilter,
		UpdateHandler: handleUpdate,
		DeleteHandler: handleDelete,
		SyncHandler:   handlerSync,
	}
	stopCh := make(chan struct{})
	defer func() {
		close(stopCh)
	}()
	notifier, err := NewEtcdNotifier(stopCh)
	assert.Nil(t, err)
	notifier.AddObservers(workerObserver, stateObserver)
	time.Sleep(100 * time.Millisecond)

	// workerObserver should set rev to 10
	wc1.ResultChan <- &etcd3.Event{
		Type: etcd3.PUT,
		Rev:  10,
	}
	// stateObserver should set rev to 20
	wc2.ResultChan <- &etcd3.Event{
		Type: etcd3.PUT,
		Rev:  20,
	}
	// make workerObserver reconnect
	close(wc1.ResultChan)
	time.Sleep(100 * time.Millisecond)
	// assert workerObserver reconnect using rev 10, not 20
	assert.Equal(t, int64(10), atomic.LoadInt64(&watchedRev))
}

func TestAddObservers(t *testing.T) {
	wc := &etcd3.EtcdWatchChan{
		ResultChan: make(chan *etcd3.Event, 1),
	}
	data := "{\"localAuth\":{\"aKey\":\"ak\",\"sKey\":\"sk\"}}"
	err := json.Unmarshal([]byte(data), common.FConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	lock.Lock()
	notifierValueMock = ""
	lock.Unlock()
	convey.Convey("TestAddObservers", t, func() {
		patches := [...]*Patches{
			ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				switch t {
				case etcd3.Meta:
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				default:
					return nil
				}
			}),
			ApplyFunc((*etcd3.EtcdClient).Watch, func(client *etcd3.EtcdClient, ctx context.Context,
				watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return wc
			}),
			ApplyFunc(os.Exit, func(code int) {
				lock.Lock()
				notifierValueMock = "exit"
				lock.Unlock()
				return
			}),
			ApplyFunc(concurrency.NewSession,
				func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
					return nil, nil
				}),
			ApplyFunc((*alarm.Client).ClearAlarm,
				func(a *alarm.Client, almType alarm.Type) { return }),
			ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				ch := make(chan time.Time, 1)
				close(ch)
				return &time.Ticker{C: ch}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		workerObserver := &EtcdObserver{
			EtcdSelection: etcd3.Meta,
			WatchPath:     common.WatchPathPrefixWithWorkers,
			Filter:        pathFilter,
			UpdateHandler: handleUpdate,
			DeleteHandler: handleDelete,
			SyncHandler:   handlerSync,
		}
		stopCh := make(chan struct{})
		notifier, err := NewEtcdNotifier(stopCh)
		assert.Nil(t, err)

		notifier.AddObservers(workerObserver)
		defaultEvent := &etcd3.Event{
			Type: 99,
		}
		wc.ResultChan <- defaultEvent
		time.Sleep(100 * time.Millisecond)
		lock.Lock()
		assert.Equal(t, "", notifierValueMock)
		lock.Unlock()

		updateEvent := &etcd3.Event{
			Type: etcd3.PUT,
		}
		wc.ResultChan <- updateEvent
		time.Sleep(100 * time.Millisecond)
		lock.Lock()
		assert.Equal(t, "update", notifierValueMock)
		lock.Unlock()

		deleteEvent := &etcd3.Event{
			Type: etcd3.DELETE,
		}
		wc.ResultChan <- deleteEvent
		time.Sleep(100 * time.Millisecond)
		lock.Lock()
		assert.Equal(t, "delete", notifierValueMock)
		lock.Unlock()

		syncEvent := &etcd3.Event{
			Type: etcd3.SYNCED,
		}
		wc.ResultChan <- syncEvent
		time.Sleep(100 * time.Millisecond)
		lock.Lock()
		assert.Equal(t, "sync", notifierValueMock)
		lock.Unlock()

		close(stopCh)
	})
}

func TestAddObserversChanClosed(t *testing.T) {
	lock := sync.Mutex{}
	notifierValueMock := ""
	wc := &etcd3.EtcdWatchChan{
		ResultChan: make(chan *etcd3.Event, 1),
	}
	data := "{\"localAuth\":{\"aKey\":\"ak\",\"sKey\":\"sk\"}}"
	err := json.Unmarshal([]byte(data), common.FConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	convey.Convey("Observers chan closed", t, func() {
		patches := [...]*Patches{
			ApplyFunc(etcd3.GetETCDClient, func(t etcd3.EtcdType) *etcd3.EtcdClient {
				switch t {
				case etcd3.Meta:
					return &etcd3.EtcdClient{}
				default:
					return nil
				}
			}),
			ApplyFunc((*etcd3.EtcdClient).Watch, func(client *etcd3.EtcdClient, ctx context.Context,
				watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return wc
			}),
			ApplyFunc(os.Exit, func(code int) {
				lock.Lock()
				notifierValueMock = "exit"
				lock.Unlock()
				return
			}),
			ApplyFunc(concurrency.NewSession,
				func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
					return nil, nil
				}),
			ApplyFunc((*alarm.Client).ClearAlarm,
				func(a *alarm.Client, almType alarm.Type) { return }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		workerObserver := &EtcdObserver{
			EtcdSelection: etcd3.Meta,
			WatchPath:     common.WatchPathPrefixWithWorkers,
			Filter:        pathFilter,
			UpdateHandler: handleUpdate,
			DeleteHandler: handleDelete,
			SyncHandler:   handlerSync,
		}
		stopCh := make(chan struct{}, 1)
		notifier, err := NewEtcdNotifier(stopCh)
		assert.Nil(t, err)

		notifier.AddObservers(workerObserver)
		defaultEvent := &etcd3.Event{
			Type: 99,
		}
		wc.ResultChan <- defaultEvent
		time.Sleep(1 * time.Millisecond)
		lock.Lock()
		assert.Equal(t, "", notifierValueMock)
		lock.Unlock()

		close(wc.ResultChan)

		stopCh <- struct{}{}
		time.Sleep(100 * time.Millisecond)
	})
}

func TestStartServiceDiscovery(t *testing.T) {
	convey.Convey("TestAddObservers", t, func() {
		patches := [...]*Patches{
			ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			ApplyFunc(concurrency.NewSession,
				func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
					return nil, nil
				}),
			ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		stopCh := make(chan struct{}, 1)
		err := StartServiceDiscovery(stopCh)
		assert.Nil(t, err)
	})
}

func handleUpdate(ev *etcd3.Event) {
	lock.Lock()
	notifierValueMock = "update"
	lock.Unlock()
}

func handleDelete(ev *etcd3.Event) {
	lock.Lock()
	notifierValueMock = "delete"
	lock.Unlock()
}

func handlerSync(ev *etcd3.Event) {
	lock.Lock()
	notifierValueMock = "sync"
	lock.Unlock()
}

func pathFilter(path string) bool {
	return true
}

type testObserver struct {
}

func (t testObserver) Selection() etcd3.EtcdType {
	return 111
}

func (t testObserver) Object() interface{} {
	return 111
}

var testHandleUpdate = 1

func (t testObserver) HandleUpdate(i interface{}) {
	testHandleUpdate = 2
	return
}

func (t testObserver) HandleDelete(i interface{}) {
	testHandleUpdate = 2
	return
}

func (t testObserver) HandleSync(i interface{}) {
	testHandleUpdate = 2
	return
}

func TestStartNotifyAssertFailed(t *testing.T) {
	convey.Convey("TestStartNotifyAssertFailed", t, func() {
		patches := [...]*Patches{
			ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			ApplyFunc(concurrency.NewSession,
				func(client *clientv3.Client, opts ...concurrency.SessionOption) (*concurrency.Session, error) {
					return nil, nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		stopCh := make(chan struct{})
		notifier, _ := NewEtcdNotifier(stopCh)
		notifier.StartNotify(&testObserver{})
		assert.Equal(t, testHandleUpdate, 1)
	})
}
