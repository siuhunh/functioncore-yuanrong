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

package frontend

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/frontend-invoke/constant"
)

func TestProcessWatcher(t *testing.T) {
	convey.Convey("Test Process Watcher", t, func() {
		convey.Convey("check init succeed", func() {
			filterFlag := true
			HandlerFlag := true
			stopCh := make(chan struct{})
			var (
				filterSucceedCount  int32
				handlerSucceedCount int32
			)
			opt := &watchOption{
				Path:      constant.WorkerInstancePrefix,
				SuccessCh: make(chan string),
				Filter: func(s string) bool {
					if filterFlag {
						filterFlag = false
						return false
					}
					atomic.AddInt32(&filterSucceedCount, 1)
					return true
				},
				Handler: func(*etcd3.Event) error {
					if HandlerFlag {
						HandlerFlag = false
						return errors.New("")
					}
					atomic.AddInt32(&handlerSucceedCount, 1)
					return nil
				},
				StopCh: stopCh,
			}
			watchCh := &etcd3.EtcdWatchChan{
				ResultChan: make(chan *etcd3.Event),
			}
			constant.FConfig.LocalAuth = &localauth.AuthConfig{}
			etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetETCDClient, func(selection etcd3.EtcdType) *etcd3.EtcdClient {
					return etcdClient
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(_ *etcd3.EtcdClient,
					ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
					return watchCh
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			go processWatcher(opt)
			watchCh.ResultChan <- &etcd3.Event{}
			watchCh.ResultChan <- &etcd3.Event{}
			watchCh.ResultChan <- &etcd3.Event{}
			time.Sleep(500 * time.Millisecond)
			convey.So(atomic.LoadInt32(&filterSucceedCount), convey.ShouldEqual, int32(2))
			convey.So(atomic.LoadInt32(&handlerSucceedCount), convey.ShouldEqual, int32(1))
			time.Sleep(100 * time.Millisecond)

		})
	})
}

func TestProcessWatcherWithRecordRev(t *testing.T) {
	stopCh := make(chan struct{})
	defer func() {
		close(stopCh)
	}()

	opt := &watchOption{
		Path:      constant.WorkerInstancePrefix,
		SuccessCh: make(chan string),
		Filter: func(s string) bool {
			return true
		},
		Handler: func(*etcd3.Event) error {
			return nil
		},
		StopCh: stopCh,

		RecordRev: true,
	}

	var (
		watchRev int64
		reWatch  int32
	)
	createdCh := make(chan struct{}, 1)

	watchCh := &etcd3.EtcdWatchChan{ResultChan: make(chan *etcd3.Event)}
	reWatchCh := &etcd3.EtcdWatchChan{ResultChan: make(chan *etcd3.Event)}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(etcd3.GetETCDClient, func(selection etcd3.EtcdType) *etcd3.EtcdClient {
			return etcdClient
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch", func(_ *etcd3.EtcdClient,
			ctx context.Context, watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
			watchRev = rev
			createdCh <- struct{}{}
			if atomic.AddInt32(&reWatch, 1) > 1 {
				return reWatchCh
			}
			return watchCh
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()

	go processWatcher(opt)
	<-createdCh
	watchCh.ResultChan <- &etcd3.Event{Rev: 1230}
	watchCh.ResultChan <- &etcd3.Event{Rev: 1234}
	watchCh.ResultChan <- &etcd3.Event{Rev: 123}
	watchCh.ResultChan <- &etcd3.Event{Rev: 0}
	close(watchCh.ResultChan)
	<-createdCh
	// re-watch using recorded revision
	assert.Equal(t, int64(1234), watchRev)

	reWatchCh.ResultChan <- &etcd3.Event{Rev: 1235}
	reWatchCh.ResultChan <- &etcd3.Event{Rev: 1237}
	reWatchCh.ResultChan <- &etcd3.Event{Type: etcd3.ERROR, Value: []byte("etcdserver: mvcc: required revision has been compacted")}
	close(reWatchCh.ResultChan)
	<-createdCh
	// recorded revision has been compacted, re-watch using rev 0
	assert.Equal(t, int64(0), watchRev)
}
