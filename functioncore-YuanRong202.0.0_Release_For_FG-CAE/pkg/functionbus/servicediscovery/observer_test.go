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
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/functionbus/common"
)

var (
	etcdValueMock = ""
)

func TestEtcdObserver(t *testing.T) {
	workerObserver := &EtcdObserver{
		WatchPath:     common.WatchPathPrefixWithWorkers,
		Filter:        workerPathFilter,
		UpdateHandler: updateProviderOnObserve,
		DeleteHandler: deleteProviderOnObserve,
		SyncHandler:   syncHandler,
	}
	convey.Convey("HandleUpdate", t, func() {
		etcdValueMock = ""
		convey.Convey("invalid event", func() {
			event := ""
			workerObserver.HandleUpdate(event)
			assert.Equal(t, "", etcdValueMock)
		})
		convey.Convey("invalid key", func() {
			event := &etcd3.Event{
				Type:  0,
				Key:   "",
				Value: []byte("value"),
			}
			workerObserver.HandleUpdate(event)
			assert.Equal(t, "", etcdValueMock)
		})
		convey.Convey("success event", func() {
			event := &etcd3.Event{
				Type:  0,
				Key:   "key",
				Value: []byte("value"),
			}
			workerObserver.HandleUpdate(event)
			assert.Equal(t, "update", etcdValueMock)
		})
	})

	convey.Convey("HandleDelete", t, func() {
		etcdValueMock = ""
		convey.Convey("invalid event", func() {
			event := ""
			workerObserver.HandleDelete(event)
			assert.Equal(t, "", etcdValueMock)
		})
		convey.Convey("invalid key", func() {
			event := &etcd3.Event{
				Type:  0,
				Key:   "",
				Value: []byte("value"),
			}
			workerObserver.HandleDelete(event)
			assert.Equal(t, "", etcdValueMock)
		})
		convey.Convey("success event", func() {
			event := &etcd3.Event{
				Type:  0,
				Key:   "key",
				Value: []byte("value"),
			}
			workerObserver.HandleDelete(event)
			assert.Equal(t, "delete", etcdValueMock)
		})
	})

	convey.Convey("HandleSync", t, func() {
		etcdValueMock = ""
		convey.Convey("invalid event", func() {
			event := ""
			workerObserver.HandleSync(event)
			assert.Equal(t, "", etcdValueMock)
		})
		convey.Convey("success event", func() {
			event := &etcd3.Event{
				Type:  0,
				Key:   "key",
				Value: []byte("value"),
			}
			workerObserver.HandleSync(event)
			assert.Equal(t, "sync", etcdValueMock)
		})
	})

}

func updateProviderOnObserve(ev *etcd3.Event) {
	etcdValueMock = "update"
}

func deleteProviderOnObserve(ev *etcd3.Event) {
	etcdValueMock = "delete"
}

func syncHandler(ev *etcd3.Event) {
	etcdValueMock = "sync"
}

func workerPathFilter(path string) bool {
	return path != ""
}
