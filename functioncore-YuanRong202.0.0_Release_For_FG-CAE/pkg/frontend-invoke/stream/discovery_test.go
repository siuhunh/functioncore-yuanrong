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

package stream

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/test/utils"
)

func TestInitDiscovery(t *testing.T) {
	stopCh := signals.WaitForSignal()
	alarm.NewClient("127.0.0.1:31329", constants.FunctionAccessor, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth("ak", "sk"))
	convey.Convey("TestInitDiscovery", t, func() {
		convey.Convey("RouterEtcdClient is nil", func() {
			err := InitDiscovery(stopCh)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "route etcd client is nil")
		})

		convey.Convey("GetValues error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return nil, errors.New("GetValues err")
				}),
			})
			err := InitDiscovery(stopCh)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "GetValues err")
			patches.ResetAll()
		})

		convey.Convey("No health nodes", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return []string{"[\"127.0.0.1\", \"127.3.0\", \"127.0.0.2\"]"}, nil
				}),
			})
			err := InitDiscovery(stopCh)
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(manager.HealthNodes), convey.ShouldEqual, 0)
			convey.So(len(manager.NodesIP), convey.ShouldEqual, 3)

			_, err = GetRandomStreamsAddr()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "there are no healthy streamHandler instance")
			patches.ResetAll()
		})

		convey.Convey("InitDiscovery success", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
					return []string{"[\"127.0.0.1\", \"127.3.0\", \"127.0.0.2\"]"}, nil
				}),
				gomonkey.ApplyFunc((*http.Client).Get, func(client *http.Client,
					url string) (resp *http.Response, err error) {
					if strings.Contains(url, "127.0.0.1") {
						return &http.Response{Body: http.NoBody}, nil
					}
					return nil, errors.New("connect timeout")
				}),
			})
			err := InitDiscovery(stopCh)
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(manager.HealthNodes), convey.ShouldEqual, 1)
			convey.So(len(manager.NodesIP), convey.ShouldEqual, 3)

			node, err := GetRandomStreamsAddr()
			convey.So(err, convey.ShouldBeNil)
			convey.So(node, convey.ShouldEqual, "127.0.0.1")
			patches.ResetAll()
		})
	})
}

func TestProcessStreamEvent(t *testing.T) {
	convey.Convey("TestProcessStreamEvent", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc((*http.Client).Get, func(client *http.Client,
				url string) (resp *http.Response, err error) {
				if strings.Contains(url, "127.0.0.1") {
					return &http.Response{Body: http.NoBody}, nil
				}
				return nil, errors.New("connect timeout")
			}),
		})
		defer patches.ResetAll()
		putEventInvalid := &etcd3.Event{
			Type:  etcd3.PUT,
			Value: []byte("111"),
		}
		err := ProcessStreamEvent(putEventInvalid)
		convey.So(err, convey.ShouldNotBeNil)
		putEventNil := &etcd3.Event{
			Type: etcd3.PUT,
		}
		err = ProcessStreamEvent(putEventNil)
		convey.So(err, convey.ShouldBeNil)
		putEvent := &etcd3.Event{
			Type:  etcd3.PUT,
			Value: []byte("[\"127.0.0.1\", \"127.3.0\", \"127.0.0.2\"]"),
		}
		err = ProcessStreamEvent(putEvent)
		err = ProcessStreamEvent(putEvent)
		err = ProcessStreamEvent(putEvent)
		convey.So(err, convey.ShouldBeNil)
		node, err := GetRandomStreamsAddr()
		convey.So(err, convey.ShouldBeNil)
		convey.So(node, convey.ShouldEqual, "127.0.0.1")

		putEvent = &etcd3.Event{
			Type:  etcd3.PUT,
			Value: []byte("[\"127.0.0.1\"]"),
		}
		err = ProcessStreamEvent(putEvent)
		convey.So(err, convey.ShouldBeNil)
		node, err = GetRandomStreamsAddr()
		convey.So(err, convey.ShouldBeNil)
		convey.So(node, convey.ShouldEqual, "127.0.0.1")

		delEvent := &etcd3.Event{
			Type: etcd3.DELETE,
		}
		err = ProcessStreamEvent(delEvent)
		convey.So(err, convey.ShouldBeNil)
		_, err = GetRandomStreamsAddr()
		convey.So(err, convey.ShouldNotBeNil)
		errEvent := &etcd3.Event{
			Type: etcd3.ERROR,
		}
		err = ProcessStreamEvent(errEvent)
		convey.So(err, convey.ShouldNotBeNil)

		convey.So(IsStreamEvent(InstanceKey), convey.ShouldEqual, true)
	})

}
