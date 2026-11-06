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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/faas-scheduler/servicerouter"
)

type mockLease struct {
}

func (m mockLease) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{ID: 1}, nil
}

func (m mockLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return nil, nil
}

func (m mockLease) TimeToLive(ctx context.Context, id clientv3.LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return nil, nil
}

func (m mockLease) Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error) {
	return nil, nil
}

func (m mockLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (m mockLease) KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return nil, nil
}

func (m mockLease) Close() error {
	return nil
}

func TestRegister(t *testing.T) {
	convey.Convey("TestRegister", t, func() {
		alarm.NewClient("", "", nil)
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})

		patch.ApplyFunc(clientv3.NewLease, func(c *clientv3.Client) clientv3.Lease {
			return &mockLease{}
		})
		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
			return nil
		})

		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Grant", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
			return 1, nil
		})

		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "KeepAliveOnce", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, leaseID clientv3.LeaseID) error {
			return nil
		})

		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Revoke", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, leaseID clientv3.LeaseID) error {
			return nil
		})

		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("key")}}}, nil
		})

		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete", func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
			return nil
		})

		defer func() {
			patch.Reset()
		}()

		stopCh := make(chan struct{})
		config := servicerouter.Config{
			Key:           "/sn/faas-scheduler/instances/cluster001/127.0.0.1/faas-scheduler-5b5886db99-66gn8",
			Value:         "{}",
			LeaseTTL:      2,
			LeaseInterval: 1,
			StopCh:        stopCh,
		}
		register := CreateRegister()
		err := register.Register(config)
		convey.So(err, convey.ShouldBeNil)
		time.Sleep(2 * time.Second)
		close(stopCh)
		time.Sleep(1 * time.Second)
	})
}

func TestMonitorRouterEtcdLease(t *testing.T) {
	convey.Convey("TestMonitorRouterEtcdLease", t, func() {
		alarm.NewClient("", "", nil)
		var isReported bool
		patch := gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
			func(a *alarm.Client, almType alarm.Type, specAddition string) {
				isReported = true
			})
		patch.ApplyFunc((*alarm.Client).ClearAlarm, func(a *alarm.Client, almType alarm.Type) {})

		r := ServiceRegisterAdapter{
			leaseMonitor: etcd3.CreateLeaseMonitor(),
		}
		r.leaseMonitor.Update()
		leaseDetectInterval = 100 * time.Millisecond
		leaseLongTimeout = 500 * time.Millisecond
		go r.monitorRouterEtcdLease(nil)

		stopCh := make(chan struct{})
		go r.monitorRouterEtcdLease(stopCh)
		time.Sleep(1 * time.Second)
		convey.So(isReported, convey.ShouldBeTrue)
		close(stopCh)
		time.Sleep(1 * time.Second)
	})
}

func TestPutInstanceInfoToEtcd(t *testing.T) {
	convey.Convey("TestPutInstanceInfoToEtcd", t, func() {
		r := ServiceRegisterAdapter{}
		err := r.putInstanceInfoToEtcd()
		convey.So(err, convey.ShouldNotBeNil)

		r.etcdClient = &etcd3.EtcdClient{}

		patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Grant", func(_ *etcd3.EtcdClient,
			ctx etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
			return 1, errors.New("error")
		})
		err = r.putInstanceInfoToEtcd()
		convey.So(err, convey.ShouldNotBeNil)
		patch.Reset()

		patch = gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Grant", func(_ *etcd3.EtcdClient,
			ctx etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
			return 1, nil
		})
		patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
			ctx etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
			return errors.New("error")
		})
		err = r.putInstanceInfoToEtcd()
		convey.So(err, convey.ShouldNotBeNil)
		patch.Reset()
	})
}

func TestUpdateConfig(t *testing.T) {
	convey.Convey("TestUpdateConfig", t, func() {
		r := ServiceRegisterAdapter{
			config: servicerouter.Config{
				LeaseTTL:      10,
				LeaseInterval: 5,
			},
		}
		r.UpdateConfig(servicerouter.Config{
			LeaseTTL:      30,
			LeaseInterval: 15,
		})
		convey.So(r.config.LeaseTTL, convey.ShouldEqual, 30)
		convey.So(r.config.LeaseInterval, convey.ShouldEqual, 15)
	})
}
