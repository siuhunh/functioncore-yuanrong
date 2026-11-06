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
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
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
	panic("implement me")
}

type fakeKV struct {
	put    uint32
	get    uint32
	delete uint32
	do     uint32
}

func (fk *fakeKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	atomic.AddUint32(&fk.put, 1)
	return &clientv3.PutResponse{}, nil
}

func (fk *fakeKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	atomic.AddUint32(&fk.get, 1)
	return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{}}}, nil
}

func (fk *fakeKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	atomic.AddUint32(&fk.delete, 1)
	return &clientv3.DeleteResponse{}, nil
}

func (fakeKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (fk *fakeKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	atomic.AddUint32(&fk.do, 1)
	return clientv3.OpResponse{}, nil
}

func (fakeKV) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

func TestRegisterInstance_PutInstanceToEtcd(t *testing.T) {
	convey.Convey("test put instance info", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{KV: &fakeKV{}, Lease: &mockLease{}}}
		})

		defer func() {
			patch.Reset()
		}()

		convey.Convey("lease id not exist", func() {
			var keyInput string
			defer gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
				keyInput = key
				return nil
			}).Reset()
			defer gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return key
			}).Reset()
			var getInstanceKeyErr error
			instanceKey, getInstanceKeyErr = getInstanceKey()
			err := putInstanceInfoToEtcd()
			convey.So(err, convey.ShouldBeNil)
			convey.So(getInstanceKeyErr, convey.ShouldBeNil)
			convey.So(keyInput, convey.ShouldEqual, "/sn/frontend/instances/CLUSTER_ID/NODE_IP/POD_NAME")
			convey.So(leaseID, convey.ShouldEqual, 1)
		})

		convey.Convey("put error", func() {
			var keyInput string
			defer gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
				return fmt.Errorf("put failed")
			}).Reset()
			defer gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return key
			}).Reset()
			err := putInstanceInfoToEtcd()
			var getInstanceKeyErr error
			instanceKey, getInstanceKeyErr = getInstanceKey()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(keyInput, convey.ShouldBeBlank)
			convey.So(getInstanceKeyErr, convey.ShouldBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "put failed")
		})

		convey.Convey("validate clusterId", func() {
			err := validateEnvs("", "ip", "podname")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "CLUSTER_ID env not found")
			err = validateEnvs("cluster", "", "podname")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "NODE_IP env not found")
			err = validateEnvs("cluster", "ip", "")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "POD_NAME env not found")
		})
	})
}

func prepareEnv() {
	_ = os.Setenv("CLUSTER_ID", "local_test")
	_ = os.Setenv("NODE_IP", "127.0.0.1")
	_ = os.Setenv("POD_NAME", "frontend_****")
}

func cleanEnv() {
	_ = os.Setenv("CLUSTER_ID", "")
	_ = os.Setenv("NODE_IP", "")
	_ = os.Setenv("POD_NAME", "")
}

func Test_registerInstance(t *testing.T) {
	prepareEnv()
	patch := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{Lease: &mockLease{}}}
	})
	patch.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(_ *etcd3.EtcdClient,
		ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
		return nil
	})
	defer func() {
		patch.Reset()
		cleanEnv()
	}()

	registerInstance(nil)
}

func Test_isKeyExist(t *testing.T) {
	convey.Convey("Test isKeyExist", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{
				Client: &clientv3.Client{},
			}
		})
		defer patch.Reset()
		convey.Convey("get etcd key return empty", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
			})
			defer patch.Reset()
			existed := isKeyExist()
			convey.So(existed, convey.ShouldBeFalse)
		})

		convey.Convey("succeed to get etcd key", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("key")}}}, nil
			})
			defer patch.Reset()
			existed := isKeyExist()
			convey.So(existed, convey.ShouldBeTrue)
		})

		convey.Convey("failed to get etcd key", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return nil, errors.New("failed")
			})
			defer patch.Reset()
			existed := isKeyExist()
			convey.So(existed, convey.ShouldBeFalse)
		})
	})
}

func Test_startRefreshLeaseJob(t *testing.T) {
	kv := &fakeKV{}
	patches := gomonkey.NewPatches()
	patches.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
		return nil
	})
	patches.ApplyFunc((*clientv3.Client).Ctx, func(_ *clientv3.Client) context.Context { return context.TODO() })
	patches.ApplyFunc((*clientv3.Client).Close, func(_ *clientv3.Client) error { return nil })
	patches.ApplyFunc(clientv3.NewKV, func(c *clientv3.Client) clientv3.KV {
		return kv
	})
	patches.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{KV: kv, Lease: &mockLease{}}}
	})
	defer func() {
		patches.Reset()
	}()
	refreshInterval = 1 * time.Millisecond

	// stop chan is nil, will not trigger refresh
	go startRefreshLeaseJob(nil)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&kv.get))
	assert.Equal(t, uint32(0), atomic.LoadUint32(&kv.put))
	assert.Equal(t, uint32(0), atomic.LoadUint32(&kv.do))

	stopCh := make(chan struct{})
	go startRefreshLeaseJob(stopCh)
	time.Sleep(100 * time.Millisecond)
	assert.NotEqual(t, uint32(0), atomic.LoadUint32(&kv.get))
	close(stopCh)
	time.Sleep(1 * time.Second)
}

func TestUpdateLeaseConfig(t *testing.T) {
	convey.Convey("TestUpdateLeaseConfig", t, func() {
		UpdateLeaseConfig(10, 10)
		assert.NotEqual(t, leaseTTL, 10)
		assert.NotEqual(t, refreshInterval.Seconds(), 10)
	})
}

func TestRefreshLease(t *testing.T) {
	flag := false
	convey.Convey("TestRefreshLease", t, func() {
		patches := gomonkey.NewPatches()
		patches.ApplyFunc(isKeyExist, func() bool {
			return true
		})
		defer patches.Reset()
		patches.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
			kv := &fakeKV{}
			return &etcd3.EtcdClient{Client: &clientv3.Client{KV: kv, Lease: &mockLease{}}}
		})
		defer patches.Reset()
		gomonkey.ApplyFunc((*etcd3.EtcdClient).KeepAliveOnce,
			func(_ *etcd3.EtcdClient, ctx etcd3.EtcdCtxInfo, leaseID clientv3.LeaseID) error {
				return errors.New("error")
			})
		defer patches.Reset()
		patches.ApplyFunc(putInstanceInfoToEtcd, func() error {
			flag = true
			return errors.New("error")
		})
		defer patches.Reset()
		refreshLease()
		assert.True(t, flag)
	})
}
