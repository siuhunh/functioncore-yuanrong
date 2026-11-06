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

// Package channelbusconn is used for routing messages between functiontask with bus proxy
package channelbusconn

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/utils"
)

type fakeLease struct{}

func (fakeLease) Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
	return &clientv3.LeaseGrantResponse{}, nil
}

func (fakeLease) Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	return &clientv3.LeaseRevokeResponse{}, nil
}

func (fakeLease) TimeToLive(ctx context.Context, id clientv3.LeaseID, opts ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
	return &clientv3.LeaseTimeToLiveResponse{}, nil
}

func (fakeLease) Leases(ctx context.Context) (*clientv3.LeaseLeasesResponse, error) {
	return &clientv3.LeaseLeasesResponse{}, nil
}

func (fakeLease) KeepAlive(ctx context.Context, id clientv3.LeaseID) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	return make(<-chan *clientv3.LeaseKeepAliveResponse), nil
}
func (fakeLease) KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	return nil, errors.New("some error")
}

func (fakeLease) Close() error {
	return nil
}

type fakeKV struct{}

func (fakeKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return &clientv3.PutResponse{}, nil
}

func (fakeKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{}}}, nil
}

func (fakeKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return &clientv3.DeleteResponse{}, nil
}

func (fakeKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return &clientv3.CompactResponse{}, nil
}

func (fakeKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (fakeKV) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

func setup() {
	common.FConfig.LocalAuth = &localauth.AuthConfig{}
	common.FConfig.AlarmConfig = &common.AlarmConfig{
		RouterEtcdLeaseDetectInterval: 1,
		RouterEtcdLeaseTimeout:        2,
	}
}

func TestGetFunctionTaskRegisterInfoKey(t *testing.T) {
	var regInfo = RegisterInfo{
		FuncName:        "function-task",
		NodeID:          "els-211-129",
		FunctionVersion: "$latest",
		Tenant:          "0",
		WorkerInstance:  WorkerInstance{},
	}
	etcdRegister := &etcdRegister{
		Info:             regInfo,
		stop:             nil,
		routerEtcdClient: nil,
	}
	key := etcdRegister.getFunctionTaskRegisterInfoKey()
	assert.Equal(t, key, "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/els-211-129")
}

func TestRegistry(t *testing.T) {
	setup()
	patches := gomonkey.NewPatches()
	defer func() {
		patches.Reset()
	}()

	client := &clientv3.Client{Lease: fakeLease{}, KV: fakeKV{}}
	patches.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: client}
	})
	patches.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: client}
	})
	patches.ApplyFunc(utils.DeleteValueInEtcd, func(key string) error {
		return nil
	})
	patches.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}
		return &time.Ticker{C: ch}
	})
	patches.ApplyFunc((*time.Ticker).Reset, func(_ *time.Ticker, d time.Duration) {
		return
	})
	patches.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
		return nil
	})
	patches.ApplyFunc((*clientv3.Client).Ctx, func(_ *clientv3.Client) context.Context { return context.TODO() })
	patches.ApplyFunc((*clientv3.Client).Close, func(_ *clientv3.Client) error { return nil })
	patches.ApplyFunc((*alarm.Client).RegisterAlarmType,
		func(a *alarm.Client, almType alarm.Type) { return })
	patches.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
		ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
	})

	ch := make(chan struct{})
	i := NewRegistry(RegisterInfo{}, ch, nil)
	err := i.Register()
	assert.NoError(t, err)
	err = i.Unregister()
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)
	close(ch)
	time.Sleep(1 * time.Second)
}

func TestRegistryStateClientIsNil(t *testing.T) {
	setup()
	patches := gomonkey.NewPatches()
	defer func() {
		patches.Reset()
	}()

	register := &etcdRegister{caeRouterEtcdClient: &etcd3.EtcdClient{Client: &clientv3.Client{}}}
	register.routerEtcdClient = &etcd3.EtcdClient{Client: &clientv3.Client{}}
	register.stateClient = nil
	err := register.Register()
	assert.Error(t, err)
}

func TestRegistryCAERouter(t *testing.T) {
	setup()
	patches := gomonkey.NewPatches()
	defer func() {
		patches.Reset()
	}()

	register := &etcdRegister{caeRouterEtcdClient: &etcd3.EtcdClient{Client: &clientv3.Client{}}}
	register.routerEtcdClient = &etcd3.EtcdClient{Client: &clientv3.Client{}}
	register.stateClient = &etcd3.EtcdClient{Client: &clientv3.Client{}}

	patches.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
		return []byte("value"), nil
	})
	patches.ApplyFunc(common.IsCAERouterEtcdDefined, func() bool { return true })
	patches.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put", func(client *etcd3.EtcdClient,
		ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
		return nil
	})
	patches.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Grant", func(_ *etcd3.EtcdClient,
		ctx etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
		return 1, errors.New("error")
	})
	err := register.Register()
	assert.Error(t, err)
}

func TestRegistryFailed(t *testing.T) {
	setup()
	patches := gomonkey.NewPatches()
	defer func() {
		patches.Reset()
	}()
	register := &etcdRegister{routerEtcdClient: &etcd3.EtcdClient{}}
	err := register.Register()
	assert.Error(t, err)
	patches.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, errors.New("failed to marshal")
	})
	register.routerEtcdClient.Client = &clientv3.Client{}
	register.stateClient = &etcd3.EtcdClient{}
	err = register.Register()
	assert.Error(t, err)
}

func TestPutKey(t *testing.T) {
	setup()
	patches := gomonkey.NewPatches()
	defer func() {
		patches.Reset()
	}()
	patches.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
		return errors.New("failed to put")
	})
	patches.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
		ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
	})
	client := &clientv3.Client{Lease: fakeLease{}, KV: fakeKV{}}
	register := &etcdRegister{
		routerEtcdClient: &etcd3.EtcdClient{
			Client: client,
		},
		stateClient: &etcd3.EtcdClient{
			Client: client,
		},
		Info: RegisterInfo{
			WorkerInstance: WorkerInstance{},
		},
	}
	err := register.Register()
	assert.Error(t, err)
	stateLeaser := etcdLeaser{
		client:   register.stateClient,
		stop:     register.stop,
		key:      register.key,
		value:    register.value,
		etcdType: etcd3.State,
	}
	err = stateLeaser.putKeyWithLease()
	assert.Error(t, err)
	patches.ApplyFunc((*fakeLease).Grant, func(lease *fakeLease, ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
		return nil, errors.New("failed to grant")
	})
	err = register.Register()
	assert.Error(t, err)
	err = stateLeaser.putKeyWithLease()
	assert.Error(t, err)
}

func TestRevoke(t *testing.T) {
	setup()
	patches := gomonkey.NewPatches()
	defer func() {
		patches.Reset()
	}()
	patches.ApplyFunc(utils.DeleteValueInEtcd, func(key string) error {
		return errors.New("failed to delete")
	})
	patches.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
		ch := make(chan time.Time, 1)
		ch <- time.Time{}
		return &time.Ticker{C: ch}
	})
	patches.ApplyFunc((*time.Ticker).Reset, func(_ *time.Ticker, d time.Duration) {
		return
	})
	patches.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
		return nil
	})
	patches.ApplyFunc((*clientv3.Client).Ctx, func(_ *clientv3.Client) context.Context { return context.TODO() })
	patches.ApplyFunc((*clientv3.Client).Close, func(_ *clientv3.Client) error { return nil })
	patches.ApplyFunc((*fakeLease).Revoke, func(_ *fakeLease, ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
		return nil, errors.New("revoke error")
	})
	patches.ApplyFunc((*fakeKV).Get, func(_ *fakeKV, ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return nil, errors.New("failed to get")
	})
	patches.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
		ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return nil, errors.New("failed to get")
	})

	client := &clientv3.Client{Lease: fakeLease{}, KV: fakeKV{}}
	ch := make(chan struct{})
	register := &etcdRegister{
		routerEtcdClient: &etcd3.EtcdClient{
			Client: client,
		},
		stateClient: &etcd3.EtcdClient{
			Client: client,
		},
		Info: RegisterInfo{
			WorkerInstance: WorkerInstance{},
		},
		stop: ch,
	}
	err := register.Register()
	assert.NoError(t, err)
	patches.ApplyFunc((*fakeLease).Grant, func(lease *fakeLease, ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error) {
		return nil, errors.New("failed to grant")
	})
	err = register.Unregister()
	assert.Error(t, err)
	close(ch)
	time.Sleep(1 * time.Second)
}

func TestEtcdRegisterRevoke(t *testing.T) {
	convey.Convey("TestEtcdRegisterRevoke", t, func() {
		patches := gomonkey.NewPatches()
		defer func() {
			patches.Reset()
		}()
		patches.ApplyFunc(etcd3.CreateEtcdCtxInfoWithTimeout,
			func(ctx context.Context, duration time.Duration) etcd3.EtcdCtxInfo {
				return etcd3.EtcdCtxInfo{}
			})
		patches.ApplyFunc((*etcd3.EtcdClient).Revoke,
			func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, leaseID clientv3.LeaseID) error {
				return nil
			})

		client := &clientv3.Client{Lease: fakeLease{}, KV: fakeKV{}}
		ch := make(chan struct{})
		register := &etcdRegister{
			routerEtcdClient: &etcd3.EtcdClient{
				Client: client,
			},
			Info: RegisterInfo{
				WorkerInstance: WorkerInstance{},
			},
			stop: ch,
		}

		r := etcdLeaser{
			client: register.stateClient,
		}

		err := r.revoke()
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestEtcdRegisterCheckAlarm(t *testing.T) {
	convey.Convey("TestEtcdRegisterCheckAlarm", t, func() {
		patches := gomonkey.NewPatches()
		defer func() {
			patches.Reset()
		}()

		patches.ApplyFunc((*etcdLeaser).checkKeyExist,
			func(_ *etcdLeaser) error {
				return errors.New("error")
			})
		patches.ApplyFunc((*etcdLeaser).putKeyWithLease,
			func(_ *etcdLeaser) error {
				return errors.New("error")
			})

		client := &clientv3.Client{Lease: fakeLease{}, KV: fakeKV{}}
		ch := make(chan struct{})
		leaser := &etcdLeaser{
			client: &etcd3.EtcdClient{
				Client: client,
			},
			stop:    ch,
			monitor: etcd3.CreateLeaseMonitor(),
		}

		leaser.checkAlarm()
	})
}

func TestGeTValueFromEnv(t *testing.T) {
	defaultValue := 1
	key := "test-key"
	os.Setenv(key, "error number")
	defer os.Unsetenv(key)
	value := getValueFromEnv(key, defaultValue)
	assert.Equal(t, defaultValue, value)
	os.Setenv(key, "2")
	value = getValueFromEnv(key, defaultValue)
	assert.Equal(t, 2, value)
}

func TestUpdateLeaseInfo(t *testing.T) {
	UpdateLeaseInfo(10, 10)
	assert.Equal(t, 10, leaseTTL)
	assert.Equal(t, 10, renewTTL)
}

func TestMain(m *testing.M) {
	alarm.NewClient("testurl-test", constants.FunctionTask, nil)
	m.Run()
}
