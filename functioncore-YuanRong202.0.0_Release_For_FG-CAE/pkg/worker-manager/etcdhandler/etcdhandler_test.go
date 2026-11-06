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

package etcdhandler

import (
	"context"
	"encoding/json"
	"errors"
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

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	mockUtils "functioncore/test/utils"
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

func TestPutInstanceToETCD(t *testing.T) {
	type args struct {
		ctx      context.Context
		instance v1alpha1.WorkerInstance
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case1 succeed to put instance to etcd",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID: "001",
				BusinessID: "busid",
				TenantID:   "abcd",
				FuncName:   "0-counter-addone",
				Version:    "latest",
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			"case2 failed to unmarshal json",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID: "001",
				BusinessID: "busid",
				TenantID:   "abcd",
				FuncName:   "0-counter-addone",
				Version:    "latest",
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"case3 instanceKey.ParseFrom error",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID: "001/002",
				BusinessID: "busid",
				TenantID:   "abcd",
				FuncName:   "0-counter-addone",
				Version:    "latest",
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			"case4 etcd does not work ,time out",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID: "001",
				BusinessID: "busid",
				TenantID:   "abcd",
				FuncName:   "0-counter-addone",
				Version:    "latest",
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(OperateETCDWithRetry,
						func(ctx context.Context, path, value, operate string) error {
							return errors.New("timed out waiting for the condition")
						}),
				})
				return patches
			},
		},
		{
			"case5 succeed to put instance to cae router etcd",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID:   "001",
				BusinessID:   "busid",
				TenantID:     "abcd",
				FuncName:     "0-counter-addone",
				Version:      "latest",
				BusinessType: constants.BusinessTypeCAE,
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			PutInstanceToETCD(tt.args.ctx, tt.args.instance)
		})
		patches.ResetAll()
	}
}

func TestDeleteInstanceFromETCD(t *testing.T) {
	type args struct {
		ctx      context.Context
		instance v1alpha1.WorkerInstance
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case1 succeed to delete instance in etcd",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID: "001",
				BusinessID: "busid",
				TenantID:   "abcd",
				FuncName:   "0-counter-addone",
				Version:    "latest",
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			"case1 delete instance in etcd failed",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID: "001",
				BusinessID: "busid",
				TenantID:   "abcd",
				FuncName:   "0-counter-addone",
				Version:    "latest",
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) error {
							return errors.New("delete instance in etcd failed")
						}),
				})
				return patches
			},
		},
		{
			"case1 succeed to delete instance in CAE ROUTER etcd",
			args{ctx: context.TODO(), instance: v1alpha1.WorkerInstance{
				InstanceID:   "001",
				BusinessID:   "busid",
				TenantID:     "abcd",
				FuncName:     "0-counter-addone",
				Version:      "latest",
				BusinessType: constants.BusinessTypeCAE,
			}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			DeleteInstanceFromETCD(tt.args.ctx, tt.args.instance)
		})
		patches.ResetAll()
	}
}

func TestDeleteInstanceByPrefixFromETCD(t *testing.T) {
	type args struct {
		ctx     context.Context
		service *faasfunction.FunctionService
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to delete instance by prefix in etcd",
			args{
				ctx: context.TODO(),
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessID:      "busid",
							TenantID:        "abcd",
							FunctionName:    "0-counter-addone",
							FunctionVersion: "latest",
						},
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			}},
		{"case2 succeed to delete instance by prefix in cae router etcd",
			args{
				ctx: context.TODO(),
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessID:      "busid",
							TenantID:        "abcd",
							FunctionName:    "0-counter-addone",
							FunctionVersion: "latest",
							BusinessType:    constants.BusinessTypeCAE,
						},
					},
				},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			DeleteInstanceByPrefixFromETCD(tt.args.ctx, tt.args.service)
		})
		patches.ResetAll()
	}
}

func TestPutSNStateWorkerFromETCD(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
			return nil
		}),
		gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		}),
	})
	defer patches.ResetAll()
	requestInfo := &config.RequestInfo{}
	PutSNStateWorkerFromETCD(requestInfo, v1alpha1.WorkerInstance{
		InstanceID: "001",
		BusinessID: "busid",
		TenantID:   "abcd",
		FuncName:   "0-counter-addone",
		Version:    "latest",
	})
}

func Test_putInstanceInfoToEtcd(t *testing.T) {
	instanceKey := "/sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d"
	type args struct {
		instanceKey string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1 succeed to putInstanceInfoToEtcd",
			args: args{
				instanceKey: instanceKey,
			},
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
						return nil
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Grant, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
						return 1231, nil
					}),
				})
				return patches
			},
		},
		{
			name: "case2 caeRouterClient is nil",
			args: args{
				instanceKey: instanceKey,
			},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
		},
		{
			name: "case3 grant error",
			args: args{
				instanceKey: instanceKey,
			},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Grant, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
						return 0, errors.New("test error")
					}),
				})
				return patches
			},
		},
		{
			name: "case4 put error",
			args: args{
				instanceKey: instanceKey,
			},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
						return nil
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Grant, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, ttl int64) (clientv3.LeaseID, error) {
						return 1231, errors.New("test error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			if err := putInstanceInfoToEtcd(tt.args.instanceKey); (err != nil) != tt.wantErr {
				t.Errorf("putInstanceInfoToEtcd() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		patches.ResetAll()
	}
}

func TestRegisterToCAERouterETCD(t *testing.T) {
	convey.Convey("RegisterToCAERouterETCD fail", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(getWorkerManagerInstanceKey, func() (string, error) {
				return "", nil
			}),
			gomonkey.ApplyFunc(putInstanceInfoToEtcd, func(instanceKey string) error {
				return errors.New("fail")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		stopChan := make(chan struct{})
		RegisterToCAERouterETCD(stopChan)
	})
}

func Test_getWorkerManagerInstanceKey(t *testing.T) {
	tests := []struct {
		name        string
		want        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:    "case test valid WorkerManagerInstanceKey",
			want:    "/sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d",
			wantErr: false,
			patchesFunc: mockUtils.PatchesFunc(func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "CLUSTER_ID" {
							return "cluster001"
						}
						if key == "NODE_IP" {
							return "10.247.13.123"
						}
						if key == "POD_NAME" {
							return "worker-manager-6bc89d77fb-58w2d"
						}
						return ""
					}),
				})
				return patches
			}),
		},
		{
			name:    "case test invalid WorkerManagerInstanceKey2",
			want:    "",
			wantErr: true,
			patchesFunc: mockUtils.PatchesFunc(func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "CLUSTER_ID" {
							return "cluster001"
						}
						if key == "NODE_IP" {
							return ""
						}
						if key == "POD_NAME" {
							return "worker-manager-6bc89d77fb-58w2d"
						}
						return ""
					}),
				})
				return patches
			}),
		},
		{
			name:    "case test invalid WorkerManagerInstanceKey3",
			want:    "",
			wantErr: true,
			patchesFunc: mockUtils.PatchesFunc(func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						if key == "CLUSTER_ID" {
							return "cluster001"
						}
						if key == "NODE_IP" {
							return "10.247.13.123"
						}
						if key == "POD_NAME" {
							return ""
						}
						return ""
					}),
				})
				return patches
			}),
		},
		{
			name:    "case test invalid WorkerManagerInstanceKey",
			want:    "",
			wantErr: true,
			patchesFunc: mockUtils.PatchesFunc(func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			}),
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			got, err := getWorkerManagerInstanceKey()
			if (err != nil) != tt.wantErr {
				t.Errorf("getWorkerManagerInstanceKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("getWorkerManagerInstanceKey() got = %v, want %v", got, tt.want)
			}
		})

		patches.ResetAll()
	}
}

func Test_isKeyExist(t *testing.T) {
	instanceKey := "/sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d"
	convey.Convey("Test isKeyExist", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{
				Client: &clientv3.Client{},
			}
		})
		defer patch.Reset()
		convey.Convey("get cae router etcd key return empty", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
			})
			defer patch.Reset()
			existed := isKeyExist(instanceKey)
			convey.So(existed, convey.ShouldBeFalse)
		})

		convey.Convey("succeed to get cae router etcd key", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("key")}}}, nil
			})
			defer patch.Reset()
			existed := isKeyExist(instanceKey)
			convey.So(existed, convey.ShouldBeTrue)
		})

		convey.Convey("failed to get cae router etcd key", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponseWithSerializable", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return nil, errors.New("failed")
			})
			defer patch.Reset()
			existed := isKeyExist(instanceKey)
			convey.So(existed, convey.ShouldBeFalse)
		})
	})
}

func Test_isKeyExist_etcdNil(t *testing.T) {
	instanceKey := "/sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d"
	convey.Convey("Test isKeyExist with etcd nil", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		defer patch.Reset()
		existed := isKeyExist(instanceKey)
		convey.So(existed, convey.ShouldBeFalse)
	})
}

func Test_startRefreshLeaseJob(t *testing.T) {
	instanceKey := "/sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d"
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
	patches.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{KV: kv, Lease: &mockLease{}}}
	})
	defer func() {
		patches.Reset()
	}()
	refreshInterval = 1 * time.Millisecond

	// stop chan is nil, will not trigger refresh
	go startRefreshLeaseJob(instanceKey, nil)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint32(0), atomic.LoadUint32(&kv.get))
	assert.Equal(t, uint32(0), atomic.LoadUint32(&kv.put))
	assert.Equal(t, uint32(0), atomic.LoadUint32(&kv.do))

	stopCh := make(chan struct{})
	go startRefreshLeaseJob(instanceKey, stopCh)
	time.Sleep(100 * time.Millisecond)
	assert.NotEqual(t, uint32(0), atomic.LoadUint32(&kv.get))
	close(stopCh)
	time.Sleep(1 * time.Second)
}

func TestOperateCaeRouterETCD(t *testing.T) {
	client := &etcd3.EtcdClient{Client: &clientv3.Client{}}
	convey.Convey("OperateCaeRouterETCD put operation", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return client
			}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		OperateCaeRouterETCD(context.Background(), "path", "value", PutOperation)
	})

	convey.Convey("OperateCaeRouterETCD del operation", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return client
			}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		OperateCaeRouterETCD(context.Background(), "path", "value", DelOperation)
	})

	convey.Convey("OperateCaeRouterETCD unknown operation", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return client
			}),
			gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		OperateCaeRouterETCD(context.Background(), "path", "value", "unknown")
	})
}

func Test_putValueIntoMetaETCD(t *testing.T) {
	type args struct {
		path     string
		value    string
		instance v1alpha1.WorkerInstance
	}
	actulErr := false
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case1 stateClient is nil",
			args: args{
				"path",
				"value",
				v1alpha1.WorkerInstance{},
			},
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
						actulErr = true
						return nil
					}),
				})
				return patches
			},
		},
		{
			name: "case2 put etcd error",
			args: args{
				"path",
				"value",
				v1alpha1.WorkerInstance{},
			},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
						actulErr = true
						return errors.New("Test error")
					}),
				})
				return patches
			},
		},
		{
			name: "case3 put etcd success",
			args: args{
				"path",
				"value",
				v1alpha1.WorkerInstance{},
			},
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key, val string, opts ...clientv3.OpOption) error {
						actulErr = true
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			actulErr = false
			putValueIntoMetaETCD(tt.args.path, tt.args.value, tt.args.instance)
			if tt.wantErr != actulErr {
				t.Errorf("putValueIntoMetaETCD %s error", tt.name)
			}
		})
		patches.ResetAll()
	}
}
