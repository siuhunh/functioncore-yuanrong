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
package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/cache-manager/pb"
	"functioncore/pkg/cache-manager/pkg/server"
	"functioncore/pkg/cache-manager/pkg/status"
	utils2 "functioncore/pkg/cache-manager/pkg/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/types"
	"functioncore/test/utils"
)

func Test_readAll(t *testing.T) {
	type args struct {
		path string
	}
	var a args
	a.path = "/tmp/cache-manager/cache"
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"case1", a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readAll(tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cleanPersistence(t *testing.T) {
	patch := gomonkey.ApplyFunc(status.Get, func() status.Status { return status.Persistence })
	defer patch.Reset()
	type args struct {
		objectID string
		isLayer  bool
	}
	var a args
	a.objectID = "a"
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanPersistence(tt.args.objectID, tt.args.isLayer)
		})
	}
}

func TestWatcherUserFunction(t *testing.T) {
	client := &etcd3.EtcdClient{}
	resp := &clientv3.GetResponse{}
	kv := []*mvccpb.KeyValue{}
	resp.Kvs = kv
	c := &CacheHandler{Deployer: nil}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
		gomonkey.ApplyFunc(server.NewDeployer,
			func(*config.CacheManagerConfig) (server.Deployer, error) {
				return nil, nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(client), "GetResponse",
			func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return resp, nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Watch",
			func(_ *etcd3.EtcdClient, _ context.Context, _ string, _ int64, _ bool) *etcd3.EtcdWatchChan {
				return nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(c), "RunWatcher",
			func(_ *CacheHandler, _ *etcd3.EtcdWatchChan, _ *etcd3.EtcdClient, nil <-chan struct{}) {
				return
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		conf   *config.CacheManagerConfig
		stopCh <-chan struct{}
	}
	var a args
	conf := &config.CacheManagerConfig{
		HTTPSConfig: nil,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{
			Enable: true,
		},
	}
	a.conf = conf

	var b args
	confs := &config.CacheManagerConfig{
		HTTPSConfig: nil,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{
			Enable:          true,
			PersistencePath: "/tmp",
		},
	}
	b.conf = confs
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, false},
		{"case2", b, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WatcherUserFunction(tt.args.conf, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("WatcherUserFunction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWatcherUserFunction_3(t *testing.T) {
	client := &etcd3.EtcdClient{}
	resp := &clientv3.GetResponse{}
	kv := []*mvccpb.KeyValue{}
	resp.Kvs = kv
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
		gomonkey.ApplyFunc(server.NewDeployer,
			func(*config.CacheManagerConfig) (server.Deployer, error) {
				return nil, errors.New("aaa")
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(client), "GetResponse",
			func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return resp, nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		conf   *config.CacheManagerConfig
		stopCh <-chan struct{}
	}
	var a args
	conf := &config.CacheManagerConfig{
		HTTPSConfig: nil,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{
			Enable: true,
		},
	}
	a.conf = conf
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WatcherUserFunction(tt.args.conf, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("WatcherUserFunction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCacheHandler_parseUserFunctionEvent(t *testing.T) {
	type fields struct {
		Deployer server.Deployer
	}
	type args struct {
		event *etcd3.Event
	}
	var f fields
	event1 := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	var a args
	a.event = event1

	event2 := &etcd3.Event{
		Type:      1,
		Key:       "",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	var b args
	b.event = event2

	event3 := &etcd3.Event{
		Type:      105,
		Key:       "",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	var c args
	c.event = event3
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
		{"case2", f, b},
		{"case2", f, c},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheHandler{
				Deployer: tt.fields.Deployer,
			}
			c.parseUserFunctionEvent(tt.args.event)
		})
	}
}

func TestCacheHandler_handlePutEvent(t *testing.T) {
	type fields struct {
		Deployer server.Deployer
	}
	type args struct {
		event *etcd3.Event
	}
	var f fields
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), nil
			}),
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	var a args
	event := &etcd3.Event{
		Type:      0,
		Key:       "aaa",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	a.event = event
	var b args
	value := config.FunctionMetaInfo{
		FuncMetaData: config.FuncMetaData{
			CodeSize: limitCodeSize + 1,
		},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	values, _ := json.Marshal(value)
	event2 := &etcd3.Event{
		Type:      0,
		Key:       "aaa",
		Value:     values,
		PrevValue: nil,
		Rev:       0,
	}
	b.event = event2
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
		{"case1", f, b},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheHandler{
				Deployer: tt.fields.Deployer,
			}
			c.handlePutEvent(tt.args.event)
		})
	}
}

func TestCacheHandler_handleDeleteEvent(t *testing.T) {
	server.LruCacheMap.AddCache("aaa", server.CodeCache{
		CodeBuffer: []byte{'a'},
		Checksum:   "aaa",
	}, int64(len([]byte{'a'})))
	type fields struct {
		Deployer server.Deployer
	}
	type args struct {
		event *etcd3.Event
	}
	var f fields
	var a args
	event := &etcd3.Event{
		Type:      0,
		Key:       "aaa",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	a.event = event
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheHandler{
				Deployer: tt.fields.Deployer,
			}
			c.handleDeleteEvent(tt.args.event)
		})
	}
}

func TestCacheHandler_handleDeleteEvent2(t *testing.T) {
	server.LruCacheMap.AddCache("aaa", server.CodeCache{
		CodeBuffer: []byte{'a'},
		Checksum:   "aaa",
	}, int64(len([]byte{'a'})))
	type fields struct {
		Deployer server.Deployer
	}
	type args struct {
		event *etcd3.Event
	}
	var f fields
	var a args
	info := config.FunctionMetaInfo{
		FuncMetaData: config.FuncMetaData{
			FunctionVersionURN: "test123",
			CodeSha512:         "sha512",
			Layers: []*types.Layer{
				{
					ObjectID: "objectid-layer3",
					Name:     "layer3",
					ETag:     "33",
				},
			},
		},
	}
	metaData, _ := json.Marshal(info)
	event := &etcd3.Event{
		Type:      0,
		Key:       "aaa",
		Value:     nil,
		PrevValue: metaData,
		Rev:       0,
	}
	a.event = event
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheHandler{
				Deployer: tt.fields.Deployer,
			}
			c.handleDeleteEvent(tt.args.event)
		})
	}
}

func Test_extractCache(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(ioutil.ReadDir,
			func(_ string) ([]os.FileInfo, error) {
				return []os.FileInfo{}, nil
			}),
		gomonkey.ApplyFunc(readAll,
			func(_ string) []string {
				return []string{"aaa"}
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name string
	}{
		{"case1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		extractCache()
	}
}

func Test_extractCache_2(t *testing.T) {
	cacheCode := config.CacheCode{}
	cacheCode.TimeStamp = time.Now()
	cache, err := json.Marshal(cacheCode)
	if err != nil {
		t.Error(err)
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(ioutil.ReadDir,
			func(_ string) ([]os.FileInfo, error) {
				return []os.FileInfo{}, nil
			}),
		gomonkey.ApplyFunc(readAll,
			func(_ string) []string {
				return []string{"aaa"}
			}),
		gomonkey.ApplyFunc(ioutil.ReadFile,
			func(_ string) ([]byte, error) {
				return []byte{5}, nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return cache, nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name string
	}{
		{"case1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		extractCache()
	}
}

func TestCacheHandler_RunWatcher(t *testing.T) {
	type fields struct {
		Deployer server.Deployer
	}
	type args struct {
		watchChan *etcd3.EtcdWatchChan
		stopCh    <-chan struct{}
	}
	var f fields
	var a args
	channel := &etcd3.EtcdWatchChan{}
	stop := make(<-chan struct{})
	a.stopCh = stop
	a.watchChan = channel
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := CacheHandler{
				Deployer: tt.fields.Deployer,
			}
			go c.RunWatcher(tt.args.watchChan, nil, nil)
		})
	}
}

func Test_RunWatcher(t *testing.T) {
	var called bool
	type args struct {
		conf   *etcd3.EtcdWatchChan
		stopCh chan struct{}
		client *etcd3.EtcdClient
	}
	var a args

	conf := &etcd3.EtcdWatchChan{}
	a.conf = conf
	done := make(chan struct{})
	a.stopCh = done
	c := &CacheHandler{}
	tests := []struct {
		name       string
		args       args
		calledWant bool
	}{
		{
			name:       "test1",
			args:       a,
			calledWant: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			if tt.args.stopCh != nil {
				go func() {
					close(tt.args.stopCh)
				}()
			}
			c.RunWatcher(tt.args.conf, tt.args.client, tt.args.stopCh)
			if called != tt.calledWant {
				t.Error(" error")
			}
		})
	}
}

func Test_extractCache_3(t *testing.T) {
	var called bool
	cacheCode := config.CacheCode{}
	cacheCode.TimeStamp = time.Now()
	cache, err := json.Marshal(cacheCode)
	if err != nil {
		t.Error(err)
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(ioutil.ReadDir,
			func(_ string) ([]os.FileInfo, error) {
				fmt.Println("2323232")
				return []os.FileInfo{}, nil
			}),
		gomonkey.ApplyFunc(readAll,
			func(_ string) []string {
				return []string{"aaa"}
			}),
		gomonkey.ApplyFunc(ioutil.ReadFile,
			func(_ string) ([]byte, error) {
				return []byte{5}, nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return cache, nil
			}),
		gomonkey.ApplyFunc(time.Since,
			func(t time.Time) time.Duration {
				called = true
				return 361 * time.Hour
			}),
		gomonkey.ApplyFunc(os.Remove,
			func(name string) error {
				return errors.New("remove error")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name  string
		want  bool
		Setup func(t *testing.T)
	}{
		{name: "test",
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		called = false
		extractCache()
		if called != tt.want {
			t.Error(" error")
		}
	}
}

func TestWatcherUserFunction_5(t *testing.T) {
	client := &etcd3.EtcdClient{}
	resp := &clientv3.GetResponse{}
	kv := []*mvccpb.KeyValue{
		{
			Key:     nil,
			Version: 10,
		},
	}
	resp.Kvs = kv
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "GetResponse",
			func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, _ string, _ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				return resp, nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		conf   *config.CacheManagerConfig
		stopCh <-chan struct{}
	}
	var a args
	conf := &config.CacheManagerConfig{
		HTTPSConfig: nil,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{
			Enable: true,
		},
	}
	a.conf = conf
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := WatcherUserFunction(tt.args.conf, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("WatcherUserFunction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_needUpdate(t *testing.T) {
	type args struct {
		objectID string
		urn      string
		checksum string
		isLayer  bool
	}
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		arg         args
		needUpdate  bool
	}{
		{
			name: "file not exists",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, os.ErrNotExist
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: true,
		},
		{
			name: "read file failed",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return nil, errors.New("failed to read file")
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: true,
		},
		{
			name: "decrypt failed",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return nil, errors.New("failed to decrypt")
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: true,
		},
		{
			name: "unmarshal failed",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: true,
		},
		{
			name: "check sum mismatched",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
					gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						return nil
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: true,
		},
		{
			name: "check sum mismatched",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
					gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						return nil
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: true,
		},
		{
			name: "check sum ok",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						if mode != 0600 {
							return errors.New("should change file mod to 0600")
						}
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
					gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						m.(*pb.CacheCode).Checksum = "123"
						return nil
					}),
				})
				return patches
			},
			arg:        args{objectID: "123", urn: "123", checksum: "123", isLayer: false},
			needUpdate: false,
		},
	}

	server.LruCacheMap = utils2.NewLru(&sync.Map{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &CacheHandler{}
			needUpdate := c.needUpdateFile(tt.arg.objectID, tt.arg.urn, tt.arg.checksum, tt.arg.isLayer)
			assert.Equal(t, tt.needUpdate, needUpdate)
			patches.ResetAll()
		})
	}
}

type fakeDeployer struct {
	wantError bool
}

func (f *fakeDeployer) Deploy(info *config.FunctionMetaInfo) error {
	if !f.wantError {
		return nil
	}
	return errors.New("failed to deploy")
}

func Test_handleSyncPut(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
			return nil
		}),
	}
	defer func() {
		server.EvsEnable = false
		for i := range patches {
			patches[i].Reset()
		}
	}()

	tests := []struct {
		name      string
		deployer  server.Deployer
		evsEnable bool
	}{
		{
			name: "evs not enable",
		},
		{
			name:      "evs enable, deploy failed",
			deployer:  &fakeDeployer{wantError: true},
			evsEnable: true,
		},
		{
			name:      "evs enable, deploy success",
			deployer:  &fakeDeployer{wantError: false},
			evsEnable: true,
		},
	}

	server.LruCacheMap = utils2.NewLru(&sync.Map{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server.EvsEnable = tt.evsEnable
			c := &CacheHandler{
				Deployer: tt.deployer,
			}
			c.handleSyncPut(&etcd3.Event{})
		})
	}
}

func Test_handleSynced(t *testing.T) {
	var deleted []string

	tests := []struct {
		name       string
		evsEnable  bool
		syncedFile []string
		allFile    []string

		wanted []string
	}{
		{
			name:       "evs disabled",
			syncedFile: []string{"a", "b"},
			allFile:    []string{"a", "b", "c", "d"},
		},
	}

	defer func() {
		server.EvsEnable = false
	}()

	for _, tt := range tests {
		deleted = nil
		c := &CacheHandler{}
		for i := range tt.syncedFile {
			c.syncedObj.Store(tt.syncedFile[i], struct{}{})
		}
		c.handleSynced()
		assert.Equal(t, tt.wanted, deleted)
	}
}
func Test_loadFromDisk(t *testing.T) {
	tests := []struct {
		name        string
		key         interface{}
		isLayer     bool
		patchesFunc utils.PatchesFunc

		hasError bool
	}{
		{
			name: "key type error",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			key:      []byte("key error"),
			hasError: true,
		},
		{
			name: "read file failed",
			key:  "obj",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return nil, errors.New("failed to read file")
					}),
				})
				return patches
			},
			hasError: true,
		},
		{
			name: "decrypt failed",
			key:  "obj",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return nil, errors.New("failed to decrypt")
					}),
				})
				return patches
			},
			hasError: true,
		},
		{
			name: "unmarshal failed",
			key:  "obj",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
				})
				return patches
			},
			hasError: true,
		},
		{
			name: "load success",
			key:  "obj",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
					gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						return nil
					}),
				})
				return patches
			},
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, _, err := loadFromDisk(tt.key, tt.isLayer)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
			}
			patches.ResetAll()
		})
	}
}

func TestCacheHandler_startRunWatcher(t *testing.T) {
	type fields struct {
		Deployer   server.Deployer
		etcdSynced uint32
		syncedObj  sync.Map
	}
	type args struct {
		client *etcd3.EtcdClient
		stopCh chan struct{}
	}
	var f fields
	var a args
	a.stopCh = make(chan struct{})
	a.client = &etcd3.EtcdClient{Client: nil}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &CacheHandler{
				Deployer:   tt.fields.Deployer,
				etcdSynced: tt.fields.etcdSynced,
				syncedObj:  tt.fields.syncedObj,
			}
			go c.startRunWatcher(a.client, tt.args.stopCh)
			time.Sleep(5 * time.Second)
			close(tt.args.stopCh)
		})
	}
}

func Test_needLayersUpdate(t *testing.T) {
	type args struct {
		info *config.FunctionMetaInfo
	}
	type fields struct {
		Deployer server.Deployer
	}
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		arg         args
		field       fields
		needUpdate  bool
	}{
		{
			name: "needLayersUpdate false",
			arg: args{
				info: &config.FunctionMetaInfo{
					FuncMetaData: config.FuncMetaData{
						Layers: []*types.Layer{
							nil, nil,
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{})
				return patches
			},
			needUpdate: false,
		},
		{
			name: "needLayersUpdate false",
			arg: args{
				info: &config.FunctionMetaInfo{
					FuncMetaData: config.FuncMetaData{
						Layers: []*types.Layer{
							{
								ObjectID: "objectid-layer3",
								Name:     "layer3",
								ETag:     "33",
							},
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
					gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						m.(*pb.CacheCode).Checksum = "33"
						return nil
					}),
				})
				return patches
			},
			needUpdate: false,
		},
		{
			name: "needLayersUpdate true",
			arg: args{
				info: &config.FunctionMetaInfo{
					FuncMetaData: config.FuncMetaData{
						Layers: []*types.Layer{
							{
								ObjectID: "objectid-layer3",
								Name:     "layer3",
								ETag:     "33",
							},
							{
								ObjectID: "objectid-layer4",
								Name:     "layer4",
								ETag:     "44",
							},
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
						return nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("content"), nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte(src), nil
					}),
					gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
						m.(*pb.CacheCode).Checksum = "33"
						return nil
					}),
				})
				return patches
			},
			needUpdate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &CacheHandler{
				Deployer: tt.field.Deployer,
			}
			needUpdate := c.needLayersUpdate(tt.arg.info)
			assert.Equal(t, tt.needUpdate, needUpdate)
			patches.ResetAll()
		})
	}
}
