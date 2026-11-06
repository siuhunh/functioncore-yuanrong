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

package etcd3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	commontls "functioncore/pkg/common/tls"
	tools "functioncore/pkg/common/utils"
)

var defaultPatches = map[reflect.Value]reflect.Value{}

func TestMain(m *testing.M) {
	addFunc := func(target, double interface{}) {
		t := reflect.ValueOf(target)
		d := reflect.ValueOf(double)
		defaultPatches[t] = d
	}
	addFunc(clientv3.New, func(clientv3.Config) (*clientv3.Client, error) {
		return &clientv3.Client{}, nil
	})
	addFunc(commontls.GetX509CACertPool, func(_ string) (caCertPool *x509.CertPool, err error) {
		return nil, nil
	})
	addFunc(commontls.LoadServerTLSCertificate, func(_ string, _ string, _ string, _ bool,
	) ([]tls.Certificate, error) {
		return nil, nil
	})

	code := m.Run()
	os.Exit(code)
}

type Monkey struct {
	patches map[reflect.Value]reflect.Value
	t       *testing.T
	kv      *fakeKV
	watcher *fakeWatcher
}

func NewMonkey(t *testing.T) *Monkey {
	patches := make(map[reflect.Value]reflect.Value, len(defaultPatches))
	for k, v := range defaultPatches {
		patches[k] = v
	}
	return &Monkey{
		patches: patches,
		t:       t,
		kv:      &fakeKV{t: t},
		watcher: &fakeWatcher{t: t},
	}
}

func (m *Monkey) AddFunc(target, double interface{}) {
	t := reflect.ValueOf(target)
	d := reflect.ValueOf(double)
	m.patches[t] = d
}

func (m *Monkey) AddSequence(asserters ...Asserter) {
	for _, asserter := range asserters {
		asserter.visit(m)
	}

	m.AddFunc(clientv3.NewKV, func(_ *clientv3.Client) clientv3.KV {
		return m.kv
	})
	m.AddFunc(clientv3.NewWatcher, func(_ *clientv3.Client) clientv3.Watcher {
		return m.watcher
	})
}

func (m *Monkey) Apply() *gomonkey.Patches {
	patches := gomonkey.NewPatches()
	for target, double := range m.patches {
		patches.ApplyCore(target, double)
	}
	return patches
}

type Asserter interface {
	visit(m *Monkey)
}

type PutAsserter struct {
	key, val *string
	res      *clientv3.PutResponse
	err      error
}

var _ Asserter = (*PutAsserter)(nil)

func NewPutAsserter() *PutAsserter { return &PutAsserter{} }

func (a *PutAsserter) Assert(key, val string) *PutAsserter {
	a.key = &key
	a.val = &val
	return a
}

func (a *PutAsserter) OutputResult(res *clientv3.PutResponse) *PutAsserter {
	a.res = res
	return a
}

func (a *PutAsserter) OutputError(err error) *PutAsserter {
	a.err = err
	return a
}

func (a *PutAsserter) visit(m *Monkey) {
	m.kv.functions = append(m.kv.functions, func(_ context.Context, key string, val string, opts ...clientv3.OpOption,
	) (*clientv3.PutResponse, error) {
		if a.key != nil {
			assert.Equal(m.t, *a.key, key)
		}
		if a.val != nil {
			assert.Equal(m.t, *a.key, key)
		}
		return a.res, a.err
	})
}

type GetAsserter struct {
	key *string
	res *clientv3.GetResponse
	err error
}

var _ Asserter = (*GetAsserter)(nil)

func NewGetAsserter() *GetAsserter { return &GetAsserter{} }

func (a *GetAsserter) Assert(key string) *GetAsserter {
	a.key = &key
	return a
}

func (a *GetAsserter) OutputResult(res *clientv3.GetResponse) *GetAsserter {
	a.res = res
	return a
}

func (a *GetAsserter) OutputError(err error) *GetAsserter {
	a.err = err
	return a
}

func (a *GetAsserter) visit(m *Monkey) {
	m.kv.functions = append(m.kv.functions, func(_ context.Context, key string, opts ...clientv3.OpOption,
	) (*clientv3.GetResponse, error) {
		if a.key != nil {
			assert.Equal(m.t, *a.key, key)
		}
		return a.res, a.err
	})
}

type DeleteAsserter struct {
	key *string
	res *clientv3.DeleteResponse
	err error
}

var _ Asserter = (*DeleteAsserter)(nil)

func NewDeleteAsserter() *DeleteAsserter { return &DeleteAsserter{} }

func (a *DeleteAsserter) Assert(key string) *DeleteAsserter {
	a.key = &key
	return a
}

func (a *DeleteAsserter) OutputResult(res *clientv3.DeleteResponse) *DeleteAsserter {
	a.res = res
	return a
}

func (a *DeleteAsserter) OutputError(err error) *DeleteAsserter {
	a.err = err
	return a
}

func (a *DeleteAsserter) visit(m *Monkey) {
	m.kv.functions = append(m.kv.functions, func(_ context.Context, key string, opts ...clientv3.OpOption,
	) (*clientv3.DeleteResponse, error) {
		if a.key != nil {
			assert.Equal(m.t, *a.key, key)
		}
		return a.res, a.err
	})
}

type WatchAsserter struct {
	key *string
	ch  chan clientv3.WatchResponse
}

var _ Asserter = (*WatchAsserter)(nil)

func NewWatchAsserter() *WatchAsserter {
	return &WatchAsserter{}
}

func (a *WatchAsserter) Assert(key string) *WatchAsserter {
	a.key = &key
	return a
}

func (a *WatchAsserter) OutputChan(ch chan clientv3.WatchResponse) *WatchAsserter {
	a.ch = ch
	return a
}

func (a *WatchAsserter) visit(m *Monkey) {
	m.watcher.functions = append(m.watcher.functions, func(_ context.Context, key string, opts ...clientv3.OpOption,
	) clientv3.WatchChan {
		if a.key != nil {
			assert.Equal(m.t, *a.key, key)
		}
		return a.ch
	})
}

type fakeKV struct {
	t         *testing.T
	idx       int
	functions []interface{}
}

var _ clientv3.KV = (*fakeKV)(nil)

func (kv *fakeKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption,
) (*clientv3.PutResponse, error) {
	if kv.idx >= len(kv.functions) {
		kv.t.Fatal("got more calls than expected")
	}
	functionIf := kv.functions[kv.idx]
	function, ok := functionIf.(func(context.Context, string, string, ...clientv3.OpOption,
	) (*clientv3.PutResponse, error))
	if !ok {
		kv.t.Fatalf("expect a kv.Put call, got %T", functionIf)
	}
	kv.idx++
	return function(ctx, key, val, opts...)
}

func (kv *fakeKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption,
) (*clientv3.GetResponse, error) {
	if kv.idx >= len(kv.functions) {
		kv.t.Fatal("got more calls than expected")
	}
	functionIf := kv.functions[kv.idx]
	function, ok := functionIf.(func(context.Context, string, ...clientv3.OpOption,
	) (*clientv3.GetResponse, error))
	if !ok {
		kv.t.Fatalf("expect a kv.Get call, got %T", functionIf)
	}
	kv.idx++
	return function(ctx, key, opts...)
}

func (kv *fakeKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption,
) (*clientv3.DeleteResponse, error) {
	if kv.idx >= len(kv.functions) {
		kv.t.Fatal("got more calls than expected")
	}
	functionIf := kv.functions[kv.idx]
	function, ok := functionIf.(func(context.Context, string, ...clientv3.OpOption,
	) (*clientv3.DeleteResponse, error))
	if !ok {
		kv.t.Fatalf("expect a kv.Delete call, got %T", functionIf)
	}
	kv.idx++
	return function(ctx, key, opts...)
}

func (kv *fakeKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption,
) (*clientv3.CompactResponse, error) {
	return nil, nil
}

func (kv *fakeKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}

func (kv *fakeKV) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

type fakeWatcher struct {
	t         *testing.T
	idx       int
	functions []interface{}
}

var _ clientv3.Watcher = (*fakeWatcher)(nil)

func (w *fakeWatcher) Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	if w.idx >= len(w.functions) {
		w.t.Fatal("got more calls than expected")
	}
	functionIf := w.functions[w.idx]
	function, ok := functionIf.(func(context.Context, string, ...clientv3.OpOption,
	) clientv3.WatchChan)
	if !ok {
		w.t.Fatalf("expect a watcher.Watch call, got %T", functionIf)
	}
	w.idx++
	return function(ctx, key, opts...)
}

func (w *fakeWatcher) RequestProgress(ctx context.Context) error {
	return nil
}

func (w *fakeWatcher) Close() error {
	return nil
}

func TestEtcdWatcherSimple(t *testing.T) {
	myPutError := errors.New("my put error")
	myGetResult := &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
		{Value: []byte("aa")},
		{Value: []byte("bbb")},
	}}
	myGetError := errors.New("my get error")
	myDeleteError := errors.New("my delete error")

	monkey := NewMonkey(t)
	monkey.AddSequence(
		NewPutAsserter().Assert("apple-key", "apple-value"),
		NewPutAsserter().Assert("bear-key", "bear-value").OutputError(myPutError),
		NewGetAsserter().Assert("cat-key").OutputResult(myGetResult),
		NewGetAsserter().Assert("dog-key").OutputError(myGetError),
		NewGetAsserter().Assert("eagle-key").OutputResult(myGetResult),
		NewGetAsserter().Assert("freak-key").OutputError(myGetError),
		NewDeleteAsserter().Assert("gee-key").OutputError(myDeleteError),
	)
	patches := monkey.Apply()
	defer patches.Reset()

	client := &EtcdClient{Client: &clientv3.Client{}}

	err := client.Put(CreateEtcdCtxInfo(context.TODO()), "apple-key", "apple-value")
	assert.NoError(t, err)
	err = client.Put(CreateEtcdCtxInfo(context.TODO()), "bear-key", "bear-value")
	assert.Equal(t, myPutError, err)

	getResult, err := client.GetResponse(CreateEtcdCtxInfo(context.TODO()), "cat-key")
	assert.NoError(t, err)
	assert.Equal(t, myGetResult, getResult)
	_, err = client.GetResponse(CreateEtcdCtxInfo(context.TODO()), "dog-key")
	assert.Equal(t, myGetError, err)

	vals, err := client.GetValues(CreateEtcdCtxInfo(context.TODO()), "eagle-key")
	assert.NoError(t, err)
	assert.Equal(t, []string{"aa", "bbb"}, vals)
	_, err = client.GetValues(CreateEtcdCtxInfo(context.TODO()), "freak-key")
	assert.Equal(t, myGetError, err)

	err = client.Delete(CreateEtcdCtxInfo(context.TODO()), "gee-key")
	assert.Equal(t, myDeleteError, err)
}

func TestEtcdClientNilGetValues(t *testing.T) {
	c := &EtcdClient{etcdType: CAEMeta}
	resp, err := c.GetValues(CreateEtcdCtxInfo(context.TODO()), "")
	assert.Nil(t, resp, "response should be nil")
	assert.NotNil(t, err, "should return err")
	assert.Equal(t, "caeMeta etcd client is nil", err.Error())
}

func TestEtcdWatcherCtxInfo(t *testing.T) {
	CreateEtcdCtxInfo(context.TODO())
	CreateEtcdCtxInfoWithTimeout(context.TODO(), 1*time.Second)
}

func TestEtcdWatcherWatch(t *testing.T) {
	key1 := &mvccpb.KeyValue{Key: []byte("apple-key"), Value: []byte("apple-value"), ModRevision: 1234}
	key2 := &mvccpb.KeyValue{Key: []byte("bear-key"), Value: []byte("bear-value"), ModRevision: 4567}
	watch1 := &mvccpb.KeyValue{Key: []byte("cat-key"), Value: []byte("cat-value"), ModRevision: 91011}
	watch2 := &mvccpb.KeyValue{Key: []byte("dog-key"), Value: []byte("dog-value"), ModRevision: 1111}
	watch3 := &mvccpb.KeyValue{Key: []byte("egg-key"), Value: []byte("egg-value"), ModRevision: 4321}
	watch4 := &mvccpb.KeyValue{Key: []byte("freak-key"), Value: []byte("freak-value"), ModRevision: 5555}

	myGetResult := &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1111},
		Kvs: []*mvccpb.KeyValue{
			key1,
			key2,
		},
	}

	myWatchChan := make(chan clientv3.WatchResponse)

	monkey := NewMonkey(t)
	monkey.AddSequence(
		NewGetAsserter().Assert("/").OutputResult(myGetResult),
		NewWatchAsserter().Assert("/").OutputChan(myWatchChan),
	)
	patches := monkey.Apply()
	defer patches.Reset()

	client := &EtcdClient{
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}

	w := client.Watch(context.TODO(), "", 0, true)
	event1 := <-w.ResultChan
	assert.Equal(t, PUT, event1.Type)
	assert.Equal(t, string(key1.Key), event1.Key)
	assert.Equal(t, key1.Value, event1.Value)
	assert.Equal(t, key1.ModRevision, event1.Rev)

	event2 := <-w.ResultChan
	assert.Equal(t, PUT, event2.Type)
	assert.Equal(t, string(key2.Key), event2.Key)
	assert.Equal(t, key2.Value, event2.Value)
	assert.Equal(t, key2.ModRevision, event2.Rev)

	event3 := <-w.ResultChan
	assert.Equal(t, SYNCED, event3.Type)

	myWatchChan <- clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{Kv: watch1},
			{Kv: watch2, PrevKv: watch3},
		},
	}

	event4 := <-w.ResultChan
	assert.Equal(t, PUT, event4.Type)
	assert.Equal(t, string(watch1.Key), event4.Key)
	assert.Equal(t, watch1.Value, event4.Value)
	assert.Equal(t, watch1.ModRevision, event4.Rev)

	event5 := <-w.ResultChan
	assert.Equal(t, PUT, event5.Type)
	assert.Equal(t, string(watch2.Key), event5.Key)
	assert.Equal(t, watch2.Value, event5.Value)
	assert.Equal(t, watch2.ModRevision, event5.Rev)
	assert.Equal(t, watch3.Value, event5.PrevValue)

	myWatchChan <- clientv3.WatchResponse{
		Events: []*clientv3.Event{
			{Kv: watch4, Type: mvccpb.DELETE},
		},
	}

	event6 := <-w.ResultChan
	assert.Equal(t, DELETE, event6.Type)
	assert.Equal(t, string(watch4.Key), event6.Key)
	assert.Equal(t, watch4.Value, event6.Value)
	assert.Equal(t, watch4.ModRevision, event6.Rev)

	close(myWatchChan)

	event7 := <-w.ResultChan
	assert.Equal(t, ERROR, event7.Type)
}

func TestEtcdWatcherDoFullSync(t *testing.T) {
	var partitionContent []byte
	var metaInfoContent []byte
	var err error
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(tools.WriteFileToPath, func(writePath string, buffer []byte) error {
			if strings.Contains(writePath, "partition") {
				partitionContent = buffer
			}
			if strings.Contains(writePath, "meta_info") {
				metaInfoContent = buffer
			}
			return nil
		}),
		gomonkey.ApplyFuncSeq(os.ReadFile, []gomonkey.OutputCell{
			{Values: gomonkey.Params{nil, errors.New("cache file not exited")}, Times: 0},
		}),
		gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
			return nil, nil
		}),
		gomonkey.ApplyFunc(io.Copy, func(dst io.Writer, src io.Reader) (written int64, err error) {
			return 0, nil
		}),
		gomonkey.ApplyFunc(hex.EncodeToString, func(src []byte) string {
			return "123456"
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	watchPath := "/"
	record1 := &mvccpb.KeyValue{Key: []byte("apple-key"), Value: []byte("apple-value"), ModRevision: 1234}
	record2 := &mvccpb.KeyValue{Key: []byte("bear-key"), Value: []byte("bear-value"), ModRevision: 1235}

	getResult := &clientv3.GetResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 1235},
		Kvs: []*mvccpb.KeyValue{
			record1,
			record2,
		},
	}

	watchChan := make(chan clientv3.WatchResponse)

	monkey := NewMonkey(t)
	monkey.AddSequence(
		NewGetAsserter().Assert(watchPath).OutputResult(getResult),
		NewWatchAsserter().Assert(watchPath).OutputChan(watchChan),
	)
	patches1 := monkey.Apply()
	defer patches1.Reset()

	client := &EtcdClient{
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}
	cacheConfig := &EtcdCacheConfig{
		UpdateCacheFilePeriod:  15,
		CacheFilePartitionSize: 10,
		EnableEtcdCache:        true,
	}
	ctx := context.WithValue(context.Background(), EtcdCacheConfigKey, cacheConfig)
	w := client.Watch(ctx, "", 0, true)
	event1 := <-w.ResultChan
	assert.Equal(t, PUT, event1.Type)
	assert.Equal(t, string(record1.Key), event1.Key)
	assert.Equal(t, record1.Value, event1.Value)
	assert.Equal(t, record1.ModRevision, event1.Rev)

	event2 := <-w.ResultChan
	assert.Equal(t, PUT, event2.Type)
	assert.Equal(t, string(record2.Key), event2.Key)
	assert.Equal(t, record2.Value, event2.Value)
	assert.Equal(t, record2.ModRevision, event2.Rev)

	event3 := <-w.ResultChan
	assert.Equal(t, SYNCED, event3.Type)

	var etcdCache EtcdCache
	err = json.Unmarshal(partitionContent, &etcdCache)
	assert.Nil(t, err)
	assert.Equal(t, string(record1.Value), string(etcdCache.Events[string(record1.Key)].Value))
	assert.Equal(t, string(record2.Value), string(etcdCache.Events[string(record2.Key)].Value))
	var metaInfo EtcdCacheMetaInfo
	err = json.Unmarshal(metaInfoContent, &metaInfo)
	assert.Equal(t, getResult.Header.Revision, metaInfo.Rev)
	assert.Equal(t, 10, metaInfo.PartitionSize)
	assert.Equal(t, "123456", metaInfo.MD5s["partition_1"])
}

func TestEtcdWatcherDoDiffSync(t *testing.T) {
	etcdCache := &EtcdCache{
		Events: map[string]*Event{
			"apple-key": {
				Type:      PUT,
				Key:       "apple-key",
				Value:     []byte("apple-value"),
				PrevValue: []byte("pre-apple-value"),
				Rev:       1234,
			},
			"bear-key": {
				Type:      PUT,
				Key:       "bear-key",
				Value:     []byte("bear-value"),
				PrevValue: []byte("pre-bear-value"),
				Rev:       1235,
			},
		},
	}
	metaInfo := &EtcdCacheMetaInfo{
		Rev:           1234,
		PartitionSize: 10,
		MD5s: map[string]string{
			"partition_1": "123456",
		},
		Version: "v1.0",
	}
	metaInfoContent, err := json.Marshal(metaInfo)
	partitionContent1, err := json.Marshal(etcdCache)
	partitionContent2, err := json.Marshal(etcdCache)
	watch1 := &mvccpb.KeyValue{Key: []byte("apple-key"), Value: []byte("apple-value"), ModRevision: 1238}
	watch2 := &mvccpb.KeyValue{Key: []byte("dog-key"), Value: []byte("dog-value"), ModRevision: 1239}
	watchResponse := clientv3.WatchResponse{
		Header: *&etcdserverpb.ResponseHeader{
			Revision: 1239,
		},
		Events: []*clientv3.Event{
			{Kv: watch1, Type: mvccpb.DELETE},
			{Kv: watch2, Type: mvccpb.PUT},
		},
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFuncSeq(os.ReadFile, []gomonkey.OutputCell{
			{Values: gomonkey.Params{metaInfoContent, err}, Times: 1},
			{Values: gomonkey.Params{partitionContent1, err}, Times: 0},
			{Values: gomonkey.Params{partitionContent2, err}, Times: 2},
		}),
		gomonkey.ApplyFunc(tools.WriteFileToPath, func(writePath string, buffer []byte) error {
			if strings.Contains(writePath, "partition_1") {
				partitionContent1 = buffer
			}
			if strings.Contains(writePath, "partition_9") {
				partitionContent2 = buffer
			}
			if strings.Contains(writePath, "meta_info") {
				metaInfoContent = buffer
			}
			return nil
		}),
		gomonkey.ApplyFunc(tools.FileMD5, func(filePath string) (string, error) {
			return "123456", nil
		}),
		gomonkey.ApplyFunc(clientv3.NewWatcher, func(_ *clientv3.Client) clientv3.Watcher {
			return &fakeWatcher{}
		}),
		gomonkey.ApplyFunc((*fakeWatcher).Watch, func(w *fakeWatcher, ctx context.Context, key string,
			opts ...clientv3.OpOption) clientv3.WatchChan {
			ch := make(chan clientv3.WatchResponse, 1)
			ch <- watchResponse
			return ch
		}),
		gomonkey.ApplyFunc(clientv3.NewKV, func(c *clientv3.Client) clientv3.KV {
			return &fakeKV{}
		}),
		gomonkey.ApplyFunc((*fakeKV).Get, func(kv *fakeKV, ctx context.Context, key string,
			opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			kv1 := &mvccpb.KeyValue{Key: []byte("apple-key"), Value: []byte("apple-value"), ModRevision: 1239}
			kvs := make([]*mvccpb.KeyValue, 0)
			kvs = append(kvs, kv1)
			return &clientv3.GetResponse{
				Kvs: kvs,
			}, nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	client := &EtcdClient{
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}
	cacheConfig := &EtcdCacheConfig{
		UpdateCacheFilePeriod:  15,
		CacheFilePartitionSize: 10,
		EnableEtcdCache:        true,
		EtcdDiffSyncTimeout:    15,
		FlushCacheThreshold:    10,
	}
	ctx := context.WithValue(context.Background(), EtcdCacheConfigKey, cacheConfig)
	w := client.Watch(ctx, "", 0, true)
	event1 := <-w.ResultChan
	event := etcdCache.Events[event1.Key]
	assert.Equal(t, event.Type, event1.Type)
	assert.Equal(t, event.Key, event1.Key)
	assert.Equal(t, string(event.Value), string(event1.Value))
	assert.Equal(t, event.Rev, event1.Rev)

	event2 := <-w.ResultChan
	event = etcdCache.Events[event2.Key]
	assert.Equal(t, event.Type, event2.Type)
	assert.Equal(t, event.Key, event2.Key)
	assert.Equal(t, string(event.Value), string(event2.Value))
	assert.Equal(t, event.Rev, event2.Rev)

	event3 := <-w.ResultChan
	assert.Equal(t, DELETE, event3.Type)
	assert.Equal(t, string(watch1.Key), event3.Key)
	assert.Equal(t, string(watch1.Value), string(event3.Value))
	assert.Equal(t, watch1.ModRevision, event3.Rev)

	event4 := <-w.ResultChan
	assert.Equal(t, PUT, event4.Type)
	assert.Equal(t, string(watch2.Key), event4.Key)
	assert.Equal(t, string(watch2.Value), string(event4.Value))
	assert.Equal(t, watch2.ModRevision, event4.Rev)

	event5 := <-w.ResultChan
	assert.Equal(t, SYNCED, event5.Type)

	var etcdCache1 EtcdCache
	err = json.Unmarshal(partitionContent1, &etcdCache1)
	assert.Equal(t, nil, err)
	//assert.Equal(t, watchResponse.Header.Revision, etcdCache1.Rev)
	assert.Equal(t, 1, len(etcdCache1.Events))
	assert.Nil(t, etcdCache1.Events[event3.Key])
	var etcdCache2 EtcdCache
	err = json.Unmarshal(partitionContent2, &etcdCache2)
	assert.Equal(t, 1, len(etcdCache2.Events))
	assert.NotNil(t, etcdCache2.Events[event4.Key])
	var newMetaInfo EtcdCacheMetaInfo
	err = json.Unmarshal(metaInfoContent, &newMetaInfo)
	assert.Equal(t, 1239, int(newMetaInfo.Rev))
	assert.Equal(t, 10, newMetaInfo.PartitionSize)
	assert.Equal(t, 2, len(newMetaInfo.MD5s))
}

func TestEtcdWatcherDoFullSyncGetErr(t *testing.T) {
	var content []byte
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(tools.WriteFileToPath, func(writePath string, buffer []byte) error {
			content = buffer
			return nil
		}),
		gomonkey.ApplyFuncSeq(os.ReadFile, []gomonkey.OutputCell{
			{Values: gomonkey.Params{nil, errors.New("cache file not exited")}, Times: 0},
		}),
		gomonkey.ApplyFunc(clientv3.NewKV, func(c *clientv3.Client) clientv3.KV {
			return &fakeKV{}
		}),
		gomonkey.ApplyFunc((*fakeKV).Get, func(kv *fakeKV, ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return nil, errors.New("etcd get failed")
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	watchPath := "/"

	watchChan := make(chan clientv3.WatchResponse)

	monkey := NewMonkey(t)
	monkey.AddSequence(
		NewWatchAsserter().Assert(watchPath).OutputChan(watchChan),
	)
	patches1 := monkey.Apply()
	defer patches1.Reset()

	client := &EtcdClient{
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}
	ctx := context.WithValue(context.Background(), EtcdCacheConfigKey, true)
	client.Watch(ctx, "", 0, true)
	time.Sleep(time.Millisecond * 20)
	assert.Nil(t, content)
}

func TestEtcdWatcherDoFullSyncWriteFileError(t *testing.T) {
	content := make([]byte, 0, 1)
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(tools.WriteFileToPath, func(writePath string, buffer []byte) error {
			content = nil
			return errors.New("write cache file failed")
		}),
		gomonkey.ApplyFuncSeq(os.ReadFile, []gomonkey.OutputCell{
			{Values: gomonkey.Params{nil, errors.New("cache file not exited")}, Times: 0},
		}),
		gomonkey.ApplyFunc(clientv3.NewKV, func(c *clientv3.Client) clientv3.KV {
			return &fakeKV{}
		}),
		gomonkey.ApplyFunc(os.Stat, func(name string) (fs.FileInfo, error) {
			return nil, nil
		}),
		gomonkey.ApplyFunc(os.RemoveAll, func(name string) error {
			return errors.New("delete cache file failed")
		}),
		gomonkey.ApplyFunc((*fakeKV).Get, func(kv *fakeKV, ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			record1 := &mvccpb.KeyValue{Key: []byte("apple-key"), Value: []byte("apple-value"), ModRevision: 1234}
			record2 := &mvccpb.KeyValue{Key: []byte("bear-key"), Value: []byte("bear-value"), ModRevision: 1235}
			getResult := &clientv3.GetResponse{
				Header: &etcdserverpb.ResponseHeader{Revision: 1235},
				Kvs: []*mvccpb.KeyValue{
					record1,
					record2,
				},
			}
			return getResult, nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	watchPath := "/"

	watchChan := make(chan clientv3.WatchResponse)

	monkey := NewMonkey(t)
	monkey.AddSequence(
		NewWatchAsserter().Assert(watchPath).OutputChan(watchChan),
	)
	patches1 := monkey.Apply()
	defer patches1.Reset()

	client := &EtcdClient{
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}
	assert.Equal(t, 1, cap(content))
	cacheConfig := &EtcdCacheConfig{
		UpdateCacheFilePeriod:  15,
		CacheFilePartitionSize: 10,
		EnableEtcdCache:        true,
		EtcdDiffSyncTimeout:    15,
		FlushCacheThreshold:    10,
	}
	ctx := context.WithValue(context.Background(), EtcdCacheConfigKey, cacheConfig)
	client.Watch(ctx, "", 0, true)
	time.Sleep(time.Millisecond * 20)
	assert.Nil(t, content)
}

func TestFlushEventsToCacheFile(t *testing.T) {
	wc := &EtcdWatchChan{}
	events := make([]*Event, 1)
	events[0] = &Event{
		Type:      ERROR,
		Key:       "key",
		Value:     []byte("value"),
		PrevValue: nil,
		Rev:       5,
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFuncSeq(os.ReadFile, []gomonkey.OutputCell{
			{Values: gomonkey.Params{nil, errors.New("cache file not exited")}, Times: 0},
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	err := wc.flushEventsToCacheFile(&events)
	assert.Error(t, err)
}

func TestStartWatching(t *testing.T) {
	incomeCh := make(chan *Event, 1)
	incomeCh <- &Event{}
	backCtx, cancel := context.WithCancel(context.Background())
	cancel()
	backCtx.Done()
	etcdClient := &EtcdClient{}
	wc := &EtcdWatchChan{
		ctx:               backCtx,
		incomingEventChan: incomeCh,
		watcher:           etcdClient,
	}
	wc.EtcdCacheConfig = &EtcdCacheConfig{
		EnableEtcdCache:       true,
		UpdateCacheFilePeriod: 1,
	}
	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(wc), "DoDiffSync", func(wc *EtcdWatchChan) error {
			return nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(wc), "DoFullSync", func(wc *EtcdWatchChan) error {
			panic("DoDiffSync ok, shouldn't call DoFullSync")
		}),
		gomonkey.ApplyFunc(clientv3.NewWatcher, func(_ *clientv3.Client) clientv3.Watcher {
			return &fakeWatcher{}
		}),
		gomonkey.ApplyFunc((*fakeWatcher).Watch, func(w *fakeWatcher, ctx context.Context, key string,
			opts ...clientv3.OpOption) clientv3.WatchChan {
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	ch := make(chan struct{})
	wc.startWatching(ch)
}

func TestDoDiffSync(t *testing.T) {
	etcdClient := &EtcdClient{}
	wc := &EtcdWatchChan{
		watcher: etcdClient,
	}
	wc.EtcdCacheConfig = &EtcdCacheConfig{
		EnableEtcdCache:        true,
		UpdateCacheFilePeriod:  1,
		CacheFilePartitionSize: 10,
	}
	metaInfo := &EtcdCacheMetaInfo{
		Rev:           1234,
		PartitionSize: 10,
		MD5s: map[string]string{
			"partition_1": "123456",
		},
		Version: "v1.0",
	}
	etcdCache := &EtcdCache{
		Events: map[string]*Event{
			"apple-key": {
				Type:      PUT,
				Key:       "apple-key",
				Value:     []byte("apple-value"),
				PrevValue: []byte("pre-apple-value"),
				Rev:       1234,
			},
			"bear-key": {
				Type:      PUT,
				Key:       "bear-key",
				Value:     []byte("bear-value"),
				PrevValue: []byte("pre-bear-value"),
				Rev:       1235,
			},
		},
	}
	metaInfoContent, err := json.Marshal(metaInfo)
	partitionContent1, err := json.Marshal(etcdCache)
	partitionContent2, err := json.Marshal(etcdCache)
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFuncSeq(os.ReadFile, []gomonkey.OutputCell{
			{Values: gomonkey.Params{metaInfoContent, err}, Times: 1},
			{Values: gomonkey.Params{partitionContent1, err}, Times: 0},
			{Values: gomonkey.Params{partitionContent2, err}, Times: 2},
		}),
		gomonkey.ApplyFunc(clientv3.NewWatcher, func(_ *clientv3.Client) clientv3.Watcher {
			return &fakeWatcher{}
		}),
		gomonkey.ApplyFunc((*fakeWatcher).Watch, func(w *fakeWatcher, ctx context.Context, key string,
			opts ...clientv3.OpOption) clientv3.WatchChan {
			return nil
		}),
		gomonkey.ApplyFunc(tools.FileMD5, func(filePath string) (string, error) {
			return "123456", nil
		}),
		gomonkey.ApplyFunc(clientv3.NewKV, func(c *clientv3.Client) clientv3.KV {
			return &fakeKV{}
		}),
		gomonkey.ApplyFunc((*fakeKV).Get, func(kv *fakeKV, ctx context.Context,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return nil, errors.New("etcd get failed")
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	err = wc.DoDiffSync()
	assert.Equal(t, "do diff sync failed, get latest revision failed", err.Error())
}

func TestEtcdWatcherTxn(t *testing.T) {
	etcdClient := &EtcdClient{
		Client:       &clientv3.Client{},
		clientExitCh: make(chan struct{}),
	}
	gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "Txn", func(w *EtcdClient,
		ctxInfo EtcdCtxInfo, ops ...clientv3.Op) error {
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	etcdCtx := EtcdCtxInfo{Ctx: ctx, Cancel: cancel}
	err := etcdClient.Txn(etcdCtx)
	assert.Nil(t, err)
}

func Test_GetResponseWithSerializable(t *testing.T) {
	optNum := 0
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(clientv3.NewKV, func(c *clientv3.Client) clientv3.KV {
			return &fakeKV{}
		}),
		gomonkey.ApplyFunc((*fakeKV).Get, func(kv *fakeKV, ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			optNum = len(opts)
			return nil, nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()

	etcdProxyClient := &EtcdClient{
		Client:       &clientv3.Client{},
		etcdType:     MetaProxy,
		clientExitCh: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	etcdCtx := EtcdCtxInfo{Ctx: ctx, Cancel: cancel}
	_, err := etcdProxyClient.GetResponseWithSerializable(etcdCtx, "key")
	assert.NoError(t, err)
	assert.Equal(t, 0, optNum)

	etcdClient := &EtcdClient{
		Client:       &clientv3.Client{},
		etcdType:     Meta,
		clientExitCh: make(chan struct{}),
	}
	ctx, cancel = context.WithCancel(context.Background())
	etcdCtx = EtcdCtxInfo{Ctx: ctx, Cancel: cancel}
	_, err = etcdClient.GetResponseWithSerializable(etcdCtx, "key")
	assert.NoError(t, err)
	assert.Equal(t, 1, optNum)
}

func TestHandleCellPrefix(t *testing.T) {
	type args struct {
		path string
	}
	cellID = "CELL_ID"
	var a1 args
	a1.path = "/cell/test"
	var a2 args
	a2.path = "test"
	var a3 args
	a3.path = "/sn/test"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a1, "/cell/test"},
		{"case2", a2, "/cell/CELL_ID/test"},
		{"case3", a3, "/cell/CELL_ID/sn/test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, HandleCellPrefix(tt.args.path), "HandleCellPrefix(%v)", tt.args.path)
		})
	}
	cellID = ""
}

func TestDeleteCellPrefix(t *testing.T) {
	type args struct {
		path string
	}
	cellID = "CELL_ID"
	var a1 args
	a1.path = "test"
	var a2 args
	a2.path = "/cell/CELL_ID/sn/test"
	var a3 args
	a3.path = "/sn/test"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a1, ""},
		{"case2", a2, "/sn/test"},
		{"case3", a3, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, DeleteCellPrefix(tt.args.path), "DeleteCellPrefix(%v)", tt.args.path)
		})
	}
	cellID = ""
}
