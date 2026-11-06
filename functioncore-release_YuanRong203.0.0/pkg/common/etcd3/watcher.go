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

// Package etcd3 implements crud and watch operations based etcd clientv3
package etcd3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/clientv3util"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const (
	// We have set a buffer in order to reduce times of context switches.
	incomingBufSize   = 200
	outgoingBufSize   = 200
	mateEtcdCacheFile = "/home/sn/etcd/"
	// DefaultMetaEtcdHomePath -
	DefaultMetaEtcdHomePath = "/home/sn/etcd/_sn_functions_business_yrk_tenant_"
	// EtcdCacheConfigKey the key to trans EtcdCacheConfigKey flag via context
	EtcdCacheConfigKey = "etcdCacheConfigKey"
	// PartitionCacheFilePrefix -
	PartitionCacheFilePrefix = "partition_"
	// PartitionCacheMetaInfo -
	PartitionCacheMetaInfo = "meta_info"
	invalidRevisionID      = -1
	minRevisionID          = 1
	// CacheVersion -
	CacheVersion = "v1.0"
)

const (
	// The short keepalive timeout and interval have been chosen to aggressively
	// detect a failed etcd server without introducing much overhead.
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second

	// dialTimeout is the timeout for establishing a connection.
	// 20 seconds as times should be set shorter than that will cause TLS connections to fail
	dialTimeout = 20 * time.Second

	keepConnAliveTTL = 3 * time.Second

	// DurationContextTimeout default context duration timeout
	DurationContextTimeout = 5 * time.Second
)

var (
	// cellID is used to distinguish different cell clusters. The default value is an empty string
	cellID = os.Getenv("CELL_ID")
)

type contextKey string

const (
	// CellEnable -
	CellEnable contextKey = "cellEnable"
)

// GetResponse get etcd value and return pointer of GetResponse struct
func (w *EtcdClient) GetResponse(ctxInfo EtcdCtxInfo, key string,
	opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()

	w.rwMutex.RLock()
	kv := clientv3.NewKV(w.Client)
	getResp, err := kv.Get(ctx, key, opts...)
	w.rwMutex.RUnlock()
	log.GetLogger().Debugf("%s etcd client get key: %s", w.etcdType.String(), key)
	return getResp, err
}

// GetResponseWithSerializable get etcd value with serializable and return pointer of GetResponse struct
func (w *EtcdClient) GetResponseWithSerializable(ctxInfo EtcdCtxInfo, key string, opts ...clientv3.OpOption) (
	*clientv3.GetResponse, error) {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()

	w.rwMutex.RLock()
	kv := clientv3.NewKV(w.Client)
	var getResp *clientv3.GetResponse
	var err error
	if w.etcdType == MetaProxy || w.etcdType == CAEMetaProxy {
		getResp, err = kv.Get(ctx, key, opts...)
	} else {
		opts = append(opts, clientv3.WithSerializable())
		getResp, err = kv.Get(ctx, key, opts...)
	}
	w.rwMutex.RUnlock()
	log.GetLogger().Debugf("%s etcd client get key: %s", w.etcdType.String(), key)
	return getResp, err
}

// Put put context key and value
func (w *EtcdClient) Put(ctxInfo EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()

	w.rwMutex.RLock()
	kv := clientv3.NewKV(w.Client)
	_, err := kv.Put(ctx, key, value, opts...)
	w.rwMutex.RUnlock()
	log.GetLogger().Debugf("%s etcd client put key: %s", w.etcdType.String(), key)
	return err
}

// PutOnlyKeyMissing put context key and value only if key is missing
func (w *EtcdClient) PutOnlyKeyMissing(ctxInfo EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()

	w.rwMutex.RLock()
	kv := clientv3.NewKV(w.Client)
	resp, err := kv.Txn(ctx).If(clientv3util.KeyMissing(key)).Then(clientv3.OpPut(key, value, opts...)).
		Else(clientv3.OpGet(key)).Commit()
	if err != nil || !resp.Succeeded {
		err = errors.New("put key error, key exists")
	}
	w.rwMutex.RUnlock()
	log.GetLogger().Debugf("%s etcd client put only key missing, key: %s", w.etcdType.String(), key)
	return err
}

// Txn do ops with transaction
func (w *EtcdClient) Txn(ctxInfo EtcdCtxInfo, ops ...clientv3.Op) error {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()
	w.rwMutex.RLock()
	_, err := w.Client.Txn(ctx).Then(ops...).Commit()
	w.rwMutex.RUnlock()
	return err
}

// Delete delete key
func (w *EtcdClient) Delete(ctxInfo EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()

	w.rwMutex.RLock()
	kv := clientv3.NewKV(w.Client)
	_, err := kv.Delete(ctx, key, opts...)
	w.rwMutex.RUnlock()
	log.GetLogger().Debugf("%s etcd client delete key: %s", w.etcdType.String(), key)
	return err
}

// GetValues return list of object for key
func (w *EtcdClient) GetValues(ctxInfo EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
	ctx, cancel := ctxInfo.Ctx, ctxInfo.Cancel
	defer cancel()

	w.rwMutex.RLock()
	if w.Client == nil {
		w.rwMutex.RUnlock()
		return nil, fmt.Errorf("%s etcd client is nil", w.etcdType.String())
	}
	kv := clientv3.NewKV(w.Client)
	response, err := kv.Get(ctx, key, opts...)
	w.rwMutex.RUnlock()
	log.GetLogger().Debugf("%s etcd client get values key: %s", w.etcdType.String(), key)
	if err != nil {
		return nil, err
	}
	values := make([]string, len(response.Kvs), len(response.Kvs))

	for index, v := range response.Kvs {
		values[index] = string(v.Value)
	}
	return values, err
}

// CreateEtcdCtxInfo return context with cancle function
func CreateEtcdCtxInfo(ctx context.Context) EtcdCtxInfo {
	ctx, cancle := context.WithCancel(ctx)
	return EtcdCtxInfo{Ctx: ctx, Cancel: cancle}
}

// CreateEtcdCtxInfoWithTimeout create a context with timeout, default timeout is DurationContextTimeout
func CreateEtcdCtxInfoWithTimeout(ctx context.Context, duration time.Duration) EtcdCtxInfo {
	ctx, cancle := context.WithTimeout(ctx, duration)
	return EtcdCtxInfo{Ctx: ctx, Cancel: cancle}
}

// Watch start to watch based on input parameters
func (w *EtcdClient) Watch(ctx context.Context, watchPath string, rev int64, recursive bool) *EtcdWatchChan {
	w.rwMutex.RLock()
	defer w.rwMutex.RUnlock()
	var cellEnable bool
	result, ok := ctx.Value(CellEnable).(bool)
	if ok {
		cellEnable = result
	}
	if w.DowngradesEtcdClient != nil {
		return w.DowngradesEtcdClient.Watch(ctx, watchPath, rev, recursive)
	}
	if recursive && !strings.HasSuffix(watchPath, "/") {
		watchPath += "/"
	}
	if cellID != "" && cellEnable {
		if !strings.HasPrefix(watchPath, "/") {
			watchPath = "/" + watchPath
		}
		watchPath = "/cell/" + cellID + watchPath
	}
	wc := w.createWatchChan(ctx, watchPath, rev, recursive)
	wc.CellEnable = cellEnable
	go wc.run(w.clientExitCh)
	log.GetLogger().Debugf("%s etcd client watch path: %s", w.etcdType.String(), watchPath)
	return wc
}

func (w *EtcdClient) createWatchChan(ctx context.Context, key string, rev int64, recursive bool) *EtcdWatchChan {
	wc := &EtcdWatchChan{
		watcher:            w,
		key:                key,
		cacheFilePath:      mateEtcdCacheFile + strings.ReplaceAll(key, "/", "_"),
		initialRev:         rev,
		recursive:          recursive,
		incomingEventChan:  make(chan *Event, incomingBufSize),
		ResultChan:         make(chan *Event, outgoingBufSize),
		errChan:            make(chan error, 1),
		mux:                new(sync.Mutex),
		CacheCfgUpdateChan: make(chan *EtcdCacheConfig, 10),
		source:             w.etcdType,
	}
	etcdCacheConfigIf := ctx.Value(EtcdCacheConfigKey)
	if etcdCacheConfig, ok := etcdCacheConfigIf.(*EtcdCacheConfig); ok {
		wc.EtcdCacheConfig = etcdCacheConfig
		wc.updateCacheEventChan = make(chan *Event, etcdCacheConfig.CacheUpdateBufSize)
	} else {
		wc.EtcdCacheConfig = &EtcdCacheConfig{}
	}
	wc.ctx, wc.cancel = context.WithCancel(ctx)
	return wc
}

func (wc *EtcdWatchChan) run(clientExitCh chan struct{}) {
	if clientExitCh == nil {
		return
	}
	go wc.startWatching(clientExitCh)

	var resultChanWG sync.WaitGroup
	resultChanWG.Add(1)
	go wc.processEvent(&resultChanWG)

	select {
	case err, ok := <-wc.errChan:
		if !ok {
			log.GetLogger().Warnf("error channel is closed")
			break
		}
		log.GetLogger().Errorf("error channel receives error: %s", err.Error())
		if err == context.Canceled {
			break
		}
		errResult := parseErr(err, wc.source)
		if errResult != nil {
			// error result is guaranteed to be received by user before closing ResultChan.
			select {
			case wc.ResultChan <- errResult:
			case <-wc.ctx.Done(): // user has given up all results
			}
		}
	case _, ok := <-clientExitCh:
		if !ok {
			log.GetLogger().Warnf("watch closed channel is closed")
			break
		}
	case _, ok := <-wc.ctx.Done():
		if !ok {
			log.GetLogger().Warnf("context done channel is closed")
			return
		}
	}

	// use wc.ctx to notify all goroutines to stop executing
	wc.cancel()

	// wait to close resultChan until resultChan ends processing event
	resultChanWG.Wait()
	close(wc.ResultChan)
}

func (wc *EtcdWatchChan) checkCacheMetaInfo(metaInfo *EtcdCacheMetaInfo) error {
	if metaInfo.Version != CacheVersion {
		return errors.New("cache file version not match")
	}
	if wc.EtcdCacheConfig.CacheFilePartitionSize != metaInfo.PartitionSize {
		return errors.New("cache file partition size not match")
	}
	for i := 0; i < metaInfo.PartitionSize; i++ {
		partitionName := fmt.Sprintf("%s%d", PartitionCacheFilePrefix, i)
		if metaInfo.MD5s[partitionName] == "" {
			continue
		}
		partitionPath := fmt.Sprintf("%s/%s", wc.cacheFilePath, partitionName)
		md5, err := utils.FileMD5(partitionPath)
		if err != nil {
			return err
		}
		if metaInfo.MD5s[partitionName] != md5 {
			return errors.New("cache file md5 not match")
		}
	}
	return nil
}

func (wc *EtcdWatchChan) partitionEvents(partitionedEventsPtr *map[int][]*Event, event *Event) {
	partitionID := utils.FnvHashInt(event.Key) % wc.EtcdCacheConfig.CacheFilePartitionSize
	partitionedEvents := *partitionedEventsPtr
	events := partitionedEvents[partitionID]
	if events == nil {
		partitionedEvents[partitionID] = make([]*Event, 0)
	}
	partitionedEvents[partitionID] = append(partitionedEvents[partitionID], event)
}

func (wc *EtcdWatchChan) createOrUpdatePartitionCacheFile(partitionedEvents *map[int][]*Event,
	metaInfo *EtcdCacheMetaInfo) error {
	metaInfoPath := fmt.Sprintf("%s/%s", wc.cacheFilePath, PartitionCacheMetaInfo)
	if metaInfo == nil {
		metaInfo = &EtcdCacheMetaInfo{}
		metaInfo.PartitionSize = wc.EtcdCacheConfig.CacheFilePartitionSize
		metaInfo.MD5s = make(map[string]string)
		metaInfo.Version = CacheVersion
		metaInfo.Rev = minRevisionID
	}
	maxRevision := int64(invalidRevisionID)
	for k, v := range *partitionedEvents {
		partitionName := fmt.Sprintf("%s%d", PartitionCacheFilePrefix, k)
		path := fmt.Sprintf("%s/%s", wc.cacheFilePath, partitionName)
		var etcdCache *EtcdCache
		if metaInfo.MD5s[partitionName] == "" {
			etcdCache = &EtcdCache{
				Events: make(map[string]*Event),
			}
		} else {
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			err = json.Unmarshal(content, &etcdCache)
			if err != nil {
				return err
			}
		}
		for _, val := range v {
			if val.Rev > maxRevision {
				maxRevision = val.Rev
			}
			switch val.Type {
			case PUT:
				etcdCache.Events[val.Key] = val
			case DELETE:
				delete(etcdCache.Events, val.Key)
			default:
				log.GetLogger().Infof("ignore unsupported event type: %v", val.Type)
			}
		}
		content, err := json.Marshal(etcdCache)
		if err != nil {
			return err
		}
		err = utils.WriteFileToPath(path, content)
		if err != nil {
			return err
		}
		md5, err := utils.FileMD5(path)
		if err != nil {
			return err
		}
		metaInfo.MD5s[partitionName] = md5
	}
	if maxRevision != invalidRevisionID {
		metaInfo.Rev = maxRevision
	}
	content, err := json.Marshal(metaInfo)
	if err != nil {
		return err
	}
	err = utils.WriteFileToPath(metaInfoPath, content)
	if err != nil {
		return err
	}
	return nil
}

// sync get history events in etcd and send these events
func (wc *EtcdWatchChan) sync() error {
	var opts []clientv3.OpOption
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}

	kv := clientv3.NewKV(wc.watcher.Client)
	ctx, cancel := context.WithCancel(wc.ctx)
	defer cancel()
	response, err := kv.Get(ctx, wc.key, opts...)
	if err != nil {
		log.GetLogger().Errorf("failed to get value from etcd, key: %s, err: %s", wc.key, err.Error())
		return err
	}
	wc.initialRev = response.Header.Revision
	if wc.EtcdCacheConfig.EnableEtcdCache {
		groupedEvents := make(map[int][]*Event)
		for _, kv := range response.Kvs {
			evt := parseKV(kv, wc.source, wc.CellEnable)
			wc.partitionEvents(&groupedEvents, evt)
			wc.sendEvent(evt)
		}
		err := wc.createOrUpdatePartitionCacheFile(&groupedEvents, nil)
		if err != nil {
			wc.deleteCacheFile()
			wc.EtcdCacheConfig.EnableEtcdCache = false
			log.GetLogger().Warnf("do full sync success, but store cache file failed: %s", err.Error())
		}
	} else {
		for _, kv := range response.Kvs {
			wc.sendEvent(parseKV(kv, wc.source, wc.CellEnable))
		}
	}
	return nil
}

// DoFullSync do full sync
func (wc *EtcdWatchChan) DoFullSync() error {
	if err := wc.sync(); err != nil {
		log.GetLogger().Errorf("do full sync failed: %s", err.Error())
		wc.sendError(err)
		return err
	}
	return nil
}

// DoDiffSync do diff sync
func (wc *EtcdWatchChan) DoDiffSync() error {
	metaInfoPath := fmt.Sprintf("%s/%s", wc.cacheFilePath, PartitionCacheMetaInfo)
	content, err := os.ReadFile(metaInfoPath)
	if err != nil {
		return err
	}
	var metaInfo EtcdCacheMetaInfo
	err = json.Unmarshal(content, &metaInfo)
	if err != nil {
		return err
	}
	err = wc.checkCacheMetaInfo(&metaInfo)
	if err != nil {
		return err
	}
	// Get the latest revision record with the key prefix
	latestRevOpt := []clientv3.OpOption{clientv3.WithPrevKV()}
	latestRevOpt = append(latestRevOpt, clientv3.WithLastRev()...)
	kv := clientv3.NewKV(wc.watcher.Client)
	latestRev, err := kv.Get(wc.ctx, wc.key, latestRevOpt...)
	if err != nil {
		return errors.New("do diff sync failed, get latest revision failed")
	}
	// Watch the events from metaInfo.Rev to latestRev.Rev
	opts := []clientv3.OpOption{clientv3.WithPrevKV(), clientv3.WithRev(metaInfo.Rev)}
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	ctx, cancel := context.WithTimeout(wc.ctx, time.Second*time.Duration(wc.EtcdCacheConfig.EtcdDiffSyncTimeout))
	defer cancel()
	wch := clientv3.NewWatcher(wc.watcher.Client).Watch(ctx, wc.key, opts...)
	if wch == nil {
		return errors.New("do diff sync failed, watch chan is nil")
	}
	err = wc.sendCachedEvents(metaInfo)
	if err != nil {
		return err
	}
	flag := false
	for {
		select {
		case response, ok := <-wch:
			if !ok {
				return errors.New("do diff sync failed, watch chan error")
			}
			if response.Err() != nil {
				return response.Err()
			}
			groupedEvents := make(map[int][]*Event)
			for _, event := range response.Events {
				evt := parseEvent(event, wc.source, wc.CellEnable)
				switch evt.Type {
				case PUT:
					wc.partitionEvents(&groupedEvents, evt)
				case DELETE:
					wc.partitionEvents(&groupedEvents, evt)
				default:
					log.GetLogger().Infof("do diff sync ignore unsupported event type: %v", event.Type)
				}
				wc.sendEvent(evt)
				if len(latestRev.Kvs) == 0 || latestRev.Kvs[0].ModRevision == evt.Rev {
					flag = true
				}
			}
			err := wc.createOrUpdatePartitionCacheFile(&groupedEvents, &metaInfo)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return errors.New("do diff sync timeout")
		}
		if flag {
			break
		}
	}
	wc.initialRev = metaInfo.Rev
	log.GetLogger().Infof("do diff sync success for %s", wc.key)
	return nil
}

func (wc *EtcdWatchChan) sendCachedEvents(metaInfo EtcdCacheMetaInfo) error {
	for i := 0; i < metaInfo.PartitionSize; i++ {
		partitionName := fmt.Sprintf("%s%d", PartitionCacheFilePrefix, i)
		if metaInfo.MD5s[partitionName] == "" {
			continue
		}
		Path := fmt.Sprintf("%s/%s", wc.cacheFilePath, partitionName)
		content, err := os.ReadFile(Path)
		if err != nil {
			return err
		}
		var etcdCache EtcdCache
		err = json.Unmarshal(content, &etcdCache)
		if err != nil {
			return err
		}
		events := etcdCache.Events
		for k, v := range events {
			v.Key = DeleteCellPrefix(k)
			wc.sendEvent(v)
		}
	}
	return nil
}

func (wc *EtcdWatchChan) deleteCacheFile() {
	_, err := os.Stat(wc.cacheFilePath)
	if err == nil {
		err = os.RemoveAll(wc.cacheFilePath)
		if err != nil {
			log.GetLogger().Errorf("delete cache file [%s] failed", wc.cacheFilePath)
		}
	} else {
		if os.IsNotExist(err) {
			log.GetLogger().Infof("cache file [%s] not exist, ignore it", wc.cacheFilePath)
		} else {
			log.GetLogger().Errorf("cache file [%s] error before do full sync", wc.cacheFilePath)
		}
	}
}

func (wc *EtcdWatchChan) updateCacheFilePeriodically(clientExitCh chan struct{}) {
	if clientExitCh == nil {
		return
	}
	ticker := time.NewTicker(time.Minute * time.Duration(wc.EtcdCacheConfig.UpdateCacheFilePeriod))
	defer ticker.Stop()
	flushCacheThreshold := wc.EtcdCacheConfig.FlushCacheThreshold
	events := make([]*Event, 0, flushCacheThreshold)
	var err error
	for {
		select {
		case <-ticker.C:
			err = wc.flushEventsToCacheFile(&events)
			events = events[0:0]
		case event := <-wc.updateCacheEventChan:
			events = append(events, event)
			if len(events) >= flushCacheThreshold {
				err = wc.flushEventsToCacheFile(&events)
				events = events[0:0]
			}
		case cacheCfg := <-wc.CacheCfgUpdateChan:
			if !cacheCfg.EnableEtcdCache {
				err = errors.New("etcd cache disabled, terminate update cache file task and exit")
			} else {
				ticker.Reset(time.Minute * time.Duration(cacheCfg.UpdateCacheFilePeriod))
				flushCacheThreshold = cacheCfg.FlushCacheThreshold
			}
		case <-clientExitCh:
			log.GetLogger().Infof("client exit, terminate update cache file task and exit")
			err = errors.New("client exit, terminate update cache file task and exit")
		}
		if err != nil {
			log.GetLogger().Warnf("update cache file periodically failed, delete cache file and exit")
			wc.deleteCacheFile()
			wc.mux.Lock()
			wc.EtcdCacheConfig.EnableEtcdCache = false
			wc.mux.Unlock()
			break
		}
	}
}

func (wc *EtcdWatchChan) flushEventsToCacheFile(eventsPtr *[]*Event) error {
	groupedEvents := make(map[int][]*Event)
	for _, event := range *eventsPtr {
		switch event.Type {
		case PUT:
			wc.partitionEvents(&groupedEvents, event)
		case DELETE:
			wc.partitionEvents(&groupedEvents, event)
		default:
			log.GetLogger().Infof("ignore unsupported event type: %v", event.Type)
		}
	}

	if len(*eventsPtr) > 0 {
		metaInfoPath := fmt.Sprintf("%s/%s", wc.cacheFilePath, PartitionCacheMetaInfo)
		metaInfo := &EtcdCacheMetaInfo{}
		content, err := os.ReadFile(metaInfoPath)
		if err != nil {
			return err
		}
		err = json.Unmarshal(content, metaInfo)
		if err != nil {
			return err
		}
		err = wc.createOrUpdatePartitionCacheFile(&groupedEvents, metaInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wc *EtcdWatchChan) startWatching(clientExitCh chan struct{}) {
	if clientExitCh == nil {
		return
	}
	err := errors.New("etcd cache is disable")
	if wc.EtcdCacheConfig.EnableEtcdCache {
		err = wc.DoDiffSync()
	}
	if err != nil && wc.initialRev == 0 {
		log.GetLogger().Warnf("%s, delete cache file and do full sync", err.Error())
		wc.deleteCacheFile()
		err = wc.DoFullSync()
		if err != nil {
			return
		}
	}
	wc.sendEvent(parseSync(time.Now(), wc.source))
	if wc.EtcdCacheConfig.EnableEtcdCache {
		go wc.updateCacheFilePeriodically(clientExitCh)
	}
	opts := []clientv3.OpOption{clientv3.WithPrevKV(), clientv3.WithRev(wc.initialRev + 1)}
	if wc.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	wch := clientv3.NewWatcher(wc.watcher.Client).Watch(wc.ctx, wc.key, opts...)
	if wch == nil {
		log.GetLogger().Errorf("failed to watch etcd ,watch chan is nil")
		wc.sendError(fmt.Errorf("watch chan is nil"))
		return
	}
	for {
		select {
		case v, ok := <-wch:
			if !ok {
				// If there is an error on server, the channel will return it before closed.
				log.GetLogger().Errorf("watch chan error")
				wc.sendError(fmt.Errorf("watch chan error"))
				return
			}
			if v.Err() != nil {
				log.GetLogger().Errorf("watch chan error: %s", v.Err())
				wc.sendError(v.Err())
				return
			}
			for _, event := range v.Events {
				evt := parseEvent(event, wc.source, wc.CellEnable)
				wc.sendEvent(evt)
				if wc.EtcdCacheConfig.EnableEtcdCache && (evt.Type == PUT || evt.Type == DELETE) {
					wc.mux.Lock()
					if wc.EtcdCacheConfig.EnableEtcdCache {
						wc.updateCacheEventChan <- evt
					}
					wc.mux.Unlock()
				}
			}
		case <-clientExitCh:
			log.GetLogger().Errorf("client exit, try to re-watch")
			return
		}
	}
}

func (wc *EtcdWatchChan) sendEvent(e *Event) {
	if len(wc.incomingEventChan) == incomingBufSize {
		// Probably caused by slow decoding, user not receiving fast, or other processing logic
		log.GetLogger().Warnf("fastWatcherSlowProcessing | bufEvent %d", incomingBufSize)
	}
	select {
	case wc.incomingEventChan <- e:
	case <-wc.ctx.Done():
	}
}

func (wc *EtcdWatchChan) sendError(err error) {
	select {
	case wc.errChan <- err:
	case <-wc.ctx.Done():
	}
}

// processEvent receives events from etcd watcher and sends results to resultChan
// or receives done message and ends processing
func (wc *EtcdWatchChan) processEvent(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case e, ok := <-wc.incomingEventChan:
			if !ok {
				break
			}
			// If ResultChan couldn't receive results fast enough, incomingEventChan(incomingBufSize events) will be blocked.
			select {
			case wc.ResultChan <- e:
			case <-wc.ctx.Done():
				return
			}
		case <-wc.ctx.Done():
			return
		}
	}
}

// Stop stop watching
func (wc *EtcdWatchChan) Stop() {
	if wc.cancel != nil {
		wc.cancel()
	}
}

// HandleCellPrefix -
func HandleCellPrefix(path string) string {
	if cellID == "" {
		return path
	}
	if strings.HasPrefix(path, "/cell") {
		return path
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return "/cell/" + cellID + path
}

// DeleteCellPrefix -
func DeleteCellPrefix(path string) string {
	if cellID == "" {
		return path
	}
	if !strings.HasPrefix(path, "/cell") {
		return ""
	}
	return strings.TrimPrefix(path, "/cell/"+cellID)
}
