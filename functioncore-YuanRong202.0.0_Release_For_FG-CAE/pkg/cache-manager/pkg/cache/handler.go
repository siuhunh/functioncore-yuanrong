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

// Package cache cache function deploy info
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/cache-manager/pb"
	"functioncore/pkg/cache-manager/pkg/server"
	"functioncore/pkg/cache-manager/pkg/status"
	"functioncore/pkg/cache-manager/pkg/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/strutil"
)

// CacheHandler represents handler of cache-manager
type CacheHandler struct {
	Deployer   server.Deployer
	etcdSynced uint32
	syncedObj  sync.Map
}

const (
	persistenceTimeout = 360 * time.Hour
	keepConnAliveTTL   = 3 * time.Second
	fileMode           = 0600
)

var (
	limitCodeSize int
)

func loadFromDisk(key interface{}, isLayer bool) (interface{}, int64, error) {
	objID, ok := key.(string)
	if !ok {
		return nil, 0, errors.New("invalid objID")
	}
	file := server.CachePath + "/" + utils.GenerateMD5(utils.ParseObjectID(objID, isLayer))
	enc, err := ioutil.ReadFile(file)
	if err != nil {
		log.GetLogger().Warnf("failed to read file, obj: %s, err: %s", objID, err.Error())
		return nil, 0, err
	}
	dec, err := localauth.Decrypt(strutil.BytesToString(enc))
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt, obj: %s, err: %s", objID, err.Error())
		return nil, 0, err
	}
	var codeCache pb.CacheCode
	if err = proto.Unmarshal(dec, &codeCache); err != nil {
		log.GetLogger().Errorf("failed to unmarshal, obj: %s, err: %s", objID, err.Error())
		return nil, 0, err
	}
	log.GetLogger().Infof("objID %s in disk, file %s, Checksum %s", objID, file, codeCache.Checksum)
	return server.CodeCache{
		CodeBuffer: codeCache.Buffer,
		Checksum:   codeCache.Checksum,
	}, int64(len(codeCache.Buffer)), nil
}

// WatcherUserFunction watch etcd user function path
func WatcherUserFunction(conf *config.CacheManagerConfig, stopCh <-chan struct{}) error {
	limitCodeSize = conf.LimitCodeSize
	if limitCodeSize == 0 {
		limitCodeSize = config.DefaultLimitCodeSize
	}
	log.GetLogger().Infof("limitCodeSize is %d", limitCodeSize)
	server.LruCacheMap = utils.NewLru(&server.CacheMap, utils.WithLRULoadFunc(loadFromDisk))

	if conf.Persistence.Enable == true {
		status.Set(status.Persistence)
		if conf.Persistence.PersistencePath != "" {
			server.CachePath = conf.Persistence.PersistencePath
		} else {
			server.CachePath = "/tmp"
		}
		if !server.EvsEnable {
			extractCache()
		}
	}
	client := etcd3.GetMetaEtcdClient()
	deployer, err := server.NewDeployer(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to new deployer: %s", err.Error())
		return err
	}
	c := &CacheHandler{Deployer: deployer}

	go c.startRunWatcher(client, stopCh)

	return nil
}

func (c *CacheHandler) startRunWatcher(client *etcd3.EtcdClient, stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Infof("cache-manager stopCh is nil")
		return
	}
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if client.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			watchChan := client.Watch(context.Background(), utils.WatchPathPrefixWithFunction, 0, true)
			go c.RunWatcher(watchChan, client, stopCh)
			ticker.Stop()
			return
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed and quits keep meta etcd conn alive task")
			}
			ticker.Stop()
			return
		}
	}
}

// RunWatcher run etcd watcher
func (c *CacheHandler) RunWatcher(watchChan *etcd3.EtcdWatchChan, client *etcd3.EtcdClient, stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Infof("cache-manager stopCh is nil")
		return
	}
	log.GetLogger().Infof("cache-manager succeed to run meta ETCD watcher")
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if ok {
				c.parseUserFunctionEvent(event)
				continue
			}
			log.GetLogger().Warnf("watch channel may be closed and try to reconnect")
			watchChan.Stop()
			log.GetLogger().Info("success stop client and watchChan, start to remake client")
			watchChan = client.Watch(context.Background(), utils.WatchPathPrefixWithFunction, 0, true)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed, return")
				return
			}
			log.GetLogger().Infof("received signal to quit")
			return
		}
	}
}

func (c *CacheHandler) parseUserFunctionEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		if atomic.LoadUint32(&c.etcdSynced) == 0 {
			c.handleSyncPut(event)
		} else {
			c.handlePutEvent(event)
		}
	case etcd3.DELETE:
		c.handleDeleteEvent(event)
	case etcd3.SYNCED:
		c.handleSynced()
	default:
		log.GetLogger().Infof("unknown event type: %d", event.Type)
	}
}

func (c *CacheHandler) handleSynced() {
	defer func() {
		atomic.StoreUint32(&c.etcdSynced, 1)
		log.GetLogger().Infof("etcd events are synced")
	}()
	if !server.EvsEnable {
		return
	}
	allFile := readAll(server.CachePath)
	for _, file := range allFile {
		if _, loaded := c.syncedObj.Load(file); loaded {
			continue
		}
		err := os.Remove(file)
		if err != nil {
			log.GetLogger().Warnf("failed to remove file %s, err: %s", file, err.Error())
		}
	}
	c.syncedObj = sync.Map{}
	return
}

func (c *CacheHandler) handleSyncPut(event *etcd3.Event) {
	key := etcdkey.AnonymizeTenantCommonEtcdKey(event.Key)
	log.GetLogger().Infof("handle sync put, key: %s", key)
	var info config.FunctionMetaInfo
	err := json.Unmarshal(event.Value, &info)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal function meta info %s: %s", key, err.Error())
		return
	}
	if !server.EvsEnable {
		return
	}
	updateFuncAndLayerCache(&info)
	updateFunction := c.needUpdateFile(info.S3MetaData.ObjectID, info.FuncMetaData.FunctionVersionURN,
		info.FuncMetaData.CodeSha512, false)
	updateLayer := c.needLayersUpdate(&info)
	if !updateFunction && !updateLayer {
		log.GetLogger().Infof("function %s file is in disk and it is not update", info.FuncMetaData.FunctionURN)
		return
	}
	err = c.Deployer.Deploy(&info)
	if err != nil {
		log.GetLogger().Errorf("failed to Deploy function: %s", err.Error())
		return
	}
}

func (c *CacheHandler) needLayersUpdate(info *config.FunctionMetaInfo) bool {
	for _, layer := range info.FuncMetaData.Layers {
		if layer == nil {
			continue
		}
		if c.needUpdateFile(layer.ObjectID, info.FuncMetaData.FunctionURN, layer.ETag, true) {
			return true
		}
	}
	return false
}

func (c *CacheHandler) needUpdateFile(objectID, urn, checksum string, isLayer bool) bool {
	fileName := utils.GenerateMD5(utils.ParseObjectID(objectID, isLayer))
	file := server.CachePath + "/" + fileName
	c.syncedObj.Store(file, struct{}{})
	_, err := os.Stat(file)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		log.GetLogger().Warnf("file %s is not exist, err: %s", file, err.Error())
		return true
	}
	err = os.Chmod(file, fileMode)
	if err != nil {
		log.GetLogger().Warnf("failed to change file mod, obj: %s, err: %s", objectID, err.Error())
		return true
	}
	enc, err := ioutil.ReadFile(file)
	if err != nil {
		log.GetLogger().Errorf("failed to read file, obj: %s, err: %s", objectID, err.Error())
		return true
	}
	dec, err := localauth.Decrypt(strutil.BytesToString(enc))
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt, obj: %s, err: %s", objectID, err.Error())
		return true
	}
	var codeCache pb.CacheCode
	if err = proto.Unmarshal(dec, &codeCache); err != nil {
		log.GetLogger().Errorf("failed to unmarshal, obj: %s, err: %s", objectID, err.Error())
		return true
	}
	if codeCache.Checksum != checksum {
		return true
	}
	key := urn + "-" + objectID
	server.GetObjSizeCacheInstance().UpdatePersistObj(key,
		server.FileInfo{
			ObjSize:  (int64)(len(codeCache.Buffer)),
			Checksum: checksum,
			Name:     fileName})
	server.LruCacheMap.AddCache(codeCache.ObjectID, server.CodeCache{
		CodeBuffer: codeCache.Buffer,
		Checksum:   codeCache.Checksum,
	}, int64(len(codeCache.Buffer)))
	return false
}

func (c *CacheHandler) handlePutEvent(event *etcd3.Event) {
	key := etcdkey.AnonymizeTenantCommonEtcdKey(event.Key)
	log.GetLogger().Infof("receive put event key: %s", key)
	var info config.FunctionMetaInfo
	err := json.Unmarshal(event.Value, &info)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal function meta info %s: %s", key, err.Error())
		return
	}
	if !server.EvsEnable && info.FuncMetaData.CodeSize > limitCodeSize {
		log.GetLogger().Infof("unsupported code size %d of funcKey %s", info.FuncMetaData.CodeSize, key)
		cleanPersistence(info.S3MetaData.ObjectID, false)
		server.LruCacheMap.DelCache(info.S3MetaData.ObjectID)
		return
	}
	updateFuncAndLayerCache(&info)
	err = c.Deployer.Deploy(&info)
	if err != nil {
		log.GetLogger().Errorf("failed to Deploy function: %s", err.Error())
		return
	}
}

func (c *CacheHandler) handleDeleteEvent(event *etcd3.Event) {
	key := etcdkey.AnonymizeTenantCommonEtcdKey(event.Key)
	log.GetLogger().Infof("receive delete event key: %s", key)
	var info config.FunctionMetaInfo
	err := json.Unmarshal(event.PrevValue, &info)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal function meta info: %s %s", key, err.Error())
		return
	}
	server.LruCacheMap.DelCache(info.S3MetaData.ObjectID)
	funcKey := info.FuncMetaData.FunctionVersionURN + "-" + info.S3MetaData.ObjectID
	server.GetObjSizeCacheInstance().DelPersistObj(funcKey)
	server.GetObjSizeCacheInstance().DeleteLayerCache(&info)
	cleanPersistence(info.S3MetaData.ObjectID, false)
	cleanPersistenceLayers(&info)
}

func extractCache() {
	allFile := readAll(server.CachePath)
	for _, data := range allFile {
		fileData, err := ioutil.ReadFile(data)
		if err != nil {
			log.GetLogger().Errorf("failed to read cache file data: %s", err.Error())
			continue
		}
		fileDatas, err := localauth.Decrypt(strutil.BytesToString(fileData))
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt cache file data: %s", err.Error())
			continue
		}
		cacheCode := &config.CacheCode{}
		err = json.Unmarshal(fileDatas, cacheCode)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal cache file data: %s", err.Error())
			continue
		}
		if time.Since(cacheCode.TimeStamp) > persistenceTimeout {
			err = os.Remove(data)
			if err != nil {
				log.GetLogger().Errorf("failed to remove timeout cache file: %s", err.Error())
				continue
			}
		}
		if server.LruCacheMap != nil {
			server.LruCacheMap.AddCache(cacheCode.ObjectID, server.CodeCache{
				CodeBuffer: cacheCode.Buffer,
				Checksum:   cacheCode.Checksum,
			}, int64(len(cacheCode.Buffer)))
		}
	}
}

func readAll(path string) []string {
	var allFile []string
	fileInfo, err := ioutil.ReadDir(path)
	if err != nil {
		log.GetLogger().Errorf("failed to read cache direction: %s, error: %s", path, err.Error())
		return allFile
	}
	for _, file := range fileInfo {
		realPath := path + "/" + file.Name()
		if file.IsDir() {
			continue
		}
		allFile = append(allFile, realPath)
	}
	return allFile
}

func cleanPersistence(objectID string, isLayer bool) {
	if status.Get() == status.Persistence {
		result := utils.ParseObjectID(objectID, isLayer)
		name := utils.GenerateMD5(result)
		err := os.Remove(server.CachePath + "/" + name)
		if err != nil {
			log.GetLogger().Warnf("failed to remove delete cache file: %s", err.Error())
			return
		}
	}
}

func cleanPersistenceLayers(info *config.FunctionMetaInfo) {
	if status.Get() == status.Persistence {
		for _, layer := range info.FuncMetaData.Layers {
			if layer == nil {
				continue
			}
			cleanPersistence(layer.ObjectID, true)
			server.LruCacheMap.DelCache(layer.ObjectID)
			layerKey := info.FuncMetaData.FunctionVersionURN + "-" + layer.ObjectID
			server.GetObjSizeCacheInstance().DelPersistObj(layerKey)
		}
	}
}

func updateFuncAndLayerCache(info *config.FunctionMetaInfo) {
	// layers in meta info have not code size
	funcKey := info.FuncMetaData.FunctionVersionURN + "-" + info.S3MetaData.ObjectID
	server.GetObjSizeCacheInstance().UpdatePersistObjSize(funcKey, (int64)(info.FuncMetaData.CodeSize))
	deletedlayers := server.GetObjSizeCacheInstance().GetNeedDeletedLayers(info)
	for key := range deletedlayers {
		cleanPersistence(key, true)
		server.LruCacheMap.DelCache(key)
	}
}
