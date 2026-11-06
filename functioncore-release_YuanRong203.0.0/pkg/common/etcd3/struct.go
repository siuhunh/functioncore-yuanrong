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
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type restartCallback func(etcdClient *EtcdClient)

// EtcdClient etcd client struct
type EtcdClient struct {
	Client        *clientv3.Client
	config        *ClientConfig
	etcdTimer     *time.Timer
	isAlarmEnable bool
	// router etcd status lost contact after defaultEtcdLostContactTime, true is healthy, false is unhealthy
	etcdStatusAfterLostContact bool
	etcdStatusNow              uint32
	stopChan                   chan struct{}
	stopCh                     <-chan struct{}
	clientExitCh               chan struct{}
	rwMutex                    sync.RWMutex
	operateETCD                operateETCDFunc
	redoFailedOperations       redoOperationsFunc
	etcdType                   EtcdType
	abnormalContinuouslyTimes  int
	restartCallback            restartCallback
	DowngradesEtcdClient       *EtcdClient
}

// ClientConfig -
type ClientConfig struct {
	ServerList []string `json:"servers" valid:",optional"`
	User       string   `json:"user" valid:",optional"`
	Password   string   `json:"password" valid:",optional"`
	CaFile     string   `json:"cafile,omitempty" valid:",optional"`
	CertFile   string   `json:"certfile,omitempty" valid:",optional"`
	KeyFile    string   `json:"keyfile,omitempty" valid:",optional"`
	SslEnable  bool     `json:"sslEnable,omitempty"`
}

// EtcdCacheConfig etcd cache config
type EtcdCacheConfig struct {
	EnableEtcdCache        bool `json:"enable_meta_etcd_cache"`
	UpdateCacheFilePeriod  int  `json:"update_cache_file_period"`
	FlushCacheThreshold    int  `json:"flush_cache_threshold"`
	EtcdDiffSyncTimeout    int  `json:"etcd_diff_sync_timeout"`
	CacheUpdateBufSize     int  `json:"cache_update_buf_size"`
	CacheFilePartitionSize int  `json:"cache_file_partition_size"`
}

// EtcdWatchChan implements watch.Interface.
type EtcdWatchChan struct {
	incomingEventChan    chan *Event
	updateCacheEventChan chan *Event
	ResultChan           chan *Event
	errChan              chan error
	watcher              *EtcdClient
	key                  string
	cacheFilePath        string
	initialRev           int64
	recursive            bool
	CellEnable           bool
	ctx                  context.Context
	cancel               context.CancelFunc
	mux                  *sync.Mutex
	EtcdCacheConfig      *EtcdCacheConfig
	CacheCfgUpdateChan   chan *EtcdCacheConfig
	source               EtcdType
}

// ETCDAuth contain etcd User and Passwd
type ETCDAuth struct {
	User      string
	Passwd    []byte
	SslEnable bool
}

// EtcdCtxInfo etcd context info
type EtcdCtxInfo struct {
	Ctx    context.Context
	Cancel context.CancelFunc
}

type etcdInitParam struct {
	metaEtcdConfig         *ClientConfig
	routerEtcdConfig       *ClientConfig
	caeMetaEtcdConfig      *ClientConfig
	caeRouterEtcdConfig    *ClientConfig
	metaEtcdProxyConfig    *ClientConfig
	caeMetaEtcdProxyConfig *ClientConfig
	stopCh                 <-chan struct{}
	enableAlarm            bool
}

// EtcdCache the cache object of etcd events
type EtcdCache struct {
	Events map[string]*Event
}

// EtcdCacheMetaInfo the cache mate info of etcd events
type EtcdCacheMetaInfo struct {
	Rev           int64
	Version       string
	PartitionSize int
	MD5s          map[string]string
}
