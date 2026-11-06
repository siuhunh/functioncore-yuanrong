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

// Package config defines general configurations for function task.
package config

import "time"

// Config defines configurations for function task.
type Config struct {
	MaxSingleWaitFutureCnt int32         `json:"max_wait_future_cnt"`
	ResolveDeadlock        bool          `json:"resolve_deadlock"`
	PersistentState        bool          `json:"persistent_state"`
	PersistentFuture       bool          `json:"persistent_future"`
	CallTimeout            int64         `json:"call_timeout"`
	FutureTTL              time.Duration `json:"future_ttl"`
	ExternalFutureTTL      time.Duration `json:"external_future_ttl"`
	InitStateClient        bool          `json:"init_state_client"`
	MaxNumsOfStateID       int           `json:"max_nums_of_state_id"`
	EnableMetaEtcdCache    bool          `json:"enable_meta_etcd_cache"`
	EtcdDiffSyncTimeout    int           `json:"etcd_diff_sync_timeout"`
	UpdateCacheFilePeriod  int           `json:"update_cache_file_period"`
	CacheUpdateBufSize     int           `json:"cache_update_buf_size"`
	FlushCacheThreshold    int           `json:"flush_cache_threshold"`
	CacheFilePartitionSize int           `json:"cache_file_partition_size"`
}

// Conf is global configuration used by function task.
var Conf = DefaultConfig()

// default configs.
const (
	DefaultMaxSingleWaitFutureCnt = 1000
	DefaultCallTimeout            = 60 * time.Second
	defaultResolveDeadlock        = true
	DefaultFutureTTL              = 15 * time.Minute
	DefaultExternalFutureTTL      = 24 * 3600 // the external future will be deleted after 24 * 3600 seconds.
	DefaultMaxNumsOfStateID       = 10000     // max nums of stateID in every serviceID
	DefaultEtcdDiffSyncTimeout    = 10        // DefaultEtcdDiffSyncTimeout
	DefaultUpdateCacheFilePeriod  = 15        // DefaultUpdateCacheFilePeriod
	DefaultCacheUpdateBufSize     = 2000      // DefaultCacheUpdateBufSize
	DefaultFlushCacheThreshold    = 1600      // DefaultFlushCacheThreshold
	DefaultCacheFilePartitionSize = 20        // DefaultCacheFilePartitionSize
)

// DefaultConfig returns default configuration.
func DefaultConfig() *Config {
	return &Config{
		MaxSingleWaitFutureCnt: DefaultMaxSingleWaitFutureCnt,
		CallTimeout:            int64(DefaultCallTimeout),
		ResolveDeadlock:        defaultResolveDeadlock,
		FutureTTL:              DefaultFutureTTL,
		ExternalFutureTTL:      DefaultExternalFutureTTL,
		MaxNumsOfStateID:       DefaultMaxNumsOfStateID,
	}
}
