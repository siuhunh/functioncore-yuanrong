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

package metadata

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/config"
)

const (
	etcdPathPrefix = "/functioncore/pkg/state-manager/scheduler/business/yrk/"
	etcdSyncPeriod = 30 * time.Second
)

// InitSync Initialize data synchronization
func InitSync(conf *config.FConfig) error {
	var err error
	// Get map from etcd firstly
	affinityTable.Lock()
	affinityTable.table, err = get(etcdPathPrefix)
	affinityTable.Unlock()
	if err != nil {
		return err
	}
	log.GetLogger().Infof("succeed to sync metadata")
	go syncAffinityTable()
	return nil
}

// syncAffinityTable sync affinityTable to etcd periodic
func syncAffinityTable() {
	ticker := time.NewTicker(etcdSyncPeriod)
	for range ticker.C {
		targetTable := make(map[string]map[string]struct{}, len(affinityTable.table))
		affinityTable.RLock()
		// Copy from affinityTable to targetTable
		for key, m := range affinityTable.table {
			targetMap := make(map[string]struct{}, len(m))
			for k, v := range m {
				targetMap[k] = v
			}
			targetTable[key] = targetMap
		}
		affinityTable.RUnlock()
		if err := put(etcdPathPrefix, targetTable); err != nil {
			log.GetLogger().Errorf("sync metadata error: %s", err.Error())
			return
		}
	}
}

func get(prefix string) (map[string]map[string]struct{}, error) {
	recMap, err := listen(prefix)
	if err != nil {
		log.GetLogger().Errorf("get data from etcd error: %s", err.Error())
		return nil, err
	}

	res := make(map[string]map[string]struct{}, len(recMap))
	for k, v := range recMap {
		m := map[string]struct{}{}
		ele := strings.Split(k, "/")
		key := ele[len(ele)-1]
		if err := json.Unmarshal(v, &m); err != nil {
			log.GetLogger().Errorf("unmarshal json data error: %s", err.Error())
			return nil, err
		}
		res[key] = m
	}

	return res, nil
}

func put(prefix string, m map[string]map[string]struct{}) error {
	for k, v := range m {
		keyPath := prefix + k
		data, err := json.Marshal(v)
		if err != nil {
			log.GetLogger().Errorf("marshal map data error: %s", err.Error())
			return err
		}
		stateClient := etcd3.GetStateEtcdClient()
		if stateClient == nil || stateClient.Client == nil {
			log.GetLogger().Warnf("failed to init state ETCD client")
			return nil
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		if err := stateClient.Put(defaultEtcdCtx, keyPath, string(data)); err != nil {
			log.GetLogger().Errorf("put data to etcd error: %s", err.Error())
			return err
		}
	}
	return nil
}

func listen(prefix string) (map[string][]byte, error) {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("failed to init state ETCD client")
		return nil, nil
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	data, err := stateClient.GetResponse(defaultEtcdCtx, prefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("list etcd prefix(%s) err: %s", prefix, err.Error())
		return nil, err
	}
	res := make(map[string][]byte, len(data.Kvs))
	for _, kv := range data.Kvs {
		key := string(kv.Key)
		value := kv.Value
		res[key] = value
	}
	return res, nil
}
