/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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

// Package main -
package main

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

// InitBridgeManager init bridge manager
func InitBridgeManager() error {
	log.GetLogger().Infof("[bridgeplugin] Begin init watch.")
	if err := startWatch(); err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to watch bridge function meta in etcd when init, err: %s",
			err.Error())
		return err
	}
	bridgeCache := cache.Instance()
	err := bridgeCache.LoadCacheFromEtcd()
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to load cache from etcd when init, err: %s", err.Error())
		return err
	}
	stateWorkers := loadStateWorkersFromEtcd()
	clearIllegalBridgeIDRef(stateWorkers)
	bridgeCache.LoadClientCiteBridgeInfo()
	return nil
}

func clearIllegalBridgeIDRef(stateWorkers map[string]map[string]string) {
	bridgeCache := cache.Instance()
	bridgeIns := &bgtypes.BridgeInstance{}
	traceID := uuid.New().String()
	bridgeCache.StateIDInfoCache.Range(func(stateIDIf, stateInfoIf any) bool {
		stateID, ok := stateIDIf.(string)
		if !ok {
			log.GetLogger().Warnf("assert stateid failed for %s while check loaded stateid validity", stateID)
			return true
		}
		newStateWorker := stateWorkers[stateID]
		if newStateWorker == nil {
			log.GetLogger().Infof("stateid %s is invalid, release it", stateID)
			err := action.ReleaseBridgeWhenSuicide(bridgeIns, stateID, traceID)
			if err != nil {
				log.GetLogger().Warnf("release stateid %s failed while check loaded stateid validity, "+
					"err %s", stateID, err.Error())
				return true
			}
		} else {
			stateInfo, ok := stateInfoIf.(*cache.StateIDInfo)
			if !ok {
				log.GetLogger().Warnf("assert cache.StateIDInfo of stateid %s failed "+
					"while check loaded stateid validity", stateID)
				return true
			}
			oldCluster := stateInfo.BelongCluster
			newCluster := newStateWorker["cluster"]
			if len(newCluster) != 0 && newCluster != oldCluster {
				log.GetLogger().Infof("clusterid changed from %s to %s, update to etcd", oldCluster, newCluster)
				stateInfo.BelongCluster = newCluster
				err := bridgeCache.StoreSpecificStateIDInfoToEtcd(stateID, traceID)
				if err != nil {
					log.GetLogger().Warnf("store stateInfo of stateid %s to etcd failed "+
						"while check loaded stateid validity, err: %s", stateID, err.Error())
					stateInfo.BelongCluster = oldCluster
					return true
				}
			}
		}
		return true
	})
}

func loadStateWorkersFromEtcd() map[string]map[string]string {
	stateWorkers := make(map[string]map[string]string)
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Error("get stateWorkers from etcd failed, etcd client is nil")
		return stateWorkers
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := stateClient.GetResponse(defaultEtcdCtx, utils.StateWorkersPrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("get stateWorkers from etcd failed: %s", err.Error())
		return stateWorkers
	}
	for _, kv := range resp.Kvs {
		stateID, err := utils.ParseStateIDFromStateWorkersKey(string(kv.Key))
		if err != nil {
			continue
		}
		stateWorker := make(map[string]string)
		err = json.Unmarshal(kv.Value, &stateWorker)
		if err != nil {
			continue
		}
		stateWorkers[stateID] = stateWorker
	}
	return stateWorkers
}

// ApplyStateKey method provided in plugin for distributing stateKey to caller
func ApplyStateKey(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
	return action.ApplyStateKey(client, traceID)
}

// ReleaseBridge method provided in plugin for release bridgeFunc
// and determine whether it need to delete bridge instance or not
func ReleaseBridge(client *bgtypes.ClientInstance, traceID string) (bool, error) {
	return action.ReleaseBridge(client, false, traceID)
}

func startWatch() error {
	stopCh := signals.WaitForSignal()
	metaReactor := reactor.NewEventDrive()
	return metaReactor.Run(stopCh)
}
