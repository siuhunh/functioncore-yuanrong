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
	"errors"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/resourceusage"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/scalehandler"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"functioncore/pkg/state-manager/config"
	statemanagerutils "functioncore/pkg/state-manager/utils"
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
	go checkFuncInstances()

	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}

	stopCh := signals.WaitForSignal()

	// start calculate of scale
	scaleHandler := scalehandler.GetHandler().
		WithSyncPeriod(cfg.ScaleData.SyncPeriod).
		WithStopCh(stopCh).
		Run()

	// start get data from redis
	resourceusage.GetHandler().
		WithStopCh(stopCh).
		WithClusterID(utils.ClusterID).
		WithTickerTime(cfg.ScaleData.TickerTime).
		WithMonitoringClient(statemanagerutils.GetRedisCmd()).
		RegisterRefreshCacheFunc(scaleHandler.RefreshResourceMetricCache).
		HandlerResourceUsage()

	return nil
}

func checkFuncInstances() {
	stopCh := signals.WaitForSignal()
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil, do not check function instances")
		return
	}

	if err := loadClusterInfoFromEtcd(); err == nil {
		checkClientInstances()
		checkBridgeInstances()
	}

	checkInterval := utils.GetCheckInstancesInterval()
	timer := time.NewTicker(time.Duration(checkInterval) * time.Second)
	for {
		select {
		case <-timer.C:
			if isNodeLoaded() {
				checkClientInstances()
				checkBridgeInstances()
			}
		case <-stopCh:
			timer.Stop()
			return
		}
	}
}

func isNodeLoaded() bool {
	if nodemgr.Instance().GetNode() == "" {
		if err := loadClusterInfoFromEtcd(); err != nil {
			return false
		}
	}
	return true
}

func checkBridgeInstances() {
	log.GetLogger().Infof("start to check bridge instances")
	// outer key is funcSvc, inner key is stateID, inner value is podName
	bridgeInsMap := make(map[string]map[string]string)
	cache.Instance().StateIDInfoCache.Range(func(key, value interface{}) bool {
		stateID, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to convert key interface of StateIDInfoCache to string, key type: %T", key)
			return true
		}
		bridgeInstance, ok := value.(*cache.StateIDInfo)
		if !ok {
			log.GetLogger().Errorf("failed to convert value interface of StateIDInfoCache to StateIDInfo, "+
				"key(stateID): %s, value type: %T", key, value)
			return true
		}
		tenantID := utils.GetTenantIDFromURN(bridgeInstance.BridgeFuncURN)
		funcName := utils.GetComplexFuncNameFromURN(bridgeInstance.BridgeFuncURN)
		if tenantID == "" || funcName == "" {
			return true
		}
		funcSvc := urnutils.CrName(tenantID, funcName, utils.BridgeVersionLatest)
		if _, ok = bridgeInsMap[funcSvc]; !ok {
			queryIns, err := nodemgr.Instance().GetFuncInstances(funcSvc)
			if err != nil {
				log.GetLogger().Errorf("failed to get bridge function instances from worker-manager, funcSvc: %s, err: %s",
					funcSvc, err.Error())
				return true
			}
			if queryIns.BridgePodsInfo == nil {
				log.GetLogger().Infof("bridge pods info queried from worker-manager is nil")
				return true
			}
			bridgeInsMap[funcSvc] = queryIns.BridgePodsInfo
		}
		if podName, ok := bridgeInsMap[funcSvc][stateID]; ok {
			if podName != bridgeInstance.PodName {
				updateBridgeInstance(stateID, podName, bridgeInstance)
			}
		}
		return true
	})
}

func updateBridgeInstance(stateID string, newPodName string, bridgeInstance *cache.StateIDInfo) {
	traceID := uuid.New().String()
	log.GetLogger().Infof("bridge instance %s in local cache is incorrect, start to update to %s, stateID: %s, "+
		"traceID: %s", bridgeInstance.PodName, newPodName, stateID, traceID)

	lockIf, _ := cache.Instance().StateIDLockMap.LoadOrStore(stateID, &sync.Mutex{})
	lock, ok := lockIf.(*sync.Mutex)
	if !ok {
		log.GetLogger().Errorf("failed to convert value interface of StateIDLockMap to Mutex, key(stateID): %s, "+
			"value type: %T, traceID: %s", stateID, lockIf, traceID)
		return
	}
	lock.Lock()
	defer lock.Unlock()

	oldPodName := bridgeInstance.PodName
	bridgeInstance.PodName = newPodName
	cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
	if err := cache.Instance().StoreSpecificStateIDInfoToEtcd(stateID, traceID); err != nil {
		log.GetLogger().Errorf("failed to store StateIDInfo of stateID %s to etcd, traceID: %s, err: %s",
			stateID, traceID, err.Error())
		bridgeInstance.PodName = oldPodName
		cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
		return
	}
	log.GetLogger().Infof("succeed to update bridge instance of stateID %s, traceID: %s", stateID, traceID)
}

func checkClientInstances() {
	log.GetLogger().Infof("start to check client instances")
	// outer key is funcSvc, inner key is podName
	clientInsMap := make(map[string]map[string]struct{})
	cache.Instance().ClientPodClientInstCache.Range(func(key, value interface{}) bool {
		clientInstance, ok := value.(*bgtypes.ClientInstance)
		if !ok {
			log.GetLogger().Errorf("failed to convert value interface of ClientPodClientInstCache to ClientInstance, "+
				"key: %s, value type: %T", key, value)
			return true
		}
		fullName := fmt.Sprintf("%s@%s", clientInstance.FuncInfo.ServiceID, clientInstance.FuncName)
		funcSvc := urnutils.CrName(clientInstance.TenantID, fullName, clientInstance.Version)
		if _, ok = clientInsMap[funcSvc]; !ok {
			queryIns, err := nodemgr.Instance().GetFuncInstances(funcSvc)
			if err != nil {
				log.GetLogger().Errorf("failed to get client function instances from worker-manager, funcSvc: %s, err: %s",
					funcSvc, err.Error())
				return true
			}
			podsMap := make(map[string]struct{}, len(queryIns.PodsName))
			for _, podName := range queryIns.PodsName {
				podsMap[podName] = struct{}{}
			}
			clientInsMap[funcSvc] = podsMap
		}
		if _, ok = clientInsMap[funcSvc][clientInstance.PodName]; !ok {
			traceID := uuid.New().String()
			log.GetLogger().Infof("client instance %s does not exist in queried instances from worker-manager, "+
				"start to clear local cache, traceID: %s", clientInstance.PodName, traceID)
			if _, err := action.ProcessRelatedInfo(clientInstance, true, traceID); err != nil {
				log.GetLogger().Errorf("failed to clear client instance %s in local cache, traceID: %s, err: %s",
					clientInstance.PodName, traceID, err.Error())
				return true
			}
			log.GetLogger().Infof("succeed to clear client instance %s in local cache, traceID: %s",
				clientInstance.PodName, traceID)
		}
		return true
	})
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

func loadClusterInfoFromEtcd() error {
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || client.Client == nil {
		errMsg := "failed to get router etcd client"
		log.GetLogger().Errorf(errMsg)
		return errors.New(errMsg)
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := client.GetResponse(defaultEtcdCtx, utils.WorkerManagerInstancePrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to get worker-manager instances from etcd, err: %s", err.Error())
		return err
	}
	for _, kv := range resp.Kvs {
		clusterID, nodeIP, err := utils.ParseWorkerManagerClusterInfoFromKey(string(kv.Key))
		if err != nil {
			continue
		}
		if statemanagerutils.GetClusterID() != clusterID {
			continue
		}
		nodemgr.Instance().AddNode(nodeIP)
	}
	return nil
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

// HotLoadConfig hot load config
func HotLoadConfig(bridgeManagerConfig *bgtypes.BridgeManagerConfig) error {
	if bridgeManagerConfig == nil {
		return fmt.Errorf("bridgeManagerConfig is nil")
	}
	cache.UpdatePullInstanceTimeSpan(bridgeManagerConfig.PullInstanceTimeSpan)
	return nil
}

func startWatch() error {
	stopCh := signals.WaitForSignal()
	metaReactor := reactor.NewEventDrive()
	return metaReactor.Run(stopCh)
}
