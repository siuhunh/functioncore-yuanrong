/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package exceptionhandler handling exceptions
package exceptionhandler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/state/instancemanager"
	"functioncore/pkg/state-manager/utils"
)

var (
	mutex     sync.RWMutex
	migrating bool
)

// InitClusterChecker init cluster status checker
func InitClusterChecker(stopChan <-chan struct{}) error {
	metaClient := etcd3.GetMetaEtcdClient()
	go watchClusterStatus(metaClient, stopChan)
	return nil
}

func watchClusterStatus(metaClient *etcd3.EtcdClient, stopChan <-chan struct{}) {
	watchChan := createClusterStatusWatcher(metaClient)
	log.GetLogger().Infof("begin to watch cluster status, watchChan is %+v", watchChan)
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect")
				ctx := context.WithValue(context.Background(), etcd3.CellEnable, true)
				watchChan = metaClient.Watch(ctx, clusterstatusutil.ClusterMetadataKey, 0, false)
				continue
			}
			err := processClusterStatusEvent(event)
			if err != nil {
				log.GetLogger().Warnf("failed to process cluster status event %v: %s", event, err.Error())
				continue
			}
		case _, ok := <-stopChan:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed")
			}
			return
		}
	}
}

func createClusterStatusWatcher(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if metaClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, true)
			watchChan := metaClient.Watch(ctx, clusterstatusutil.ClusterMetadataKey, 0, false)
			ticker.Stop()
			return watchChan
		}
	}
}

func processClusterStatusEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		clusterStatusMap, err := parseClusterStatusInfo(event)
		if err != nil {
			log.GetLogger().Errorf("failed to parse cluster status meta info, err:%s", err.Error())
			return errors.New("invalid cluster status event value")
		}
		clusterstatusutil.ClusterStatusCache.UpdateClusterStatusMap(clusterStatusMap)
		mutex.RLock()
		defer mutex.RUnlock()
		if migrating {
			log.GetLogger().Infof("The stateIDs of this cluster is migrating, ignore flowouting event")
			return nil
		}
		go doReleaseStateIDInUpgradingCluster()
		go doReBalanceStateIDWhenOtherClusterFlowIn()
	default:
		log.GetLogger().Warnf("unsupported event type: %d, event value %s", event.Type, event.Value)
	}
	return nil
}

func parseClusterStatusInfo(event *etcd3.Event) (
	map[string]clusterstatusutil.ClusterFlowState, error) {
	var clusterStatusMap map[string]clusterstatusutil.ClusterFlowState

	err := json.Unmarshal(event.Value, &clusterStatusMap)
	if err != nil {
		log.GetLogger().Errorf("unmarshal cluster status info with key: %s, error: %s", event.Key, err.Error())
		return clusterStatusMap, err
	}
	log.GetLogger().Infof("cluster status event key %s, keyType %d, value %v in etcd",
		event.Key, event.Type, clusterStatusMap)
	return clusterStatusMap, nil
}

func doReBalanceStateIDWhenOtherClusterFlowIn() {
	target := clusterstatusutil.GetOtherFlowiningCluster()
	if target == "" {
		return
	}
	if !utils.IsClusterInCurrentCell(target) {
		log.GetLogger().Infof("flowining cluster %s is not in current cell, skip", target)
		return
	}
	mutex.Lock()
	migrating = true
	mutex.Unlock()
	log.GetLogger().Infof("other cluster is flowining, begin to re-balance stateID, cluster: %s", target)
	instancemanager.ReBalance(clusterstatusutil.GetCurrentClusterID(), target)
	mutex.Lock()
	migrating = false
	mutex.Unlock()
}

func doReleaseStateIDInUpgradingCluster() {
	if clusterstatusutil.IsCurrentClustersStatusFlowouting() {
		mutex.Lock()
		migrating = true
		mutex.Unlock()
		log.GetLogger().Infof("Begin to migrate stateID, status is flowouting")
		stateIDToBeRelease := instancemanager.GetStateIDListOfCluster()
		if len(stateIDToBeRelease) == 0 {
			log.GetLogger().Infof("No need to migrate stateID, because the size of stateIDToBeRelease is 0")
			mutex.Lock()
			migrating = false
			mutex.Unlock()
			return
		}
		for _, stateID := range stateIDToBeRelease {
			result := instancemanager.MigrateStateIDToOtherCluster(stateID)
			if result {
				instancemanager.DecrStateCount(stateID)
			}
			log.GetLogger().Infof("Migrate stateID %s to other cluster, result %t", stateID, result)
		}
		mutex.Lock()
		migrating = false
		mutex.Unlock()
	}
}
