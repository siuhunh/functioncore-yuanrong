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

// Package iamwatcher iam watcher
package iamwatcher

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	commontils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/utils"
)

// RunIAMTokenWatcher run iam token watcher
func RunIAMTokenWatcher(stopCh <-chan struct{}) {
	log.GetLogger().Infof("start to watch iam token from fg meta etcd")
	if stopCh == nil {
		log.GetLogger().Errorf("failed to watch iam token: stopCh is nil")
		return
	}

	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient == nil {
		log.GetLogger().Errorf("failed to watch iam token: fg meta client is nil")
		return
	}

	cache.InitExpiringIAMTokenMap()
	iamWatchChan := getIAMTokenWatchChan(metaClient)
	if iamWatchChan == nil {
		return
	}

	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Infof("iam token watcher received signal to quit")
				return
			}
		case event, ok := <-iamWatchChan.ResultChan:
			if ok {
				parseIAMEvent(event)
				continue
			}
			log.GetLogger().Warnf("the iam token channel received the result may by closed and try to reconnect")
			iamWatchChan.Stop()
			iamWatchChan = metaClient.Watch(context.Background(), utils.GetIAMTokenPrefix(), 0, true)
		}
	}
}

func updateWorkerAgency(tenantID string) {
	fetcher := podfetcher.GetPodFetcher()
	if fetcher == nil {
		log.GetLogger().Errorf("pod fetcher is nil")
		return
	}
	workerAgencyMap := cache.WorkerCache.GetWorkerAgencyMap()
	for _, workerAgency := range workerAgencyMap {
		if len(workerAgency.WorkerIns) == 0 {
			continue
		}
		if !commontils.CheckIAMTokenFunc(workerAgency.FuncMeta.Spec.Configuration.Runtime.Language,
			workerAgency.FuncMeta.Spec.Configuration.BusinessType) {
			continue
		}
		if workerAgency.FuncMeta.Spec.Configuration.TenantID != tenantID {
			continue
		}
		for _, instance := range workerAgency.WorkerIns {
			traceID := "update-agency-" + uuid.New().String()
			fetcher.SendUpdateAgencyInfoRequestToWorker(instance, traceID,
				instance.InstanceID, workerAgency.FuncMeta, instance.PodName)
		}
	}
}

func getIAMTokenWatchChan(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(utils.KeepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if metaClient.Client == nil {
				log.GetLogger().Errorf("failed to init meta etcd client")
				continue
			}
			iamWatchChan := metaClient.Watch(context.Background(), utils.GetIAMTokenPrefix(), 0, true)
			log.GetLogger().Infof("success to get iam token watch channel")
			ticker.Stop()
			return iamWatchChan
		}
	}
}

func parseIAMEvent(event *etcd3.Event) {
	if !isIAMEvent(event.Key) {
		return
	}

	tenantID, err := cache.ParseIAMTokenETCDKey(event.Key)
	if err != nil {
		log.GetLogger().Errorf("invalid iam token key: %s, err: %s", event.Key, err.Error())
		return
	}

	switch event.Type {
	case etcd3.PUT:
		log.GetLogger().Infof("iam token add event, key: %s", event.Key)
		var tokenSalt cache.TokenSalt
		if err = json.Unmarshal(event.Value, &tokenSalt); err != nil {
			log.GetLogger().Errorf("invalid tokenSalt value of: %s, err: %s", event.Key, err.Error())
			return
		}
		token, ok := utils.ParseIAMToken(event.Key, tokenSalt.Token)
		if !ok {
			return
		}
		tokenSalt.Token = token
		if err = cache.PutExpiringIAMTokenMap(tenantID, &tokenSalt); err != nil {
			log.GetLogger().Warnf("failed to put iam token into map, tenantID: %s, error: %s", tenantID, err.Error())
			return
		}
		log.GetLogger().Infof("succeed to put iam token into map, tenantID: %s", tenantID)
		updateWorkerAgency(tenantID)
	case etcd3.DELETE:
		log.GetLogger().Infof("iam token delete event, key: %s", event.Key)
		cache.DeleteIAMToken(tenantID)
		log.GetLogger().Infof("succeed to delete iam token from map, tenantID: %s", tenantID)
	default:
		log.GetLogger().Infof("unknown iam token event type: %d", event.Type)
	}
}

// key: /yr/iam/token/new/{clusterID}/{tenantID}
func isIAMEvent(key string) bool {
	return strings.HasPrefix(key, utils.GetIAMTokenPrefix())
}
