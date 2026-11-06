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

// Package cache defines instance cache
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types"
)

const (
	redisPersistenceTTL = 15 * time.Minute
	redisRetryTimes     = 3
	redisRetryInterval  = 100 * time.Millisecond
	redisPersistenceKey = "/sn/autosacler/persistence"
	queryInstancesURL   = "/worker-manager/v1/functions/instances"
	defaultVersion      = "0"
)

var (
	redisAlarmCount = 0
)

// ASPersistence set Persistent data of instances
type ASPersistence struct {
	// Version instances struct version
	Version   string                           `json:"version"`
	Timestamp int64                            `json:"timestamp"`
	Instances map[string]*config.InstanceCache `json:"instances"`
}

func (asp *ASPersistence) updateFuncInsCacheFromRedis() {
	for k, v := range asp.Instances {
		GetInstance().FunctionInstanceCache.Store(k, v)
	}
}

func (asp *ASPersistence) setInstances() {
	asp.Instances = make(map[string]*config.InstanceCache)
	GetInstance().FunctionInstanceCache.Range(func(key, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to string: %v", value)
			return false
		}
		v, ok := value.(*config.InstanceCache)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to InstanceCache: %v", value)
			return false
		}
		asp.Instances[k] = v
		return true
	})
}

// SaveInsCacheToRedis save FunctionInstanceCache to redis
func SaveInsCacheToRedis() {
	redisPersistenceVersion := defaultVersion
	conf, err := config.GetConfig()
	if err == nil {
		redisPersistenceVersion = conf.RedisPersistenceVersion
	} else {
		log.GetLogger().Warnf("failed to get autoscaler config, error: %s", err.Error())
	}
	asp := ASPersistence{
		Version:   redisPersistenceVersion,
		Timestamp: time.Now().UnixMilli(),
	}
	asp.setInstances()

	aspBytes, err := json.Marshal(asp)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal autoscaler persistence, error: %s", err.Error())
		return
	}

	if ok := redisclient.SetWithRetry(redisPersistenceKey, aspBytes, redisPersistenceTTL); ok {
		redisAlarmCount = 0
		alarm.GetClient().ClearAlarm(alarm.RedisConnection)
		return
	}
	redisAlarmCount++
	if redisAlarmCount >= redisRetryTimes {
		alarm.GetClient().ReportAlarm(alarm.RedisConnection,
			fmt.Sprintf("error msg=%s,", "failed to save ins cache to redis"))
	}
}

// RecoverPersistence -
func RecoverPersistence() {
	log.GetLogger().Infof("started to recover FuncInsCache from redis")
	redisPersistenceVersion := defaultVersion
	conf, err := config.GetConfig()
	if err == nil {
		redisPersistenceVersion = conf.RedisPersistenceVersion
	} else {
		log.GetLogger().Warnf("failed to get autoscaler config, error: %s", err.Error())
	}
	var asp ASPersistence
	if redisclient.GetRedisCmd() == nil {
		log.GetLogger().Warnf("autoscaler redis is not ready")
		UpdateFuncInsCache()
		return
	}
	aspJSON, err := redisclient.GetRedisCmd().Get(context.TODO(), redisPersistenceKey).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to get persistence from redis, errors: %s", err.Error())
		UpdateFuncInsCache()
		return
	}
	if err = json.Unmarshal([]byte(aspJSON), &asp); err != nil {
		log.GetLogger().Warnf("failed to unmarshal aspJSON, %s", err.Error())
		UpdateFuncInsCache()
		return
	}
	if asp.Version != redisPersistenceVersion {
		UpdateFuncInsCache()
		return
	}
	asp.updateFuncInsCacheFromRedis()
	UpdateFuncInsCache()
	log.GetLogger().Infof("finished to recover FuncInsCache from redis")
}

func queryInstancesFromWmByIP(funcSvc string, IP string) (types.QueryInstanceResponse, error) {
	u, err := url.Parse(fmt.Sprintf("%s%s", utils.GetWorkerManagerBaseURLByIP(IP), queryInstancesURL))
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to parse url, %s", err.Error())
	}
	params := url.Values{}
	params.Add(constants.FuncSvcID, funcSvc)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to create request, %s", err.Error())
	}
	req.URL.RawQuery = params.Encode()
	utils.FillInWorkerManagerRequestHeaders(req)
	resp, err := utils.GetWorkerManagerGetInstanceClient().Do(req)
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to send request to worker-manager, %s",
			err.Error())
	}
	defer closeResponse(resp)
	if resp != nil && resp.StatusCode != http.StatusOK {
		if resp.Body != nil {
			if respBody, err := ioutil.ReadAll(resp.Body); err == nil {
				return types.QueryInstanceResponse{}, fmt.Errorf("resp code: %d, resp body: %s",
					resp.StatusCode, respBody)
			}
		}
		return types.QueryInstanceResponse{}, fmt.Errorf("resp code: %d", resp.StatusCode)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to read resp body, %s", err.Error())
	}
	var queryInsResp types.QueryInstanceResponse
	if err = json.Unmarshal(respBytes, &queryInsResp); err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to unmarshal resp body, %s", err.Error())
	}
	return queryInsResp, nil
}

// UpdateFuncInsCache update FunctionMetaCache
func UpdateFuncInsCache() {
	GetInstance().FunctionMetaCache.Range(func(key, value interface{}) bool {
		k, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to string: %v", value)
			return true
		}
		updateFuncInsCacheBySvc(k)
		return true
	})
	log.GetLogger().Info("succeed to update funcInsCache")
}

func updateFuncInsCacheBySvc(funcSvc string) {
	totalCount := 0
	insCache := &config.InstanceCache{}
	for _, cluster := range clustermgr.GetClusterMgr().GetAllAvailableCluster() {
		workerManagerIP := cluster.ActiveNodes.IP
		queryInsResp, err := queryInstancesFromWmByIP(funcSvc, workerManagerIP)
		if err != nil {
			log.GetLogger().Errorf("failed to query funcInsCache for worker manager node: %s, "+
				"funcSvcID: %s, error: %s", workerManagerIP, funcSvc, err.Error())
			continue
		}
		log.GetLogger().Debugf("funcSvcID: %s, query instance response: %+v", funcSvc, queryInsResp)
		if queryInsResp.Code != http.StatusOK {
			log.GetLogger().Errorf("code of queryInstanceResponse is not OK "+
				"for worker manager node: %s, funcSvcID: %s, code: %s, message: %s",
				workerManagerIP, funcSvc, queryInsResp.Code, queryInsResp.Message)
			continue
		}
		clusterInsCache := config.ClusterInsCache{}
		clusterInsCache.ClusterID = queryInsResp.Instances.ClusterID
		clusterInsCache.Count = len(queryInsResp.Instances.PodsName)
		totalCount += clusterInsCache.Count
		insCache.ClusterInsCache = append(insCache.ClusterInsCache, clusterInsCache)
	}
	insCache.TotalCurrentCount = int64(totalCount)
	GetInstance().FunctionInstanceCache.Store(funcSvc, insCache)
	return
}

func closeResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		log.GetLogger().Errorf("failed to close response body, err: %s", err.Error())
	}
}
