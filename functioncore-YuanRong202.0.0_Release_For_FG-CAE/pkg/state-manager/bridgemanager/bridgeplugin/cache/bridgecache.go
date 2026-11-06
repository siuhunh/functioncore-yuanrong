/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package cache -
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	comutils "functioncore/pkg/common/utils"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
)

var (
	mgrCache *BridgeCache

	once = sync.Once{}
)

// BridgeCache store the relationship of client instance, stateID, and bridge instance/function
type BridgeCache struct {
	// ClientPodClientInstCache key is userFunc podName+tenantId+bridgeFuncName, value is clientInstance
	ClientPodClientInstCache sync.Map

	// FuncNameInfoCache key is bridge function name, value is *bridgeFuncRestrict
	FuncNameInfoCache sync.Map

	// StateIDInfoCache key is StateID, value is StateIDInfo object
	StateIDInfoCache sync.Map

	// StateIDLockMap key is stateID, value is *sync.Mutex lock
	StateIDLockMap sync.Map

	// ClientPodNameLockMap key is userFunc podName, value is *sync.Mutex lock
	ClientPodNameLockMap sync.Map

	// ClientCiteBridgeFuncMap
	ClientCiteBridgeFuncMap sync.Map

	// FuncNameInfoFromEtcdOnce key is funcUrn, value is sync.Once, for query etcd once
	FuncNameInfoFromEtcdOnce sync.Map

	// StateWorkersLock StateWorkers Lock
	StateWorkersLock sync.RWMutex

	// StateWorkersCache key is stateID, val stateID exist or not
	StateWorkersCache map[string]bool
}

// StateIDInfo record relative information of stateID
type StateIDInfo struct {
	StateKey string `json:"stateKey"`
	// PodName is bridgeFunc podName
	PodName           string `json:"bridgePodName"`
	RefCount          int    `json:"refCount"`
	BridgeFuncURN     string `json:"bridgeFuncURN,omitempty"`
	MaxBgInstQua      int    `json:"maxBgInstQua,omitempty"`
	MaxActiveQua      int    `json:"maxActiveQua,omitempty"`
	BelongCluster     string `json:"belongCluster,omitempty"`
	LastModifyCluster string `json:"lastModifyCluster,omitempty"`
}

type bridgeFuncRestrict struct {
	maxActive      int
	maxStateKeyQua int
}

func newStateIDInfo() *StateIDInfo {
	return &StateIDInfo{}
}

const (
	maxBaaSConnections                  = "maxBaaSConnections"
	maxActive                           = "maxActive"
	initialBridgeFuncMaxQua             = 0
	defaultMaxClientForSingleBridgeFunc = 10
	versionLatest                       = "latest"
)

// Instance get the BridgeCache singleton instance
func Instance() *BridgeCache {
	once.Do(func() {
		mgrCache = initEmptyBridgeCache()
	})
	return mgrCache
}

func initEmptyBridgeCache() *BridgeCache {
	return &BridgeCache{
		StateWorkersCache: make(map[string]bool),
	}
}

func (b *BridgeCache) updateRestrict(functionURN string, newMaxStateKey int, newMaxActive int) error {
	defer utils.TimeCost("updateRestrict")()
	if newMaxStateKey <= 0 {
		newMaxStateKey = 1
	}
	val, ok := b.FuncNameInfoCache.Load(functionURN)
	if !ok || val == nil {
		funcRestrict := newBridgeRestrict()
		funcRestrict.maxActive = newMaxActive
		funcRestrict.maxStateKeyQua = newMaxStateKey
		log.GetLogger().Debugf("updateRestrict update FuncNameInfoCache, key:%s, val:%+v", functionURN, funcRestrict)
		b.FuncNameInfoCache.Store(functionURN, funcRestrict)
		log.GetLogger().Infof("[bridgeplugin] create instance info, functionURN: %s, maxStateKey: %d, "+
			"maxActive: %d", functionURN, newMaxStateKey, newMaxActive)
		return nil
	}
	oldRestrict, ok := val.(*bridgeFuncRestrict)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] failed to assert type *bridgeFuncRestrict. value: %T", oldRestrict)
		return errors.New("[bridgeplugin] failed to assert type *bridgeFuncRestrict in updateRestrict")
	}
	oldMaxActive := oldRestrict.maxActive
	oldMaxStateKey := oldRestrict.maxStateKeyQua
	oldRestrict.maxActive = newMaxActive
	oldRestrict.maxStateKeyQua = newMaxStateKey
	log.GetLogger().Debugf("updateRestrict update FuncNameInfoCache, key:%s, val:%+v", functionURN, oldRestrict)
	b.FuncNameInfoCache.Store(functionURN, oldRestrict)
	log.GetLogger().Infof("[bridgeplugin] update maxActive of bridgefunc [%s] from %v to %v; "+
		"update maxStateKey from %v to %v",
		functionURN, oldMaxActive, newMaxActive, oldMaxStateKey, newMaxStateKey)
	return nil
}

// LoadClientCiteBridgeInfo load client cite bridge funcName info
func (b *BridgeCache) LoadClientCiteBridgeInfo() {
	defer utils.TimeCost("loadClientCiteBridgeInfo")()
	b.ClientPodClientInstCache.Range(func(key, value interface{}) bool {
		if clientItem, ok := value.(*bgtypes.ClientInstance); ok {
			var citeBridgeInfo []string
			v, ok := b.ClientCiteBridgeFuncMap.Load(clientItem.PodName)
			if !ok {
				citeBridgeInfo = make([]string, 0)
				citeBridgeInfo = append(citeBridgeInfo, clientItem.FuncInfo.BridgeFuncName)
			} else {
				matchFlag := false
				citeBridgeInfo, ok = v.([]string)
				if !ok {
					log.GetLogger().Errorf("[bridgeplugin] convert slice failed when restore, "+
						"client podName:%s, bridgeFuncName:%s", clientItem.PodName, clientItem.FuncInfo.BridgeFuncName)
				}
				for _, value := range citeBridgeInfo {
					if value == clientItem.FuncInfo.BridgeFuncName {
						matchFlag = true
						break
					}
				}
				if !matchFlag {
					citeBridgeInfo = append(citeBridgeInfo, clientItem.FuncInfo.BridgeFuncName)
				}
			}
			b.ClientCiteBridgeFuncMap.Store(clientItem.PodName, citeBridgeInfo)
			log.GetLogger().Infof("[bridgeplugin] restore clientCiteBridgeInfo, clientItem podName:%s"+
				"citeBridgeInfo %+v", clientItem.PodName, citeBridgeInfo)
		}
		return true
	})
}

// LoadCacheFromEtcd load persistent data from etcd to restore cache
func (b *BridgeCache) LoadCacheFromEtcd() error {
	defer utils.TimeCost("LoadCacheFromEtcd")()
	log.GetLogger().Debugf("[bridgeplugin] begin to load data to cache from etcd")
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		return nil
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := stateClient.GetResponse(defaultEtcdCtx, utils.BridgeCachePrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to get %s data from meta etcd err: %s",
			utils.BridgeCachePrefix, err.Error())
		return err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.GetLogger().Infof("[bridgeplugin] no persistent bridge stateIDInfo cache data exist in meta etcd," +
			"try to get them from router etcd.")
		routerEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		resp, err = etcd3.GetRouterEtcdClient().
			GetResponse(routerEtcdCtx, utils.BridgeCachePrefix, clientv3.WithPrefix())
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] failed to get %s data from router etcd err: %s",
				utils.BridgeCachePrefix, err.Error())
			return err
		}
		if resp == nil || len(resp.Kvs) == 0 {
			log.GetLogger().Infof("[bridgeplugin] no persistent bridge stateIDInfo cache " +
				"data exist in both meta and router etcd")
			return nil
		}
	}
	for _, kv := range resp.Kvs {
		stateID := utils.GetStateIDByPath(string(kv.Key))
		if stateID == "" {
			log.GetLogger().Errorf("[bridgeplugin] get stateID from etcd key: %s failed.", string(kv.Key))
			continue
		}
		stateIDInfo := &StateIDInfo{}
		err := json.Unmarshal(kv.Value, &stateIDInfo)
		if err != nil || stateIDInfo == nil {
			log.GetLogger().Errorf("[bridgeplugin] failed to unmarshal stateIDInfo, err: %s", err.Error())
			return err
		}
		log.GetLogger().Debugf("[bridgeplugin] load one record of stateID: %s from etcd", stateID)
		b.StateIDInfoCache.Store(stateID, stateIDInfo)
		bridgeRestrict := newBridgeRestrict()
		bridgeRestrict.maxActive = stateIDInfo.MaxActiveQua
		bridgeRestrict.maxStateKeyQua = stateIDInfo.MaxBgInstQua
		b.FuncNameInfoCache.Store(stateIDInfo.BridgeFuncURN, bridgeRestrict)
	}
	return b.loadClientCacheFromEtcd()
}

func (b *BridgeCache) loadClientCacheFromEtcd() error {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
		return errors.New(utils.InitStateETCDError)
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := stateClient.GetResponse(defaultEtcdCtx, utils.ClientCachePrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to connect to etcd err: %s", err.Error())
		return err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.GetLogger().Infof("[bridgeplugin] no persistent bridge client cache data exist in meta etcd," +
			"try to get them from router etcd.")
		routerEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		resp, err = etcd3.GetRouterEtcdClient().
			GetResponse(routerEtcdCtx, utils.ClientCachePrefix, clientv3.WithPrefix())
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] failed to get %s data from router etcd err: %s",
				utils.ClientCachePrefix, err.Error())
			return err
		}
		if resp == nil || len(resp.Kvs) == 0 {
			log.GetLogger().Infof("[bridgeplugin] no persistent bridge client cache data exist in etcd.")
			return nil
		}
	}
	for _, kv := range resp.Kvs {
		clientPodName := utils.GetPodNameByPath(string(kv.Key))
		if clientPodName == "" {
			log.GetLogger().Errorf("[bridgeplugin] get clientPodName from etcd key: %s failed.", string(kv.Key))
			continue
		}
		clientInstance := &bgtypes.ClientInstance{}
		if err := json.Unmarshal(kv.Value, &clientInstance); err != nil {
			log.GetLogger().Errorf("[bridgeplugin] failed to unmarshal clientInstance, err: %s", err.Error())
			continue
		}
		log.GetLogger().Debugf("[bridgeplugin] load one record of ClientPod: %s, bridgeFuncName: %s from etcd",
			clientPodName, clientInstance.FuncInfo.BridgeFuncName)
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance), clientInstance)
	}
	return nil
}

// StoreSpecificStateIDInfoToEtcd store the record of stateID in StateIDInfoCache to etcd
func (b *BridgeCache) StoreSpecificStateIDInfoToEtcd(stateID string, traceID string) error {
	defer utils.TimeCost("StoreSpecificStateIDInfoCache")()
	if value, ok := b.StateIDInfoCache.Load(stateID); ok {
		stateIDInfo, ok := value.(*StateIDInfo)
		if !ok {
			errMsg := fmt.Sprintf("[bridgeplugin] failed to assert type StateIDInfo. value: %T, traceID %s",
				stateIDInfo, traceID)
			return errors.New(errMsg)
		}
		stateIDInfo.LastModifyCluster = utils.ClusterID
		putvalue, err := json.Marshal(stateIDInfo)
		if err != nil {
			return err
		}
		routeKey := fmt.Sprintf(utils.BridgeCacheStoreKey, utils.GetTenantIDFromURN(stateIDInfo.BridgeFuncURN),
			utils.GetComplexFuncNameFromURN(stateIDInfo.BridgeFuncURN), stateID)
		stateClient := etcd3.GetStateEtcdClient()
		if stateClient == nil || stateClient.Client == nil {
			log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
			return errors.New(utils.InitStateETCDError)
		}
		putEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		log.GetLogger().Debugf("StoreSpecificStateIDInfoToEtcd put etcd, key:%s, val:%+v", routeKey, string(putvalue))
		err = stateClient.Put(putEtcdCtx, routeKey, string(putvalue))
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] StoreSpecificStateIDInfoCache failed to write stateIDInfo to "+
				"etcd: %s, record: %s, traceID: %s", routeKey, string(putvalue), traceID)
			return err
		}
		log.GetLogger().Infof("[bridgeplugin] StoreSpecificStateIDInfoCache succeeded to put etcd: %s, "+
			"record: %s, traceID: %s", routeKey, string(putvalue), traceID)
	} else {
		log.GetLogger().Errorf("[bridgeplugin] stateID :%s does not exist in StateIDInfoCache when store to etcd, "+
			"traceID: %s", stateID, traceID)
	}
	return nil
}

// RollbackUpdateStateIDInfoToEtcd rollback the last modify of stateID in etcd
func (b *BridgeCache) RollbackUpdateStateIDInfoToEtcd(stateID string, traceID string) error {
	defer utils.TimeCost("RollbackUpdateStateIDInfoToEtcd")()
	stateInfo, ok := b.StateIDInfoCache.Load(stateID)
	if !ok || stateInfo == nil {
		return nil
	}

	stateIDInfo, ok := stateInfo.(*StateIDInfo)
	if !ok {
		errMsg := fmt.Sprintf("[bridgeplugin] failed to assert type StateIDInfo. value: %T, traceID: %s",
			stateIDInfo, traceID)
		return errors.New(errMsg)
	}

	putValue, err := json.Marshal(stateIDInfo)
	if err != nil {
		return err
	}
	routeKey := fmt.Sprintf(utils.BridgeCacheStoreKey, utils.GetTenantIDFromURN(stateIDInfo.BridgeFuncURN),
		utils.GetComplexFuncNameFromURN(stateIDInfo.BridgeFuncURN), stateID)
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
		return errors.New(utils.InitStateETCDError)
	}
	putEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	log.GetLogger().Debugf("RollbackUpdateStateIDInfoToEtcd put etcd, key:%s, val:%+v", routeKey, string(putValue))
	err = stateClient.Put(putEtcdCtx, routeKey, string(putValue))
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] rollbackUpdateStateIDInfoToEtcd failed to put stateIDInfo to "+
			"etcd: %s, record: %s, traceID: %s", routeKey, string(putValue), traceID)
		return err
	}

	return nil
}

// StoreSpecificClientInstToEtcd store the record of clientInstance to etcd
func (b *BridgeCache) StoreSpecificClientInstToEtcd(clientInstance *bgtypes.ClientInstance, traceID string) error {
	defer utils.TimeCost("StoreSpecificClientInstCache")()
	if clientInstInfo, ok := b.GetClientInfoFromCache(clientInstance, traceID); ok {
		return b.UpdateSpecificClientInstToEtcd(clientInstInfo, traceID)
	}

	log.GetLogger().Infof("[bridgeplugin] podName :%s does not exist in ClientPodClientInstCache when "+
		"store to etcd, traceID: %s", clientInstance.PodName, traceID)
	return nil
}

// UpdateSpecificClientInstToEtcd store the record of clientInstance to etcd
func (b *BridgeCache) UpdateSpecificClientInstToEtcd(clientInstance *bgtypes.ClientInstance, traceID string) error {
	defer utils.TimeCost("UpdateSpecificClientInstCache")()
	putvalue, err := json.Marshal(clientInstance)
	if err != nil {
		return err
	}
	clientInstance.LastModifyClusterID = utils.ClusterID
	routeKey := fmt.Sprintf(utils.ClientCacheStoreKey, clientInstance.FuncInfo.TenantID,
		utils.NewComplexFuncName(clientInstance.FuncInfo.ServiceID, clientInstance.FuncInfo.BridgeFuncName),
		comutils.GenerateInstanceID(clientInstance.PodName))
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
		return errors.New(utils.InitStateETCDError)
	}
	putEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	log.GetLogger().Debugf("UpdateSpecificClientInstToEtcd put etcd, key:%s, val:%+v", routeKey, string(putvalue))
	err = stateClient.Put(putEtcdCtx, routeKey, string(putvalue))
	if err != nil {
		return err
	}
	log.GetLogger().Infof("[bridgeplugin] UpdateSpecificClientInstCache succeeded to put etcd: %s, record: %s, "+
		"traceID: %s, stateID: %s, client: %s", routeKey, string(putvalue), traceID, clientInstance.AssignStateID,
		clientInstance.PodName)

	return nil
}

// DeleteStateIDInfoETCDByBridgeInfo delete the record of stateID in StateIDInfoCache from etcd
func (b *BridgeCache) DeleteStateIDInfoETCDByBridgeInfo(tenantID string, complexFuncName string, stateID string,
	traceID string) error {
	defer utils.TimeCost("DeleteStateIDInfoETCDByBridgeInfo")()
	stateIDInfoKey := fmt.Sprintf(utils.BridgeCacheStoreKey, tenantID, complexFuncName, stateID)
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient.Client == nil {
		log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
		return errors.New(utils.InitStateETCDError)
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	log.GetLogger().Debugf("DeleteStateIDInfoETCDByBridgeInfo delete etcd, key:%s, val:%+v", stateIDInfoKey, nil)
	deleteErr := stateClient.Delete(defaultEtcdCtx, stateIDInfoKey)
	if deleteErr != nil {
		log.GetLogger().Errorf("[bridgeplugin] Failed to delete stateIDInfo in etcd by bridge info, "+
			"stateIDInfoKey: %s, errMsg: %s, traceID: %s, stateID: %s", stateIDInfoKey, deleteErr.Error(),
			traceID, stateID)
		return deleteErr
	}
	log.GetLogger().Infof("[bridgeplugin] DeleteStateIDInfoETCDByBridgeInfo succeeded to delete etcd: %s, "+
		"traceID: %s, stateID: %s, tenantID:%s, complexFuncName:%s", stateIDInfoKey, traceID, stateID, tenantID,
		complexFuncName)
	return nil
}

// DeleteSpecificStateIDInfoETCD delete the record of stateID in StateIDInfoCache from etcd
func (b *BridgeCache) DeleteSpecificStateIDInfoETCD(stateIDInfo *StateIDInfo, stateID string, traceID string) error {
	defer utils.TimeCost("DeleteSpecificStateIDInfoETCD")()
	stateIDInfoKey := fmt.Sprintf(utils.BridgeCacheStoreKey, utils.GetTenantIDFromURN(stateIDInfo.BridgeFuncURN),
		utils.GetComplexFuncNameFromURN(stateIDInfo.BridgeFuncURN), stateID)
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
		return errors.New(utils.InitStateETCDError)
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	log.GetLogger().Debugf("DeleteSpecificStateIDInfoETCD delete etcd, key:%s, val:%+v", stateIDInfoKey, nil)
	deleteErr := stateClient.Delete(defaultEtcdCtx, stateIDInfoKey)
	if deleteErr != nil {
		log.GetLogger().Errorf("[bridgeplugin] Failed to delete stateIDInfo in etcd, stateIDInfoKey: %s, errMsg: "+
			"%s, traceID: %s, stateID: %s", stateIDInfoKey, deleteErr.Error(), traceID, stateID)
		return deleteErr
	}
	log.GetLogger().Infof("[bridgeplugin] DeleteSpecificStateIDInfoETCD succeeded to delete etcd: %s, traceID: "+
		"%s, stateID: %s", stateIDInfoKey, traceID, stateID)
	return nil
}

// DeleteSpecificClientInfoETCD  delete the record of clientInstance in StateIDInfoCache from etcd
func (b *BridgeCache) DeleteSpecificClientInfoETCD(clientInstance *bgtypes.ClientInstance, traceID string) error {
	defer utils.TimeCost("DeleteSpecificClientInfoETCD")()
	clientKey := fmt.Sprintf(utils.ClientCacheStoreKey, clientInstance.FuncInfo.TenantID,
		utils.NewComplexFuncName(clientInstance.FuncInfo.ServiceID, clientInstance.FuncInfo.BridgeFuncName),
		comutils.GenerateInstanceID(clientInstance.PodName))
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("[bridgeplugin] " + utils.InitStateETCDError)
		return errors.New(utils.InitStateETCDError)
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	log.GetLogger().Debugf("DeleteSpecificClientInfoETCD delete etcd, key:%s, val:%+v", clientKey, nil)
	deleteErr := stateClient.Delete(defaultEtcdCtx, clientKey)
	if deleteErr != nil {
		log.GetLogger().Errorf("[bridgeplugin] Failed to delete client info in etcd, clientKey: %s, errMsg: %s, "+
			"stateID: %s, traceID: %s, client: %s", clientKey, deleteErr.Error(), clientInstance.AssignStateID,
			traceID, clientInstance.PodName)
		return deleteErr
	}
	log.GetLogger().Infof("[bridgeplugin] DeleteSpecificClientInfoETCD succeeded to delete etcd: %s, stateID: %s, "+
		"traceID: %s, client: %s", clientKey, clientInstance.AssignStateID, traceID, clientInstance.PodName)
	return nil
}

// UpdateBridgeConfigByMeta calculate max stateKey and maxActive of bridgefunc and update cache
func (b *BridgeCache) UpdateBridgeConfigByMeta(funcMeta *config.FunctionMetaInfo) error {
	maxStateKeyQua, maxActiveQua, err := b.calcRestrict(funcMeta)
	if err != nil {
		return err
	}
	return b.updateRestrict(funcMeta.FuncMetaData.FunctionURN, maxStateKeyQua, maxActiveQua)
}

func (b *BridgeCache) calcRestrict(funcMeta *config.FunctionMetaInfo) (int, int, error) {
	var maxStateKeyQua int
	var maxActiveQua int
	var maxConnectionsVal int
	envs, err := decryptEnv(funcMeta)
	if err != nil {
		return 0, 0, err
	}
	if envs == nil {
		return 0, 0, errors.New("[bridgeplugin] envs is nil, calculate max number of stateKey failed")
	}
	if envs[maxBaaSConnections] == nil {
		return 0, 0, errors.New("[bridgeplugin] maxBaaSConnections configuration is nil")
	}
	maxConnectionsVal, err = strconv.Atoi(envs[maxBaaSConnections].(string))
	if err != nil {
		return 0, 0, errors.Errorf("[bridgeplugin] invalid key [%s] of envs, type [%s]",
			maxBaaSConnections, reflect.TypeOf(envs[maxBaaSConnections]))
	}

	if maxConnectionsVal <= 0 {
		return 0, 0, errors.Errorf("[bridgeplugin] maxBaaSConnections is not a positive number")
	}

	if envs[maxActive] == nil {
		maxActiveQua = defaultMaxClientForSingleBridgeFunc
	} else {
		maxActiveQua, err = strconv.Atoi(envs[maxActive].(string))
		if err != nil {
			return 0, 0, errors.Errorf("[bridgeplugin] invalid key [%s] of envs, type [%s]",
				maxActive, reflect.TypeOf(envs[maxActive]))
		}
	}

	if maxConnectionsVal < maxActiveQua {
		return 0, 0, errors.Errorf("[bridgeplugin] maxBaaSConnections is less than maxActive")
	}
	maxStateKeyQua = maxConnectionsVal / maxActiveQua
	return maxStateKeyQua, maxActiveQua, nil
}

func decryptEnv(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
	funcName := funcMeta.FuncMetaData.FuncName
	encryptedUserData := funcMeta.EnvMetaData.Environment
	if encryptedUserData == "" {
		return nil, errors.Errorf("[bridgeplugin] env of bridgefunc [%s] is empty", funcName)
	}
	var userData []byte
	var err error
	if userData, err = localauth.Decrypt(encryptedUserData); err != nil {
		return nil, errors.Errorf("[bridgeplugin] decrypt user's env error: %s, funcName: %s", err.Error(), funcName)
	}
	var userDataMap map[string]interface{}
	if err = json.Unmarshal(userData, &userDataMap); err != nil {
		return nil, errors.Errorf("[bridgeplugin] unmashal env error: %s, funcName: %s", err.Error(), funcName)
	}
	return userDataMap, nil
}

// PickAvailableStateKey choose existing stateKey of the bridgeFuncURN
// pick one existing stateKey which the refCount is less than its maxActiveQua
func (b *BridgeCache) PickAvailableStateKey(bridgeFuncURN string, traceID string) (string, *StateIDInfo, bool) {
	var stateID string
	statInfo := &StateIDInfo{}
	available := false
	b.StateIDInfoCache.Range(func(key, value interface{}) bool {
		stateIDIf, ok := key.(string)
		if !ok {
			log.GetLogger().Warnf("[bridgeplugin] failed to assert type string. key: %T, traceID: %s", stateIDIf,
				traceID)
			return true
		}
		stateIDInfo, ok := value.(*StateIDInfo)
		if !ok {
			log.GetLogger().Warnf("[bridgeplugin] failed to assert type *StateIDInfo. value: %T, traceID: %s",
				stateIDInfo, traceID)
			return true
		}
		if stateIDInfo.BridgeFuncURN != bridgeFuncURN {
			return true
		}
		if stateIDInfo.BelongCluster != utils.ClusterID {
			return true
		}
		if stateIDInfo.RefCount < stateIDInfo.MaxActiveQua {
			stateID = stateIDIf
			statInfo = stateIDInfo
			available = true
			return false
		}
		return true
	})
	log.GetLogger().Infof("[bridgeplugin] pickAvailableStateKey, key:%s, id:%s,  available：%t, podName: %s, "+
		"traceID: %s", statInfo.StateKey, stateID, available, statInfo.PodName, traceID)
	return stateID, statInfo, available
}

// GetStateIDInfoFromCache get StateIDInfo of stateID
func (b *BridgeCache) GetStateIDInfoFromCache(stateID string, traceID string) (*StateIDInfo, bool) {
	v, ok := b.StateIDInfoCache.Load(stateID)
	if !ok {
		log.GetLogger().Infof("[bridgeplugin] stateID: %s does not exist in StateIDInfoCache, traceID: %s",
			stateID, traceID)
		return nil, false
	}
	stateIDInfo, ok := v.(*StateIDInfo)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] failed to assert type StateIDInfo, stateID: %s, traceID: %s",
			stateID, traceID)
		return nil, false
	}
	return stateIDInfo, true
}

// UpdateRefCountCacheAndDeleteWhenRefZero update StateIDInfoCache refCount of designated stateID
func (b *BridgeCache) UpdateRefCountCacheAndDeleteWhenRefZero(stateID string, changeCount int,
	isDeleteEtcdData bool, traceID string) error {
	stateIDInfo, ok := b.GetStateIDInfoFromCache(stateID, traceID)
	if !ok && changeCount > 0 {
		stateIDInfo := newStateIDInfo()
		stateIDInfo.RefCount = changeCount
		log.GetLogger().Debugf("UpdateRefCountCacheAndDeleteWhenRefZero update StateIDInfoCache, "+
			"key:%s, val:%+v", stateID, stateIDInfo)
		b.StateIDInfoCache.Store(stateID, stateIDInfo)
		return nil
	}

	if !ok && changeCount < 0 {
		return errors.New("[bridgeplugin] Minus refCount not allow nonexistent stateID")
	}

	stateIDInfo.RefCount += changeCount
	if stateIDInfo.RefCount != 0 {
		return nil
	}

	if isDeleteEtcdData {
		if err := b.DeleteSpecificStateIDInfoETCD(stateIDInfo, stateID, traceID); err != nil {
			return err
		}
	}
	log.GetLogger().Errorf("[bridgeplugin]UpdateRefCountCacheAndDeleteWhenRefZero, stateID: %s, traceID: %s",
		stateID, traceID)
	log.GetLogger().Debugf("UpdateRefCountCacheAndDeleteWhenRefZero delete StateIDInfoCache, "+
		"key:%s, val:%+v", stateID, nil)
	b.StateIDInfoCache.Delete(stateID)
	b.StateIDLockMap.Delete(stateID)

	return nil
}

// UpdateRefCountCache update StateIDInfoCache refCount of designated stateID
func (b *BridgeCache) UpdateRefCountCache(stateID string, changeCount int, traceID string) error {
	stateIDInfo, ok := b.GetStateIDInfoFromCache(stateID, traceID)
	if ok {
		stateIDInfo.RefCount += changeCount
	} else if changeCount > 0 {
		stateIDInfo := newStateIDInfo()
		stateIDInfo.RefCount = changeCount
		log.GetLogger().Debugf("UpdateRefCountCache update StateIDInfoCache, key:%s, val:%+v", stateID, stateIDInfo)
		b.StateIDInfoCache.Store(stateID, stateIDInfo)
	} else {
		return fmt.Errorf("[bridgeplugin] Minus refCount not allow nonexistent stateID, stateID %s, traceID %s",
			stateID, traceID)
	}

	return nil
}

// GetRefCountFromCache get refCount of designated stateID from StateIDInfoCache
func (b *BridgeCache) GetRefCountFromCache(stateID string, traceID string) int {
	stateIDInfo, ok := b.GetStateIDInfoFromCache(stateID, traceID)
	if !ok {
		log.GetLogger().Debugf("[bridgeplugin] stateId does not exist in cache when getting refCount, "+
			"may have been released, stateID: %s, traceID: %s", stateID, traceID)
		return -1
	}
	log.GetLogger().Debugf("[bridgeplugin] get current refCount is %d of stateID %s, traceID: %s",
		stateIDInfo.RefCount, stateID, traceID)
	return stateIDInfo.RefCount
}

// UpdateBridgeFuncPodCache update StateIDInfoCache bridgePodName of designated stateID
func (b *BridgeCache) UpdateBridgeFuncPodCache(stateID string, podName string, traceID string) (string, error) {
	var currentPodName string
	stateIDInfo, ok := b.GetStateIDInfoFromCache(stateID, traceID)
	if ok {
		currentPodName = stateIDInfo.PodName
		stateIDInfo.PodName = podName
		return currentPodName, nil
	}
	return "", errors.Errorf("[bridgeplugin] UpdateBridgeFuncPodCache stateID: %s does not exist, traceID: "+
		"%s, client: %s", stateID, traceID, podName)
}

// GetRestrictByURN get restrictions by function URN
func (b *BridgeCache) GetRestrictByURN(functionURN string) (int, int, error) {
	v, exist := b.FuncNameInfoCache.Load(functionURN)
	if exist {
		return b.getBridgeConfigFromCache(v)
	}
	onceVal, _ := b.FuncNameInfoFromEtcdOnce.LoadOrStore(functionURN, new(sync.Once))
	onceObj, ok := onceVal.(*sync.Once)
	if !ok {
		return 0, 0, fmt.Errorf("[bridgeplugin] failed to convert to sync.Once, funcUrn %s", functionURN)
	}
	onceObj.Do(func() {
		tenantID, serviceID, funcName := utils.GetFunctionInfoByURN(functionURN)
		etcdFuncKeyFormat := "/sn/functions/business/yrk/tenant/%s/function/%s@%s/version/%s"
		functionKey := fmt.Sprintf(etcdFuncKeyFormat, tenantID, serviceID, funcName, versionLatest)
		resp, err := utils.GetValueFromMetaEtcd(functionKey)
		if err == nil {
			functionMetaInfo := &config.FunctionMetaInfo{}
			err = json.Unmarshal(resp, functionMetaInfo)
			if err != nil {
				log.GetLogger().Errorf("[bridgeplugin] failed to convert resp to FunctionMetaInfo,"+
					"functionURN: %s, functionKey: %s, err: %s", functionURN, functionKey, err.Error())
				b.FuncNameInfoFromEtcdOnce.Delete(functionURN)
				return
			}
			// only bridge function config should be stored
			if functionMetaInfo.FuncMetaData.IsBridgeFunction {
				err = b.UpdateBridgeConfigByMeta(functionMetaInfo)
				if err != nil {
					log.GetLogger().Errorf("[bridgeplugin] failed to UpdateBridgeConfigByMeta,"+
						"functionURN: %s, functionKey: %s, err: %s", functionURN, functionKey, err.Error())
				}
			}
		} else {
			log.GetLogger().Warnf("[bridgeplugin] failed to query from etcd, functionURN: %s, functionKey: %s, err: %s",
				functionURN, functionKey, err.Error())
		}
		// delete anyway to avoid memory waste. If query failed, we can wait for etcd event or next query
		b.FuncNameInfoFromEtcdOnce.Delete(functionURN)
	})
	v, exist = b.FuncNameInfoCache.Load(functionURN)
	if exist {
		return b.getBridgeConfigFromCache(v)
	}
	log.GetLogger().Errorf("[bridgeplugin] functionURN: %s does not exist in FuncNameInfoCache "+
		"when get restrict", functionURN)
	return 0, 0, errors.New("[bridgeplugin] failed to get function configuration, " +
		"please check configuration")

}

func (b *BridgeCache) getBridgeConfigFromCache(v any) (int, int, error) {
	bridgeRestrict, ok := v.(*bridgeFuncRestrict)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] failed to assert type *bridgeFuncRestrict. value: %T",
			bridgeRestrict)
		return 0, 0, errors.New("[bridgeplugin] failed to assert type *bridgeFuncRestrict in GetRestrictByURN")
	}
	maxActiveQua := bridgeRestrict.maxActive
	maxStateKeyQua := bridgeRestrict.maxStateKeyQua
	return maxStateKeyQua, maxActiveQua, nil
}

// RegisterRefCount update ref count in cache when assigned stateID
func (b *BridgeCache) RegisterRefCount(stateID string, traceID string) error {
	stateIDInfo, ok := b.GetStateIDInfoFromCache(stateID, traceID)
	if ok {
		stateIDInfo.RefCount++
	} else {
		log.GetLogger().Errorf("[bridgeplugin] StateID: %s is not exist when register refCount, traceID: %s",
			stateID, traceID)
		return errors.Errorf("[bridgeplugin] StateID: %s is not exist when register refCount, traceID: %s",
			stateID, traceID)
	}

	log.GetLogger().Debugf("[bridgeplugin] update current refCount to %d of stateID %s, traceID: %s",
		stateIDInfo.RefCount, stateID, traceID)
	return nil
}

// IsReachMaxStateKeyCount judge whether current stateKey count is reach max
func (b *BridgeCache) IsReachMaxStateKeyCount(functionURN string, traceID string) (bool, error) {
	maxStateKeyQua, _, err := b.GetRestrictByURN(functionURN)
	if err != nil {
		return true, err
	}
	stateSum := 0
	b.StateIDInfoCache.Range(func(key, value interface{}) bool {
		stateIDInfo, ok := value.(*StateIDInfo)
		if !ok {
			log.GetLogger().Warnf("[bridgeplugin] failed to assert type int. value: %T, traceID: %s",
				stateIDInfo, traceID)
			return true
		}
		if stateIDInfo.BridgeFuncURN == functionURN {
			stateSum++
			stateIDInfo.MaxBgInstQua = maxStateKeyQua
		}
		return true
	})
	log.GetLogger().Infof("[bridgeplugin] functionURN:%s of maxStateKeyQua:%d, stateSum:%d, traceID: %s",
		functionURN, maxStateKeyQua, stateSum, traceID)
	return stateSum >= maxStateKeyQua, nil
}

func newBridgeRestrict() *bridgeFuncRestrict {
	return &bridgeFuncRestrict{
		maxActive:      defaultMaxClientForSingleBridgeFunc,
		maxStateKeyQua: initialBridgeFuncMaxQua,
	}
}

// GetClientInfoFromCache get clientInstance info of podName with bridgeFunc Info
func (b *BridgeCache) GetClientInfoFromCache(client *bgtypes.ClientInstance,
	traceID string) (*bgtypes.ClientInstance, bool) {
	v, ok := b.ClientPodClientInstCache.Load(utils.GetClientCacheInfoKey(client))
	if !ok {
		log.GetLogger().Infof("[bridgeplugin] client podName: %s bridgeFuncName: %s does not exist in "+
			"ClientPodClientInstCache, traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
		return nil, false
	}
	clientInfo, ok := v.(*bgtypes.ClientInstance)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] failed to assert type clientInfo, client podName:%s, "+
			"bridgeFuncName:%s, traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
		return nil, false
	}
	return clientInfo, true
}

// RegisterClientInsBridgeFunUseInfo register special stateID use info of client instance
func (b *BridgeCache) RegisterClientInsBridgeFunUseInfo(client *bgtypes.ClientInstance, stateID string, changeCount int,
	traceID string) error {
	log.GetLogger().Infof("[bridgeplugin] client have registered stateID %s, need to update bridgeFunc use "+
		"info of client podName: %s, traceID %s", stateID, client.PodName, traceID)

	clientInstInfo, ok := b.GetClientInfoFromCache(client, traceID)
	if ok {
		if clientInstInfo.FuncUseInfo == nil {
			clientInstInfo.FuncUseInfo = make(map[string]int, 1)
		}
		clientInstInfo.FuncUseInfoLock.Lock()
		clientInstInfo.FuncUseInfo[stateID] += changeCount
		log.GetLogger().Infof("[bridgeplugin] funcUseInfo of stateID %s client %s has updated to %d, traceID %s",
			client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[stateID], traceID)
		clientInstInfo.FuncUseInfoLock.Unlock()
	} else if changeCount > 0 {
		clientInstInfo = client
		clientInstInfo.AssignStateID = stateID
		clientInstInfo.FuncUseInfoLock.Lock()
		clientInstInfo.FuncUseInfo = make(map[string]int, 1)
		clientInstInfo.FuncUseInfo[clientInstInfo.AssignStateID] = changeCount
		clientInstInfo.LastModifyClusterID = utils.ClusterID
		cacheInfoKey := utils.GetClientCacheInfoKey(client)
		log.GetLogger().Debugf("RegisterClientBridgeFunUseInfo update ClientPodClientInstCache, "+
			"key:%s, val:%+v", cacheInfoKey, clientInstInfo)
		b.ClientPodClientInstCache.Store(cacheInfoKey, clientInstInfo)
		log.GetLogger().Infof("[bridgeplugin] funcUseInfo of stateID %s client podName %s, bridgeFuncName %s "+
			"has updated to %d, traceID %s",
			client.AssignStateID, client.PodName, client.FuncInfo.BridgeFuncName,
			clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
		clientInstInfo.FuncUseInfoLock.Unlock()
	} else {
		return errors.New(fmt.Sprintf("[bridgeplugin] Minus refCount not allow unexisted PodName, stateID: %s,"+
			"client podName: %s, bridgeFuncName: %s, traceID: %s", stateID, client.PodName,
			client.FuncInfo.BridgeFuncName, traceID))
	}
	return nil
}

// UpdateClientInsBridgeFunUseInfo update special stateID use info of client instance
func (b *BridgeCache) UpdateClientInsBridgeFunUseInfo(client *bgtypes.ClientInstance, changeCount int,
	traceID string) error {
	log.GetLogger().Infof("[bridgeplugin] client have registered stateID %s, need to update bridgefunc useInfo "+
		"of client %s", client.AssignStateID, client.PodName)

	clientInstInfo, ok := b.GetClientInfoFromCache(client, traceID)
	if !ok && changeCount > 0 {
		clientInstInfo = client
		clientInstInfo.FuncUseInfoLock.Lock()
		defer clientInstInfo.FuncUseInfoLock.Unlock()
		clientInstInfo.FuncUseInfo = make(map[string]int, 1)
		log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s before update is %d, traceID %s",
			client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
		clientInstInfo.FuncUseInfo[clientInstInfo.AssignStateID] += changeCount
		clientInstInfo.LastModifyClusterID = utils.ClusterID
		cacheInfoKey := utils.GetClientCacheInfoKey(client)
		log.GetLogger().Debugf("UpdateClientBridgeFunUseInfo update ClientPodClientInstCache, "+
			"key:%s, val:%+v", cacheInfoKey, clientInstInfo)
		b.ClientPodClientInstCache.Store(cacheInfoKey, clientInstInfo)
		log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s has updated to %d, traceID %s",
			client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
		return nil
	} else if !ok && changeCount <= 0 {
		return errors.New(fmt.Sprintf("[bridgeplugin] Minus refCount not allow unexisted PodName, traceID %s, "+
			"client %s", traceID, client.PodName))
	}
	clientInstInfo.FuncUseInfoLock.Lock()
	defer clientInstInfo.FuncUseInfoLock.Unlock()
	if clientInstInfo.FuncUseInfo == nil {
		clientInstInfo.FuncUseInfo = make(map[string]int, 1)
	}
	log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s before update is %d, traceID %s",
		client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
	clientInstInfo.FuncUseInfo[client.AssignStateID] += changeCount
	log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s has updated to %d, traceID %s",
		client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)

	return nil
}

// UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty update special stateID use info of client instance and delete cache
// when empty
func (b *BridgeCache) UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(client *bgtypes.ClientInstance, changeCount int,
	traceID string) error {
	log.GetLogger().Infof("[bridgeplugin] client have registered stateID %s, need to update bridgeFunc "+
		"useInfo of client pod:%s, traceID %s", client.AssignStateID, client.PodName, traceID)

	clientInstInfo, ok := b.GetClientInfoFromCache(client, traceID)
	if !ok && changeCount > 0 {
		clientInstInfo = client
		clientInstInfo.FuncUseInfoLock.Lock()
		defer clientInstInfo.FuncUseInfoLock.Unlock()
		clientInstInfo.FuncUseInfo = make(map[string]int, 1)
		log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s before update is %d, need to "+
			"changeCount: %d, traceID %s", client.AssignStateID, client.PodName,
			clientInstInfo.FuncUseInfo[client.AssignStateID], changeCount, traceID)
		clientInstInfo.FuncUseInfo[clientInstInfo.AssignStateID] += changeCount
		clientInstInfo.LastModifyClusterID = utils.ClusterID
		cacheInfoKey := utils.GetClientCacheInfoKey(client)
		log.GetLogger().Debugf("UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty, "+
			"key:%s, val:%+v", cacheInfoKey, clientInstInfo)
		b.ClientPodClientInstCache.Store(cacheInfoKey, clientInstInfo)
		log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client podName %s bridgeFuncName %s "+
			"has updated to %d, traceID %s", client.AssignStateID, client.PodName, client.FuncInfo.BridgeFuncName,
			clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
		return nil
	}
	if !ok && changeCount <= 0 {
		return errors.New(fmt.Sprintf("[bridgeplugin] Minus refCount not allow unexisted PodName, traceID %s, "+
			"client %s", traceID, client.PodName))
	}
	clientInstInfo.FuncUseInfoLock.Lock()
	if clientInstInfo.FuncUseInfo == nil {
		clientInstInfo.FuncUseInfo = make(map[string]int, 1)
	}
	log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s before update is %d, traceID %s",
		client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
	clientInstInfo.FuncUseInfo[client.AssignStateID] += changeCount
	log.GetLogger().Debugf("[bridgeplugin] funcUseInfo of stateID %s client %s has updated to %d, traceID %s",
		client.AssignStateID, client.PodName, clientInstInfo.FuncUseInfo[client.AssignStateID], traceID)
	clientInstInfo.FuncUseInfoLock.Unlock()
	if utils.IsClientInsFuncUseInfoEmpty(clientInstInfo) {
		cacheInfoKey := utils.GetClientCacheInfoKey(client)
		log.GetLogger().Debugf("IsClientInsFuncUseInfoEmpty delete ClientPodClientInstCache, "+
			"key:%s, val:%+v", cacheInfoKey, nil)
		b.ClientPodClientInstCache.Delete(cacheInfoKey)
		b.ClientPodNameLockMap.Delete(clientInstInfo.PodName)
	}
	return nil
}

// EmptyClientInsStateIDCiteFuncUseInfo update the corresponding stateID for FuncUseInfo
func (b *BridgeCache) EmptyClientInsStateIDCiteFuncUseInfo(client *bgtypes.ClientInstance, stateID string,
	traceID string) error {
	log.GetLogger().Infof("[bridgeplugin] client have registered stateID %s, need to empty bridgeFunc useInfo "+
		"of client %s in bridge suicide procedure", client.AssignStateID, client.PodName)

	clientInstInfo, ok := b.GetClientInfoFromCache(client, traceID)
	if !ok {
		log.GetLogger().Debugf("[bridgeplugin] failed to get clientInstInfo form cache, stateID %s client %s",
			client.AssignStateID, client.PodName)
		return nil
	}
	if clientInstInfo.FuncUseInfo == nil {
		log.GetLogger().Debugf("[bridgeplugin] FunUseInfo of stateID %s client %s is nil, no need to empty, "+
			"traceID %s", client.AssignStateID, client.PodName, traceID)
		return nil
	}
	clientInstInfo.FuncUseInfoLock.Lock()
	clientInstInfo.FuncUseInfo[stateID] = 0
	clientInstInfo.FuncUseInfoLock.Unlock()
	log.GetLogger().Debugf("[bridgeplugin] FunUseInfo of stateID %s client %s has updated empty, traceID %s",
		client.AssignStateID, client.PodName, traceID)
	return nil
}

// GetRequiredDeductionRefCount get special stateID use info of client instance
func (b *BridgeCache) GetRequiredDeductionRefCount(client *bgtypes.ClientInstance, traceID string) int {
	updateCount := 0
	clientInstInfo, ok := b.GetClientInfoFromCache(client, traceID)
	if ok && clientInstInfo.FuncUseInfo != nil {
		clientInstInfo.FuncUseInfoLock.RLock()
		updateCount = clientInstInfo.FuncUseInfo[client.AssignStateID]
		clientInstInfo.FuncUseInfoLock.RUnlock()
	}

	log.GetLogger().Debugf("[bridgeplugin] get current updateCount of client %s stateID %s is %d, traceID %s",
		client.PodName, client.AssignStateID, updateCount, traceID)

	return updateCount
}

// IsStateIDRelatedWithCurrentClientIns to judge whether the stateID of clientInstance is used by current client
func (b *BridgeCache) IsStateIDRelatedWithCurrentClientIns(client *bgtypes.ClientInstance, stateID string) bool {
	clientInst, ok := b.ClientPodClientInstCache.Load(utils.GetClientCacheInfoKey(client))
	if !ok {
		return false
	}

	clientInstance, ok := clientInst.(*bgtypes.ClientInstance)
	if !ok || clientInstance == nil {
		return false
	}

	if clientInstance.FuncUseInfo != nil {
		clientInstance.FuncUseInfoLock.RLock()
		_, ok := clientInstance.FuncUseInfo[stateID]
		clientInstance.FuncUseInfoLock.RUnlock()
		if ok {
			return true
		}
	}

	return false
}

// RefreshStateInfoCache refresh stateIDInfoCache when watched etcd update event
func (b *BridgeCache) RefreshStateInfoCache(stateID string, stateIDInfo *StateIDInfo) error {
	stateLock, _ := b.StateIDLockMap.LoadOrStore(stateID, &sync.Mutex{})
	lock, ok := stateLock.(*sync.Mutex)
	if !ok || lock == nil {
		return errors.Errorf("[bridgeplugin] failed to convert an interface to a lock "+
			"when refresh stateIDInfoCache,stateID: %s", stateID)
	}
	lock.Lock()
	defer lock.Unlock()
	log.GetLogger().Debugf("RefreshStateInfoCache update StateIDInfoCache, key:%s, val:%+v", stateID, stateIDInfo)
	b.StateIDInfoCache.Store(stateID, stateIDInfo)
	return nil
}

// ClearStateInfoCache clear stateIDInfoCache when watched etcd delete event
func (b *BridgeCache) ClearStateInfoCache(stateID string) error {
	stateLock, _ := b.StateIDLockMap.LoadOrStore(stateID, &sync.Mutex{})
	lock, ok := stateLock.(*sync.Mutex)
	if !ok || lock == nil {
		return errors.Errorf("[bridgeplugin] failed to convert an interface to a lock "+
			"when refresh stateIDInfoCache,stateID: %s", stateID)
	}
	lock.Lock()
	defer lock.Unlock()
	log.GetLogger().Infof("[bridgeplugin] clear stateID: %s in StateIDInfoCache", stateID)
	log.GetLogger().Debugf("ClearStateInfoCache delete StateIDInfoCache, key:%s, val:%+v", stateID, nil)
	b.StateIDInfoCache.Delete(stateID)
	return nil
}

// RefreshClientCache refresh clientCache when watched etcd update event
func (b *BridgeCache) RefreshClientCache(podName string, clientInstance *bgtypes.ClientInstance) error {
	podLock, _ := b.ClientPodNameLockMap.LoadOrStore(podName, &sync.Mutex{})
	lock, ok := podLock.(*sync.Mutex)
	if !ok || lock == nil {
		return errors.Errorf("[bridgeplugin] failed to convert an interface to a lock "+
			"when refresh clientCache,podName: %s", podName)
	}
	lock.Lock()
	defer lock.Unlock()
	refreshValue, err := json.Marshal(clientInstance)
	if err != nil {
		return errors.Errorf("[bridgeplugin] fail to marshal clientInstance info")
	}
	log.GetLogger().Infof("[bridgeplugin] refresh podName: %s info to %s", podName, string(refreshValue))
	cacheInfoKey := utils.GetClientCacheInfoKey(clientInstance)
	log.GetLogger().Debugf("RefreshClientCache update ClientPodClientInstCache, "+
		"key:%s, val:%+v", cacheInfoKey, clientInstance)
	b.ClientPodClientInstCache.Store(cacheInfoKey, clientInstance)
	return nil
}

// ClearClientCache clear clientCache when watched etcd delete event
func (b *BridgeCache) ClearClientCache(podName string, bridgeFuncName string, tenantID string) error {
	podLock, _ := b.ClientPodNameLockMap.LoadOrStore(podName, &sync.Mutex{})
	lock, ok := podLock.(*sync.Mutex)
	if !ok || lock == nil {
		return errors.Errorf("[bridgeplugin] failed to convert an interface to a lock "+
			"when refresh clientCache,podName: %s", podName)
	}
	lock.Lock()
	defer lock.Unlock()
	log.GetLogger().Infof("[bridgeplugin] clear podName: %s, bridgeFuncName: %s in ClientPodClientInstCache",
		podName, bridgeFuncName)
	clientCacheKey := fmt.Sprintf("%s_%s_%s", podName, tenantID, bridgeFuncName)
	log.GetLogger().Debugf("ClearClientCache delete ClientPodClientInstCache, key:%s, val:%+v", clientCacheKey, nil)
	b.ClientPodClientInstCache.Delete(clientCacheKey)
	return nil
}
