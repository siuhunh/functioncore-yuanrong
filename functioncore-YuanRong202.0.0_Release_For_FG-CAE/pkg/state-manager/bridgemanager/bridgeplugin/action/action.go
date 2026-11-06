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

// Package action -
package action

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"functioncore/pkg/state-manager/state/instancemanager"
	stateutils "functioncore/pkg/state-manager/utils"
)

const (
	cacheInfoKeyPartLens             = 3
	podNameIdxInCacheInfoKeyParts    = 0
	bridgeFuncIdxInCacheInfoKeyParts = 2
	stateWorkerTimeout               = 10
	bridgeInstanceTimeout            = 30
)

// ApplyStateKey Allocate state key when calling getBridgeFunc interface
func ApplyStateKey(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
	defer utils.TimeCost("ApplyStateKey")()
	log.GetLogger().Infof("[bridgeplugin] ApplyStateKey in plugin begin, client podName: %s,"+
		"assignStateId: %s, traceID: %s", client.PodName, client.AssignStateID, traceID)
	if client.AssignStateID != "" {
		log.GetLogger().Infof("[bridgeplugin] client podName: %s have assigned specific stateID %s, traceID: %s",
			client.PodName, client.AssignStateID, traceID)
		return registerAssignStateKey(client, traceID)
	}
	log.GetLogger().Infof("[bridgeplugin] client podName: %s does not assign specific stateID, "+
		"apply new stateKey, traceID %s", client.PodName, traceID)
	return applyStateKey(client, traceID)
}

func applyStateKey(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
	bridgeCache := cache.Instance()
	funcInfo := client.FuncInfo
	podMutex, err := getClientInsPodLock(client, bridgeCache, traceID)
	if err != nil {
		return "", false, err
	}
	podMutex.Lock()
	defer podMutex.Unlock()
	functionURN := utils.NewFunctionURN(funcInfo.TenantID, funcInfo.ServiceID, funcInfo.BridgeFuncName)
	lock, stateIDInfo, err := getStateIDInfo(client, traceID, functionURN, bridgeCache)
	if lock != nil {
		defer lock.Unlock()
	}
	if err != nil {
		return "", false, err
	}
	if strings.Contains(client.PodName, "checkpoint") {
		log.GetLogger().Infof("client podName:%s no need to update ref count, traceID %s", client.PodName, traceID)
		return stateIDInfo.StateKey, stateIDInfo.IsNewStateKeyScenario, nil
	}
	err = bridgeCache.RegisterClientInsBridgeFunUseInfo(client, stateIDInfo.StateID, utils.RefCountAddOne, traceID)
	if err != nil {
		log.GetLogger().Errorf("register failed, client podName: %s,traceID: %s", client.PodName, traceID)
		return "", false, err
	}
	err = recordStateIDInfo(stateIDInfo, functionURN, traceID)
	if err != nil {
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(client, utils.RefCountMinusOne, traceID)
		return "", false, err
	}
	if err = persistStateIDInfo(bridgeCache, client, stateIDInfo, traceID); err != nil {
		return "", false, err
	}
	log.GetLogger().Infof("applyStateKey finish, client podName: %s, stateKey:%s, isNewStateKeyScenario: %t, "+
		"traceID: %s", client.PodName, stateIDInfo.StateKey, stateIDInfo.IsNewStateKeyScenario, traceID)
	return stateIDInfo.StateKey, stateIDInfo.IsNewStateKeyScenario, nil
}

func persistStateIDInfo(bridgeCache *cache.BridgeCache, client *bgtypes.ClientInstance,
	stateIDInfo *bgtypes.StateIDInfo, traceID string) error {
	clientInstInfo, ok := bridgeCache.GetClientInfoFromCache(client, traceID)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] client instance info of podName: %s is not exist, traceID: %s",
			client.PodName, traceID)
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(client, utils.RefCountMinusOne, traceID)
		_ = bridgeCache.UpdateRefCountCacheAndDeleteWhenRefZero(stateIDInfo.StateID, utils.RefCountMinusOne, false,
			traceID)
		return fmt.Errorf("[bridgeplugin] client instance info of podName: %s is not exist, traceID: %s",
			client.PodName, traceID)
	}
	clientInstInfo.AssignStateID = stateIDInfo.StateID
	clientInstInfo.LastModifyClusterID = utils.ClusterID
	cacheInfoKey := utils.GetClientCacheInfoKey(client)
	log.GetLogger().Debugf("applyStateKey update ClientPodClientInstCache, "+
		"key:%s, val:%+v", cacheInfoKey, clientInstInfo)
	bridgeCache.ClientPodClientInstCache.Store(cacheInfoKey, clientInstInfo)
	return persistCacheInApply(clientInstInfo, stateIDInfo.StateID, traceID)
}

func updateClientCiteBridgeFuncInfo(client *bgtypes.ClientInstance, bridgeCache *cache.BridgeCache, traceID string) {
	if client.FuncInfo == nil {
		log.GetLogger().Infof("[bridgeplugin] no client bridge func info, no need to update, "+
			"client podName: %s,traceID: %s", client.PodName, traceID)
		return
	}
	log.GetLogger().Infof("[bridgeplugin] update ClientCiteBridgeFunc, client podName: %s, bridgeFuncName：%s, "+
		"traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
	var citeBridgeInfo []string
	v, ok := bridgeCache.ClientCiteBridgeFuncMap.Load(client.PodName)
	if !ok {
		log.GetLogger().Infof("[bridgeplugin] no client bridge func info, need to create, client podName: %s,"+
			"bridgeFuncName：%s, traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
		citeBridgeInfo = make([]string, 0)
		citeBridgeInfo = append(citeBridgeInfo, client.FuncInfo.BridgeFuncName)
	} else {
		matchFlag := false
		citeBridgeInfo, ok = v.([]string)
		if !ok {
			log.GetLogger().Errorf("[bridgeplugin] failed to convert slice failed, client podName:%s, "+
				"bridgeFuncName:%s, traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
			return
		}
		for _, value := range citeBridgeInfo {
			if value == client.FuncInfo.BridgeFuncName {
				matchFlag = true
				break
			}
		}
		if !matchFlag {
			citeBridgeInfo = append(citeBridgeInfo, client.FuncInfo.BridgeFuncName)
		}
	}
	bridgeCache.ClientCiteBridgeFuncMap.Store(client.PodName, citeBridgeInfo)
	log.GetLogger().Infof("[bridgeplugin] update ClientCiteBridgeFunc succeed, client podName: "+
		"%s, citeBridgeInfo:%+v, traceID: %s", client.PodName, citeBridgeInfo, traceID)
}

func deleteClientCiteBridgeFuncInfo(client *bgtypes.ClientInstance, traceID string, bridgeCache *cache.BridgeCache) {
	log.GetLogger().Infof("[bridgeplugin] delete ClientCiteBridgeFunc, client podName: %s, "+
		"bridgeFuncName： %s, traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
	var citeBridgeInfo []string
	var originalCiteBridgeInfo []string
	v, ok := bridgeCache.ClientCiteBridgeFuncMap.Load(client.PodName)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] no cache clientCite bridgeFunc,client podName:%s, "+
			"bridgeFuncName:%s,traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
		return
	}
	originalCiteBridgeInfo, ok = v.([]string)
	if !ok {
		log.GetLogger().Errorf("[bridgeplugin] failed to convert slice failed, client podName:%s, bridgeFuncName:%s,"+
			" traceID: %s", client.PodName, client.FuncInfo.BridgeFuncName, traceID)
		return
	}
	citeBridgeInfo = originalCiteBridgeInfo[:0]
	for _, v := range originalCiteBridgeInfo {
		if v != client.FuncInfo.BridgeFuncName {
			citeBridgeInfo = append(citeBridgeInfo, v)
		}
	}
	if len(citeBridgeInfo) == 0 {
		log.GetLogger().Infof("[bridgeplugin] need to delete ClientCiteBridgeFunc, client podName: %s, "+
			"traceID: %s,", client.PodName, traceID)
		bridgeCache.ClientCiteBridgeFuncMap.Delete(client.PodName)
	} else {
		log.GetLogger().Infof("[bridgeplugin] need to update ClientCiteBridge, client podName:"+
			"%s, citeBridgeInfo:%+v, traceID: %s, ", client.PodName, citeBridgeInfo, traceID)
		bridgeCache.ClientCiteBridgeFuncMap.Store(client.PodName, citeBridgeInfo)
	}
	log.GetLogger().Infof("[bridgeplugin] delete ClientCiteBridgeFunc succeed, client podName: %s, "+
		"citeBridgeInfo:%+v, traceID: %s, ", client.PodName, citeBridgeInfo, traceID)
}

func getStateIDInfo(client *bgtypes.ClientInstance, traceID string, functionURN string,
	bridgeCache *cache.BridgeCache) (*sync.Mutex, *bgtypes.StateIDInfo, error) {
	isNewStateKeyScenario := false
	stateID, stateInfo, availableExist := bridgeCache.PickAvailableStateKey(functionURN, traceID)
	lock, err := getStateIDLock(client, bridgeCache, stateID, traceID)
	if err == nil {
		lock.Lock()
	} else {
		return lock, nil, err
	}
	stateKey := stateInfo.StateKey
	bridgeCache.StateWorkersLock.RLock()
	isStateWorkerExist := bridgeCache.StateWorkersCache[stateID]
	bridgeCache.StateWorkersLock.RUnlock()
	if availableExist && !isStateWorkerExist && len(stateInfo.PodName) != 0 {
		releaseIllegalStateID(stateID, traceID)
		availableExist = false
	}
	isCheckPoint := strings.Contains(client.PodName, "checkpoint")
	if !availableExist || isCheckPoint {
		log.GetLogger().Infof("[bridgeplugin] no available existed stateKey can be used, "+
			"try to generate new one, client podName: %s, traceID: %s", client.PodName, traceID)
		isRejectNew, err := bridgeCache.IsReachMaxStateKeyCount(functionURN, traceID)
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] judge whether reach max stateKey failed. "+
				"functionURN: %s, traceID: %s", functionURN, traceID)
			return lock, nil, err
		}
		if isRejectNew {
			log.GetLogger().Errorf("pick available stateKey failed. traceID: %s, "+
				"client podName: %s", traceID, client.PodName)
			return lock, nil, fmt.Errorf("apply stateKey failed, traceID: %s, client: %s", traceID, client.PodName)
		}
		stateKey = genStateKey()
		if !isCheckPoint {
			go checkBridgeIDRefValidity(client, bridgeCache, stateKey, traceID)
		}
		isNewStateKeyScenario = true
	}
	if availableExist && len(stateInfo.PodName) == 0 {
		err := checkBridgeInstanceReady(stateID, stateKey, bridgeCache)
		if err != nil {
			return lock, nil, err
		}
	}
	stateID = genStateIDByKey(client, stateKey, traceID)
	stateIDInfo := buildStateIDInfo(stateKey, stateID, stateInfo.PodName, isNewStateKeyScenario)
	return lock, &stateIDInfo, nil
}

func checkBridgeInstanceReady(stateID, stateKey string, bridgeCache *cache.BridgeCache) error {
	log.GetLogger().Infof("find available stateID:%s stateKey:%s, but bridge instance is not ready", stateID, stateKey)
	// load the latest stateInfo to judge bridge instance status
	stateIDInfoIf, ok := bridgeCache.StateIDInfoCache.Load(stateID)
	if !ok {
		return fmt.Errorf("can't find stateIdInfo of %s, action:retry", stateKey)
	}
	stateInfo, ok := stateIDInfoIf.(*cache.StateIDInfo)
	if !ok {
		return fmt.Errorf("assert stateIdInfo failed of %s, action:retry", stateKey)
	}
	if len(stateInfo.PodName) == 0 {
		return fmt.Errorf("bridge instance of %s still not ready, action:retry", stateKey)
	}
	return nil
}

func checkBridgeIDRefValidity(client *bgtypes.ClientInstance, bridgeCache *cache.BridgeCache,
	stateKey string, traceID string) {
	time.Sleep(stateWorkerTimeout * time.Second)
	stateID := genStateIDByKey(client, stateKey, traceID)
	bridgeCache.StateWorkersLock.RLock()
	isStateWorkerExist := bridgeCache.StateWorkersCache[stateID]
	bridgeCache.StateWorkersLock.RUnlock()
	funcInfo := client.FuncInfo
	if !isStateWorkerExist {
		log.GetLogger().Infof("state worker not exist, release bridgeid ref of "+
			"stateID:%s, traceID:%s", stateID, traceID)
		releaseIllegalBridgeIDAndStateWorker(stateID, traceID, funcInfo)
		return
	}
	restTime := bridgeInstanceTimeout - stateWorkerTimeout
	time.Sleep(time.Duration(restTime) * time.Second)
	stateIDInfoIf, ok := bridgeCache.StateIDInfoCache.Load(stateID)
	if !ok {
		log.GetLogger().Infof("can't find stateIdInfo, release bridgeid ref of "+
			"stateID:%s, traceID:%s", stateID, traceID)
		releaseIllegalBridgeIDAndStateWorker(stateID, traceID, funcInfo)
		return
	}
	stateInfo, ok := stateIDInfoIf.(*cache.StateIDInfo)
	if !ok {
		log.GetLogger().Infof("assert stateIdInfo failed, release bridgeid ref of "+
			"stateID:%s, traceID:%s", stateID, traceID)
		releaseIllegalBridgeIDAndStateWorker(stateID, traceID, funcInfo)
		return
	}
	if len(stateInfo.PodName) == 0 {
		log.GetLogger().Infof("bridge instance still not ready, release bridgeid ref of "+
			"stateID:%s, traceID:%s", stateID, traceID)
		releaseIllegalBridgeIDAndStateWorker(stateID, traceID, funcInfo)
		return
	}
	log.GetLogger().Debugf("new applied stateID %s is ok, traceID:%s", stateID, traceID)
}

func releaseIllegalBridgeIDAndStateWorker(stateID string, traceID string, funcInfo *bgtypes.BridgeFuncInfo) {
	err := ReleaseBridgeWhenSuicide(&bgtypes.BridgeInstance{}, stateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("release bridgeid ref of stateID:%s failed:%s, "+
			"traceID:%s", stateID, err.Error(), traceID)
	}
	if funcInfo == nil {
		log.GetLogger().Errorf("funcInfo is nil, can't delete persistent state of stateID:%s, "+
			"traceID:%s", stateID, traceID)
		return
	}
	err = deletePersistentState(funcInfo.TenantID, funcInfo.ServiceID, funcInfo.BridgeFuncName, stateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("delete persistent state of stateID:%s failed:%s, "+
			"traceID:%s", stateID, err.Error(), traceID)
	}
}

func buildStateIDInfo(stateKey string, stateID string, podName string, isNewStateKey bool) bgtypes.StateIDInfo {
	stateIDInfo := bgtypes.StateIDInfo{}
	stateIDInfo.StateKey = stateKey
	stateIDInfo.StateID = stateID
	stateIDInfo.IsNewStateKeyScenario = isNewStateKey
	stateIDInfo.PodName = podName
	return stateIDInfo
}

func releaseIllegalStateID(stateID string, traceID string) {
	log.GetLogger().Infof("find illegal stateID:%s, release it and apply a new one, traceID:%s", stateID, traceID)
	err := ReleaseBridgeWhenSuicide(&bgtypes.BridgeInstance{}, stateID, traceID)
	if err != nil {
		log.GetLogger().Warnf("release illegal stateID:%s failed:%s, still apply a new one, "+
			"traceID:%s", stateID, err.Error(), traceID)
	}
}

func persistCacheInApply(client *bgtypes.ClientInstance, stateID string, traceID string) error {
	bridgeCache := cache.Instance()
	err := bridgeCache.StoreSpecificStateIDInfoToEtcd(stateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] store StateIdInfoCache of stateID: %s to etcd failed, "+
			"errMsg: %s, traceID: %s", stateID, err.Error(), traceID)
		// rollback stateIdInfoCache cache change and ClientPodClientInstCache change
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(client, utils.RefCountMinusOne, traceID)
		_ = bridgeCache.UpdateRefCountCacheAndDeleteWhenRefZero(stateID, utils.RefCountMinusOne, false,
			traceID)
		return err
	}
	err = bridgeCache.StoreSpecificClientInstToEtcd(client, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] store ClientPodClientInstCache to etcd failed, errMsg: %s, "+
			"traceID: %s", err.Error(), traceID)
		// rollback clientPodClientInstCache cache change
		// rollback stateIdInfoCache cache change and ClientPodClientInstCache change
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(client, utils.RefCountMinusOne, traceID)
		_ = bridgeCache.UpdateRefCountCacheAndDeleteWhenRefZero(stateID, utils.RefCountMinusOne, true,
			traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return err
	}
	updateClientCiteBridgeFuncInfo(client, bridgeCache, traceID)
	return nil
}

func recordStateIDInfo(stateInfo *bgtypes.StateIDInfo, functionURN string, traceID string) error {
	bridgeCache := cache.Instance()
	maxStateKeyQua, maxActiveQua, err := bridgeCache.GetRestrictByURN(functionURN)
	if err != nil {
		return err
	}
	stateID := stateInfo.StateID
	stateKey := stateInfo.StateKey
	stateIDInfo, ok := bridgeCache.GetStateIDInfoFromCache(stateID, traceID)
	if ok {
		stateIDInfo.RefCount++
		stateIDInfo.StateKey = stateKey
		stateIDInfo.BridgeFuncURN = functionURN
		stateIDInfo.MaxBgInstQua = maxStateKeyQua
		stateIDInfo.MaxActiveQua = maxActiveQua
	} else {
		clusterRefCount := make(map[string]int, 1)
		clusterRefCount[utils.ClusterID]++
		stateInfo := &cache.StateIDInfo{
			PodName:       stateInfo.PodName,
			StateKey:      stateKey,
			RefCount:      1,
			BridgeFuncURN: functionURN,
			MaxBgInstQua:  maxStateKeyQua,
			MaxActiveQua:  maxActiveQua,
			BelongCluster: utils.ClusterID,
		}
		log.GetLogger().Debugf("recordStateIDInfo update StateIDInfoCache, key:%s, val:%+v", stateID, stateInfo)
		bridgeCache.StateIDInfoCache.Store(stateID, stateInfo)
	}
	return nil
}

func retryRecordStateIDInfo(client *bgtypes.ClientInstance, traceID string) error {
	log.GetLogger().Errorf("[bridgeplugin] stateId cache is empty when register refCount, need to store,"+
		"tenantId:%s, serviceId:%s, assignStateKey:%s, bridgeFuncName:%s, traceID: %s",
		client.FuncInfo.TenantID, client.FuncInfo.ServiceID, client.AssignStateKey, client.FuncInfo.BridgeFuncName,
		traceID)
	functionURN := utils.NewFunctionURN(client.FuncInfo.TenantID, client.FuncInfo.ServiceID,
		client.FuncInfo.BridgeFuncName)
	stateInfo := &bgtypes.StateIDInfo{StateID: client.AssignStateID, StateKey: client.AssignStateKey}
	return recordStateIDInfo(stateInfo, functionURN, traceID)
}

func registerAssignStateKey(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
	bridgeCache := cache.Instance()
	podMutex, err := getClientInsPodLock(client, bridgeCache, traceID)
	if err != nil {
		return "", false, err
	}
	podMutex.Lock()
	defer podMutex.Unlock()

	lock, err := getStateIDLock(client, bridgeCache, client.AssignStateID, traceID)
	if err != nil {
		return "", false, err
	}
	lock.Lock()
	defer lock.Unlock()

	err = bridgeCache.RegisterRefCount(client.AssignStateID, traceID)
	if err != nil {
		err = retryRecordStateIDInfo(client, traceID)
		if err != nil {
			return "", false, err
		}
	}
	err = bridgeCache.StoreSpecificStateIDInfoToEtcd(client.AssignStateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] store StateIdInfoCache of stateID: %s to etcd failed, errMsg: %s, "+
			"traceID: %s, client: %s", client.AssignStateID, err.Error(), traceID, client.PodName)
		_ = bridgeCache.UpdateRefCountCacheAndDeleteWhenRefZero(client.AssignStateID, utils.RefCountMinusOne,
			false, traceID)
		return "", false, err
	}

	err = bridgeCache.UpdateClientInsBridgeFunUseInfo(client, utils.RefCountAddOne, traceID)
	if err != nil {
		_ = bridgeCache.UpdateRefCountCacheAndDeleteWhenRefZero(client.AssignStateID, utils.RefCountMinusOne,
			true, traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return "", false, err
	}

	err = bridgeCache.StoreSpecificClientInstToEtcd(client, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] store ClientPodClientInstCache to etcd failed, errMsg: %s, "+
			"traceID: %s, client: %s", err.Error(), traceID, client.PodName)
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(client, utils.RefCountMinusOne, traceID)
		_ = bridgeCache.UpdateRefCountCacheAndDeleteWhenRefZero(client.AssignStateID, utils.RefCountMinusOne,
			true, traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return "", false, err
	}
	updateClientCiteBridgeFuncInfo(client, bridgeCache, traceID)
	return "", false, nil
}

// NewBridge Update relationship when watch bridgefunc instance added
func NewBridge(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
	if bgInst == nil {
		log.GetLogger().Errorf("[bridgeplugin] bridgeInstance is nil when NewBridge, stateID: %s, traceID: %s",
			stateID, traceID)
		return fmt.Errorf("[bridgeplugin] bridgeInstance is nil when NewBridge, stateID: %s, traceID: %s",
			stateID, traceID)
	}
	defer utils.TimeCost("NewBridge")()
	log.GetLogger().Infof("[bridgeplugin] NewBridge in plugin begin, stateID: %s, traceID: %s, client: %s",
		stateID, traceID, bgInst.PodName)
	bridgeCache := cache.Instance()
	exPodName, err := bridgeCache.UpdateBridgeFuncPodCache(stateID, bgInst.PodName, traceID)
	if err != nil {
		return err
	}
	err = bridgeCache.StoreSpecificStateIDInfoToEtcd(stateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] store specific stateInfo to etcd failed, errMsg: %s, stateID: %s, "+
			"traceID: %s, client: %s", err.Error(), stateID, traceID, bgInst.PodName)
		_, _ = bridgeCache.UpdateBridgeFuncPodCache(stateID, exPodName, traceID)
		return err
	}
	return nil
}

func deleteClientEtcdInfoAndCache(bridgeCache *cache.BridgeCache, clientInstInfo *bgtypes.ClientInstance,
	stateID string, traceID string) error {
	if err := bridgeCache.DeleteSpecificClientInfoETCD(clientInstInfo, traceID); err != nil {
		log.GetLogger().Errorf("[bridgeplugin] delete etcd info to release clientInstInfo FuncUseInfo, "+
			"stateID: %s traceID: %s", stateID, traceID)
		return err
	}
	cacheInfoKey := utils.GetClientCacheInfoKey(clientInstInfo)
	log.GetLogger().Debugf("deleteClientEtcdInfoAndCache delete ClientPodClientInstCache, "+
		"key:%s, val:%+v", cacheInfoKey, nil)
	bridgeCache.ClientPodClientInstCache.Delete(cacheInfoKey)
	updateClientCiteBridgeFunc(bridgeCache, cacheInfoKey)
	bridgeCache.ClientPodNameLockMap.Delete(clientInstInfo.PodName)
	return nil
}

func updateClientCiteBridgeFunc(bridgeCache *cache.BridgeCache, cacheInfoKey string) {
	parts := strings.Split(cacheInfoKey, "_")
	if len(parts) != cacheInfoKeyPartLens {
		log.GetLogger().Warnf("update ClientCiteBridgeFuncMap of cacheInfoKey %s failed, "+
			"cacheInfoKeyPartLens not right", cacheInfoKey)
		return
	}
	podName := parts[podNameIdxInCacheInfoKeyParts]
	bridgeFunc := parts[bridgeFuncIdxInCacheInfoKeyParts]
	bridgeFuncsIf, ok := bridgeCache.ClientCiteBridgeFuncMap.Load(podName)
	if !ok {
		log.GetLogger().Warnf("client %s cite bridgefuncs not found, ignore", podName)
		return
	}
	restBridgeFuncs := make([]string, 0)
	bridgeFuncs, ok := bridgeFuncsIf.([]string)
	if !ok {
		log.GetLogger().Warnf("client %s cite bridgefuncs assert failed", podName)
		return
	}
	for _, item := range bridgeFuncs {
		if item != bridgeFunc {
			restBridgeFuncs = append(restBridgeFuncs, item)
		}
	}
	if len(restBridgeFuncs) == 0 {
		log.GetLogger().Infof("delete client %s cite info from ClientCiteBridgeFuncMap", podName)
		bridgeCache.ClientCiteBridgeFuncMap.Delete(podName)
	} else {
		log.GetLogger().Infof("update client cite info, podName:%s, restBridgeFuncs:%+v", podName, restBridgeFuncs)
		bridgeCache.ClientCiteBridgeFuncMap.Store(podName, restBridgeFuncs)
	}
}

func releaseClientInfoWhenSuicide(clientInstInfo *bgtypes.ClientInstance, stateID string, traceID string) error {
	defer utils.TimeCost("releaseClientInfoWhenSuicide")()
	bridgeCache := cache.Instance()

	if clientInstInfo.FuncUseInfo == nil {
		log.GetLogger().Errorf("[bridgeplugin] clientInstInfo FuncUseInfo is nil, stateID: %s traceID: %s, "+
			"need to release client Instance", stateID, traceID)
		if err := deleteClientEtcdInfoAndCache(bridgeCache, clientInstInfo, stateID, traceID); err != nil {
			return err
		}
		return nil
	}
	if !utils.IsClientInsFuncUseInfoEmpty(clientInstInfo) {
		log.GetLogger().Infof("[bridgeplugin] client instance use func Info is not empty, need to update "+
			"FuncUserInfo, stateID: %s traceID: %s", stateID, traceID)
		return bridgeCache.UpdateSpecificClientInstToEtcd(clientInstInfo, traceID)
	}

	log.GetLogger().Infof("[bridgeplugin] client Instance all funcUseInfo is empty, need to delete etcd info, "+
		"stateID: %s traceID: %s", stateID, traceID)
	if err := deleteClientEtcdInfoAndCache(bridgeCache, clientInstInfo, stateID, traceID); err != nil {
		return err
	}

	return nil
}

func extractCiteSuicideBridgePodClientInstance(bridgeCache *cache.BridgeCache, stateID string) sync.Map {
	var citeBridgePodClientInstMap sync.Map
	bridgeCache.ClientPodClientInstCache.Range(func(key, value interface{}) bool {
		clientInstance, ok := value.(*bgtypes.ClientInstance)
		if !ok {
			log.GetLogger().Errorf("[bridgeplugin] clientInst cache map value reflect to ClientInstance error")
			return true
		}
		clientInstance.FuncUseInfoLock.RLock()
		for citeStateID := range clientInstance.FuncUseInfo {
			if citeStateID == stateID {
				log.GetLogger().Infof("[bridgeplugin] match clientInstance: %+v, citeStateID:%s", clientInstance,
					citeStateID)
				citeBridgePodClientInstMap.Store(clientInstance.PodName+citeStateID, clientInstance)
				break
			}
		}
		clientInstance.FuncUseInfoLock.RUnlock()
		return true
	})
	return citeBridgePodClientInstMap
}

// ReleaseBridgeWhenSuicide Release etcd and cache info when bridge instance suicides
func ReleaseBridgeWhenSuicide(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
	defer utils.TimeCost("ReleaseBridgeWhenSuicide")()
	log.GetLogger().Infof("[bridgeplugin] Bridge suicide process in plugin to release info begin, stateID: %s,"+
		"bridge podName: %s, traceID: %s", stateID, bgInst.PodName, traceID)

	bridgeCache := cache.Instance()

	err := doReleaseStateInfo(stateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] release state info cache and etcd info when bridge suicide "+
			"procedure err %s", err.Error())
		return err
	}

	var citeBridgePodClientInstMap = extractCiteSuicideBridgePodClientInstance(bridgeCache, stateID)
	var tenantID string
	var serviceID string
	var bridgeFuncName string
	var singleInitFlag = false
	citeBridgePodClientInstMap.Range(func(key, value interface{}) bool {
		if clientItem, ok := value.(*bgtypes.ClientInstance); ok {
			log.GetLogger().Infof("[bridgeplugin] need to process quoteClientInstance item: %+v", clientItem)
			if clientItem.FuncInfo != nil && !singleInitFlag {
				singleInitFlag = true
				tenantID = clientItem.FuncInfo.TenantID
				serviceID = clientItem.FuncInfo.ServiceID
				bridgeFuncName = clientItem.FuncInfo.BridgeFuncName
			}
			if err = processClientInfoWhenBridgeSuicide(clientItem, bridgeCache, stateID, traceID); err != nil {
				log.GetLogger().Errorf("[bridgeplugin] clientInstance item delete failed, client podName: %s, "+
					"stateID: %s, err: %s", clientItem.PodName, stateID, err.Error())
			}
		}
		return true
	})

	if tenantID != "" && serviceID != "" && bridgeFuncName != "" {
		err = DeletePersistentStateByBridgeInfo(tenantID, serviceID, bridgeFuncName, stateID, traceID)
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] delete state failed, tenantID %s, serviceID:%s, "+
				"bridgeFuncName:%s, stateID: %s, err: %s", tenantID, serviceID, bridgeFuncName, stateID, err.Error())
			return err
		}
	}
	return err
}

// DeletePersistentStateByBridgeInfo is to delete state router
func DeletePersistentStateByBridgeInfo(tenantID string, serviceID string,
	functionName string, stateID string, traceID string) error {
	err := deletePersistentState(tenantID, serviceID, functionName, stateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] delete persistent state failed in suicide procedure, "+
			"stateID: %s traceID: %s", stateID, traceID)
		return err
	}
	log.GetLogger().Infof("[bridgeplugin] succeed to delete persistent state in suicide procedure, "+
		"stateID: %s traceID: %s", stateID, traceID)
	return nil
}

func processClientInfoWhenBridgeSuicide(client *bgtypes.ClientInstance, bridgeCache *cache.BridgeCache, stateID string,
	traceID string) error {
	podMutex, err := getClientInsPodLock(client, bridgeCache, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to get client ins lock in bridge suicide procedure "+
			"err %s", err.Error())
		return err
	}
	podMutex.Lock()
	defer podMutex.Unlock()

	if err := bridgeCache.EmptyClientInsStateIDCiteFuncUseInfo(client, stateID, traceID); err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to update empty bridge func use info of client: %s, "+
			"traceID: %s stateID: %s", client.PodName, traceID, stateID)
	}

	if err := releaseClientInfoWhenSuicide(client, stateID, traceID); err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to release when bridge suicide flow "+
			"client: %s, traceID: %s, stateID: %s", client.PodName, traceID, stateID)
	}
	return nil
}

// ProcessRelatedInfo process related info
func ProcessRelatedInfo(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
	bridgeCache := cache.Instance()
	stateID := client.AssignStateID
	isRelatedWithClient := bridgeCache.IsStateIDRelatedWithCurrentClientIns(client, client.AssignStateID)
	if !isRelatedWithClient {
		log.GetLogger().Infof(
			"[bridgeplugin] ReleaseBridge in plugin, stateID %s is not related with current "+
				"client pod:%s, traceID: %s", client.AssignStateID, client.PodName, traceID)
		return false, nil
	}

	lock, err := getStateIDLock(client, bridgeCache, client.AssignStateID, traceID)
	if err != nil {
		return false, err
	}
	lock.Lock()
	defer lock.Unlock()

	currentRefCount := bridgeCache.GetRefCountFromCache(stateID, traceID)
	if currentRefCount < 0 {
		_, err = doReleaseStateWhenCrash(client, traceID, isCrashScenario, bridgeCache)
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] client crash handle failed, stateID %s, client %s, traceID: %s",
				client.AssignStateID, client.PodName, traceID)
		}

		log.GetLogger().Debugf("[bridgeplugin] currentRefCount of stateID %s client %s is %d, traceID: %s",
			stateID, client.PodName, currentRefCount, traceID)
		return false, nil
	}

	isNeed, err := doReleaseOrUpdate(client, isCrashScenario, traceID, currentRefCount)
	if err != nil {
		return isNeed, err
	}
	log.GetLogger().Infof("[bridgeplugin] ReleaseBridge end, delete action execute ? %t."+
		"client: %s, stateID: %s, traceID: %s", isNeed, client.PodName, stateID, traceID)
	if isNeed {
		deleteClientCiteBridgeFuncInfo(client, traceID, bridgeCache)
	}
	return isNeed, nil
}

// ReleaseBridge Clean relationship when calling runtime Release interface
func ReleaseBridge(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
	defer utils.TimeCost("ReleaseBridge")()
	log.GetLogger().Infof("[bridgeplugin] ReleaseBridge in plugin begin, client PodName: %s, "+
		"isCrashScenario：%t, traceID: %s", client.PodName, isCrashScenario, traceID)
	bridgeCache := cache.Instance()
	podMutex, err := getClientInsPodLock(client, bridgeCache, traceID)
	if err != nil {
		return false, err
	}
	podMutex.Lock()
	defer podMutex.Unlock()
	return ProcessRelatedInfo(client, isCrashScenario, traceID)
}

func doReleaseOrUpdate(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string,
	currentRefCount int) (bool, error) {
	bridgeCache := cache.Instance()

	var updateCount int
	if isCrashScenario {
		updateCount = bridgeCache.GetRequiredDeductionRefCount(client, traceID)
	} else {
		updateCount = 1
	}
	isNeed := currentRefCount == updateCount
	log.GetLogger().Infof("[bridgeplugin] doReleaseOrUpdate isNeed: %t, currentRefCount:%d, updateCount:%d,"+
		"client podName:%s, traceID:%s", isNeed, currentRefCount, updateCount, client.PodName, traceID)
	if isNeed {
		err := doRelease(client, isCrashScenario, updateCount, traceID)
		if err != nil {
			return isNeed, err
		}
	} else {
		err := doUpdateCacheAndEtcdInfo(client, traceID, updateCount, isCrashScenario)
		if err != nil {
			return isNeed, err
		}
	}
	return isNeed, nil
}

func getStateIDLock(client *bgtypes.ClientInstance, bridgeCache *cache.BridgeCache, stateID string,
	traceID string) (*sync.Mutex, error) {
	stateLock, _ := bridgeCache.StateIDLockMap.LoadOrStore(stateID, &sync.Mutex{})
	lock, ok := stateLock.(*sync.Mutex)
	if !ok || lock == nil {
		return nil, fmt.Errorf("[bridgeplugin] failed to convert an interface to a lock, "+
			"client pod: %s, traceID: %s", client.PodName, traceID)
	}
	return lock, nil
}

func getClientInsPodLock(client *bgtypes.ClientInstance, bridgeCache *cache.BridgeCache, traceID string) (*sync.Mutex,
	error) {
	podLock, _ := bridgeCache.ClientPodNameLockMap.LoadOrStore(client.PodName, &sync.Mutex{})
	podMutex, ok := podLock.(*sync.Mutex)
	if !ok || podMutex == nil {
		return nil, fmt.Errorf("[bridgeplugin] failed to convert an interface to a lock, traceID: %s, "+
			"client podName: %s, bridgeFuncName: %s", traceID, client.PodName, client.FuncInfo.BridgeFuncName)
	}
	return podMutex, nil
}

func doUpdateCacheAndEtcdInfo(client *bgtypes.ClientInstance, traceID string, updateCount int,
	isCrashScenario bool) error {
	bridgeCache := cache.Instance()
	err := bridgeCache.UpdateRefCountCache(client.AssignStateID, utils.RefCountMinusOne*updateCount, traceID)
	if err != nil {
		return err
	}

	err = bridgeCache.StoreSpecificStateIDInfoToEtcd(client.AssignStateID, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] update stateIDInfo of stateID: %s"+
			"failed when no need delete instance, traceID: %s", client.AssignStateID, traceID)
		_ = bridgeCache.UpdateRefCountCache(client.AssignStateID, updateCount, traceID)
		return err
	}

	if isCrashScenario {
		_, err := doReleaseClientWhenCrash(client, traceID, bridgeCache)
		return err
	}

	err = bridgeCache.UpdateClientInsBridgeFunUseInfo(client, utils.RefCountMinusOne*updateCount, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to update bridge func use info of client: %s, traceID: %s",
			client.PodName, traceID)
		_ = bridgeCache.UpdateRefCountCache(client.AssignStateID, updateCount, traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return err
	}

	if err = bridgeCache.StoreSpecificClientInstToEtcd(client, traceID); err != nil {
		log.GetLogger().Errorf("[bridgeplugin] update stateIDInfo of stateID: %s "+
			"failed when no need delete instance, traceID: %s", client.AssignStateID, traceID)
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfo(client, updateCount, traceID)
		_ = bridgeCache.UpdateRefCountCache(client.AssignStateID, updateCount, traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return err
	}
	return nil
}

func doReleaseStateWhenCrash(client *bgtypes.ClientInstance, traceID string, isCrashScenario bool,
	bridgeCache *cache.BridgeCache) (bool, error) {
	if isCrashScenario {
		log.GetLogger().Infof("[bridgeplugin] client crash, delete persistent state info, stateID %s, client %s",
			client.AssignStateID, client.PodName)
		err := deletePersistentState(client.FuncInfo.TenantID, client.FuncInfo.ServiceID,
			client.FuncInfo.BridgeFuncName, client.AssignStateID, traceID)
		if err != nil {
			return true, err
		}

		return doReleaseClientWhenCrash(client, traceID, bridgeCache)
	}
	return false, nil
}

func doReleaseClientWhenCrash(client *bgtypes.ClientInstance, traceID string, bridgeCache *cache.BridgeCache) (bool,
	error) {
	clientInst, ok := bridgeCache.GetClientInfoFromCache(client, traceID)
	if !ok {
		return true, nil
	}

	if clientInst.FuncUseInfo == nil || len(clientInst.FuncUseInfo) == 0 {
		if err := bridgeCache.DeleteSpecificClientInfoETCD(clientInst, traceID); err != nil {
			return true, err
		}
		cacheInfoKey := utils.GetClientCacheInfoKey(clientInst)
		log.GetLogger().Debugf("doReleaseClientWhenCrash delete ClientPodClientInstCache, "+
			"key:%s, val:%+v", cacheInfoKey, nil)
		bridgeCache.ClientPodClientInstCache.Delete(cacheInfoKey)
		bridgeCache.ClientPodNameLockMap.Delete(clientInst.PodName)
		return true, nil
	}
	clientInst.FuncUseInfoLock.Lock()
	delete(clientInst.FuncUseInfo, client.AssignStateID)
	clientInst.FuncUseInfoLock.Unlock()
	if clientInst.FuncUseInfo == nil || len(clientInst.FuncUseInfo) == 0 {
		if err := bridgeCache.DeleteSpecificClientInfoETCD(clientInst, traceID); err != nil {
			return true, err
		}
		cacheInfoKey := utils.GetClientCacheInfoKey(clientInst)
		log.GetLogger().Debugf("doReleaseClientWhenCrash delete ClientPodClientInstCache, "+
			"key:%s, val:%+v", cacheInfoKey, nil)
		bridgeCache.ClientPodClientInstCache.Delete(cacheInfoKey)
		bridgeCache.ClientPodNameLockMap.Delete(clientInst.PodName)
	} else {
		if err := bridgeCache.UpdateSpecificClientInstToEtcd(clientInst, traceID); err != nil {
			return true, err
		}
	}

	return true, nil
}

func doRelease(client *bgtypes.ClientInstance, isCrashScenario bool, updateCount int, traceID string) error {
	log.GetLogger().Infof("[bridgeplugin] refCount is 0, need to update stateInfo and clientInfo for stateID %s,"+
		" client %s, traceID: %s", client.AssignStateID, client.PodName, traceID)
	bridgeCache := cache.Instance()

	err := doReleaseStateInfo(client.AssignStateID, traceID)
	if err != nil {
		return err
	}

	done, err := doReleaseStateWhenCrash(client, traceID, isCrashScenario, bridgeCache)
	if done {
		return err
	}

	clientInst, ok := bridgeCache.GetClientInfoFromCache(client, traceID)
	if !ok {
		return nil
	}

	err = bridgeCache.UpdateClientInsBridgeFunUseInfo(clientInst, utils.RefCountMinusOne*updateCount, traceID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] failed to update bridge func use info of client: %s,"+
			"stateID: %s, traceID: %s", clientInst.PodName, clientInst.AssignStateID, traceID)
		_ = bridgeCache.UpdateRefCountCache(client.AssignStateID, updateCount, traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return err
	}

	if err := doReleaseClientInfo(clientInst, traceID); err != nil {
		_ = bridgeCache.UpdateClientInsBridgeFunUseInfo(client, updateCount, traceID)
		_ = bridgeCache.UpdateRefCountCache(client.AssignStateID, updateCount, traceID)
		_ = bridgeCache.RollbackUpdateStateIDInfoToEtcd(client.AssignStateID, traceID)
		return err
	}
	return nil
}

func doReleaseStateInfo(stateID string, traceID string) error {
	bridgeCache := cache.Instance()
	if v, ok := bridgeCache.StateIDInfoCache.Load(stateID); ok {
		stateIDInfo, ok := v.(*cache.StateIDInfo)
		if !ok {
			log.GetLogger().Errorf("[bridgeplugin] failed to assert type StateIDInfo. key: %T,"+
				"stateID: %s, traceID: %s", stateIDInfo, stateID, traceID)
			return fmt.Errorf("[bridgeplugin] Failed to get stateIDInfo, stateID: %s, traceID: %s",
				stateID, traceID)
		}

		err := bridgeCache.DeleteSpecificStateIDInfoETCD(stateIDInfo, stateID, traceID)
		if err != nil {
			log.GetLogger().Errorf("[bridgeplugin] delete stateIDInfo of stateID: %s traceID: %s"+
				"failed when need to delete instance", stateID, traceID)
			return err
		}
		log.GetLogger().Infof("[bridgeplugin] start to delete stateIDInfo cache, stateID: %s traceID: %s",
			stateID, traceID)
		log.GetLogger().Debugf("doReleaseStateInfo delete StateIDInfoCache, key:%s, val:%+v", stateID, nil)
		bridgeCache.StateIDInfoCache.Delete(stateID)
		bridgeCache.StateIDLockMap.Delete(stateID)
	}
	return nil
}

func doReleaseClientInfo(clientInstInfo *bgtypes.ClientInstance, traceID string) error {
	defer utils.TimeCost("doReleaseClientInfo")()
	bridgeCache := cache.Instance()

	if clientInstInfo.FuncUseInfo == nil {
		if err := bridgeCache.DeleteSpecificClientInfoETCD(clientInstInfo, traceID); err != nil {
			return err
		}
		cacheInfoKey := utils.GetClientCacheInfoKey(clientInstInfo)
		log.GetLogger().Debugf("doReleaseClientInfo delete ClientPodClientInstCache, "+
			"key:%s, val:%+v", cacheInfoKey, nil)
		bridgeCache.ClientPodClientInstCache.Delete(cacheInfoKey)
		bridgeCache.ClientPodNameLockMap.Delete(clientInstInfo.PodName)
		return nil
	}

	if !utils.IsClientInsFuncUseInfoEmpty(clientInstInfo) {
		return bridgeCache.UpdateSpecificClientInstToEtcd(clientInstInfo, traceID)
	}

	if err := bridgeCache.DeleteSpecificClientInfoETCD(clientInstInfo, traceID); err != nil {
		return err
	}
	cacheInfoKey := utils.GetClientCacheInfoKey(clientInstInfo)
	log.GetLogger().Debugf("doReleaseClientInfo delete ClientPodClientInstCache, key:%s, val:%+v", cacheInfoKey, nil)
	bridgeCache.ClientPodClientInstCache.Delete(cacheInfoKey)
	bridgeCache.ClientPodNameLockMap.Delete(clientInstInfo.PodName)

	return nil
}

func genStateKey() string {
	return strings.Replace(uuid.New().String(), "-", "", -1)
}

func genStateIDByKey(client *bgtypes.ClientInstance, stateKey string, traceID string) string {
	// if stateKey is empty, stateID is generated by default.
	if len(stateKey) == 0 {
		return uuid.New().String()
	}
	stateID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(client.FuncInfo.TenantID+
		client.FuncInfo.ServiceID+client.FuncInfo.BridgeFuncName+stateKey))
	log.GetLogger().Infof("[bridgeplugin] generation stateID : %s from stateKey: %s in bridge plugin, "+
		"traceID: %s", stateID, stateKey, traceID)
	return stateID.String()
}

func deletePersistentState(tenantID, serviceID, functionName, stateID string, traceID string) error {
	req := &stateutils.Request{
		FunctionName:  functionName,
		StateID:       stateID,
		TenantID:      tenantID,
		ProxyFunction: utils.ProxyFunction,
		TraceID:       "",
	}
	err := instancemanager.DeleteStateIDHandlerByManager(req)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] delete etcd state of stateID: %s failed, traceID: %s", stateID,
			traceID)
		return err
	}
	count, err := utils.DeleteStateInRedis(tenantID, serviceID, functionName, stateID)
	if err != nil {
		log.GetLogger().Errorf("[bridgeplugin] delete redis state of stateID: %s failed, traceID: %s", stateID,
			traceID)
		return err
	}
	log.GetLogger().Infof("[bridgeplugin] delete redis state successfully, count: %d, tenantID: %s, "+
		"serviceID: %s, functionName:%s, stateID: %s, traceID: %s",
		count, tenantID, serviceID, functionName, stateID, traceID)
	return nil
}
