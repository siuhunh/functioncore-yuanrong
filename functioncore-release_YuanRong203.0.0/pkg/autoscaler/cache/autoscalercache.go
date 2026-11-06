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

// Package cache defines AutoScalerCache
package cache

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/functiontypes"
)

var (
	mgrCache *AutoScalerCache
	once     = sync.Once{}
	// initCacheSize is the init map size of worker cache
	initCacheSize = 10
)

const (
	defaultStoreTime = int64(24 * 60 * 60)
	noUseTime        = int64(15)
)

// AutoScalerCache -
type AutoScalerCache struct {
	// key: funcSvcID  value: *functiontypes.FunctionEtcdInfo
	FunctionMetaCache sync.Map
	// key: funcSvcID  value: *functiontypes.InstanceMetaData
	InstanceMetaCache sync.Map
	// ResourceMetricCache redis metric
	ResourceMetricCache map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
				PodName]resourceusage.ResourceList
	// store predict data for the next 30min from predict model
	PredictMetricsOutputCache resourceusage.PredictResourceListMap
	// store last 10min data for predict model
	PredictMetricsInputCache resourceusage.PredictResourceListMap
	ResourceMetricCacheMutex sync.RWMutex
	// cae metrics cache
	CaeMetricsCache map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
			PodName]caemetrics.List
	CaeMetricsCacheMutex sync.RWMutex

	PredictMetricsOutputCacheMutex sync.RWMutex
	PredictMetricsInputCacheMutex  sync.RWMutex
	// key: funcSvcID value: *InstanceCache
	FunctionInstanceCache sync.Map
	// CaeAvailableClusterCache key is funcSvcID, value is clusterIDs (type of []string)
	CaeAvailableClusterCache sync.Map
}

// GetInstance get the AutoScalerCache singleton instance
func GetInstance() *AutoScalerCache {
	once.Do(func() {
		mgrCache = &AutoScalerCache{
			FunctionMetaCache: sync.Map{},
			InstanceMetaCache: sync.Map{},
			ResourceMetricCache: make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
				PodName]resourceusage.ResourceList),
			PredictMetricsOutputCache: make(resourceusage.PredictResourceListMap),
			PredictMetricsInputCache:  make(resourceusage.PredictResourceListMap),
			ResourceMetricCacheMutex:  sync.RWMutex{},
			CaeMetricsCache: make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
				PodName]caemetrics.List),
			CaeMetricsCacheMutex:           sync.RWMutex{},
			PredictMetricsOutputCacheMutex: sync.RWMutex{},
			PredictMetricsInputCacheMutex:  sync.RWMutex{},
			FunctionInstanceCache:          sync.Map{},
			CaeAvailableClusterCache:       sync.Map{},
		}
	})
	return mgrCache
}

// RefreshResourceMetricCache -
func (a *AutoScalerCache) RefreshResourceMetricCache(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
	FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList) {
	a.ResourceMetricCacheMutex.Lock()
	defer a.ResourceMetricCacheMutex.Unlock()
	a.ResourceMetricCache = resourceMetricCache
	log.GetLogger().Debugf("refresh resource metric cache: %+v", resourceMetricCache)
	go a.RefreshPredictMetricsInputCache(resourceMetricCache)
}

// RefreshCaeMetricCache -
func (a *AutoScalerCache) RefreshCaeMetricCache(caeMetricCache map[resourceusage.ClusterID]map[resourceusage.
	FuncSvcID]map[resourceusage.PodName]caemetrics.List) {
	a.CaeMetricsCacheMutex.Lock()
	defer a.CaeMetricsCacheMutex.Unlock()
	a.CaeMetricsCache = caeMetricCache
	log.GetLogger().Debugf("refresh cae metric cache: %+v", caeMetricCache)
}

// DeletePredictMetricsInputCache -
func (a *AutoScalerCache) DeletePredictMetricsInputCache(functionSvcID resourceusage.FuncSvcID) {
	a.PredictMetricsInputCacheMutex.Lock()
	defer a.PredictMetricsInputCacheMutex.Unlock()
	delete(a.PredictMetricsInputCache, functionSvcID)
	log.GetLogger().Debugf("delete predict input cache by funcSvcID :%s", functionSvcID)
}

// RefreshPredictMetricsInputCache -
func (a *AutoScalerCache) RefreshPredictMetricsInputCache(
	resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
		FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config err when get predict from redis, err: %s", err.Error())
		return
	}
	a.PredictMetricsInputCacheMutex.Lock()
	defer a.PredictMetricsInputCacheMutex.Unlock()
	aggregateSumAll := make(resourceusage.PredictResourceListMap)
	for _, functionCacheMap := range resourceMetricCache {
		for funcSvcID, podCacheMap := range functionCacheMap {
			if !CheckPredictWhiteList(cfg.PredictConfig.PredictFuncWhiteList, string(funcSvcID)) {
				continue
			}
			aggregateFunc(podCacheMap, aggregateSumAll, funcSvcID)
		}
	}
	for funcSvcID, resourceList := range aggregateSumAll {
		// all cluster sum of function
		resource := resourceList.AggregateSumPredictResource()
		a.PredictMetricsInputCache[funcSvcID] = append(a.PredictMetricsInputCache[funcSvcID], resource)
	}
	a.adjustPredictMetricsInputCache(cfg.PredictConfig.StoreTime)
}

// CheckPredictWhiteList -
func CheckPredictWhiteList(list []string, id string) bool {
	// search user config from etcd, if user config predict enable, we think it's pass too
	funcMetadata, exists := GetInstance().GetFunctionMeta(id)
	if !exists {
		log.GetLogger().Errorf("failed to get function %s meta", id)
		return false
	}

	if funcMetadata.ExtendedMetaData.EnablePredict {
		log.GetLogger().Debugf("check %s user config enable predict is true", id)
		return true
	}

	// it changed because if user not config predict but all function open predict, it's not right
	for _, funcSvcID := range list {
		if id == funcSvcID {
			return true
		}
	}
	return false
}

func aggregateFunc(podCacheMap map[resourceusage.PodName]resourceusage.ResourceList,
	aggregateSumAll resourceusage.PredictResourceListMap, funcSvcID resourceusage.FuncSvcID) {
	var funcResourceList resourceusage.PredictResourceList
	for _, resourceList := range podCacheMap {
		// avg of pod of function
		if avgResource := resourceList.GetAvgDataInTime(time.Now().Unix(), noUseTime); avgResource != nil {
			funcResourceList = append(funcResourceList, resourceusage.PredictResource{
				CPUUtil:    avgResource.CPUUsage,
				MemoryUtil: avgResource.MemoryUsage,
				Timestamp:  avgResource.ReportTime,
			})
		}
	}
	// all pod sum of function
	resource := funcResourceList.AggregateSumPredictResource()
	aggregateSumAll[funcSvcID] = append(aggregateSumAll[funcSvcID], resource)
}

func (a *AutoScalerCache) adjustPredictMetricsInputCache(storeTime int64) {
	now := time.Now().Unix()
	if storeTime <= defaultStoreTime {
		storeTime = defaultStoreTime
	}
	for funcSvcID, resourceList := range a.PredictMetricsInputCache {
		var newList resourceusage.PredictResourceList
		for _, resource := range resourceList {
			if now-resource.Timestamp > storeTime {
				continue
			}
			newList = append(newList, resource)
		}
		a.PredictMetricsInputCache[funcSvcID] = newList
		log.GetLogger().Debugf("adjust predict metrics input cache funcSvcID :%s, len: %d",
			funcSvcID, len(a.PredictMetricsInputCache[funcSvcID]))
	}
}

// RefreshPredictMetricsOutputCache -
func (a *AutoScalerCache) RefreshPredictMetricsOutputCache(cache map[resourceusage.
	FuncSvcID]resourceusage.PredictResourceList) {
	if len(cache) == 0 {
		return
	}
	a.PredictMetricsOutputCacheMutex.Lock()
	defer a.PredictMetricsOutputCacheMutex.Unlock()
	a.PredictMetricsOutputCache = cache
	log.GetLogger().Debugf("refresh prediction resource metric cache, len: %d", len(a.PredictMetricsOutputCache))
}

// GetPredictMetricsOutputCache -
func (a *AutoScalerCache) GetPredictMetricsOutputCache(funcSvcID string) resourceusage.PredictResourceList {
	a.PredictMetricsOutputCacheMutex.RLock()
	defer a.PredictMetricsOutputCacheMutex.RUnlock()
	return a.PredictMetricsOutputCache[resourceusage.FuncSvcID(funcSvcID)]
}

// GetPredictMetricsInputCache -
func (a *AutoScalerCache) GetPredictMetricsInputCache(funcSvcID string) resourceusage.PredictResourceList {
	a.PredictMetricsInputCacheMutex.RLock()
	defer a.PredictMetricsInputCacheMutex.RUnlock()
	return a.PredictMetricsInputCache[resourceusage.FuncSvcID(funcSvcID)]
}

// GetResourceMetricCache -
func (a *AutoScalerCache) GetResourceMetricCache(funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.
	PodName]resourceusage.ResourceList {
	a.ResourceMetricCacheMutex.RLock()
	defer a.ResourceMetricCacheMutex.RUnlock()

	metricCacheData := map[resourceusage.ClusterID]map[resourceusage.PodName]resourceusage.ResourceList{}
	for clusterID, clusterData := range a.ResourceMetricCache {
		for fSvcID, podData := range clusterData {
			if string(fSvcID) == funcSvcID {
				metricCacheData[clusterID] = podData
			}
		}
	}

	log.GetLogger().Debugf("get resource metric cache by Func : %s, %+v", funcSvcID, metricCacheData)
	return metricCacheData
}

// RemoveResourceMetricCacheByFunc -
func (a *AutoScalerCache) RemoveResourceMetricCacheByFunc(funcSvcID string) {
	a.ResourceMetricCacheMutex.Lock()
	defer a.ResourceMetricCacheMutex.Unlock()

	log.GetLogger().Infof("try to remove resource metric cache by Func : %s", funcSvcID)
	for clusterID, clusterData := range a.ResourceMetricCache {
		for fSvcID := range clusterData {
			if string(fSvcID) == funcSvcID {
				delete(a.ResourceMetricCache[clusterID], fSvcID)
			}
		}
	}

	return
}

// GetCaeMetricsCache -
func (a *AutoScalerCache) GetCaeMetricsCache(funcSvcID string) map[resourceusage.ClusterID]map[resourceusage.
	PodName]caemetrics.List {
	a.CaeMetricsCacheMutex.RLock()
	defer a.CaeMetricsCacheMutex.RUnlock()

	caeMetricCacheData := map[resourceusage.ClusterID]map[resourceusage.PodName]caemetrics.List{}
	for clusterID, clusterData := range a.CaeMetricsCache {
		for fSvcID, podData := range clusterData {
			if string(fSvcID) == funcSvcID {
				caeMetricCacheData[clusterID] = podData
			}
		}
	}

	log.GetLogger().Debugf("get resource metric cache by Func : %s, %+v", funcSvcID, caeMetricCacheData)
	return caeMetricCacheData
}

// RemoveCaeMetricCacheByFunc -
func (a *AutoScalerCache) RemoveCaeMetricCacheByFunc(funcSvcID string) {
	a.CaeMetricsCacheMutex.Lock()
	defer a.CaeMetricsCacheMutex.Unlock()

	log.GetLogger().Infof("try to remove resource metric cache by Func : %s", funcSvcID)
	for clusterID, clusterData := range a.CaeMetricsCache {
		for fSvcID := range clusterData {
			if string(fSvcID) == funcSvcID {
				delete(a.CaeMetricsCache[clusterID], fSvcID)
			}
		}
	}

	return
}

// GetFunctionMeta -
func (a *AutoScalerCache) GetFunctionMeta(funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
	fc, ok := a.FunctionMetaCache.Load(funcSvcID)
	if !ok {
		return nil, false
	}
	fci, ok := fc.(*functiontypes.FunctionEtcdInfo)
	return fci, ok
}

// CheckFunctionMetaExist -
func (a *AutoScalerCache) CheckFunctionMetaExist(funcSvcID string) bool {
	_, ok := a.FunctionMetaCache.Load(funcSvcID)
	return ok
}

// UpdateFunctionMeta -
func (a *AutoScalerCache) UpdateFunctionMeta(funcSvcID string, funcMeta *functiontypes.FunctionEtcdInfo) {
	a.FunctionMetaCache.Store(funcSvcID, funcMeta)
	return
}

// RemoveFunctionMeta -
func (a *AutoScalerCache) RemoveFunctionMeta(funcSvcID string) {
	a.FunctionMetaCache.Delete(funcSvcID)
	return
}

// GetCaeAvailableCluster -
func (a *AutoScalerCache) GetCaeAvailableCluster(funcSvcID string) ([]string, bool) {
	clusters, ok := a.CaeAvailableClusterCache.Load(funcSvcID)
	if !ok {
		return nil, false
	}
	availableClusters, ok := clusters.([]string)
	return availableClusters, ok
}

// UpdateCaeAvailableCluster -
func (a *AutoScalerCache) UpdateCaeAvailableCluster(funcSvcID string, clusters []string) {
	a.CaeAvailableClusterCache.Store(funcSvcID, clusters)
	return
}

// RemoveCaeAvailableCluster -
func (a *AutoScalerCache) RemoveCaeAvailableCluster(funcSvcID string) {
	a.CaeAvailableClusterCache.Delete(funcSvcID)
	return
}

// UpdateInstanceMetaCache -
func (a *AutoScalerCache) UpdateInstanceMetaCache(funcSvcID string, minInstance int64, maxInstance int64) {
	newInstanceMeta := &functiontypes.InstanceMetaData{
		MaxInstance: maxInstance,
		MinInstance: minInstance,
	}
	a.InstanceMetaCache.Store(funcSvcID, newInstanceMeta)
	return
}

// DecFuncInstanceCurrentCount -
func (a *AutoScalerCache) DecFuncInstanceCurrentCount(funcSvcID string) error {
	count, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("FunctionInstanceCache key: %s not exists", funcSvcID)
		return errors.New("failed to DecFuncInstanceCurrentCounts for funcSvcID: " + funcSvcID)
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("FunctionInstanceCache load key: %s error", funcSvcID)
		return errors.New("failed to DecFuncInstanceCurrentCounts for funcSvcID: " + funcSvcID)
	}
	if atomic.LoadInt64(&fci.TotalCurrentCount) > 1 {
		atomic.AddInt64(&fci.TotalCurrentCount, -1)
	}
	return nil
}

// GetFuncInstanceCurrentCountInCluster -
func (a *AutoScalerCache) GetFuncInstanceCurrentCountInCluster(funcSvcID, clusterID string) int64 {
	count, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("FunctionInstanceCache key: %s not exists", funcSvcID)
		return 0
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("failed to GetFuncInstanceCurrentCount for funcSvcID: %s", funcSvcID)
		return 0
	}
	log.GetLogger().Debugf("fci.ClusterInsCache is %+v, funcSvcID: %s, clusterID: %s ", fci.ClusterInsCache,
		funcSvcID, clusterID)
	for _, clusterData := range fci.ClusterInsCache {
		if clusterData.ClusterID == clusterID {
			return int64(clusterData.Count)
		}
	}

	return 0
}

// GetFuncScalingInstanceCountInCluster -
func (a *AutoScalerCache) GetFuncScalingInstanceCountInCluster(funcSvcID, clusterID string) int64 {
	instanceCache, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("instanceCache key: %s not exists", funcSvcID)
		return 0
	}
	funcInstanceCache, ok := instanceCache.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("failed to convert instanceCache for funcSvcID: %s", funcSvcID)
		return 0
	}
	for _, clusterData := range funcInstanceCache.ClusterInsCache {
		if clusterData.ClusterID == clusterID {
			return clusterData.ScalingNum
		}
	}
	return 0
}

// GetFuncInstanceCurrentCountEveryCluster -
func (a *AutoScalerCache) GetFuncInstanceCurrentCountEveryCluster(funcSvcID string) map[string]int64 {
	clusterInstanceCount := make(map[string]int64)

	count, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("FunctionInstanceCache key: %s not exists", funcSvcID)
		return clusterInstanceCount
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("failed to GetFuncInstanceCurrentCount for funcSvcID: %s", funcSvcID)
		return clusterInstanceCount
	}
	log.GetLogger().Debugf("fci.ClusterInsCache is %+v, funcSvcID: %s", fci.ClusterInsCache, funcSvcID)

	for _, clusterData := range fci.ClusterInsCache {
		clusterInstanceCount[clusterData.ClusterID] = int64(clusterData.Count)
	}

	return clusterInstanceCount
}

// GetClusterCounts -
func (a *AutoScalerCache) GetClusterCounts() map[string]int {
	clusterCounts := make(map[string]int)
	a.FunctionInstanceCache.Range(func(key, value interface{}) bool {
		instanceCache, ok := value.(*config.InstanceCache)
		if !ok {
			log.GetLogger().Errorf("failed to covert interface to InstanceCache: %v", value)
			return false
		}
		for _, cluster := range instanceCache.ClusterInsCache {
			if _, ok := clusterCounts[cluster.ClusterID]; ok {
				clusterCounts[cluster.ClusterID] += cluster.Count
			} else {
				clusterCounts[cluster.ClusterID] = cluster.Count
			}
		}
		return true
	})
	return clusterCounts
}

// IncFuncInstanceCurrentCount -
func (a *AutoScalerCache) IncFuncInstanceCurrentCount(funcSvcID string) error {
	newFunctionInsCache := &config.InstanceCache{
		TotalCurrentCount: 1,
	}
	count, exist := a.FunctionInstanceCache.LoadOrStore(funcSvcID, newFunctionInsCache)
	if !exist {
		log.GetLogger().Infof("FunctionInstanceCache stored a new key: %s", funcSvcID)
		return nil
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("FunctionInstanceCache load key: %s error", funcSvcID)
		return errors.New("failed to IncFuncInstanceCurrentCount for funcSvcID: " + funcSvcID)
	}
	atomic.AddInt64(&fci.TotalCurrentCount, 1)
	return nil
}

// GetFuncInstanceCurrentCount -
func (a *AutoScalerCache) GetFuncInstanceCurrentCount(funcSvcID string) (int64, int64) {
	count, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("FunctionInstanceCache key: %s not exists", funcSvcID)
		return 0, 0
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("failed to GetFuncInstanceCurrentCount for funcSvcID: %s", funcSvcID)
		return 0, 0
	}
	return atomic.LoadInt64(&fci.TotalCurrentCount), atomic.LoadInt64(&fci.ScalingNum)
}

// GetFuncExpectedMinIns -
func (a *AutoScalerCache) GetFuncExpectedMinIns(funcSvcID string) int64 {
	count, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("GetFuncExpectedMinIns key: %s not exists", funcSvcID)
		return 0
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("failed to GetFuncExpectedMinIns for funcSvcID: %s", funcSvcID)
		return 0
	}
	return fci.ExpectedMinIns
}

// GetFuncExpectedSnapshot -
func (a *AutoScalerCache) GetFuncExpectedSnapshot(funcSvcID string) int64 {
	count, exist := a.FunctionInstanceCache.Load(funcSvcID)
	if !exist {
		log.GetLogger().Errorf("GetFuncExpectedSnapshot key: %s not exists", funcSvcID)
		return 0
	}
	fci, ok := count.(*config.InstanceCache)
	if !ok {
		log.GetLogger().Errorf("failed to GetFuncExpectedSnapshot for funcSvcID: %s", funcSvcID)
		return 0
	}
	return fci.ExpectedSnapshot
}

// GetInstanceMetaCache -
func (a *AutoScalerCache) GetInstanceMetaCache(funcSvcID string) *functiontypes.InstanceMetaData {
	fc, ok := a.InstanceMetaCache.Load(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("InstanceMetaCache load error key: %s", funcSvcID)
		return nil
	}
	fci, ok := fc.(*functiontypes.InstanceMetaData)
	if !ok {
		log.GetLogger().Errorf("InstanceMetaCache unmarshal error key: %s", funcSvcID)
		return nil
	}
	return fci
}

// RemoveInstanceMetaCache -
func (a *AutoScalerCache) RemoveInstanceMetaCache(funcSvcID string) {
	a.InstanceMetaCache.Delete(funcSvcID)
	return
}

// RemoveFunctionInstanceCache -
func (a *AutoScalerCache) RemoveFunctionInstanceCache(funcSvcID string) {
	a.FunctionInstanceCache.Delete(funcSvcID)
	return
}

// UpdateFunctionInstanceCache -
func (a *AutoScalerCache) UpdateFunctionInstanceCache(funcSvcID string) {
	count, _ := a.GetFuncInstanceCurrentCount(funcSvcID)
	// count==0 means there is no key for funcSvcID
	if count == 0 {
		err := a.IncFuncInstanceCurrentCount(funcSvcID)
		if err != nil {
			log.GetLogger().Errorf("failed to Update FunctionInstanceCache, err: %s", err.Error())
			return
		}
		log.GetLogger().Debugf("succeed to Update FunctionInstanceCache with new key: %s", funcSvcID)
		return
	}
	log.GetLogger().Debugf("skip to Update FunctionInstanceCache with existing key: %s", funcSvcID)
	return
}

// InitAutoScalerCache inits autoscaler cache
func InitAutoScalerCache() error {
	resp, err := utils.GetFunctionListFromEtcd()
	if err != nil {
		log.GetLogger().Errorf("failed to list all instance from etcd, err: %s", err.Error())
		return err
	}
	if resp == nil || len(resp.Kvs) == 0 {
		log.GetLogger().Errorf("no persistent function exist in cae meta etcd")
		return nil
	}
	for _, kv := range resp.Kvs {
		err = updateFunctionCache(string(kv.Key), string(kv.Value))
		if err != nil {
			log.GetLogger().Errorf("failed to updateFunctionCache for %s, err: %s", kv.Key, err.Error())
			continue
		}
	}
	return nil
}

func updateFunctionCache(key string, value string) error {
	funcSvcID := utils.CrNameByKey(key)
	var functionEtcdInfo = &functiontypes.FunctionEtcdInfo{}
	if err := json.Unmarshal([]byte(value), &functionEtcdInfo); err != nil {
		log.GetLogger().Errorf("failed to Unmarshal meta func data, key:%s, err: %s", key, err.Error())
		return err
	}
	if functionEtcdInfo.FuncMetaData.BusinessType == constants.BusinessTypeCAE {
		GetInstance().UpdateFunctionMeta(funcSvcID, functionEtcdInfo)
		err := GetInstance().IncFuncInstanceCurrentCount(funcSvcID)
		if err != nil {
			log.GetLogger().Errorf("failed to IncFuncInstanceCurrentCount for funcSvcID: %s err: %s", funcSvcID, err.Error())
			return err
		}
	}
	return nil
}
