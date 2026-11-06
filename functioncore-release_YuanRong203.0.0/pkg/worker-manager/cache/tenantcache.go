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

package cache

import (
	"fmt"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

var (
	// TenantCache is cache for tenant
	TenantCache = &tenantCache{}
	// DefaultMaxOnDemandInstanceNum is default quota for max on-demand instance per tenant
	DefaultMaxOnDemandInstanceNum = int64(utils.DefaultMaxOnDemandInstanceNumPerTenant)
	// DefaultMaxReversedInstanceNum is default quota for max reversed instance per tenant
	DefaultMaxReversedInstanceNum = int64(utils.DefaultMaxReversedInstanceNumPerTenant)
)

type tenantCache struct {
	// key: tenantID   value: *tenantInstance
	tenantInstanceList sync.Map
}

type tenantInstance struct {
	maxOnDemandInstance   int64
	maxReversedInstance   int64
	onDemandInsNum        int64
	scalingOnDemandInsNum int64
	reversedInsNum        int64
	scalingReversedInsNum int64
	isUpdate              bool
	mux                   sync.RWMutex
}

// Tenant is tenant info from etcd events
type Tenant struct {
	TenantID       string
	TenantMetaInfo config.TenantMetaInfo
}

func createDefaultTenantInstance() *tenantInstance {
	tenantIns := &tenantInstance{
		maxOnDemandInstance:   DefaultMaxOnDemandInstanceNum,
		maxReversedInstance:   DefaultMaxReversedInstanceNum,
		onDemandInsNum:        0,
		scalingOnDemandInsNum: 0,
		reversedInsNum:        0,
		scalingReversedInsNum: 0,
		isUpdate:              false,
		mux:                   sync.RWMutex{},
	}
	return tenantIns
}

func (tc *tenantCache) getOrCreateCachePerTenant(tenantID string) *tenantInstance {
	tenantInsIf, exist := tc.tenantInstanceList.LoadOrStore(tenantID, createDefaultTenantInstance())
	if !exist {
		log.GetLogger().Infof("tenant cache has not been created, generate default cache, tenant: %s", tenantID)
	}
	tenantIns, ok := tenantInsIf.(*tenantInstance)
	if !ok {
		log.GetLogger().Errorf("tenant instance cache is invalid")
		return nil
	}

	return tenantIns
}

func (tc *tenantCache) addInstanceCallback(tenantID string, isReversed bool) {
	start := time.Now()
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}

	tenantIns.mux.Lock()
	if isReversed {
		tenantIns.reversedInsNum++
	} else {
		tenantIns.onDemandInsNum++
	}
	tenantIns.mux.Unlock()
	log.GetLogger().Debugf("succeed to callback tenant cache refresh when add instance, tenant: %s, cost: %s",
		tenantID, time.Since(start))
}

func (tc *tenantCache) deleteInstanceCallback(tenantID string, isReversed bool) {
	start := time.Now()
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}

	tenantIns.mux.Lock()
	if isReversed {
		tenantIns.reversedInsNum--
	} else {
		tenantIns.onDemandInsNum--
	}
	tenantIns.mux.Unlock()
	log.GetLogger().Debugf("succeed to callback tenant cache refresh when delete instance, tenant: %s, cost: %s",
		tenantID, time.Since(start))
}

func (tc *tenantCache) resetInstanceCallback(tenantID string, onDemandInstanceNum int64, reversedInstanceNum int64) {
	start := time.Now()
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}

	tenantIns.mux.Lock()
	tenantIns.reversedInsNum -= reversedInstanceNum
	tenantIns.onDemandInsNum -= onDemandInstanceNum
	tenantIns.mux.Unlock()
	log.GetLogger().Debugf("succeed to callback tenant cache refresh when reset instance, tenant: %s, cost: %s",
		tenantID, time.Since(start))
}

func (tc *tenantCache) increaseScalingNumCallback(tenantID string, isReversed bool) {
	start := time.Now()
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}

	tenantIns.mux.Lock()
	if isReversed {
		tenantIns.scalingReversedInsNum++
	} else {
		tenantIns.scalingOnDemandInsNum++
	}
	tenantIns.mux.Unlock()
	log.GetLogger().Debugf("succeed to callback tenant cache refresh when increase scalingNum, tenant: %s, cost: %s",
		tenantID, time.Since(start))
}

func (tc *tenantCache) decreaseScalingNumCallback(tenantID string, isReversed bool) {
	start := time.Now()
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}

	tenantIns.mux.Lock()
	if isReversed {
		tenantIns.scalingReversedInsNum--
	} else {
		tenantIns.scalingOnDemandInsNum--
	}
	tenantIns.mux.Unlock()
	log.GetLogger().Debugf("succeed to callback tenant cache refresh when decrease scalingNum, tenant: %s, cost: %s",
		tenantID, time.Since(start))
}

func (tc *tenantCache) resetScalingNumCallback(tenantID string, scalingOnDemandInstanceNum int64,
	scalingReversedInstanceNum int64) {
	start := time.Now()
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}

	tenantIns.mux.Lock()
	tenantIns.scalingOnDemandInsNum -= scalingOnDemandInstanceNum
	tenantIns.scalingReversedInsNum -= scalingReversedInstanceNum
	tenantIns.mux.Unlock()
	log.GetLogger().Debugf("succeed to callback tenant cache refresh when reset scalingNum, tenant: %s, cost: %s",
		tenantID, time.Since(start))
}

// UpdateOrAddTenantCache trigger by tenant meta key add or update in etcd
func (tc *tenantCache) UpdateOrAddTenantCache(tenant Tenant) {
	tenantIns := tc.getOrCreateCachePerTenant(tenant.TenantID)
	if tenantIns == nil {
		return
	}
	tenantIns.mux.Lock()
	tenantIns.maxOnDemandInstance = tenant.TenantMetaInfo.TenantInstanceMetaData.MaxOnDemandInstance
	tenantIns.maxReversedInstance = tenant.TenantMetaInfo.TenantInstanceMetaData.MaxReversedInstance
	tenantIns.isUpdate = true
	tenantIns.mux.Unlock()
	log.GetLogger().Infof("succeed to update quota in tenant cache trigger by etcd for tenant: %s", tenant.TenantID)
}

// DeleteTenantCache trigger by tenant meta key deleted in etcd
func (tc *tenantCache) DeleteTenantCache(tenantID string) {
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return
	}
	tenantIns.mux.Lock()
	tenantIns.maxOnDemandInstance = DefaultMaxOnDemandInstanceNum
	tenantIns.maxReversedInstance = DefaultMaxReversedInstanceNum
	tenantIns.isUpdate = false
	tenantIns.mux.Unlock()
	log.GetLogger().Infof("succeed to reset quota in tenant cache trigger by etcd  for tenant: %s", tenantID)
}

// UpdateDefaultQuota trigger by default tenant meta key add or update in etcd
func (tc *tenantCache) UpdateDefaultQuota(defaultTenant Tenant) {
	f := func(k, v interface{}) bool {
		tenantIns, ok := v.(*tenantInstance)
		if !ok {
			return true
		}
		tenantIns.mux.Lock()
		if !tenantIns.isUpdate {
			tenantIns.maxOnDemandInstance = defaultTenant.TenantMetaInfo.TenantInstanceMetaData.MaxOnDemandInstance
			tenantIns.maxReversedInstance = defaultTenant.TenantMetaInfo.TenantInstanceMetaData.MaxReversedInstance
		}
		tenantIns.mux.Unlock()
		return true
	}
	tc.tenantInstanceList.Range(f)
	// update global config
	DefaultMaxOnDemandInstanceNum = defaultTenant.TenantMetaInfo.TenantInstanceMetaData.MaxOnDemandInstance
	DefaultMaxReversedInstanceNum = defaultTenant.TenantMetaInfo.TenantInstanceMetaData.MaxReversedInstance
	log.GetLogger().Infof("succeed to reset default quota in tenant cache trigger by etcd, "+
		"DefaultMaxOnDemandInstanceNum: %d, DefaultMaxReversedInstanceNum: %d",
		DefaultMaxOnDemandInstanceNum, DefaultMaxReversedInstanceNum)
}

// CheckReachedMaxInstance returns is up to maxOnDemandInsNumPerTenant, is up to maxReversedInsNumPerTenant, is up to maxInsNumPerFunc
func (tc *tenantCache) CheckReachedMaxInstance(functionService *faasfunction.FunctionService,
	requestInfo *config.RequestInfo) (bool, bool, bool) {
	start := time.Now()
	tenantID := functionService.Spec.Configuration.TenantID
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return false, false, WorkerCache.CheckReachedMaxInstance(functionService, requestInfo)
	}
	if tenantIns.maxOnDemandInstance == int64(utils.DefaultUnlimitedInstanceNumPerTenant) ||
		tenantIns.maxReversedInstance == int64(utils.DefaultUnlimitedInstanceNumPerTenant) {
		return false, false, WorkerCache.CheckReachedMaxInstance(functionService, requestInfo)
	}
	// check func instance quota
	reachMaxInsNum := WorkerCache.CheckReachedMaxInstance(functionService, requestInfo)
	// check tenant instance quota
	tenantIns.mux.Lock()
	defer tenantIns.mux.Unlock()
	curOnDemandInsNum := tenantIns.onDemandInsNum + tenantIns.scalingOnDemandInsNum
	curReversedInsNum := tenantIns.reversedInsNum + tenantIns.scalingReversedInsNum
	log.GetLogger().Infof("countIns | %s | curOnDemandIns %d | maxOnDemandIns %d | curReversedIns %d | "+
		"maxReversedIns %d | %s | %s | %s", urnutils.ExtractFuncName(functionService.Spec.Configuration.FunctionName),
		curOnDemandInsNum, tenantIns.maxOnDemandInstance, curReversedInsNum, tenantIns.maxReversedInstance,
		urnutils.Anonymize(tenantID), requestInfo.TraceID, time.Since(start))
	return curOnDemandInsNum > tenantIns.maxOnDemandInstance, curReversedInsNum > tenantIns.maxReversedInstance,
		reachMaxInsNum
}

// GetMaxInsNum get max instance quota for tenant
func (tc *tenantCache) GetMaxInsNum(tenantID string) (int64, int64) {
	tenantIns := tc.getOrCreateCachePerTenant(tenantID)
	if tenantIns == nil {
		return DefaultMaxOnDemandInstanceNum, DefaultMaxReversedInstanceNum
	}
	tenantIns.mux.RLock()
	defer tenantIns.mux.RUnlock()
	return tenantIns.maxOnDemandInstance, tenantIns.maxReversedInstance
}

// UpdateAllTenantCache calibrate instance num by function cache in cycle
func UpdateAllTenantCache(stopCh <-chan struct{}, interval time.Duration) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop chan is nil")
		return
	}
	// get default quota from etcd
	clusterID := os.Getenv("CLUSTER_ID")
	defaultTenantMetaKey := fmt.Sprintf("%s/%s/default/instancemetadata", utils.TenantMetadataPrefix, clusterID)
	defaultTenantMetaInfo, err := utils.GetDefaultTenantMetaInfoFromEtcd(defaultTenantMetaKey)
	if err != nil {
		log.GetLogger().Warnf("failed to get default tenant meta info, err: %s, use default", err.Error())
	} else {
		DefaultMaxOnDemandInstanceNum = defaultTenantMetaInfo.TenantInstanceMetaData.MaxOnDemandInstance
		DefaultMaxReversedInstanceNum = defaultTenantMetaInfo.TenantInstanceMetaData.MaxReversedInstance
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			TenantCache.syncTenantCacheByFunc()
		case _, ok := <-stopCh:
			if !ok {
				ticker.Stop()
				return
			}
		}
	}
}

func (tc *tenantCache) syncTenantCacheByFunc() {
	start := time.Now()
	funcTenantMap := WorkerCache.getFunctionMap()
	if len(funcTenantMap) == 0 {
		log.GetLogger().Warnf("funcTenantMap is nil, skip update all tenant cache")
		return
	}
	for tenantID, fcList := range funcTenantMap {
		onDemandInsNum := int64(0)
		scalingOnDemandInsNum := int64(0)
		reversedInsNum := int64(0)
		scalingReversedInsNum := int64(0)
		for _, fc := range fcList {
			fc.Lock()
			totalNum := int64(len(fc.instances))
			reversedInstanceNum := fc.countReversedInstance()
			onDemandInstanceNum := totalNum - reversedInstanceNum
			scalingOnDemandInstanceNum := fc.scalingOnDemandNum
			scalingReversedInstanceNum := fc.scalingNum - scalingOnDemandInstanceNum
			// increase
			onDemandInsNum += onDemandInstanceNum
			scalingOnDemandInsNum += scalingOnDemandInstanceNum
			reversedInsNum += reversedInstanceNum
			scalingReversedInsNum += scalingReversedInstanceNum
			fc.Unlock()
		}
		tenantIns := tc.getOrCreateCachePerTenant(tenantID)
		if tenantIns == nil {
			continue
		}
		tenantIns.mux.Lock()
		tenantIns.onDemandInsNum = onDemandInsNum
		tenantIns.scalingOnDemandInsNum = scalingOnDemandInsNum
		tenantIns.reversedInsNum = reversedInsNum
		tenantIns.scalingReversedInsNum = scalingReversedInsNum
		tenantIns.mux.Unlock()
		TenantCache.tenantInstanceList.Store(tenantID, tenantIns)
	}
	log.GetLogger().Infof("succeed to sync all tenant cache by function cache, cost: %s", time.Since(start))
}
