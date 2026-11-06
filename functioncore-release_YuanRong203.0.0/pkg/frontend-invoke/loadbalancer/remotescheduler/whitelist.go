/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package remotescheduler

import (
	"sync"

	"functioncore/pkg/common/logger/log"
)

var whiteList = &Whitelist{}

// Whitelist is whitelist of functions and tenants that scheduled by specific faas-scheduler
type Whitelist struct {
	// key: funcKey, (tenantID/0@serviceID@funcName/funcVersion) ==> value: faas-scheduler name
	// use sync.Map to gain better performance
	funcWhitelist sync.Map
	// key: tenantID  ==> value: faas-scheduler name
	tenantWhitelist sync.Map
}

// GetWhitelist returns global Whitelist
func GetWhitelist() *Whitelist {
	return whiteList
}

// Query returns the specific faas-scheduler if funcKey or tenantID is in whitelist
func (wl *Whitelist) Query(funcKey, tenantID string) string {
	log.GetLogger().Debugf("query faas-scheduler use funcKey:%s tenantID:%s", funcKey, tenantID)
	if valIf, loaded := wl.funcWhitelist.Load(funcKey); loaded {
		if val, ok := valIf.(string); ok {
			return val
		}
		log.GetLogger().Warnf("failed to assert funcWhitelist value type, %v", valIf)
		return ""
	}
	if valIf, loaded := wl.tenantWhitelist.Load(tenantID); loaded {
		if val, ok := valIf.(string); ok {
			return val
		}
		log.GetLogger().Warnf("failed to assert tenantWhitelist value type, %v", valIf)
		return ""
	}
	log.GetLogger().Debugf("no faas-scheduler matched for funcKey:%s tenantID:%s", funcKey, tenantID)
	return ""
}

// UpdateFuncWhitelist updates function whitelist
func (wl *Whitelist) UpdateFuncWhitelist(list map[string]string) {
	updateSyncMapByMap(&wl.funcWhitelist, list)
}

// UpdateTenantWhitelist update tenant whitelist
func (wl *Whitelist) UpdateTenantWhitelist(list map[string]string) {
	updateSyncMapByMap(&wl.tenantWhitelist, list)
}

func updateSyncMapByMap[K comparable, V any](target *sync.Map, source map[K]V) {
	target.Range(func(keyIf, valueIf any) bool {
		if source == nil {
			target.Delete(keyIf)
			return true
		}
		key, ok := keyIf.(K)
		if !ok {
			log.GetLogger().Warnf("failed to assert key type, %v", keyIf)
			return true
		}
		if _, ok := source[key]; !ok {
			target.Delete(key)
		}
		return true
	})

	for k, v := range source {
		target.Store(k, v)
	}
}
