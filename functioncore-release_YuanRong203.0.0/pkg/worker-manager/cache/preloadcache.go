/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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
	"sync"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/config"
)

var (
	preloadCache = &PreloadCache{}
)

// PreloadCache -
type PreloadCache struct {
	tenantCache sync.Map
}

type tenantPreloadCache struct {
	functions sync.Map
	layers    sync.Map
}

// GetPreloadCache -
func GetPreloadCache() *PreloadCache {
	return preloadCache
}

// AddFunction -
func (p *PreloadCache) AddFunction(tenantID string, funcVersionName string,
	preloadInfo config.PreloadInfo) error {
	v, _ := p.tenantCache.LoadOrStore(tenantID, &tenantPreloadCache{})
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return fmt.Errorf("failed to add function %s to preload cache", funcVersionName)
	}
	preloadInfo.VersionUrn = funcVersionName
	t.functions.Store(funcVersionName, preloadInfo)
	log.GetLogger().Infof("succeed to store function %s preload info", funcVersionName)
	return nil
}

// RemoveFunction -
func (p *PreloadCache) RemoveFunction(tenantID string, funcVersionName string) error {
	v, ok := p.tenantCache.Load(tenantID)
	if !ok {
		return nil
	}
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return nil
	}
	t.functions.Delete(funcVersionName)
	log.GetLogger().Infof("succeed to delete function %s preload info", funcVersionName)
	return nil
}

// AddLayer -
func (p *PreloadCache) AddLayer(tenantID string, layerVersionName string,
	preloadInfo config.PreloadInfo) error {
	v, _ := p.tenantCache.LoadOrStore(tenantID, &tenantPreloadCache{})
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return fmt.Errorf("failed to add layer %s to preload cache", layerVersionName)
	}
	preloadInfo.VersionUrn = layerVersionName
	t.layers.Store(layerVersionName, preloadInfo)
	log.GetLogger().Infof("succeed to store layer %s preload info", layerVersionName)
	return nil
}

// RemoveLayer -
func (p *PreloadCache) RemoveLayer(tenantID string, layerVersionName string) error {
	v, ok := p.tenantCache.Load(tenantID)
	if !ok {
		return nil
	}
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return nil
	}
	t.layers.Delete(layerVersionName)
	log.GetLogger().Infof("succeed to delete layer %s preload info", layerVersionName)
	return nil
}

// CheckFunctionIsPreload -
func (p *PreloadCache) CheckFunctionIsPreload(tenantID string, funcVersionName string) bool {
	v, ok := p.tenantCache.Load(tenantID)
	if !ok {
		return false
	}
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return false
	}
	_, ok = t.functions.Load(funcVersionName)
	return ok
}

// CheckLayerIsPreload -
func (p *PreloadCache) CheckLayerIsPreload(tenantID string, layerVersionName string) bool {
	v, ok := p.tenantCache.Load(tenantID)
	if !ok {
		return false
	}
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return false
	}
	_, ok = t.layers.Load(layerVersionName)
	return ok
}

// GelALLPreloadInfo -
func (p *PreloadCache) GelALLPreloadInfo() (map[string][]config.PreloadInfo,
	map[string][]config.PreloadInfo) {
	tenantFunctionPreloadMap := make(map[string][]config.PreloadInfo)
	tenantLayerPreloadMap := make(map[string][]config.PreloadInfo)
	p.tenantCache.Range(func(tenantIDIf, tenantPreloadInfoCache interface{}) bool {
		tenantID, ok := tenantIDIf.(string)
		if !ok {
			return true
		}
		var functionPreloadInfos []config.PreloadInfo
		var layerPreloadInfos []config.PreloadInfo
		t, ok := tenantPreloadInfoCache.(*tenantPreloadCache)
		if !ok {
			return true
		}
		t.functions.Range(func(_, preloadInfoIf interface{}) bool {
			preloadInfo, ok := preloadInfoIf.(config.PreloadInfo)
			if !ok {
				return true
			}
			functionPreloadInfos = append(functionPreloadInfos, preloadInfo)
			return true
		})
		t.layers.Range(func(_, preloadInfoIf interface{}) bool {
			preloadInfo, ok := preloadInfoIf.(config.PreloadInfo)
			if !ok {
				return true
			}
			layerPreloadInfos = append(layerPreloadInfos, preloadInfo)
			return true
		})
		if len(functionPreloadInfos) > 0 {
			tenantFunctionPreloadMap[tenantID] = functionPreloadInfos
		}
		if len(layerPreloadInfos) > 0 {
			tenantLayerPreloadMap[tenantID] = layerPreloadInfos
		}
		return true
	})
	return tenantFunctionPreloadMap, tenantLayerPreloadMap
}

// GelPreloadInfo -
func (p *PreloadCache) GelPreloadInfo(tenantID, typeStr, versionName string) *config.PreloadInfo {
	v, ok := p.tenantCache.Load(tenantID)
	if !ok {
		return nil
	}
	t, ok := v.(*tenantPreloadCache)
	if !ok {
		return nil
	}
	switch typeStr {
	case constants.PreloadFunctionType:
		f, ok := t.functions.Load(versionName)
		if !ok {
			return nil
		}
		preloadInfo, ok := f.(config.PreloadInfo)
		if !ok {
			return nil
		}
		return &preloadInfo
	case constants.PreloadLayerType:
		f, ok := t.layers.Load(versionName)
		if !ok {
			return nil
		}
		preloadInfo, ok := f.(config.PreloadInfo)
		if !ok {
			return nil
		}
		return &preloadInfo
	default:
		log.GetLogger().Warnf("not support preload type %s", typeStr)
		return nil
	}
}
