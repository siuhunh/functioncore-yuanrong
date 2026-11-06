/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package server cache function layer and object info
package server

import (
	"os"
	"sync"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/common/logger/log"
)

var (
	objSizeCache *ObjSizeCache = &ObjSizeCache{
		persistObj: make(map[string]FileInfo),
		layerCache: make(map[string]map[string]string),
		evsEnable:  os.Getenv("EVS_ENABLE") == "true",
	}
)

// FileInfo is used to identifies persistent file information.
type FileInfo struct {
	ObjSize  int64
	Checksum string
	Name     string
}

// ObjSizeCache is used to identifies persistent file cache and function layers cache
type ObjSizeCache struct {
	persistObj   map[string]FileInfo
	persistMutex sync.Mutex
	layerCache   map[string]map[string]string
	layerMutex   sync.Mutex
	evsEnable    bool
}

// GetObjSizeCacheInstance is used to obtain global variables objSizeCache
func GetObjSizeCacheInstance() *ObjSizeCache {
	return objSizeCache
}

// UpdatePersistObj is used to update persistObj
func (m *ObjSizeCache) UpdatePersistObj(key string, objInfo FileInfo) {
	if m.evsEnable {
		m.persistMutex.Lock()
		defer m.persistMutex.Unlock()
		m.persistObj[key] = objInfo
	}
}

// UpdatePersistObjSize is used to update persistObj size
func (m *ObjSizeCache) UpdatePersistObjSize(key string, size int64) {
	if m.evsEnable {
		m.persistMutex.Lock()
		defer m.persistMutex.Unlock()

		fileInfo, ok := m.persistObj[key]
		if !ok {
			m.persistObj[key] = FileInfo{ObjSize: size}
			log.GetLogger().Infof("UpdatePersistObjSize key %s, value: %v", key, m.persistObj[key])
			return
		}
		fileInfo.ObjSize = size
		m.persistObj[key] = fileInfo
	}
}

// CheckObjInMemThreshold is used to determine whether the current function reaches the memory threshold.
func (m *ObjSizeCache) CheckObjInMemThreshold(key string) bool {
	if m.evsEnable {
		m.persistMutex.Lock()
		defer m.persistMutex.Unlock()
		fileInfo, ok := m.persistObj[key]
		if !ok {
			log.GetLogger().Warnf("key %s is not exits in persistObj, request will be reject", key)
			return false
		}

		if fileInfo.ObjSize > config.DefaultLimitCodeSize {
			log.GetLogger().Warnf("object size %d too large, request will be reject ", fileInfo.ObjSize)
			return false
		}
	}
	return true
}

// DelPersistObj is used to delete persist object in disk, when function is deleted
func (m *ObjSizeCache) DelPersistObj(key string) {
	if m.evsEnable {
		m.persistMutex.Lock()
		delete(m.persistObj, key)
		m.persistMutex.Unlock()
	}
}

// UpdateObjPersistName is used to update persistObj name, when obj is in disk
func (m *ObjSizeCache) UpdateObjPersistName(key string, name string) {
	if m.evsEnable {
		m.persistMutex.Lock()
		defer m.persistMutex.Unlock()
		value, ok := m.persistObj[key]
		if !ok {
			log.GetLogger().Errorf("key %s is not exist in persistObj", key)
			return
		}
		value.Name = name
		m.persistObj[key] = value
	}
}

// UpdateObjSizeAndCheckSum is used to update persistObj name, when obj is uploaded
func (m *ObjSizeCache) UpdateObjSizeAndCheckSum(key string, size int64, checksum string) {
	if m.evsEnable {
		m.persistMutex.Lock()
		defer m.persistMutex.Unlock()
		value, ok := m.persistObj[key]
		if !ok {
			m.persistObj[key] = FileInfo{ObjSize: size, Checksum: checksum}
			log.GetLogger().Infof("UpdateObjSizeAndCheckSum key %s, size: %d, checksum: %s", key, size, checksum)
			return
		}
		value.ObjSize = size
		value.Checksum = checksum
		m.persistObj[key] = value
	}
}

// IsObjInDisk is used to check object in disk
func (m *ObjSizeCache) IsObjInDisk(info *config.FunctionMetaInfo, checksum, key string, isLayer bool) bool {
	if m.evsEnable {
		m.persistMutex.Lock()
		defer m.persistMutex.Unlock()
		value, ok := m.persistObj[key]
		if !ok {
			log.GetLogger().Infof("function %s not exist", key)
			return false
		}
		if !isLayer && (int64)(info.FuncMetaData.CodeSize) == value.ObjSize &&
			value.Checksum == info.FuncMetaData.CodeSha512 && len(value.Name) != 0 {
			return true
		}
		if isLayer && value.Checksum == checksum && len(value.Name) != 0 {
			return true
		}
		log.GetLogger().Infof("function %s not in disk, checksum %s, name %s", key, value.Checksum, value.Name)
	}
	return false
}

// DeleteLayerCache is used to get deleted layers, when meta info is deleted
func (m *ObjSizeCache) DeleteLayerCache(info *config.FunctionMetaInfo) {
	if m.evsEnable {
		m.layerMutex.Lock()
		defer m.layerMutex.Unlock()
		urn := info.FuncMetaData.FunctionVersionURN
		delete(m.layerCache, urn)
	}
}

// GetNeedDeletedLayers is used to get deleted layers, when meta info is updated
func (m *ObjSizeCache) GetNeedDeletedLayers(info *config.FunctionMetaInfo) map[string]string {
	if m.evsEnable {
		m.layerMutex.Lock()
		defer m.layerMutex.Unlock()
		urn := info.FuncMetaData.FunctionVersionURN
		deleteLayers := make(map[string]string)
		layers, ok := m.layerCache[urn]
		if ok {
			deleteLayers = layers
		}
		currentLayers := make(map[string]string)
		for _, value := range info.FuncMetaData.Layers {
			layerEtag, ok := deleteLayers[value.ObjectID]
			if ok && layerEtag == value.ETag {
				delete(deleteLayers, value.ObjectID)
			}
			currentLayers[value.ObjectID] = value.ETag
		}
		m.layerCache[urn] = currentLayers
		log.GetLogger().Infof("currentLayers %v , deleteLayers %v", m.layerCache[urn], deleteLayers)
		return deleteLayers
	}
	return nil
}
