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

// Package metadata function metadata sync with etcd
package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/common/urnutils"
)

const (
	// FunctionEtcdKeyLen is 10 + 1 as what we get from Split() is longer than the actual by 1
	FunctionEtcdKeyLen = 11
	// set the key timeout interval in singleFlight after etcd access fails so that subsequent requests can be retried
	singleFlightKeyTTL = 10 * time.Second
)

const (
	businessIndex = 4 + 2*iota
	tenantIndex
	funcNameIndex
	versionIndex
)

var (
	metaData               = make(map[string]*FunctionMetaData)
	metaDataLock           sync.RWMutex
	sf                     = singleflight.NewSingleFlight()
	errNotFound            = errors.New("not found")
	funcMetaUpdateCallBack FuncMetaUpdateCallBack
)

// FunctionMetaData -
type FunctionMetaData struct {
	Source            etcd3.EtcdType `json:"-"`
	ExtendedMetaData  functiontypes.ExtendedMetaData
	ResourceMetaData  functiontypes.ResourceMetaData
	InstanceMetaData  functiontypes.InstanceMetaData  `json:"instanceMetaData" valid:",optional"`
	FuncMetaDataParts functiontypes.FuncMetaDataParts `json:"funcMetaData"`
}

// FuncMetaUpdateCallBack -
type FuncMetaUpdateCallBack func(funcKey string, info *FunctionMetaData)

// SetFuncMetaUpdateCallBack -
func SetFuncMetaUpdateCallBack(callback FuncMetaUpdateCallBack) {
	funcMetaUpdateCallBack = callback
}

// LoadMetaData load metaData by function key
func LoadMetaData(funcName, tenantID, version string) (*FunctionMetaData, bool) {
	functionKey := combineFunctionKey(funcName, tenantID, version)
	metaDataLock.RLock()
	value, exist := metaData[functionKey]
	if exist {
		metaDataLock.RUnlock()
		return value, true
	}
	metaDataLock.RUnlock()
	meta, err := sf.Do(functionKey, func() (interface{}, error) {
		etcdKey := fmt.Sprintf(constants.SilentFuncKey, tenantID, funcName, version)
		metaClient := etcd3.GetMetaEtcdClient()
		if metaClient.Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			return nil, errors.New("failed to init meta ETCD client")
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		getResp, err := metaClient.GetResponse(defaultEtcdCtx, etcdKey)
		if err != nil {
			log.GetLogger().Errorf("failed to get function metadata from etcd, err: %s, func: %s",
				err.Error(), funcName)
			time.AfterFunc(singleFlightKeyTTL, func() {
				sf.Remove(functionKey)
			})
			return nil, err
		}
		if len(getResp.Kvs) == 0 {
			log.GetLogger().Warnf("function key is not exist, func: %s", funcName)
			return nil, errNotFound
		}
		meta := &FunctionMetaData{}
		if err = json.Unmarshal(getResp.Kvs[0].Value, meta); err != nil {
			log.GetLogger().Errorf("failed to unmarshal function meta, func: %s, err: %s", funcName, err.Error())
			return nil, err
		}
		meta.Source = etcd3.Meta
		metaDataLock.Lock()
		metaData[functionKey] = meta
		metaDataLock.Unlock()
		return meta, nil
	})
	if err != nil {
		return nil, false
	}
	return meta.(*FunctionMetaData), true
}

// ProcessUpdate process update extendedMetaData
func ProcessUpdate(key string, value []byte, source etcd3.EtcdType) error {
	functionKey, err := getFunctionKey(key)
	if err != nil {
		log.GetLogger().Errorf("get function key by key: %s with error: %s", key, err.Error())
		return err
	}
	metaInfo := FunctionMetaData{}
	if err = json.Unmarshal(value, &metaInfo); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the etcd event value")
		return err
	}
	metaInfo.Source = source
	if source == etcd3.CAEMeta {
		metaDataLock.Lock()
		old, ok := metaData[functionKey]
		if ok && old.Source == etcd3.Meta {
			metaDataLock.Unlock()
			log.GetLogger().Infof("function from meta etcd exists, skip update from cae, key %s", key)
			return nil
		}
		metaData[functionKey] = &metaInfo
		metaDataLock.Unlock()
	} else {
		metaDataLock.Lock()
		metaData[functionKey] = &metaInfo
		metaDataLock.Unlock()
		sf.Remove(functionKey)
	}
	if funcMetaUpdateCallBack != nil {
		funcMetaUpdateCallBack(functionKey, &metaInfo)
	}
	log.GetLogger().Debugf("add/update function from %s, function key:%s", source, key)
	return nil
}

// ProcessDelete process delete extendedMetaData
func ProcessDelete(key string, source etcd3.EtcdType) error {
	functionKey, err := getFunctionKey(key)
	if err != nil {
		log.GetLogger().Errorf("get function key by key: %s with error: %s", key, err.Error())
		return err
	}
	if source == etcd3.CAEMeta {
		metaDataLock.Lock()
		old, ok := metaData[functionKey]
		if ok && old.Source == etcd3.Meta {
			metaDataLock.Unlock()
			log.GetLogger().Infof("function from meta etcd exists, skip delete from cae, key %s", key)
			return nil
		}
		delete(metaData, functionKey)
		metaDataLock.Unlock()
	} else {
		metaDataLock.Lock()
		delete(metaData, functionKey)
		metaDataLock.Unlock()
		sf.Remove(functionKey)
	}
	log.GetLogger().Debugf("delete function from %s, function key:%s", source, key)
	return nil
}

func getFunctionKey(key string) (string, error) {
	keys := strings.Split(key, urnutils.FunctionKeySep)
	if len(keys) != FunctionEtcdKeyLen {
		return "", errors.New("incorrect etcdKey length")
	}
	return combineFunctionKey(keys[funcNameIndex], keys[tenantIndex], keys[versionIndex]), nil
}

func combineFunctionKey(funcName, tenantID, version string) string {
	return urnutils.GetFuncKey(tenantID, funcName, version)
}
