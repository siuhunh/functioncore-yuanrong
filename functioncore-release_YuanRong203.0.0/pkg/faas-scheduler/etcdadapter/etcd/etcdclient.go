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

// Package etcd contains etcd interaction
package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	retrySleepTime       = 100 * time.Millisecond
	maxRetryTime         = 3
	defaultConcurrentNum = 100000
)

// InitETCDClient -
func InitETCDClient(stopCh <-chan struct{}) error {
	if config.FConfig.CAEMetaETCD != nil && len(config.FConfig.CAEMetaETCD.ServerList) == 0 {
		log.GetLogger().Infof("empty cae meta etcd server list, skip connect")
		config.FConfig.CAEMetaETCD = nil
	}
	if config.FConfig.MetaEtcdProxy != nil && len(config.FConfig.MetaEtcdProxy.ServerList) == 0 {
		log.GetLogger().Infof("empty meta etcd proxy server list, skip connect")
		config.FConfig.MetaEtcdProxy = nil
	}
	if config.FConfig.CAEMetaEtcdProxy != nil && len(config.FConfig.CAEMetaEtcdProxy.ServerList) == 0 {
		log.GetLogger().Infof("empty cae meta etcd proxy server list, skip connect")
		config.FConfig.CAEMetaEtcdProxy = nil
	}
	// AlarmSwitch is off
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithMetaEtcdConfig(config.FConfig.Etcd).
		WithRouterEtcdConfig(config.FConfig.RouterETCD).
		WithCAEMetaEtcdConfig(config.FConfig.CAEMetaETCD).
		WithMetaEtcdProxyConfig(config.FConfig.MetaEtcdProxy).
		WithCAEMetaEtcdProxyConfig(config.FConfig.CAEMetaEtcdProxy).
		WithAlarmSwitch().
		InitClient()
}

// GetValueFromEtcd query value from etcd and retry only in case of timeout
func getValueFromEtcd(key string, etcdType etcd3.EtcdType) ([]byte, error) {
	etcdClient := etcd3.GetETCDClient(etcdType)
	if etcdClient == nil || etcdClient.GetEtcdStatusLostContact() == false || etcdClient.Client == nil {
		return nil, errors.New("etcd connection loss")
	}
	var (
		values []string
		err    error
	)
	for i := 1; i <= maxRetryTime; i++ {
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		values, err = etcdClient.GetValues(defaultEtcdCtx, key)
		if err == nil {
			break
		}
		if err != context.DeadlineExceeded {
			return nil, err
		}
		log.GetLogger().Errorf("get value from etcd with key %s timeout, try time %d", key, i)
		time.Sleep(retrySleepTime)
	}

	if len(values) == 0 {
		log.GetLogger().Errorf("failed to get value from etcd, key: %s", key)
		return nil, fmt.Errorf("the value got from etcd is empty")
	}

	return []byte(values[0]), err
}

// FetchFuncMeta -
func FetchFuncMeta(funcKey string) (*types.FunctionMetaInfo, error) {
	tenantID, funcName, funcVersion := urnutils.ParseFuncKey(funcKey)
	funcStorageKey := etcd3.HandleCellPrefix(config.SilentWatchPathPrefixWithFunction + "/" + tenantID +
		"/function/" + funcName + "/version/" + funcVersion)
	return FetchFuncMetaFromEtcd(funcStorageKey, etcd3.Meta)
}

// FetchFuncMetaFromEtcd -
func FetchFuncMetaFromEtcd(key string, etcdType etcd3.EtcdType) (*types.FunctionMetaInfo, error) {
	resp, err := getValueFromEtcd(key, etcdType)
	if err != nil {
		return nil, err
	}
	_, info, err := ParseFuncMeta(resp)
	if err != nil {
		return nil, err
	}
	info.Source = etcd3.Meta
	return info, nil
}

// ParseFuncMeta -
func ParseFuncMeta(value []byte) (string, *types.FunctionMetaInfo, error) {
	info := types.FunctionMetaInfo{}
	err := json.Unmarshal(value, &info)
	if err != nil {
		return "", nil, err
	}
	funcVersionURN, err := urnutils.GetFunctionInfo(info.FuncMetaData.FunctionVersionURN)
	if err != nil {
		return "", nil, err
	}
	if info.InstanceMetaData.ConcurrentNum < 0 {
		return "", nil, fmt.Errorf("invalid ConcurrentNum %d", info.InstanceMetaData.ConcurrentNum)
	} else if info.InstanceMetaData.ConcurrentNum == 0 {
		info.InstanceMetaData.ConcurrentNum = defaultConcurrentNum
	}
	if funcVersionURN.Version == "" {
		return "", nil, errors.New("no version in FunctionVersionURN")
	}

	funcKey := urnutils.GetFuncKey(funcVersionURN.TenantID, funcVersionURN.Name, funcVersionURN.Version)
	return funcKey, &info, nil
}
