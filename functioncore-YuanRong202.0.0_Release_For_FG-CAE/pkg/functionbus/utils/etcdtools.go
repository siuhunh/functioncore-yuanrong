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

// Package utils providers etcd function
package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functionbus/common"
)

const (
	retrySleepTime            = 100 * time.Millisecond
	maxRetryTime              = 3
	stateWorkerKeyValueCounts = 7
)

var (
	// ErrETCDNotFound no data is found from etcd
	ErrETCDNotFound = fmt.Errorf("the value got from etcd is empty")
)

type nodeInfo struct {
	NodeIP   string `json:"nodeIP"`
	NodePort string `json:"nodePort"`
}

// InitEtcdClient -
func InitEtcdClient(stopCh <-chan struct{}) error {
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithAlarmSwitch().
		WithMetaEtcdConfig(common.FConfig.EtcdConfig).
		WithRouterEtcdConfig(common.FConfig.RouterETCDConfig).
		WithCAEMetaEtcdConfig(common.FConfig.CAEMetaEtcdConfig).
		WithCaeRouterEtcdConfig(common.FConfig.CAERouterEtcdConfig).
		WithMetaEtcdProxyConfig(common.FConfig.MetaEtcdProxyConfig).
		WithCAEMetaEtcdProxyConfig(common.FConfig.CAEMetaEtcdProxyConfig).
		InitClient()
}

// DeleteValueInEtcd delete value from etcd
func DeleteValueInEtcd(key string) error {
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	errRouter := etcd3.GetRouterEtcdClient().Delete(defaultEtcdCtx, key)
	if errRouter != nil {
		log.GetLogger().Errorf("failed to delete key %s in etcd, error: %s", key, errRouter.Error())
	}
	if !common.IsCAERouterEtcdDefined() {
		return errRouter
	}
	if etcd3.GetCaeRouterEtcdClient() == nil || etcd3.GetCaeRouterEtcdClient().Client == nil {
		log.GetLogger().Errorf("cae router etcd client is nil")
		return errRouter
	}
	if errCAERouter := etcd3.GetCaeRouterEtcdClient().Delete(defaultEtcdCtx, key); errCAERouter != nil {
		log.GetLogger().Errorf("failed to delete cae key %s in etcd, error: %s", key, errCAERouter.Error())
		return errCAERouter
	}
	return nil
}

// GetValueFromEtcd query value from etcd and retry only in case of timeout
func GetValueFromEtcd(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
	etcdClient := etcd3.GetETCDClient(etcdSelection)
	if etcdClient == nil {
		log.GetLogger().Warnf("failed to init ETCD client")
		return nil, ErrETCDNotFound
	}
	if etcdClient.Client == nil && etcdSelection == etcd3.Meta {
		log.GetLogger().Warnf("failed to init meta ETCD client")
		return nil, ErrETCDNotFound
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
		return nil, ErrETCDNotFound
	}

	return []byte(values[0]), err
}

// GetPodValue -
func GetPodValue(podKey string) (string, string, error) {
	resp, err := GetValueFromEtcd(podKey, etcd3.Meta)
	if err != nil {
		return "", "", err
	}

	var info nodeInfo
	err = json.Unmarshal(resp, &info)
	if err != nil {
		return "", "", err
	}
	if !CheckIP(info.NodeIP) || !CheckPort(info.NodePort) {
		return "", "", fmt.Errorf("error value of pod, ip: %s, port: %s", info.NodeIP, info.NodePort)
	}
	return info.NodeIP, info.NodePort, nil

}

// GetStateValue -
func GetStateValue(stateKey, stateID string) (string, error) {
	resp, err := GetValueFromEtcd(stateKey, etcd3.Meta)
	if err != nil {
		return "", err
	}

	curStates := make(map[string]string, stateWorkerKeyValueCounts)
	err = json.Unmarshal(resp, &curStates)
	if err != nil {
		return "", err
	}
	return curStates[stateID], nil
}
