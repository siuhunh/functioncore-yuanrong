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

// Package instance -
package instance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/functiontypes"
)

const (
	minInstanceNumAverageThreshold = 12
	defaultMaxInstanceNum          = 1000
)

var (
	errNotFound = errors.New("not found")
)

func putInstanceInfoInEtcd(funcMetadata *functiontypes.FunctionEtcdInfo, clusterInstanceInfo map[string][]int64) error {
	log.GetLogger().Infof("begin to put instance info to etcd, funcUrn:[%s], instanceInfo:[%v]",
		funcMetadata.FuncMetaData.VersionURN, clusterInstanceInfo)
	for clusterID, value := range clusterInstanceInfo {
		funcInstanceKey := constructInstanceEtcdKey(clusterID, funcMetadata.FuncMetaData)
		instanceInfo := &Info{}
		instanceInfo.InstanceMetadata.MaxInstance = value[0]
		instanceInfo.InstanceMetadata.MinInstance = value[1]
		funcInstanceInfo, err := json.Marshal(instanceInfo)
		if err != nil {
			log.GetLogger().Errorf("failed to marshall instanceInfo:[%+v], funcUrn:[%s]",
				instanceInfo, funcMetadata.FuncMetaData.FunctionURN)
			return err
		}
		if err = PutToEtcd(funcInstanceKey, string(funcInstanceInfo)); err != nil {
			log.GetLogger().Errorf("failed to put instanceInfo to etcd, funcUrn:[%s]",
				funcMetadata.FuncMetaData.FunctionURN)
			return err
		}
	}
	log.GetLogger().Infof("succeeded to put instance info to etcd, funcUrn:[%s], instanceInfo:[%v]",
		funcMetadata.FuncMetaData.VersionURN, clusterInstanceInfo)
	return nil
}

func deleteInstanceInfoInEtcd(funcMetadata *functiontypes.FunctionEtcdInfo, clusters []string) error {
	log.GetLogger().Debugf("begin to delete instance info, funcUrn:[%v] clusters:[%v]",
		funcMetadata.FuncMetaData.VersionURN, clusters)
	for _, clusterID := range clusters {
		funcInstanceKey := constructInstanceEtcdKey(clusterID, funcMetadata.FuncMetaData)
		if err := deleteFromEtcd(funcInstanceKey); err != nil {
			log.GetLogger().Errorf("failed to delete instanceInfo in etcd, funcUrn:[%s]",
				funcMetadata.FuncMetaData.FunctionURN)
			return err
		}
	}
	return nil
}

func constructInstanceEtcdKey(clusterID string, funcMetadata functiontypes.FuncMetaData) string {
	key := fmt.Sprintf("/instances/business/yrk/cluster/%s/tenant/%s/function/%s/version/%s", clusterID,
		funcMetadata.TenantID, funcMetadata.Name, funcMetadata.Version)
	return key
}

func getFuncVPCAvailableClusters(funcMetadata *functiontypes.FunctionEtcdInfo) []string {
	if funcMetadata.ExtendedMetaData.FuncVpc.VpcID == "" &&
		funcMetadata.ExtendedMetaData.EndpointTenantVpc.VpcID == "" {
		return nil
	}
	funcAvailableClustersEtcdKey := constructFuncAvailableClustersEtcdKey(funcMetadata.FuncMetaData)
	resp, err := getFromEtcd(funcAvailableClustersEtcdKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get function vpc availableClusters from etcd, funcURN: %s, err: %s",
			funcMetadata.FuncMetaData.VersionURN, err.Error())
		return nil
	}
	var availableClusters []string
	if err = json.Unmarshal(resp, &availableClusters); err != nil {
		log.GetLogger().Errorf("failed to unmarshal function vpc availableClusters, funcURN: %s, err: %s",
			funcMetadata.FuncMetaData.VersionURN, err.Error())
		return nil
	}
	return availableClusters
}

func constructFuncAvailableClustersEtcdKey(funcMetadata functiontypes.FuncMetaData) string {
	key := fmt.Sprintf("/cae/avaiablecluster/business/yrk/tenant/%s/function/%s/version/%s",
		funcMetadata.TenantID, funcMetadata.Name, funcMetadata.Version)
	return key
}

// PutToEtcd -
func PutToEtcd(key, value string) error {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || etcd3.GetCAEMetaEtcdClient().Client == nil {
		log.GetLogger().Error("failed to get cae meta etcd client, it is nil!")
		return errors.New("failed to get cae meta etcd client, it is nil")
	}
	if err := client.Put(defaultCtx, key, value); err != nil {
		log.GetLogger().Errorf("failed to put key: %s", err.Error())
		return err
	}
	return nil
}

func getFromEtcd(key string) ([]byte, error) {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || etcd3.GetCAEMetaEtcdClient().Client == nil {
		log.GetLogger().Error("failed to get cae meta etcd client, it is nil!")
		return nil, errors.New("failed to get cae meta etcd client, it is nil")
	}
	getResp, err := client.GetResponse(defaultCtx, key)
	if err != nil {
		log.GetLogger().Errorf("failed to get data of key: %s from etcd, err: %s", key, err.Error())
		return nil, err
	}
	if len(getResp.Kvs) == 0 {
		log.GetLogger().Errorf("key %s is not exist in etcd", key)
		return nil, errNotFound
	}
	return getResp.Kvs[0].Value, nil
}

func deleteFromEtcd(key string) error {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || etcd3.GetCAEMetaEtcdClient().Client == nil {
		log.GetLogger().Error("failed to get cae meta etcd client, it is nil!")
		return errors.New("failed to get cae meta etcd client, it is nil")
	}
	if err := client.Delete(defaultCtx, key); err != nil {
		log.GetLogger().Errorf("failed to delete key %s, error:%s", key, err.Error())
		return err
	}
	return nil
}

func getAllInstanceInfoOfCluster(clusterID string) map[string]Info {
	instanceClusterKeyPrefix := fmt.Sprintf("/instances/business/yrk/cluster/%s", clusterID)
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || etcd3.GetCAEMetaEtcdClient().Client == nil {
		log.GetLogger().Error("failed to get cae meta etcd client, it is nil!")
		return nil
	}
	getResp, err := client.GetResponse(defaultCtx, instanceClusterKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to list instance from etcd by prefixKey: %s, err: %s",
			instanceClusterKeyPrefix, err.Error())
		return nil
	}
	if len(getResp.Kvs) == 0 {
		log.GetLogger().Warnf("there is no instance info fetch by prefixKey %s ", instanceClusterKeyPrefix)
		return nil
	}
	instanceInfoMap := make(map[string]Info, len(getResp.Kvs))
	for _, kv := range getResp.Kvs {
		info := Info{}
		if err := json.Unmarshal(kv.Value, &info); err != nil {
			log.GetLogger().Errorf("failed to unmarshal key:%s instance info, err:", string(kv.Key), err.Error())
			continue
		}
		instanceInfoMap[string(kv.Key)] = info
	}
	return instanceInfoMap
}
