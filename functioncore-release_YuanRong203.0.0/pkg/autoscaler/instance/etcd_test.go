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

package instance

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types/functiontypes"
)

func TestConstructInstanceEtcdKey(t *testing.T) {
	clusterID := "cluster1"
	funcMetadata := functiontypes.FuncMetaData{
		TenantID: "tenant1",
		Name:     "function1",
		Version:  "1.0",
	}

	expectedKey := fmt.Sprintf("/instances/business/yrk/cluster/%s/tenant/%s/function/%s/version/%s", clusterID,
		funcMetadata.TenantID, funcMetadata.Name, funcMetadata.Version)

	key := constructInstanceEtcdKey(clusterID, funcMetadata)

	if key != expectedKey {
		t.Errorf("Expected key %s, but got %s", expectedKey, key)
	}
}

func TestPutInstanceInfoInEtcd(t *testing.T) {
	funcMetadata := &functiontypes.FunctionEtcdInfo{
		FuncMetaData: functiontypes.FuncMetaData{
			VersionURN:  "testVersionURN",
			FunctionURN: "testFunctionURN",
		},
	}
	clusterInstanceInfo := map[string][]int64{
		"cluster1": {1, 2},
		"cluster2": {3, 4},
	}
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(PutToEtcd,
			func(key, value string) error {
				return nil
			})
		defer patch.Reset()
		err := putInstanceInfoInEtcd(funcMetadata, clusterInstanceInfo)
		assert.NoError(t, err)
	})
	t.Run("failed to putToEtcd", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(PutToEtcd,
			func(key, value string) error {
				return errors.New("error")
			})
		defer patch.Reset()
		err := putInstanceInfoInEtcd(funcMetadata, clusterInstanceInfo)
		assert.Error(t, err)
	})
}

func TestGetAllInstanceInfoOfCluster(t *testing.T) {
	info := Info{}
	data, _ := json.Marshal(info)
	t.Run("etcd client is nil", func(t *testing.T) {
		instanceInfos := getAllInstanceInfoOfCluster("test")
		assert.Nil(t, instanceInfos)
	})
	t.Run("etcd response error", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return nil, errors.New("test")
		})
		defer patch.Reset()
		instanceInfos := getAllInstanceInfoOfCluster("test")
		assert.Nil(t, instanceInfos)
	})
	t.Run("no instance info fetch by prefixKey", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
		})
		defer patch.Reset()
		instanceInfos := getAllInstanceInfoOfCluster("test")
		assert.Nil(t, instanceInfos)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("Key"), Value: data}}}, nil
		})
		defer patch.Reset()
		instanceInfos := getAllInstanceInfoOfCluster("test")
		assert.Equal(t, 1, len(instanceInfos))
	})
}

func TestDeleteFromEtcd(t *testing.T) {
	t.Run("etcd client is nil", func(t *testing.T) {
		err := deleteFromEtcd("test")
		assert.Error(t, err)
	})
	t.Run("failed to delete", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
			return errors.New("err")
		})
		defer patch.Reset()
		err := deleteFromEtcd("test")
		assert.Error(t, err)
	})
	t.Run("etcd client is nil", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient,
			ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
			return nil
		})
		defer patch.Reset()
		err := deleteFromEtcd("test")
		assert.NoError(t, err)
	})
}

func TestGetFromEtcd(t *testing.T) {
	t.Run("etcd client is nil", func(t *testing.T) {
		data, err := getFromEtcd("test")
		assert.Error(t, err)
		assert.Nil(t, data)
	})
	t.Run("failed to get", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return nil, errors.New("test")
		})
		defer patch.Reset()
		data, err := getFromEtcd("test")
		assert.Error(t, err)
		assert.Nil(t, data)
	})
	t.Run("no data get from etcd", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
		})
		defer patch.Reset()
		data, err := getFromEtcd("test")
		assert.Equal(t, errNotFound, err)
		assert.Nil(t, data)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
			return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("Key"), Value: []byte("value")}}}, nil
		})
		defer patch.Reset()
		data, err := getFromEtcd("test")
		assert.NoError(t, err)
		assert.Equal(t, []byte("value"), data)
	})
}

func TestPutToEtcd(t *testing.T) {
	t.Run("etcd client is nil", func(t *testing.T) {
		err := PutToEtcd("key", "value")
		assert.Error(t, err)
	})
	t.Run("failed to put", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, value string, opts ...clientv3.OpOption) error {
			return errors.New("test")
		})
		defer patch.Reset()
		err := PutToEtcd("key", "value")
		assert.Error(t, err)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
			func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
		patch.ApplyFunc((*etcd3.EtcdClient).Put, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
			key string, value string, opts ...clientv3.OpOption) error {
			return nil
		})
		defer patch.Reset()
		err := PutToEtcd("key", "value")
		assert.NoError(t, err)
	})
}

func TestGetFuncAvailableClusters(t *testing.T) {
	t.Run("get cache success", func(t *testing.T) {
		funcMetadata := functiontypes.FuncMetaData{
			TenantID: "tenant1",
			Name:     "function1",
			Version:  "1.0",
		}
		etcdInfo := functiontypes.FunctionEtcdInfo{
			FuncMetaData: funcMetadata,
		}
		funcSvcID := utils.CrName("yrk", "tenant1", "function1", "1.0")
		cache.GetInstance().UpdateCaeAvailableCluster(funcSvcID, []string{"cluster1", "cluster2"})
		clusters, found := GetFuncAvailableClusters(&etcdInfo)
		assert.Equal(t, 2, len(clusters))
		assert.True(t, found)
		cache.GetInstance().RemoveCaeAvailableCluster(funcSvcID)
	})
}

func TestGetMinInstanceNumAverageThreshold(t *testing.T) {
	os.Setenv("MIN_INSTANCE_NUM_AVERAGE_THRESHOLD", "")
	if result := getMinInstanceNumAverageThreshold(); result != defaultMinInstanceNumAverageThreshold {
		t.Errorf("Expected %d, but got %d", defaultMinInstanceNumAverageThreshold, result)
	}

	os.Setenv("MIN_INSTANCE_NUM_AVERAGE_THRESHOLD", "abc")
	if result := getMinInstanceNumAverageThreshold(); result != defaultMinInstanceNumAverageThreshold {
		t.Errorf("Expected %d, but got %d", defaultMinInstanceNumAverageThreshold, result)
	}

	os.Setenv("MIN_INSTANCE_NUM_AVERAGE_THRESHOLD", "-1")
	if result := getMinInstanceNumAverageThreshold(); result != defaultMinInstanceNumAverageThreshold {
		t.Errorf("Expected %d, but got %d", defaultMinInstanceNumAverageThreshold, result)
	}

	os.Setenv("MIN_INSTANCE_NUM_AVERAGE_THRESHOLD", "10")
	if result := getMinInstanceNumAverageThreshold(); result != 10 {
		t.Errorf("Expected %d, but got %d", 10, result)
	}
}

func TestGetCaeAvailableClusterSleepTime(t *testing.T) {
	os.Setenv("GET_CAE_AVAILABLE_CLUSTER_SLEEP_TIME", "")
	if result := getCaeAvailableClusterSleepTime(); result != defaultGetCaeAvailableClusterSleepTime {
		t.Errorf("Expected %d, but got %d", defaultGetCaeAvailableClusterSleepTime, result)
	}

	os.Setenv("GET_CAE_AVAILABLE_CLUSTER_SLEEP_TIME", "abc")
	if result := getCaeAvailableClusterSleepTime(); result != defaultGetCaeAvailableClusterSleepTime {
		t.Errorf("Expected %d, but got %d", defaultGetCaeAvailableClusterSleepTime, result)
	}

	os.Setenv("GET_CAE_AVAILABLE_CLUSTER_SLEEP_TIME", "-1")
	if result := getCaeAvailableClusterSleepTime(); result != defaultGetCaeAvailableClusterSleepTime {
		t.Errorf("Expected %d, but got %d", defaultGetCaeAvailableClusterSleepTime, result)
	}

	os.Setenv("GET_CAE_AVAILABLE_CLUSTER_SLEEP_TIME", "10")
	if result := getCaeAvailableClusterSleepTime(); result != 10 {
		t.Errorf("Expected %d, but got %d", 10, result)
	}
}
