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
	"context"
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types/functiontypes"
)

func TestUpdateInfo(t *testing.T) {
	t.Run("no availableCluster", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{}
			})
		defer patch.Reset()
		UpdateInfo("funcSvcID", &functiontypes.FunctionEtcdInfo{})
		err := update("funcSvcID", &functiontypes.FunctionEtcdInfo{})
		assert.Error(t, err)
		assert.Equal(t, "there is no availableCluster", err.Error())
	})
	t.Run("failed to putInstanceInfoInEtcd", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{"test1"}
			})
		patch.ApplyFunc(putInstanceInfoInEtcd, func(funcMetadata *functiontypes.FunctionEtcdInfo,
			clusterInstanceInfo map[string][]int64) error {
			return errors.New("error")
		})
		defer patch.Reset()
		err := update("funcSvcID", &functiontypes.FunctionEtcdInfo{})
		assert.Error(t, err)
	})
	t.Run("unhealthy clusters exist during update.", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{"cluster1"}
			})
		patch.ApplyFunc(putInstanceInfoInEtcd, func(funcMetadata *functiontypes.FunctionEtcdInfo,
			clusterInstanceInfo map[string][]int64) error {
			return nil
		})
		patch.ApplyFunc((*clustermgr.ClusterManager).GetAllCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{"cluster1": {}, "cluster2": {}}
			})
		defer patch.Reset()
		err := update("funcSvcID", &functiontypes.FunctionEtcdInfo{})
		assert.Error(t, err)
	})
	t.Run("success.", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{"cluster1"}
			})
		patch.ApplyFunc(putInstanceInfoInEtcd, func(funcMetadata *functiontypes.FunctionEtcdInfo,
			clusterInstanceInfo map[string][]int64) error {
			return nil
		})
		patch.ApplyFunc((*clustermgr.ClusterManager).GetAllCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{"cluster1": {}}
			})
		defer patch.Reset()
		err := update("funcSvcID", &functiontypes.FunctionEtcdInfo{})
		assert.NoError(t, err)
	})
}

func TestResetMaxInstanceNumWhenLessThanPoolLen(t *testing.T) {
	instanceInfo := make(map[string][]int64)
	instanceInfo["cluster1"] = []int64{3, 0}
	instanceInfo["cluster2"] = []int64{0, 0}
	instanceInfo["cluster3"] = []int64{0, 0}
	instanceInfo["cluster4"] = []int64{0, 0}

	clusterId := "cluster1"
	maxInstanceNum := int64(3)

	resetMaxInstanceNumWhenLessThanPoolLen(instanceInfo, clusterId, maxInstanceNum)

	if instanceInfo[clusterId][0] != 1 {
		t.Errorf("Expected cluster1's first value to be 1, but got %d", instanceInfo[clusterId][0])
	}

	if instanceInfo["cluster2"][0] != 1 {
		t.Errorf("Expected cluster2's first value to be 1, but got %d", instanceInfo["cluster2"][0])
	}

	if instanceInfo["cluster3"][0] != 1 {
		t.Errorf("Expected cluster3's first value to be 1, but got %d", instanceInfo["cluster3"][0])
	}
	if instanceInfo["cluster4"][0] != 0 {
		t.Errorf("Expected cluster4's first value to be 0, but got %d", instanceInfo["cluster4"][0])
	}
}

func TestResetMinInstanceNumWhenLessThanThreshold(t *testing.T) {
	instanceInfo := make(map[string][]int64)
	instanceInfo["cluster1"] = []int64{0, 2}
	instanceInfo["cluster2"] = []int64{0, 2}
	instanceInfo["cluster3"] = []int64{0, 2}
	instanceInfo["cluster4"] = []int64{0, 2}

	clusterId := "cluster3"
	minInstanceNum := int64(8)

	resetMinInstanceNumWhenLessThanThreshold(instanceInfo, clusterId, minInstanceNum)

	if instanceInfo[clusterId][1] != 8 {
		t.Errorf("Expected cluster3's first value to be 8, but got %d", instanceInfo[clusterId][0])
	}

	if instanceInfo["cluster1"][1] != 0 {
		t.Errorf("Expected cluster1's first value to be 0, but got %d", instanceInfo["cluster2"][0])
	}

	if instanceInfo["cluster2"][1] != 0 {
		t.Errorf("Expected cluster2's first value to be 8, but got %d", instanceInfo["cluster3"][0])
	}
	if instanceInfo["cluster4"][1] != 0 {
		t.Errorf("Expected cluster4's first value to be 0, but got %d", instanceInfo["cluster4"][0])
	}
}

func TestCheckISNeedUpdate(t *testing.T) {
	oldInfo := &functiontypes.FunctionEtcdInfo{
		InstanceMetaData: functiontypes.InstanceMetaData{
			MaxInstance: 1,
			MinInstance: 1,
		},
	}
	newInfo := &functiontypes.FunctionEtcdInfo{
		InstanceMetaData: functiontypes.InstanceMetaData{
			MaxInstance: 1,
			MinInstance: 1,
		},
	}
	t.Run("not exists", func(t *testing.T) {
		assert.True(t, CheckISNeedUpdate(false, nil, nil))
	})
	assert.True(t, CheckISNeedUpdate(false, nil, nil))
	t.Run("exists and MaxInstance and MinInstance of oldInfo and newInfo are equal", func(t *testing.T) {
		assert.False(t, CheckISNeedUpdate(true, oldInfo, newInfo))
	})
	t.Run("exists and MaxInstance and MinInstance of oldInfo and newInfo are not equal", func(t *testing.T) {
		newInfo.InstanceMetaData.MaxInstance = 2
		assert.True(t, CheckISNeedUpdate(true, oldInfo, newInfo))
	})
	t.Run("exists and vpc info is changed", func(t *testing.T) {
		newInfo.ExtendedMetaData.FuncVpc.VpcID = "11"
		assert.True(t, CheckISNeedUpdate(true, oldInfo, newInfo))
	})
}

func TestGetAveragedInstanceInfo(t *testing.T) {
	funcMetadata := &functiontypes.FunctionEtcdInfo{
		FuncMetaData: functiontypes.FuncMetaData{
			VersionURN: "test-version-urn",
		},
		InstanceMetaData: functiontypes.InstanceMetaData{
			MinInstance: 12,
			MaxInstance: 400,
		},
	}
	t.Run("no availableClusters", func(t *testing.T) {
		availableClusters := []string{}
		instanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
		assert.Equal(t, 0, len(instanceInfo))
	})

	availableClusters := []string{"cluster1", "cluster2"}
	t.Run("function without vpc", func(t *testing.T) {
		instanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
		assert.Equal(t, 2, len(instanceInfo))
		assert.Equal(t, int64(200), instanceInfo["cluster1"][0])
		assert.Equal(t, int64(6), instanceInfo["cluster1"][1])
		assert.Equal(t, int64(200), instanceInfo["cluster2"][0])
		assert.Equal(t, int64(6), instanceInfo["cluster2"][1])
	})

	t.Run("function with vpc", func(t *testing.T) {
		funcMetadata.ExtendedMetaData = functiontypes.ExtendedMetaData{
			FuncVpc: functiontypes.Vpc{
				VpcID: "1",
			},
			EndpointTenantVpc: functiontypes.Vpc{
				VpcID: "1",
			},
		}
		patch := gomonkey.ApplyFunc(getFromEtcd,
			func(key string) ([]byte, error) {
				return []byte("[\"cluster1\"]"), nil
			})
		defer func() {
			patch.Reset()
			funcMetadata.ExtendedMetaData = functiontypes.ExtendedMetaData{}
		}()
		instanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
		assert.Equal(t, 2, len(instanceInfo))
		assert.Equal(t, int64(400), instanceInfo["cluster1"][0])
		assert.Equal(t, int64(12), instanceInfo["cluster1"][1])
		assert.Equal(t, int64(0), instanceInfo["cluster2"][0])
		assert.Equal(t, int64(0), instanceInfo["cluster2"][1])
	})

	t.Run("minInstanceNum < minInstanceNumAverageThreshold", func(t *testing.T) {
		funcMetadata.InstanceMetaData.MinInstance = 11
		instanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
		assert.Equal(t, 2, len(instanceInfo))
		assert.Equal(t, int64(200), instanceInfo["cluster1"][0])
		assert.Equal(t, int64(0), instanceInfo["cluster1"][1])
		assert.Equal(t, int64(200), instanceInfo["cluster2"][0])
		assert.Equal(t, int64(11), instanceInfo["cluster2"][1])
	})

	t.Run("maxInstanceNum < poolLen", func(t *testing.T) {
		funcMetadata.InstanceMetaData.MinInstance = 12
		funcMetadata.InstanceMetaData.MaxInstance = 1
		instanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
		assert.Equal(t, 2, len(instanceInfo))
		assert.Equal(t, int64(0), instanceInfo["cluster1"][0])
		assert.Equal(t, int64(6), instanceInfo["cluster1"][1])
		assert.Equal(t, int64(1), instanceInfo["cluster2"][0])
		assert.Equal(t, int64(6), instanceInfo["cluster2"][1])
	})
}

func TestDeleteInfo(t *testing.T) {
	funcMetadata := &functiontypes.FunctionEtcdInfo{
		FuncMetaData: functiontypes.FuncMetaData{
			FunctionURN: "test-urn",
		},
	}
	t.Run("succeed to delete instance info", func(t *testing.T) {
		patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{"cluster1": {}}
			})
		patch.ApplyFunc(deleteFromEtcd, func(key string) error { return nil })
		defer patch.Reset()
		DeleteInfo(funcMetadata)
	})
	t.Run("failed to delete instance info", func(t *testing.T) {
		patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{"cluster1": {}}
			})
		patch.ApplyFunc(deleteFromEtcd, func(key string) error { return errors.New("some error") })
		defer patch.Reset()
		DeleteInfo(funcMetadata)
	})

}

func TestReBalanceUnhealthyClusterInstanceInfo(t *testing.T) {
	t.Run("all cluster is Healthy", func(t *testing.T) {
		clusters := map[string]clustermgr.Cluster{"cluster001": {State: utils.Healthy}}
		reBalanceUnhealthyClusterInstanceInfo(clusters)
	})
	t.Run("there is no instance info in unhealthy cluster", func(t *testing.T) {
		clusters := map[string]clustermgr.Cluster{"cluster001": {State: utils.UnHealthy}}
		patch := gomonkey.ApplyFunc(getAllInstanceInfoOfCluster,
			func(clusterID string) map[string]Info {
				return nil
			})
		defer patch.Reset()
		reBalanceUnhealthyClusterInstanceInfo(clusters)
	})
	t.Run("No processing when the maximum and minimum number of instances is 0", func(t *testing.T) {
		clusters := map[string]clustermgr.Cluster{"cluster001": {State: utils.UnHealthy}}
		patch := gomonkey.ApplyFunc(getAllInstanceInfoOfCluster,
			func(clusterID string) map[string]Info {
				return map[string]Info{"test": {EtcdInstanceMetaData{MaxInstance: 0, MinInstance: 0}}}
			})
		defer patch.Reset()
		reBalanceUnhealthyClusterInstanceInfo(clusters)
	})
	t.Run("No processing when the maximum and minimum number of instances is 0", func(t *testing.T) {
		clusters := map[string]clustermgr.Cluster{"cluster001": {State: utils.UnHealthy}}
		patch := gomonkey.ApplyFunc(getAllInstanceInfoOfCluster,
			func(clusterID string) map[string]Info {
				return map[string]Info{"test": {EtcdInstanceMetaData{MaxInstance: 0, MinInstance: 0}}}
			})
		defer patch.Reset()
		reBalanceUnhealthyClusterInstanceInfo(clusters)
	})
	t.Run("failed to reBalanceFuncInstanceInfo", func(t *testing.T) {
		clusters := map[string]clustermgr.Cluster{"cluster001": {State: utils.UnHealthy}}
		patch := gomonkey.ApplyFunc(getAllInstanceInfoOfCluster,
			func(clusterID string) map[string]Info {
				return map[string]Info{"test": {EtcdInstanceMetaData{MaxInstance: 10, MinInstance: 1}}}
			})
		patch.ApplyFunc(reBalanceFuncInstanceInfo, func(instanceKey string, unHealthyClusterID string) error {
			return errors.New("error")
		})
		defer patch.Reset()
		reBalanceUnhealthyClusterInstanceInfo(clusters)
	})
}

func TestReBalanceFuncInstanceInfo(t *testing.T) {
	instanceKey := "/instances/business/yrk/cluster/cluster01/tenant/123456/function/0@default@funcName/version/latest"
	funcSvcID := CrNameByInstanceKey(instanceKey)
	unHealthyClusterID := "testUnHealthyClusterID"
	funcMeta := &functiontypes.FunctionEtcdInfo{
		InstanceMetaData: functiontypes.InstanceMetaData{MinInstance: 1, MaxInstance: 400},
	}
	cache.GetInstance().UpdateFunctionMeta(funcSvcID, funcMeta)
	t.Run("funcMetadata not exists", func(t *testing.T) {
		cache.GetInstance().RemoveFunctionMeta(funcSvcID)
		defer cache.GetInstance().UpdateFunctionMeta(funcSvcID, funcMeta)
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.Nil(t, err)
	})
	t.Run("No processing when the maximum and minimum number of instances is 0", func(t *testing.T) {
		funcMeta.InstanceMetaData.MaxInstance = 0
		funcMeta.InstanceMetaData.MinInstance = 0
		defer func() {
			funcMeta.InstanceMetaData.MaxInstance = 400
			funcMeta.InstanceMetaData.MinInstance = 1
			cache.GetInstance().UpdateFunctionMeta(funcSvcID, funcMeta)
		}()
		cache.GetInstance().UpdateFunctionMeta(funcSvcID, funcMeta)
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.Nil(t, err)
	})
	t.Run("unhealthy cluster recovered to healthy", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{"testUnHealthyClusterID"}
			})
		defer patch.Reset()
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.Nil(t, err)
	})
	t.Run("cluster is empty", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{}
			})
		defer patch.Reset()
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.Nil(t, err)
	})
	t.Run("unhealthy cluster not recovered to healthy", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{"healthyClusterID"}
			})
		defer patch.Reset()
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.Error(t, err)
	})
	t.Run("failed to putInstanceInfoInEtcd", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{""}
			})
		patch.ApplyFunc(putInstanceInfoInEtcd, func(funcMetadata *functiontypes.FunctionEtcdInfo,
			clusterInstanceInfo map[string][]int64) error {
			return errors.New("errors")
		})
		defer patch.Reset()
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.NotNil(t, err)
	})
	t.Run("failed to saveFuncSvcIDWhenClusterUnHealthy", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{""}
			})
		patch.ApplyFunc(putInstanceInfoInEtcd, func(funcMetadata *functiontypes.FunctionEtcdInfo,
			clusterInstanceInfo map[string][]int64) error {
			return nil
		})
		patch.ApplyFunc(saveFuncSvcIDWhenClusterUnHealthy, func(funcSvcID string) error {
			return errors.New("errors")
		})
		defer patch.Reset()
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.NotNil(t, err)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(GetFuncAvailableCluster,
			func() []string {
				return []string{""}
			})
		patch.ApplyFunc(putInstanceInfoInEtcd, func(funcMetadata *functiontypes.FunctionEtcdInfo,
			clusterInstanceInfo map[string][]int64) error {
			return nil
		})
		patch.ApplyFunc(saveFuncSvcIDWhenClusterUnHealthy, func(funcSvcID string) error {
			return nil
		})
		defer patch.Reset()
		err := reBalanceFuncInstanceInfo(instanceKey, unHealthyClusterID)
		assert.Nil(t, err)
	})
}

func TestReBalanceFuncInstance(t *testing.T) {
	t.Run("failed to getReBalanceFuncSvcIDArrayFromRedis", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(getReBalanceFuncSvcIDArrayFromRedis,
			func() ([]string, error) {
				return nil, errors.New("errors")
			})
		defer patch.Reset()
		reBalanceFuncInstance()
	})
	t.Run("need reBalanceFuncSvcIDArray is empty", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(getReBalanceFuncSvcIDArrayFromRedis,
			func() ([]string, error) {
				return []string{}, nil
			})
		defer patch.Reset()
		reBalanceFuncInstance()
	})
	t.Run("the funcMeta of FuncSvcID not exists", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(getReBalanceFuncSvcIDArrayFromRedis,
			func() ([]string, error) {
				return []string{"testFuncSvcID"}, nil
			})
		patch.ApplyFunc(deleteFuncSvcIDInReBalanceKey, func(funcSvcID string) error { return nil })
		defer patch.Reset()
		reBalanceFuncInstance()
	})
	t.Run("failed to deleteFuncSvcIDInReBalanceKey", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(getReBalanceFuncSvcIDArrayFromRedis,
			func() ([]string, error) {
				return []string{"testFuncSvcID"}, nil
			})
		patch.ApplyFunc(deleteFuncSvcIDInReBalanceKey, func(funcSvcID string) error { return errors.New("errors") })
		defer patch.Reset()
		reBalanceFuncInstance()
	})
	t.Run("failed to update one", func(t *testing.T) {
		instanceKey := "/instances/business/yrk/cluster/cluster01/tenant/123456/function/0@default@funcName/version/latest"
		funcSvcID := CrNameByInstanceKey(instanceKey)
		funcMeta := &functiontypes.FunctionEtcdInfo{
			InstanceMetaData: functiontypes.InstanceMetaData{MinInstance: 1, MaxInstance: 400},
		}
		cache.GetInstance().UpdateFunctionMeta(funcSvcID, funcMeta)
		patch := gomonkey.ApplyFunc(getReBalanceFuncSvcIDArrayFromRedis,
			func() ([]string, error) {
				return []string{funcSvcID}, nil
			})
		patch.ApplyFunc(update, func(_ string, _ *functiontypes.FunctionEtcdInfo) error {
			return errors.New("errors")
		})
		patch.ApplyFunc(deleteFuncSvcIDInReBalanceKey, func(funcSvcID string) error { return nil })
		defer patch.Reset()
		reBalanceFuncInstance()
	})
}

func TestGetReBalanceFuncSvcIDArrayFromRedis(t *testing.T) {
	t.Run("redis is not ready", func(t *testing.T) {
		funcSvcIDArray, err := getReBalanceFuncSvcIDArrayFromRedis()
		assert.Nil(t, funcSvcIDArray)
		assert.Error(t, err)
		assert.Equal(t, "redis is not ready", err.Error())
	})
	t.Run("failed to get instances reBalance info from redis", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SMembers,
			func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringSliceCmd {
				cmd := &redis.StringSliceCmd{}
				cmd.SetVal(nil)
				cmd.SetErr(errors.New("test"))
				return cmd
			})
		defer patch.Reset()
		funcSvcIDArray, err := getReBalanceFuncSvcIDArrayFromRedis()
		assert.Nil(t, funcSvcIDArray)
		assert.Error(t, err)
	})
	t.Run("empty in redis specialReBalanceKey", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SMembers,
			func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringSliceCmd {
				cmd := &redis.StringSliceCmd{}
				cmd.SetVal([]string{})
				return cmd
			})
		funcSvcIDArray, err := getReBalanceFuncSvcIDArrayFromRedis()
		assert.Nil(t, funcSvcIDArray)
		assert.NoError(t, err)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SMembers,
			func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringSliceCmd {
				cmd := &redis.StringSliceCmd{}
				cmd.SetVal([]string{"test"})
				return cmd
			})
		funcSvcIDArray, err := getReBalanceFuncSvcIDArrayFromRedis()
		assert.Equal(t, []string{"test"}, funcSvcIDArray)
		assert.NoError(t, err)
	})
}

func TestDeleteFuncSvcIDInReBalanceKey(t *testing.T) {
	t.Run("redis is not ready", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return nil
		})
		defer patch.Reset()
		err := deleteFuncSvcIDInReBalanceKey("test")
		assert.Error(t, err)
		assert.Equal(t, "redis is not ready", err.Error())
	})
	t.Run("failed to sRem in redis", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SRem,
			func(_ *redisclient.Client, ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
				cmd := &redis.IntCmd{}
				cmd.SetErr(errors.New("test"))
				return cmd
			})
		defer patch.Reset()
		err := deleteFuncSvcIDInReBalanceKey("test")
		assert.Error(t, err)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SRem,
			func(_ *redisclient.Client, ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
				cmd := &redis.IntCmd{}
				return cmd
			})
		err := deleteFuncSvcIDInReBalanceKey("test")
		assert.NoError(t, err)
	})
}

func TestSaveFuncSvcIDWhenClusterUnHealthy(t *testing.T) {
	t.Run("redis is not ready", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return nil
		})
		defer patch.Reset()
		err := saveFuncSvcIDWhenClusterUnHealthy("test")
		assert.Error(t, err)
		assert.Equal(t, "redis is not ready", err.Error())
	})
	t.Run("failed to SAdd in redis", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SAdd,
			func(_ *redisclient.Client, ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
				cmd := &redis.IntCmd{}
				cmd.SetErr(errors.New("test"))
				return cmd
			})
		defer patch.Reset()
		err := saveFuncSvcIDWhenClusterUnHealthy("test")
		assert.Error(t, err)
	})
	t.Run("success", func(t *testing.T) {
		patch := gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
			return &redisclient.Client{}
		})
		patch.ApplyFunc((*redisclient.Client).SAdd,
			func(_ *redisclient.Client, ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
				cmd := &redis.IntCmd{}
				return cmd
			})
		err := saveFuncSvcIDWhenClusterUnHealthy("test")
		assert.NoError(t, err)
	})
}
