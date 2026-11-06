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
	"sort"
	"strings"
	"sync"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/types/functiontypes"
)

const (
	instanceKeySplitSize  = 12
	instanceBusinessIndex = 3
	instanceTenantIndex   = 7
	instanceFuncNameIndex = 9
	instanceVersionIndex  = 11

	replicasNum = 128
)

var allClusterHashMap = &sync.Map{}

// GetFuncAvailableCluster -
func GetFuncAvailableCluster() []string {
	availableCluster := clustermgr.GetClusterMgr().GetAllAvailableCluster()
	if len(availableCluster) == 0 {
		return nil
	}
	var availableClusterArray []string
	for clusterID := range availableCluster {
		availableClusterArray = append(availableClusterArray, clusterID)
	}
	sort.Strings(availableClusterArray)
	return availableClusterArray
}

// GetMaxInstanceCount -
func GetMaxInstanceCount(funcMetadata *functiontypes.FunctionEtcdInfo) int64 {
	var maxInstanceCount int64
	if funcMetadata.InstanceMetaData.MaxInstance == -1 {
		maxInstanceCount = int64(defaultMaxInstanceNum)
	} else {
		maxInstanceCount = funcMetadata.InstanceMetaData.MaxInstance
	}
	return maxInstanceCount
}

// IntersectArray -
func IntersectArray(a []string, b []string) []string {
	var inter []string
	m := make(map[string]bool)

	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		if _, exists := m[v]; exists {
			inter = append(inter, v)
		}
	}

	return inter
}

func differenceArray(a []string, b []string) []string {
	var dif []string
	m := make(map[string]bool)

	for _, v := range a {
		m[v] = true
	}
	for _, v := range b {
		if _, exists := m[v]; !exists {
			dif = append(dif, v)
		}
	}

	return dif
}

// CrNameByInstanceKey return Cr Name By instance key
func CrNameByInstanceKey(instanceKey string) string {
	functionInfo := getFunctionInfoByInstanceKey(instanceKey)
	business, tenant, funcName, version := functionInfo.Business, functionInfo.Tenant,
		functionInfo.FuncName, functionInfo.Version

	return utils.CrName(business, tenant, funcName, version)
}

// getFunctionInfoByInstanceKey /instances/business/yrk/cluster/%s/tenant/%s/function/%s/version/%s
func getFunctionInfoByInstanceKey(key string) utils.FunctionInfo {
	var functionInfo utils.FunctionInfo
	keyFields := strings.Split(key, "/")

	if len(keyFields) != instanceKeySplitSize {
		return functionInfo
	}

	functionInfo.Business = keyFields[instanceBusinessIndex]
	functionInfo.Tenant = keyFields[instanceTenantIndex]
	functionInfo.FuncName = keyFields[instanceFuncNameIndex]
	functionInfo.Version = keyFields[instanceVersionIndex]

	return functionInfo
}

func getFuncHashClusterID(availableClusters []string, functionVersionURN string) string {
	clusterHashMapKey := getClusterHashMapKey(availableClusters)
	value, ok := allClusterHashMap.Load(clusterHashMapKey)
	if !ok {
		availableClusterHashMap := loadbalance.NewConsistentHashWithVirtualNode(replicasNum)
		for _, availableCluster := range availableClusters {
			availableClusterHashMap.Add(&loadbalance.Node{Key: availableCluster})
		}
		allClusterHashMap.Store(clusterHashMapKey, availableClusterHashMap)
		node := availableClusterHashMap.Next(functionVersionURN)
		if node == nil {
			return ""
		}
		return node.Key
	}
	availableClusterHashMap, _ := value.(loadbalance.NoLockLoadBalance)
	node := availableClusterHashMap.Next(functionVersionURN)
	if node == nil {
		return ""
	}
	return node.Key
}

func getClusterHashMapKey(clusters []string) string {
	if len(clusters) == 1 {
		return clusters[0]
	}
	sort.Strings(clusters)
	return strings.Join(clusters, ",")
}
