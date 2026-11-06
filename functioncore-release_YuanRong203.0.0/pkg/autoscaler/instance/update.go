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
	"os"
	"sort"
	"time"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/status"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types/functiontypes"
)

const (
	reBalanceInterval = 120 * time.Second
)

// Function that updates the number of instances when a cluster exception occurs
var specialReBalanceKey = "/" + os.Getenv("CLUSTER_ID") + "/function/instances/special/rebalance"

// UpdateInfo -
func UpdateInfo(funcSvcID string, funcMetadata *functiontypes.FunctionEtcdInfo) {
	if err := update(funcSvcID, funcMetadata); err != nil {
		log.GetLogger().Errorf("failed to update instance info of funcUrn:[%s], error: %s",
			funcSvcID, err.Error())
	}
}

func update(funcSvcID string, funcMetadata *functiontypes.FunctionEtcdInfo) error {
	log.GetLogger().Infof("begin to update instance info of funcUrn:[%s]", funcMetadata.FuncMetaData.VersionURN)
	availableClusters := GetFuncAvailableCluster()
	if len(availableClusters) == 0 {
		return errors.New("there is no availableCluster")
	}
	clusterInstanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
	if err := putInstanceInfoInEtcd(funcMetadata, clusterInstanceInfo); err != nil {
		return err
	}
	// When a cluster is not healthy, record the funcSvcID of the function whose instance configurations are changed.
	// After the cluster is recovered, perform reBalance.
	if len(availableClusters) != len(clustermgr.GetClusterMgr().GetAllCluster()) {
		if err := saveFuncSvcIDWhenClusterUnHealthy(funcSvcID); err != nil {
			log.GetLogger().Errorf("failed to save funcSvcID:[%s] to specialReBalanceKey of redis, error: %s",
				funcSvcID, err.Error())
			return err
		}
	}
	log.GetLogger().Infof("finish to update instance info of funcUrn:[%s]", funcMetadata.FuncMetaData.VersionURN)
	return nil
}

// CheckISNeedUpdate -
func CheckISNeedUpdate(exists bool, oldInfo, newInfo *functiontypes.FunctionEtcdInfo) bool {
	// When the VPC information changes, the available VPC cluster of the function may change and needs to be updated.
	if exists && oldInfo.InstanceMetaData.MaxInstance == newInfo.InstanceMetaData.MaxInstance &&
		oldInfo.InstanceMetaData.MinInstance == newInfo.InstanceMetaData.MinInstance &&
		oldInfo.ExtendedMetaData.FuncVpc.VpcID == newInfo.ExtendedMetaData.FuncVpc.VpcID &&
		oldInfo.ExtendedMetaData.EndpointTenantVpc.VpcID == newInfo.ExtendedMetaData.EndpointTenantVpc.VpcID {
		return false
	}
	return true
}

// GetAveragedInstanceInfo get average instance info of reversed instance
func GetAveragedInstanceInfo(availableClusters []string,
	funcMetadata *functiontypes.FunctionEtcdInfo) map[string][]int64 {
	minInstanceNum := funcMetadata.InstanceMetaData.MinInstance
	maxInstanceNum := GetMaxInstanceCount(funcMetadata)

	instanceInfo := make(map[string][]int64)
	funcAvailableClusters := GetFuncAvailableClustersWithDelay(funcMetadata)
	var notAvailableClusters []string
	if len(funcAvailableClusters) > 0 {
		log.GetLogger().Infof("funcUrn:[%s] funcAvailableClusters: %v, availableClusters: %v",
			funcMetadata.FuncMetaData.VersionURN, funcAvailableClusters, availableClusters)
		notAvailableClusters = differenceArray(funcAvailableClusters, availableClusters)
		availableClusters = IntersectArray(availableClusters, funcAvailableClusters)
	} else {
		// If the available CAE clusters of a function are empty, the maximum number of instances in each cluster
		// of the function is not evenly distributed, and the minimum number of instances is 0.
		for _, clusterID := range availableClusters {
			instanceArray := make([]int64, 2)
			instanceInfo[clusterID] = instanceArray
			instanceInfo[clusterID][0] = maxInstanceNum
			instanceInfo[clusterID][1] = 0
		}
		return instanceInfo
	}
	poolLen := (int64)(len(availableClusters))
	if poolLen <= 0 {
		log.GetLogger().Errorf("funcUrn:[%s] availableClusterPool is empty, poolLen:[%d]",
			funcMetadata.FuncMetaData.VersionURN, poolLen)
		return instanceInfo
	}
	funcHashClusterID := getFuncHashClusterID(availableClusters, funcMetadata.FuncMetaData.VersionURN)
	sort.Strings(availableClusters)
	for _, clusterID := range availableClusters {
		instanceArray := make([]int64, 2)
		instanceInfo[clusterID] = instanceArray
		if funcHashClusterID == clusterID {
			instanceInfo[clusterID][0] = maxInstanceNum/poolLen + maxInstanceNum%poolLen
			instanceInfo[clusterID][1] = minInstanceNum/poolLen + minInstanceNum%poolLen
		} else {
			instanceInfo[clusterID][0] = maxInstanceNum / poolLen
			instanceInfo[clusterID][1] = minInstanceNum / poolLen
		}
	}
	if maxInstanceNum < poolLen && maxInstanceNum > 0 {
		// If the value of maxInstance is less than the number of clusters,
		// allocate instances one by one. For example, if the maximum number of instances is 3 and
		// there are four clusters, the maximum number of instances in three clusters is 1 and
		// the maximum number of instances in the other cluster group is 0.
		resetMaxInstanceNumWhenLessThanPoolLen(instanceInfo, funcHashClusterID, maxInstanceNum)
	}

	if minInstanceNum < minInstanceNumAverageThreshold {
		// When the number of minInstance is less than the threshold, the RIs are not evenly allocated.
		resetMinInstanceNumWhenLessThanThreshold(instanceInfo, funcHashClusterID, minInstanceNum)
	}
	if len(notAvailableClusters) > 0 {
		for _, clusterID := range notAvailableClusters {
			instanceInfo[clusterID] = []int64{0, 0}
		}
	}
	return instanceInfo
}

func resetMaxInstanceNumWhenLessThanPoolLen(instanceInfo map[string][]int64, clusterID string, maxInstanceNum int64) {
	instanceInfo[clusterID][0] = 1
	maxInstanceNum--
	var clusters []string
	for tempClusterID := range instanceInfo {
		clusters = append(clusters, tempClusterID)
	}
	sort.Strings(clusters)
	for _, tempClusterID := range clusters {
		if maxInstanceNum == 0 {
			break
		}
		if tempClusterID == clusterID {
			continue
		}
		instanceInfo[tempClusterID][0] = 1
		maxInstanceNum--
	}
}

func resetMinInstanceNumWhenLessThanThreshold(instanceInfo map[string][]int64, clusterID string, minInstanceNum int64) {
	for tempClusterID := range instanceInfo {
		if tempClusterID == clusterID {
			instanceInfo[tempClusterID][1] = minInstanceNum
			continue
		}
		instanceInfo[tempClusterID][1] = 0
	}
}

func saveFuncSvcIDWhenClusterUnHealthy(funcSvcID string) error {
	if redisclient.GetRedisCmd() == nil {
		log.GetLogger().Error("redis is not ready")
		return errors.New("redis is not ready")
	}
	_, err := redisclient.GetRedisCmd().SAdd(context.TODO(), specialReBalanceKey, funcSvcID).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to SAdd funcSvcID:%s to specialReBalanceKey:%s",
			funcSvcID, specialReBalanceKey)
		return err
	}
	log.GetLogger().Infof("succeeded to save funcSvcID: %s to specialReBalanceKey", funcSvcID)
	return nil
}

// CheckSpecialReBalanceKey -
func CheckSpecialReBalanceKey(stopCh <-chan struct{}) {
	ticker := time.NewTicker(reBalanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			log.GetLogger().Infof("checkSpecialReBalanceKey exit")
			return
		case <-ticker.C:
			if status.Get() == status.FlowOut {
				log.GetLogger().Infof("autoscaler in flowOut status, skip reBalance")
				continue
			}
			if len(clustermgr.GetClusterMgr().GetAllAvailableCluster()) != len(clustermgr.GetClusterMgr().GetAllCluster()) ||
				len(clustermgr.GetClusterMgr().GetAllCluster()) == 0 {
				log.GetLogger().Infof("start to reBalance instanceInfo in unhealthyCluster")
				reBalanceUnhealthyClusterInstanceInfo(clustermgr.GetClusterMgr().GetAllCluster())
				log.GetLogger().Infof("finish to reBalance instanceInfo in unhealthyCluster")
				continue
			}
			reBalanceFuncInstance()
		}
	}
}

func reBalanceUnhealthyClusterInstanceInfo(clusters map[string]clustermgr.Cluster) {
	for _, cluster := range clusters {
		if cluster.State == utils.Healthy {
			continue
		}
		clusterInstanceInfos := getAllInstanceInfoOfCluster(cluster.ClusterID)
		if len(clusterInstanceInfos) == 0 {
			log.GetLogger().Warnf("there is no instanceInfo in cluster %s cluster.ClusterID", cluster.ClusterID)
			continue
		}
		for instanceKey, instanceInfo := range clusterInstanceInfos {
			if instanceInfo.InstanceMetadata.MinInstance == 0 && instanceInfo.InstanceMetadata.MaxInstance == 0 {
				continue
			}
			if err := reBalanceFuncInstanceInfo(instanceKey, cluster.ClusterID); err != nil {
				log.GetLogger().Errorf("failed to reBalanceFuncInstanceInfo of instanceKey: %s, err: %s ",
					instanceKey, err.Error())
			}
		}
	}
}

func reBalanceFuncInstanceInfo(instanceKey string, unHealthyClusterID string) error {
	log.GetLogger().Infof("[reBalanceFuncInstanceInfo] start to reBalance instanceKey:%s "+
		"with unHealthyClusterID %s", instanceKey, unHealthyClusterID)
	funcSvcID := CrNameByInstanceKey(instanceKey)
	asCache := cache.GetInstance()
	funcMetadata, exists := asCache.GetFunctionMeta(funcSvcID)
	if !exists {
		log.GetLogger().Warnf("[reBalanceFuncInstanceInfo] the funcMetadata of instanceKey:%s "+
			"is not exists in cache", instanceKey)
		return nil
	}
	if funcMetadata.InstanceMetaData.MinInstance == 0 && funcMetadata.InstanceMetaData.MaxInstance == 0 {
		log.GetLogger().Warnf("[reBalanceFuncInstanceInfo] no need to reBalance, "+
			"the minInstance and maxInstance of instanceKey %s is 0", instanceKey)
		return nil
	}
	availableClusters := GetFuncAvailableCluster()
	if checkIsRecover(availableClusters, unHealthyClusterID) {
		log.GetLogger().Warnf("[reBalanceFuncInstanceInfo] the unhealthy cluster:%s "+
			"already recover to healthy", unHealthyClusterID)
		return nil
	}
	if len(availableClusters) == 0 {
		log.GetLogger().Warnf("[reBalanceFuncInstanceInfo] there is no available cluster"+
			", so not do reBalance, unHealth cluster is %s, instance key is %s", unHealthyClusterID, instanceKey)
		return nil
	}
	clusterInstanceInfo := GetAveragedInstanceInfo(availableClusters, funcMetadata)
	clusterInstanceInfo[unHealthyClusterID] = []int64{0, 0}
	if err := putInstanceInfoInEtcd(funcMetadata, clusterInstanceInfo); err != nil {
		return err
	}
	if err := saveFuncSvcIDWhenClusterUnHealthy(funcSvcID); err != nil {
		log.GetLogger().Errorf("[reBalanceFuncInstanceInfo] failed to save funcSvcID:[%s] to "+
			"specialReBalanceKey of redis, error: %s", funcSvcID, err.Error())
		return err
	}
	log.GetLogger().Infof("[reBalanceFuncInstanceInfo] finish to reBalance instanceKey:%s "+
		"with unHealthyClusterID %s", instanceKey, unHealthyClusterID)
	return nil
}

func checkIsRecover(availableCluster []string, unHealthyClusterID string) bool {
	for _, clusterID := range availableCluster {
		if clusterID == unHealthyClusterID {
			return true
		}
	}
	return false
}

func reBalanceFuncInstance() {
	funcSvcIDList, err := getReBalanceFuncSvcIDArrayFromRedis()
	if err != nil {
		log.GetLogger().Errorf("failed to getReBalanceFuncSvcIDArrayFromRedis, err:%s ", err.Error())
		return
	}
	if len(funcSvcIDList) == 0 {
		return
	}
	log.GetLogger().Infof("begin to reBalance function instance, len(funcSvcIDList):[%d]", len(funcSvcIDList))
	succeedCount := 0
	asCache := cache.GetInstance()
	for _, funcSvcID := range funcSvcIDList {
		functionMeta, exists := asCache.GetFunctionMeta(funcSvcID)
		if exists {
			if err := update(funcSvcID, functionMeta); err != nil {
				log.GetLogger().Warnf("failed to reBalance instance info of function:%s, err:%s",
					functionMeta.FuncMetaData.VersionURN, err.Error())
				continue
			}
		}
		if err := deleteFuncSvcIDInReBalanceKey(funcSvcID); err != nil {
			log.GetLogger().Errorf("failed to remove funcSvcID:%s in specialReBalanceKey, err:%s", err.Error())
		}
		succeedCount++
	}
	log.GetLogger().Infof("finished to reBalance function instance, succeedCount:[%d]", succeedCount)
}

func getReBalanceFuncSvcIDArrayFromRedis() ([]string, error) {
	if redisclient.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready")
		return nil, errors.New("redis is not ready")
	}
	funcSvcIDArray, err := redisclient.GetRedisCmd().SMembers(context.TODO(), specialReBalanceKey).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to get instances reBalance info from redis")
		return nil, err
	}
	if len(funcSvcIDArray) == 0 {
		log.GetLogger().Warnf("there is no instances reBalance info from redis")
		return nil, nil
	}
	return funcSvcIDArray, nil
}

func deleteFuncSvcIDInReBalanceKey(funcSvcID string) error {
	if redisclient.GetRedisCmd() == nil {
		log.GetLogger().Errorf("redis is not ready")
		return errors.New("redis is not ready")
	}
	_, err := redisclient.GetRedisCmd().SRem(context.TODO(), specialReBalanceKey, funcSvcID).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to delete funcSvcID:%s in specialReBalanceKey of redis, err:%s", err.Error())
		return err
	}
	return nil
}

// DeleteInfo -
func DeleteInfo(funcMetadata *functiontypes.FunctionEtcdInfo) {
	clusters := clustermgr.GetClusterMgr().GetAllCluster()
	var clusterIDArray []string
	for clusterID := range clusters {
		clusterIDArray = append(clusterIDArray, clusterID)
	}
	if err := deleteInstanceInfoInEtcd(funcMetadata, clusterIDArray); err != nil {
		log.GetLogger().Errorf("failed to delete instanceInfo in etcd, funcUrn:[%s]",
			funcMetadata.FuncMetaData.FunctionURN)
	}
}
