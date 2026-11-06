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

// Package clusterstatusutil sync cluster status
package clusterstatusutil

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

var (
	clusterID = os.Getenv("CLUSTER_ID")
)

const (
	// ClusterFlowOutingStatus -
	ClusterFlowOutingStatus = "flowouting"

	// ClusterFlowOutFinishStatus -
	ClusterFlowOutFinishStatus = "flowoutFinish"

	// ClusterRunningStatus -
	ClusterRunningStatus = "running"

	// ClusterFlowIningStatus -
	ClusterFlowIningStatus = "flowining"
	// ClusterFlowInFinishStatus -
	ClusterFlowInFinishStatus = "flowinFinish"
	// ClusterGrayFlowInStatus -
	ClusterGrayFlowInStatus = "grayFlowin"

	// MaxEtcdRetryAttempts define the max etcd operation times
	MaxEtcdRetryAttempts = 3

	// ClusterMetadataKey cluster status info
	ClusterMetadataKey = "fss_cluster_flow_state"
)

// ClusterFlowState defines cluster flow state
type ClusterFlowState struct {
	// State running\flow...
	State string `json:"state" valid:",optional"`
	// StateTime record time
	StateTime int64 `json:"state_time" valid:",optional"`
}

// ClusterStatus defines cluster status
type ClusterStatus struct {
	// ClusterStatusMap cluster001:ClusterFlowState
	ClusterStatusMap map[string]ClusterFlowState
	LastStatusMap    map[string]ClusterFlowState
	// DefaultClusterAmount -
	DefaultClusterAmount int
	sync.Mutex
}

// ClusterStatusCache -
var ClusterStatusCache ClusterStatus

// UpdateClusterStatusMap -
func (ct *ClusterStatus) UpdateClusterStatusMap(clusterStatusMap map[string]ClusterFlowState) {
	ct.Lock()
	ct.LastStatusMap = ct.ClusterStatusMap
	ct.ClusterStatusMap = clusterStatusMap
	ct.Unlock()
}

// InitClusterStatusCache -
func InitClusterStatusCache() {
	var clusterFlow map[string]ClusterFlowState
	for i := 0; i < MaxEtcdRetryAttempts; i++ {
		if etcd3.GetMetaEtcdClient().Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			time.Sleep(time.Second)
			continue
		}
		defaultETCDCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		resp, err := etcd3.GetMetaEtcdClient().GetResponse(defaultETCDCtx, ClusterMetadataKey, clientv3.WithPrefix())
		if err != nil {
			log.GetLogger().Errorf("list path(%s) error: %s", ClusterMetadataKey, err.Error())
			return
		}
		for _, kv := range resp.Kvs {
			if err := json.Unmarshal(kv.Value, &clusterFlow); err != nil {
				log.GetLogger().Errorf("failed to %s unmarshal the json, error: %s", kv.Value, err.Error())
				return
			}
		}
		ClusterStatusCache.ClusterStatusMap = clusterFlow
		ClusterStatusCache.DefaultClusterAmount = len(clusterFlow)
		log.GetLogger().Infof("success to init cluster status cache,with default amount: %d",
			ClusterStatusCache.DefaultClusterAmount)
		return
	}
}

// IsCurrentClustersStatusRunning -
func IsCurrentClustersStatusRunning() bool {
	ClusterStatusCache.Lock()
	defer ClusterStatusCache.Unlock()
	currentCluster := ClusterStatusCache.ClusterStatusMap[clusterID]
	if currentCluster.State == ClusterRunningStatus || currentCluster.State == "" {
		return true
	}
	log.GetLogger().Warnf("cluster: %s current status is: %s, not running", clusterID,
		currentCluster.State)
	return false
}

// IsCurrentClustersStatusFlowouting -
func IsCurrentClustersStatusFlowouting() bool {
	ClusterStatusCache.Lock()
	defer ClusterStatusCache.Unlock()
	currentCluster := ClusterStatusCache.ClusterStatusMap[clusterID]
	return currentCluster.State == ClusterFlowOutingStatus
}

// GetClusterCount get total cluster count
func GetClusterCount() int {
	ClusterStatusCache.Lock()
	defer ClusterStatusCache.Unlock()
	return len(ClusterStatusCache.ClusterStatusMap)
}

func isFlowInState(state string) bool {
	switch state {
	case ClusterFlowIningStatus, ClusterFlowInFinishStatus, ClusterGrayFlowInStatus:
		return true
	default:
		return false
	}
}

// GetOtherFlowiningCluster check if other cluster is changed from flowin to running
func GetOtherFlowiningCluster() string {
	ClusterStatusCache.Lock()
	defer ClusterStatusCache.Unlock()
	if ClusterStatusCache.LastStatusMap == nil {
		return ""
	}
	for k, v := range ClusterStatusCache.ClusterStatusMap {
		if k == clusterID {
			continue
		}
		lastStatus, ok := ClusterStatusCache.LastStatusMap[k]
		if !ok {
			continue
		}
		if v.State == ClusterRunningStatus && isFlowInState(lastStatus.State) {
			return k
		}
	}
	return ""
}

// SomeClusterIsFlowing judge if any cluster is flowing
func SomeClusterIsFlowing() bool {
	ClusterStatusCache.Lock()
	defer ClusterStatusCache.Unlock()
	if ClusterStatusCache.ClusterStatusMap == nil {
		return false
	}
	for _, v := range ClusterStatusCache.ClusterStatusMap {
		if v.State != ClusterRunningStatus {
			return true
		}
	}
	return false
}

// GetCurrentClusterID return current culsterID
func GetCurrentClusterID() string {
	return clusterID
}
