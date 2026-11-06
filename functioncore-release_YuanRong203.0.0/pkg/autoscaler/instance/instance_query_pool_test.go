/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/workermanagerutils"
	mockUtils "functioncore/test/utils"
)

func TestInstanceQueryPool(t *testing.T) {
	patches := mockUtils.InitPatchSlice()
	patches.Append(mockUtils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Range",
			func(_ *sync.Map, f func(key, value any) bool) {
				for i := 0; i < 10; i++ {
					f("funcSvc"+strconv.Itoa(i), "a")
				}
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&clustermgr.ClusterManager{}), "GetAllAvailableCluster",
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{
					"cluster1": {
						ClusterID: "111",
						ActiveNodes: clustermgr.NodeStatus{
							IP: "1.0.0.0",
						},
					},
					"cluster2": {
						ClusterID: "222",
						ActiveNodes: clustermgr.NodeStatus{
							IP: "2.0.0.0",
						},
					},
				}
			}),

		gomonkey.ApplyFunc(workermanagerutils.QueryInstancesFromWmByIP, func(funcSvc string, nodeIP string,
			enableHTTPS bool, auth *localauth.AuthConfig) (types.QueryInstanceResponse, error) {
			return types.QueryInstanceResponse{
				Code:    200,
				Message: "success",
				Instances: types.QueryInstance{
					ClusterID:      "cluster1",
					PodsName:       []string{"podA", "podB"},
					ScalingNum:     1,
					ExpectNum:      2,
					ExpectedMinIns: 3,
					ExpectedSnap:   4,
					BridgePodsInfo: nil,
				},
			}, nil
		}),
	})

	defer patches.ResetAll()

	createQueryPool().WithPoolLen(5).StartWorking()

	instanceCache, ok := cache.GetInstance().FunctionInstanceCache.Load("funcSvc0")

	if !ok {
		t.Errorf("failed to TestInstanceQueryPool")
	}

	fci, ok := instanceCache.(*config.InstanceCache)
	if !ok {
		t.Errorf("failed to TestInstanceQueryPool when assert")
	}

	if fci.TotalCurrentCount != 4 || fci.ScalingNum != 2 || fci.ExpectedMinIns != 6 || fci.ExpectedSnapshot != 8 ||
		len(fci.ClusterInsCache) != 2 {
		t.Errorf("failed to TestInstanceQueryPool when get result, fci is %+v", fci)
	}

}
