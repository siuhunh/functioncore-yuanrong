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

// Package cache -
package cache

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/functiontypes"
	mockutils "functioncore/test/utils"
)

func TestGetInstance(t *testing.T) {
	tests := []struct {
		name string
		want *AutoScalerCache
	}{
		{
			"test get instance",
			&AutoScalerCache{
				FunctionMetaCache:         sync.Map{},
				InstanceMetaCache:         sync.Map{},
				PredictMetricsOutputCache: make(resourceusage.PredictResourceListMap),
				PredictMetricsInputCache:  make(resourceusage.PredictResourceListMap),
				ResourceMetricCache: make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]resourceusage.ResourceList),
				PredictMetricsOutputCacheMutex: sync.RWMutex{},
				PredictMetricsInputCacheMutex:  sync.RWMutex{},
				ResourceMetricCacheMutex:       sync.RWMutex{},
				FunctionInstanceCache:          sync.Map{},
				// cae metrics cache
				CaeMetricsCache: make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]caemetrics.List),
				CaeMetricsCacheMutex: sync.RWMutex{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetInstance(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetInstance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAutoScalerCache_RefreshAndGetResourceMetricCache(t *testing.T) {
	resourceMetricCache := map[resourceusage.ClusterID]map[resourceusage.
		FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList{
		"cluster001": {
			"funcA": {
				"podA": {},
				"podB": {},
			},
			"funcB": {
				"podF": {},
			},
		},
		"cluster002": {
			"funcB": {
				"podC": {},
				"podD": {},
			},
		},
	}

	autoScalerCache := &AutoScalerCache{
		ResourceMetricCache: map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
			PodName]resourceusage.ResourceList{
			"cluster002": {
				"funcB": {
					"podC": {},
					"podD": {},
				},
			},
		},
		ResourceMetricCacheMutex:      sync.RWMutex{},
		PredictMetricsInputCache:      make(resourceusage.PredictResourceListMap),
		PredictMetricsInputCacheMutex: sync.RWMutex{},
	}

	resource := autoScalerCache.GetResourceMetricCache("funcA")
	if len(autoScalerCache.ResourceMetricCache["cluster002"]) != 1 || len(resource) != 0 {
		t.Errorf("failed to test resource metricCache, cache: %+v", resourceMetricCache)
	}

	autoScalerCache.RefreshResourceMetricCache(resourceMetricCache)
	if len(autoScalerCache.ResourceMetricCache) != 2 || len(autoScalerCache.ResourceMetricCache["cluster001"]) != 2 {
		t.Errorf("failed to test resource metricCache, cache: %+v", resourceMetricCache)
	}

	resource = autoScalerCache.GetResourceMetricCache("funcB")
	if len(resource) != 2 || len(resource["cluster001"]) != 1 {
		t.Errorf("failed to test resource metricCache, cache: %+v", resourceMetricCache)
	}

}

func TestAutoScalerCache_RemoveResourceMetricCacheByFunc(t *testing.T) {
	type fields struct {
		FunctionMetaCache   sync.Map
		InstanceMetaCache   sync.Map
		ResourceMetricCache map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]resourceusage.ResourceList
		ResourceMetricCacheMutex sync.RWMutex
		FunctionInstanceCache    sync.Map
	}
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"case test to RemoveResourceMetricCacheByFunc",
			fields{
				ResourceMetricCache: map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]resourceusage.ResourceList{
					"cluster002": {
						"funcB": {
							"podC": {},
							"podD": {},
						},
					},
				},
				ResourceMetricCacheMutex: sync.RWMutex{},
			},
			args{"funcB"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AutoScalerCache{
				FunctionMetaCache:        tt.fields.FunctionMetaCache,
				InstanceMetaCache:        tt.fields.InstanceMetaCache,
				ResourceMetricCache:      tt.fields.ResourceMetricCache,
				ResourceMetricCacheMutex: tt.fields.ResourceMetricCacheMutex,
				FunctionInstanceCache:    tt.fields.FunctionInstanceCache,
			}
			a.RemoveResourceMetricCacheByFunc("funcB")
		})
	}
}

func TestAutoScalerCache_GetFunctionMeta_CheckFunctionMetaExist(t *testing.T) {
	type fields struct {
		FunctionMetaCache   sync.Map
		InstanceMetaCache   sync.Map
		ResourceMetricCache map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]resourceusage.ResourceList
		ResourceMetricCacheMutex sync.RWMutex
		FunctionInstanceCache    sync.Map
	}
	testFunction := &functiontypes.FunctionEtcdInfo{}
	testFunction2 := &functiontypes.FunctionEtcdInfo{}
	testFunctionMetaCache := sync.Map{}
	testFunctionMetaCache.Store("functionSvcID", testFunction)
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *functiontypes.FunctionEtcdInfo
		want1  bool
	}{
		{
			"case test to GetFunctionMeta",
			fields{
				FunctionMetaCache: testFunctionMetaCache,
			},
			args{
				"functionSvcID",
			},
			testFunction,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AutoScalerCache{
				FunctionMetaCache:        tt.fields.FunctionMetaCache,
				InstanceMetaCache:        tt.fields.InstanceMetaCache,
				ResourceMetricCache:      tt.fields.ResourceMetricCache,
				ResourceMetricCacheMutex: tt.fields.ResourceMetricCacheMutex,
				FunctionInstanceCache:    tt.fields.FunctionInstanceCache,
			}
			got, got1 := a.GetFunctionMeta(tt.args.funcSvcID)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionMeta() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("GetFunctionMeta() got1 = %v, want %v", got1, tt.want1)
			}

			got1 = a.CheckFunctionMetaExist(tt.args.funcSvcID)
			if got1 != tt.want1 {
				t.Errorf("CheckFunctionMetaExist() got1 = %v, want %v", got1, tt.want1)
			}

			a.UpdateFunctionMeta(tt.args.funcSvcID, testFunction2)
			a.RemoveFunctionMeta(tt.args.funcSvcID)

			got1 = a.CheckFunctionMetaExist(tt.args.funcSvcID)
			if got1 != false {
				t.Errorf("CheckFunctionMetaExist() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestAutoScalerCache_UpdateInstanceMetaCache(t *testing.T) {
	type fields struct {
		FunctionMetaCache   sync.Map
		InstanceMetaCache   sync.Map
		ResourceMetricCache map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]resourceusage.ResourceList
		ResourceMetricCacheMutex sync.RWMutex
		FunctionInstanceCache    sync.Map
	}
	type args struct {
		funcSvcID   string
		minInstance int64
		maxInstance int64
	}
	testInstanceMeta := &functiontypes.InstanceMetaData{
		MaxInstance: 10,
		MinInstance: 5,
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *functiontypes.InstanceMetaData
	}{
		{
			"case test UpdateInstanceMetaCache",
			fields{
				InstanceMetaCache: sync.Map{},
			},
			args{
				"testFunctionID",
				5,
				10,
			},
			testInstanceMeta,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AutoScalerCache{
				FunctionMetaCache:        tt.fields.FunctionMetaCache,
				InstanceMetaCache:        tt.fields.InstanceMetaCache,
				ResourceMetricCache:      tt.fields.ResourceMetricCache,
				ResourceMetricCacheMutex: tt.fields.ResourceMetricCacheMutex,
				FunctionInstanceCache:    tt.fields.FunctionInstanceCache,
			}
			a.UpdateInstanceMetaCache(tt.args.funcSvcID, tt.args.minInstance, tt.args.maxInstance)
			got := a.GetInstanceMetaCache(tt.args.funcSvcID)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetInstanceMetaCache() got = %v, want %v", got, tt.want)
			}

			a.RemoveInstanceMetaCache(tt.args.funcSvcID)
			got = a.GetInstanceMetaCache(tt.args.funcSvcID)
			if got != nil {
				t.Errorf("GetInstanceMetaCache() got = %v, want nil", got)
			}
		})
	}
}

func TestAutoScalerCache_UpdateFunctionInstanceCache(t *testing.T) {
	type fields struct {
		FunctionMetaCache   sync.Map
		InstanceMetaCache   sync.Map
		ResourceMetricCache map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
					PodName]resourceusage.ResourceList
		ResourceMetricCacheMutex sync.RWMutex
		FunctionInstanceCache    sync.Map
	}
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"UpdateFunctionInstanceCache",
			fields{
				FunctionInstanceCache: sync.Map{},
			},
			args{
				"testFunctionSvcID",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AutoScalerCache{
				FunctionMetaCache:        tt.fields.FunctionMetaCache,
				InstanceMetaCache:        tt.fields.InstanceMetaCache,
				ResourceMetricCache:      tt.fields.ResourceMetricCache,
				ResourceMetricCacheMutex: tt.fields.ResourceMetricCacheMutex,
				FunctionInstanceCache:    tt.fields.FunctionInstanceCache,
			}
			a.UpdateFunctionInstanceCache(tt.args.funcSvcID)
			a.UpdateFunctionInstanceCache(tt.args.funcSvcID)
			err := a.IncFuncInstanceCurrentCount(tt.args.funcSvcID)
			if err != nil {
				t.Errorf("IncFuncInstanceCurrentCount() err = %v, want nil", err)
			}
			got, _ := a.GetFuncInstanceCurrentCount(tt.args.funcSvcID)
			if got != 2 {
				t.Errorf("GetFuncInstanceCurrentCount() got = %v, want %v", got, 2)
			}
			err = a.DecFuncInstanceCurrentCount(tt.args.funcSvcID)
			if err != nil {
				t.Errorf("DecFuncInstanceCurrentCount() err = %v, want nil", err)
			}
			got, _ = a.GetFuncInstanceCurrentCount(tt.args.funcSvcID)
			if got != 1 {
				t.Errorf("GetFuncInstanceCurrentCount() got = %v, want %v", got, 1)
			}
			insCache := &config.InstanceCache{
				TotalCurrentCount: 3,
				ClusterInsCache: []config.ClusterInsCache{
					{
						"cluster001",
						3,
						0,
					},
				},
			}
			a.FunctionInstanceCache.Store(tt.args.funcSvcID, insCache)
			got = a.GetFuncInstanceCurrentCountInCluster(tt.args.funcSvcID, "cluster001")
			if got != 3 {
				t.Errorf("GetFuncInstanceCurrentCount() got = %v, want 3", got)
			}

			got2 := a.GetClusterCounts()
			if len(got2) != 1 {
				t.Errorf("GetClusterCounts() got = %v", got2)
			}
			a.RemoveFunctionInstanceCache(tt.args.funcSvcID)
			got = a.GetFuncInstanceCurrentCountInCluster(tt.args.funcSvcID, "cluster001")
			if got != 0 {
				t.Errorf("GetFuncInstanceCurrentCount() got = %v, want 0", got)
			}

			got2 = a.GetClusterCounts()
			if len(got2) != 0 {
				t.Errorf("GetClusterCounts() got = %v", got2)
			}
		})
	}
}

func TestInitAutoScalerCache(t *testing.T) {
	functionEtcd := &functiontypes.FunctionEtcdInfo{
		FuncMetaData: functiontypes.FuncMetaData{
			BusinessType: utils.BusinessTypeCAE,
		},
	}
	data, _ := json.Marshal(functionEtcd)
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case1 test InitAutoScaler Cache",
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetFunctionListFromEtcd, func() (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("Key"), Value: data}}}, nil
					}),
				})
				return patches
			},
		},
		{
			"case2 test InitAutoScalerCache",
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetFunctionListFromEtcd, func() (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{}, errors.New("error")
					}),
				})
				return patches
			},
		},
		{
			"case3 test InitAutoScalerCache",
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetFunctionListFromEtcd, func() (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := InitAutoScalerCache(); (err != nil) != tt.wantErr {
				t.Errorf("InitAutoScalerCache() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestAutoScalerCache_GetFuncInstanceCurrentCountEveryCluster(t *testing.T) {
	a := &AutoScalerCache{}
	a.FunctionInstanceCache.Store("func1", &config.InstanceCache{ClusterInsCache: []config.ClusterInsCache{
		{ClusterID: "cluster1", Count: 1},
		{ClusterID: "cluster2", Count: 2},
	}})
	resMap := a.GetFuncInstanceCurrentCountEveryCluster("func1")
	if resMap["cluster1"] != 1 || resMap["cluster2"] != 2 {
		t.Errorf("failed to TestAutoScalerCache_GetFuncInstanceCurrentCountEveryCluster")
	}

	a = &AutoScalerCache{}
	a.FunctionInstanceCache.Store("func1", &config.InstanceCache{ClusterInsCache: []config.ClusterInsCache{
		{ClusterID: "cluster1", Count: 1},
	}})
	resMap = a.GetFuncInstanceCurrentCountEveryCluster("func2")
	if len(resMap) != 0 {
		t.Errorf("failed to TestAutoScalerCache_GetFuncInstanceCurrentCountEveryCluster")
	}

	a = &AutoScalerCache{}
	a.FunctionInstanceCache.Store("func1", "123")
	resMap = a.GetFuncInstanceCurrentCountEveryCluster("func1")
	if len(resMap) != 0 {
		t.Errorf("failed to TestAutoScalerCache_GetFuncInstanceCurrentCountEveryCluster")
	}

}

func Test_updateFunctionCache(t *testing.T) {
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name    string
		args    args
		patches mockutils.PatchesFunc
		wantErr bool
	}{
		{
			"test unmarshal error",
			args{
				key:   "test1",
				value: "{asdada'qwdqwd}",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			true,
		},
		{
			"test not business cae type",
			args{
				key:   "test1",
				value: "{\"asd\":\"asd\"}",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			false,
		},
		{
			"test business type is cae type and not err",
			args{
				key:   "test1",
				value: "{\"funcMetaData\":{\"businessType\":\"CAE\"}}",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			false,
		},
		{
			"test business type is cae type and err",
			args{
				key:   "test1",
				value: "{\"funcMetaData\":{\"businessType\":\"CAE\"}}",
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&AutoScalerCache{}),
						"IncFuncInstanceCurrentCount",
						func(_ *AutoScalerCache, funcSvcID string) error {
							return errors.New("failed to inc count")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		patches := tt.patches()
		err := updateFunctionCache(tt.args.key, tt.args.value)
		if (err != nil) != tt.wantErr {
			t.Errorf("failed to test %s", tt.name)
		}
		patches.ResetAll()
	}
}

func TestAutoScalerCache_GetPredictMetricsOutputCache(t *testing.T) {
	convey.Convey("TestAutoScalerCache_GetPredictMetricsOutputCache", t, func() {
		instance := GetInstance()
		now := time.Now()
		list := resourceusage.PredictResourceList{{
			CPUUtil:    1,
			MemoryUtil: 1,
			Timestamp:  now.Unix(),
		}}
		instance.PredictMetricsOutputCache["test_1"] = list
		cache := instance.GetPredictMetricsOutputCache("test_1")
		convey.So(cache, convey.ShouldResemble, resourceusage.PredictResourceList{{
			CPUUtil:    1,
			MemoryUtil: 1,
			Timestamp:  now.Unix(),
		}})

		instance.PredictMetricsInputCache["test_1"] = list
		instance.DeletePredictMetricsInputCache("test_1")
		inputCache := instance.GetPredictMetricsInputCache("test_1")
		convey.So(len(inputCache), convey.ShouldEqual, 0)
	})
}

func TestGetFuncScalingInstanceCountInCluster(t *testing.T) {
	// Mock test data
	testFuncSvcID := "test-func"
	testClusterID1 := "cluster-1"
	testClusterID2 := "cluster-2"
	nonExistClusterID := "cluster-not-exist"

	// Test case 1: Function not exist in cache
	t.Run("Function not found in cache", func(t *testing.T) {
		a := AutoScalerCache{}
		count := a.GetFuncScalingInstanceCountInCluster(testFuncSvcID, testClusterID1)
		assert.Equal(t, int64(0), count, "Should return 0 when function not exist")
	})

	// Test case 2: Type assertion failed
	t.Run("Cache type mismatch", func(t *testing.T) {
		a := AutoScalerCache{}
		a.FunctionInstanceCache.Store(testFuncSvcID, "invalid-type") // Store wrong type

		count := a.GetFuncScalingInstanceCountInCluster(testFuncSvcID, testClusterID1)
		assert.Equal(t, int64(0), count, "Should return 0 when type assertion failed")
	})

	// Test case 3: Cluster data exists and match
	t.Run("Cluster data exists and matched", func(t *testing.T) {
		a := AutoScalerCache{}
		instanceCache := &config.InstanceCache{
			ClusterInsCache: []config.ClusterInsCache{
				{
					ClusterID:  testClusterID1,
					ScalingNum: 5,
				},
				{
					ClusterID:  testClusterID2,
					ScalingNum: 10,
				},
			},
		}
		a.FunctionInstanceCache.Store(testFuncSvcID, instanceCache)

		count := a.GetFuncScalingInstanceCountInCluster(testFuncSvcID, testClusterID1)
		assert.Equal(t, int64(5), count, "Should return cluster1 instance count 5")

		count = a.GetFuncScalingInstanceCountInCluster(testFuncSvcID, testClusterID2)
		assert.Equal(t, int64(10), count, "Should return cluster2 instance count 10")
	})

	// Test case 4: Cluster data exists but not match
	t.Run("Cluster data exists but not matched", func(t *testing.T) {
		a := AutoScalerCache{}
		instanceCache := &config.InstanceCache{
			ClusterInsCache: []config.ClusterInsCache{
				{
					ClusterID:  testClusterID1,
					ScalingNum: 5,
				},
			},
		}
		a.FunctionInstanceCache.Store(testFuncSvcID, instanceCache)

		count := a.GetFuncScalingInstanceCountInCluster(testFuncSvcID, nonExistClusterID)
		assert.Equal(t, int64(0), count, "Should return 0 when cluster not matched")
	})

	// Test case 5: Empty cluster data
	t.Run("Empty cluster data", func(t *testing.T) {
		a := AutoScalerCache{}
		instanceCache := &config.InstanceCache{
			ClusterInsCache: []config.ClusterInsCache{},
		}
		a.FunctionInstanceCache.Store(testFuncSvcID, instanceCache)

		count := a.GetFuncScalingInstanceCountInCluster(testFuncSvcID, testClusterID1)
		assert.Equal(t, int64(0), count, "Should return 0 for empty cluster data")
	})
}

func TestAutoScalerCache_RefreshCaeMetricCache(t *testing.T) {
	a := AutoScalerCache{
		CaeMetricsCacheMutex: sync.RWMutex{},
		CaeMetricsCache: make(map[resourceusage.ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.
			PodName]caemetrics.List),
	}

	a.RefreshCaeMetricCache(map[resourceusage.ClusterID]map[resourceusage.
		FuncSvcID]map[resourceusage.PodName]caemetrics.List{
		"cluster001": {
			"funcSvc1": {
				"podA": {},
			},
		},
	})

	caeMetricCacheData := a.GetCaeMetricsCache("funcSvc1")
	if len(caeMetricCacheData) == 0 {
		t.Errorf("faild to get cae metrics cache")
	}
	a.RemoveCaeMetricCacheByFunc("funcSvc1")
	if len(a.CaeMetricsCache["cluster001"]) != 0 {
		t.Errorf("faild to get remove metrics cache")
	}
}
