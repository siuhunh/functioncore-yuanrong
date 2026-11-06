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

// Package resourceusage deal the resource usage
package resourceusage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/caemetrics"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/resourceusage"
	"functioncore/pkg/common/types/functiontypes"
	"functioncore/test/utils"
)

func TestGetHandler(t *testing.T) {
	tests := []struct {
		name string
		want *Handler
	}{
		{
			"test get handler",
			&Handler{tickerTime: defaultTickerTime},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetHandler(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandler_RegisterCheckFunctionMetaExist(t *testing.T) {
	h := &Handler{
		checkFunctionMetaExist: func(funcSvcID string) bool {
			return false
		},
	}
	h.RegisterCheckFunctionMetaExist(func(funcSvcID string) bool {
		return true
	})
	if h.checkFunctionMetaExist("") != true {
		t.Errorf("RegisterCheckFunctionMetaExist() = %v, want %v", false, true)
	}
}

func TestHandler_RegisterRefreshCacheFunc(t *testing.T) {
	h := &Handler{}
	h.RegisterRefreshCacheFunc(func(resourceMetricCache map[resourceusage.
		ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList) {
		return
	})
	if h.refreshCacheFunc == nil {
		t.Errorf("RegisterCheckFunctionMetaExist() = %v, wantNil %v", true, false)
	}
}

func TestHandler_WithClusters(t *testing.T) {
	h := &Handler{}
	h.WithClusters([]string{"a", "b"})
	if len(h.clusters) != 2 {
		t.Errorf("WithClusters error")
	}
}

func TestHandler_WithMonitoringClient(t *testing.T) {
	h := &Handler{}
	h.WithMonitoringClient(&redisclient.Client{})
	if h.monitoringClient == nil {
		t.Errorf("WithClusters error")
	}
}

func TestHandler_WithStopCh(t *testing.T) {
	h := &Handler{}
	ch := make(chan struct{})
	h.WithStopCh(ch)
	if h.stopCh == nil {
		t.Errorf("WithStopCh error")
	}
}

func TestHandler_WithTickerTime(t *testing.T) {
	h := &Handler{}
	h.WithTickerTime(100)
	if h.tickerTime != 100 {
		t.Errorf("WithTickerTime error")
	}
}

func TestHandler_changeUploadResourceData(t *testing.T) {
	h := &Handler{
		monitoringClient: nil,
		checkFunctionMetaExist: func(funcSvcID string) bool {
			return true
		},
	}
	uploadResourceDataList := map[resourceusage.NodeName]UploadResourceData{}
	result := h.changeUploadResourceData("cluster001", uploadResourceDataList)
	if len(result) != 0 {
		t.Errorf("first changeUploadResourceData err : %+v", result)
	}

	uploadResourceDataList = map[resourceusage.NodeName]UploadResourceData{
		"node1": {
			Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
				"111/funcA/latest": {
					"podA": resourceusage.ResourceList{
						{
							CPUUsage: 100,
						}, {
							CPUUsage: 120,
						},
					},
				},
			},
			UploadTime: time.Now().Unix(),
		},
	}
	result = h.changeUploadResourceData("cluster001", uploadResourceDataList)
	if len(result) != 1 || len(result["yrk111-funcA-latest-4022077341"]) != 1 ||
		len(result["yrk111-funcA-latest-4022077341"]["podA"]) != 2 {
		t.Errorf("second changeUploadResourceData err : %+v", result)
	}

	uploadResourceDataList = map[resourceusage.NodeName]UploadResourceData{
		"node1": {
			Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
				"111/funcA/latest": {
					"podA": resourceusage.ResourceList{
						{
							CPUUsage: 100,
						}, {
							CPUUsage: 120,
						},
					},
				},
			},
			UploadTime: time.Now().Unix(),
		},
		"node2": {
			Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
				"111/funcA/latest": {
					"podB": resourceusage.ResourceList{
						{
							CPUUsage: 100,
						}, {
							CPUUsage: 120,
						},
					},
				},
			},
			UploadTime: time.Now().Unix(),
		},
	}

	result = h.changeUploadResourceData("cluster001", uploadResourceDataList)
	if len(result["yrk111-funcA-latest-4022077341"]) != 2 {
		t.Errorf("sixth changeUploadResourceData err : %+v", result)
	}

	uploadResourceDataList = map[resourceusage.NodeName]UploadResourceData{
		"node1": {
			Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
				"111/funcA/latest": {
					"podA": resourceusage.ResourceList{
						{
							CPUUsage: 100,
						}, {
							CPUUsage: 120,
						},
					},
				},
			},
			UploadTime: time.Now().Unix() - 601,
		},
	}

	result = h.changeUploadResourceData("cluster001", uploadResourceDataList)
	if len(result) != 0 {
		t.Errorf("third changeUploadResourceData err : %+v", result)
	}

	h.checkFunctionMetaExist = func(funcSvcID string) bool {
		return false
	}

	uploadResourceDataList = map[resourceusage.NodeName]UploadResourceData{
		"node1": {
			Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
				"111/funcA/latest": {
					"podA": resourceusage.ResourceList{
						{
							CPUUsage: 100,
						}, {
							CPUUsage: 120,
						},
					},
				},
			},
			UploadTime: time.Now().Unix(),
		},
	}

	result = h.changeUploadResourceData("cluster001", uploadResourceDataList)
	if len(result) != 0 {
		t.Errorf("fourth changeUploadResourceData err : %+v", result)
	}

	uploadResourceDataList = map[resourceusage.NodeName]UploadResourceData{
		"node1": {
			Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
				"111/funcA/latest": {
					"podA": resourceusage.ResourceList{
						{
							CPUUsage: 100,
						}, {
							CPUUsage: 120,
						},
					},
				},
			},
			UploadTime: time.Now().Unix() - 601,
		},
	}

	result = h.changeUploadResourceData("cluster001", uploadResourceDataList)
	if len(result) != 0 {
		t.Errorf("fifth changeUploadResourceData err : %+v", result)
	}
}

func TestHandler_deleteFromExpireRedis(t *testing.T) {
	type fields struct {
		tickerTime       int64
		stopCh           <-chan struct{}
		monitoringClient *redisclient.Client
		clusters         []string
		refreshCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
		checkFunctionMetaExist func(funcSvcID string) bool
		refreshClusterFunc     func(map[string]config.ClusterUsage)
	}
	type args struct {
		clusterID string
		nodeName  string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		patches utils.PatchesFunc
	}{
		{
			"test delete from redis, monitoringClient is nil",
			fields{
				monitoringClient: nil,
			},
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
		},
		{
			"test delete from redis not success",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HDel",
						func(_ *redisclient.Client, ctx context.Context, key string, fields ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return errors.New("failed to hdel")
						}),
				})
				return patches
			},
		},
		{
			"test delete from redis success",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HDel",
						func(_ *redisclient.Client, ctx context.Context, key string, fields ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patches()

		h := &Handler{
			tickerTime:             tt.fields.tickerTime,
			stopCh:                 tt.fields.stopCh,
			monitoringClient:       tt.fields.monitoringClient,
			clusters:               tt.fields.clusters,
			refreshCacheFunc:       tt.fields.refreshCacheFunc,
			checkFunctionMetaExist: tt.fields.checkFunctionMetaExist,
		}
		h.deleteFromExpireRedis(tt.args.clusterID, tt.args.nodeName)

		patches.ResetAll()
	}
}

func TestHandler_getResourceUsageFromRedis(t *testing.T) {
	type fields struct {
		tickerTime       int64
		stopCh           <-chan struct{}
		monitoringClient *redisclient.Client
		clusters         []string
		refreshCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
		checkFunctionMetaExist func(funcSvcID string) bool
		refreshClusterFunc     func(map[string]config.ClusterUsage)
	}
	type args struct {
		clusterID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		patches utils.PatchesFunc
		want    map[resourceusage.NodeName]UploadResourceData
	}{
		{
			"test monitoringClient is nil",
			fields{monitoringClient: nil},
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			map[resourceusage.NodeName]UploadResourceData{},
		},
		{
			"test get data error",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							return &redis.MapStringStringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.MapStringStringCmd{}), "Result",
						func(_ *redis.MapStringStringCmd) (map[string]string, error) {
							return map[string]string{}, errors.New("failed to get result")
						}),
				})
				return patches
			},
			map[resourceusage.NodeName]UploadResourceData{},
		},
		{
			"test get data",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							return &redis.MapStringStringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.MapStringStringCmd{}), "Result",
						func(_ *redis.MapStringStringCmd) (map[string]string, error) {
							resultMap := map[string]string{}
							resultMap["a"] = "{\"resource\":{\"funcA\":{\"podA\":[{\"cpuUsage\":0.3}]}},\"uploadTime\":1698407137}"
							return resultMap, nil
						}),
				})
				return patches
			},
			map[resourceusage.NodeName]UploadResourceData{
				"a": {
					Resource: map[resourceusage.FuncKey]map[resourceusage.PodName]resourceusage.ResourceList{
						"funcA": {
							"podA": resourceusage.ResourceList{
								{
									CPUUsage: 0.3,
								},
							},
						},
					},
					UploadTime: 1698407137,
				},
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patches()
		h := &Handler{
			tickerTime:             tt.fields.tickerTime,
			stopCh:                 tt.fields.stopCh,
			monitoringClient:       tt.fields.monitoringClient,
			clusters:               tt.fields.clusters,
			refreshCacheFunc:       tt.fields.refreshCacheFunc,
			checkFunctionMetaExist: tt.fields.checkFunctionMetaExist,
		}

		if got := h.getResourceUsageFromRedis(tt.args.clusterID); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("getResourceUsageFromRedis() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()
	}
}

func TestHandler_handlerResourceUsage(t *testing.T) {
	type fields struct {
		tickerTime       int64
		stopCh           chan struct{}
		monitoringClient *redisclient.Client
		clusters         []string
		refreshCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
		checkFunctionMetaExist     func(funcSvcID string) bool
		refreshClusterFunc         func(map[string]config.ClusterUsage)
		refreshCaeMetricsCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
						FuncSvcID]map[resourceusage.PodName]caemetrics.List)
	}
	tests := []struct {
		name    string
		fields  fields
		patches utils.PatchesFunc
	}{
		{
			"test ticker time err",
			fields{
				tickerTime: 0,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
		},

		{
			"test stop chan is nil",
			fields{tickerTime: defaultTickerTime},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
		},
		{
			"test get stop ch",
			fields{
				tickerTime: 1,
				stopCh:     make(chan struct{}),
				refreshCacheFunc: func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList) {
					return
				},
				refreshClusterFunc: func(map[string]config.ClusterUsage) {
					return
				},
				refreshCaeMetricsCacheFunc: func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]caemetrics.List) {
					return
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(clustermgr.GetClusterMgr, func() *clustermgr.ClusterManager {
						return &clustermgr.ClusterManager{}
					}),
					gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
						func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
							return map[string]clustermgr.Cluster{"cluster1": {}}
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{
				tickerTime:                 tt.fields.tickerTime,
				predictDownloadTicker:      100000,
				predictUploadTicker:        100000,
				stopCh:                     tt.fields.stopCh,
				monitoringClient:           tt.fields.monitoringClient,
				clusters:                   tt.fields.clusters,
				refreshCacheFunc:           tt.fields.refreshCacheFunc,
				checkFunctionMetaExist:     tt.fields.checkFunctionMetaExist,
				refreshClusterFunc:         tt.fields.refreshClusterFunc,
				refreshCaeMetricsCacheFunc: tt.fields.refreshCaeMetricsCacheFunc,
			}
			if h.stopCh != nil {
				go func() {
					time.Sleep(2 * time.Second)
					tt.fields.stopCh <- struct{}{}
				}()
			}
			patches := tt.patches()
			h.handlerResourceUsage()
			patches.ResetAll()
		})
	}
}

type mockPredictionResult struct {
	funcName string
	value    resourceusage.PredictResourceList
}

func mockRedisGetAll(results []*mockPredictionResult) map[string]string {
	m := make(map[string]string)
	for _, predictionResult := range results {
		value, err := json.Marshal(predictionResult.value)
		if err != nil {
			fmt.Println("json marshal err: ", err.Error())
			continue
		}
		m[predictionResult.funcName] = string(value)
	}
	return m
}

func TestHandlerResourceUsage_predictDownloadTicker(t *testing.T) {
	convey.Convey("predictDownloadTicker", t, func() {
		existFunctionMeta := map[string]struct{}{
			"function1": struct{}{},
			"function2": struct{}{},
		}
		var getResultsFromRedis map[resourceusage.FuncSvcID]resourceusage.PredictResourceList
		redisCli := &redisclient.Client{}
		stop := make(chan struct{})
		handler := GetHandler()
		handler.WithPredictDownloadTickerTime(2).
			WithPredictUploadTicker(1000).
			WithTickerTime(10000).
			WithClusters([]string{"cluster001"}).
			WithMonitoringClient(redisCli).
			WithPredictEnable(true).
			WithStopCh(stop).
			RegisterCheckFunctionMetaExist(func(funcSvcID string) bool {
				_, ok := existFunctionMeta[funcSvcID]
				return ok
			}).RegisterPredictResourceFunc(func(m map[resourceusage.FuncSvcID]resourceusage.PredictResourceList) {
			getResultsFromRedis = m
			cache.GetInstance().RefreshPredictMetricsOutputCache(m)
		})

		mockValue := mockRedisGetAll([]*mockPredictionResult{
			{
				funcName: "function1",
				value: resourceusage.PredictResourceList{
					{
						CPUUtil:    1,
						MemoryUtil: 1,
						Timestamp:  time.Now().Unix(),
					},
					{
						CPUUtil:    2,
						MemoryUtil: 2,
						Timestamp:  time.Now().Unix() + 60,
					},
					{
						CPUUtil:    3,
						MemoryUtil: 3,
						Timestamp:  time.Now().Unix() + 120,
					},
				},
			},
			{
				funcName: "function3", // not exist in function meta cache
				value: resourceusage.PredictResourceList{
					{
						CPUUtil:    1,
						MemoryUtil: 1,
						Timestamp:  time.Now().Unix(),
					},
					{
						CPUUtil:    2,
						MemoryUtil: 2,
						Timestamp:  time.Now().Unix() + 60,
					},
					{
						CPUUtil:    3,
						MemoryUtil: 3,
						Timestamp:  time.Now().Unix() + 120,
					},
				},
			},
		})

		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(redisCli), "HGetAll",
				func(r *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
					result := &redis.MapStringStringCmd{}
					result.SetVal(mockValue)
					result.SetErr(nil)
					return result
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(redisCli), "HDel",
				func(r *redisclient.Client, ctx context.Context, key string, fields ...string) *redis.IntCmd {
					result := &redis.IntCmd{}
					result.SetErr(nil)
					return result
				}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					PredictConfig: config.PredictionConfig{
						PredictDownloadTicker: 0,
						PredictUploadTicker:   0,
						PredictFuncWhiteList:  []string{"function1", "function2", "function3"},
						PredictEnable:         false,
						PredictUseResult:      false,
					},
				}, nil
			}),
			gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
				func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
					return &functiontypes.FunctionEtcdInfo{}, true
				}),
		}

		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
			close(stop)
		}()
		cache.GetInstance().FunctionMetaCache.Store("function1", struct{}{})
		handler.HandlerResourceUsage()
		time.Sleep(3500 * time.Millisecond)
		// check predictDownloadTicker
		_, ok1 := getResultsFromRedis["function1"]
		_, ok2 := getResultsFromRedis["function3"]
		convey.So(ok1, convey.ShouldBeTrue)
		convey.So(ok2, convey.ShouldBeTrue)
	})
}

func TestHandlerResourceUsage_predictUploadTicker(t *testing.T) {
	convey.Convey("predictUploadTicker", t, func() {
		var put2Redis []interface{}
		stop := make(chan struct{})
		redisCli := &redisclient.Client{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(redisCli), "HMSet",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
					put2Redis = values
					result := &redis.BoolCmd{}
					result.SetErr(nil)
					return result
				}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					PredictConfig: config.PredictionConfig{
						PredictDownloadTicker: 0,
						PredictUploadTicker:   0,
						PredictFuncWhiteList:  []string{"function1", "function2"},
						PredictEnable:         false,
						PredictUseResult:      false,
					},
				}, nil
			}),
			gomonkey.ApplyFunc((*cache.AutoScalerCache).GetFunctionMeta,
				func(_ *cache.AutoScalerCache, funcSvcID string) (*functiontypes.FunctionEtcdInfo, bool) {
					return &functiontypes.FunctionEtcdInfo{}, true
				}),
		}

		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
			close(stop)
		}()
		existFunctionMeta := map[string]struct{}{
			"function1": struct{}{},
			"function2": struct{}{},
		}
		handler := GetHandler()
		handler.WithPredictDownloadTickerTime(20000).
			WithPredictUploadTicker(2).
			WithTickerTime(10000).
			WithClusters([]string{"cluster001"}).
			WithMonitoringClient(redisCli).
			WithPredictEnable(true).
			WithStopCh(stop).
			RegisterCheckFunctionMetaExist(func(funcSvcID string) bool {
				_, ok := existFunctionMeta[funcSvcID]
				return ok
			}).RegisterRefreshCacheFunc(cache.GetInstance().RefreshResourceMetricCache)

		metricsFromRedis := make(map[resourceusage.ClusterID]map[resourceusage.
			FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
		metricsFromRedis["cluster001"] = map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList{
			"function1": {
				"pod1": resourceusage.ResourceList{
					{
						CPUUsage:    1,
						MemoryUsage: 1,
						CPULimit:    1,
						MemoryLimit: 1,
						ReportTime:  time.Now().Unix(),
					},
					{
						CPUUsage:    2,
						MemoryUsage: 2,
						CPULimit:    2,
						MemoryLimit: 2,
						ReportTime:  time.Now().Add(1 * time.Second).Unix(),
					},
					{
						CPUUsage:    3,
						MemoryUsage: 3,
						CPULimit:    3,
						MemoryLimit: 3,
						ReportTime:  time.Now().Add(2 * time.Second).Unix(),
					},
				},
				"pod2": resourceusage.ResourceList{
					{
						CPUUsage:    4,
						MemoryUsage: 4,
						CPULimit:    4,
						MemoryLimit: 4,
						ReportTime:  time.Now().Unix(),
					},
					{
						CPUUsage:    5,
						MemoryUsage: 5,
						CPULimit:    5,
						MemoryLimit: 5,
						ReportTime:  time.Now().Add(1 * time.Second).Unix(),
					},
					{
						CPUUsage:    6,
						MemoryUsage: 6,
						CPULimit:    6,
						MemoryLimit: 6,
						ReportTime:  time.Now().Add(2 * time.Second).Unix(),
					},
				},
			},
		}
		handler.refreshCacheFunc(metricsFromRedis)

		metricsFromRedis1 := make(map[resourceusage.ClusterID]map[resourceusage.
			FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
		metricsFromRedis1["cluster001"] = map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList{
			"function1": {
				"pod3": resourceusage.ResourceList{
					{
						CPUUsage:    7,
						MemoryUsage: 7,
						CPULimit:    7,
						MemoryLimit: 7,
						ReportTime:  time.Now().Unix(),
					},
					{
						CPUUsage:    8,
						MemoryUsage: 8,
						CPULimit:    8,
						MemoryLimit: 8,
						ReportTime:  time.Now().Add(80 * time.Second).Unix(),
					},
					{
						CPUUsage:    9,
						MemoryUsage: 9,
						CPULimit:    9,
						MemoryLimit: 9,
						ReportTime:  time.Now().Add(81 * time.Second).Unix(),
					},
				},
			},
		}
		handler.refreshCacheFunc(metricsFromRedis1)

		cache.GetInstance().FunctionMetaCache.Store("function1", struct{}{})
		handler.HandlerResourceUsage()
		time.Sleep(3500 * time.Millisecond)
		// check predictUploadTicker
		convey.So(2, convey.ShouldEqual, len(put2Redis))
		convey.So("function1", convey.ShouldEqual, put2Redis[0])
	})
}

func TestHandler_getClusterResourceUsageFromRedis(t *testing.T) {
	type fields struct {
		tickerTime       int64
		stopCh           <-chan struct{}
		monitoringClient *redisclient.Client
		clusters         []string
		refreshCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]resourceusage.ResourceList)
		refreshClusterFunc     func(clusterMetricCache map[string]config.ClusterUsage)
		checkFunctionMetaExist func(funcSvcID string) bool
	}
	type args struct {
		clusterID string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		want        config.ClusterUsage
	}{
		{
			"case 1 test monitoringClient is nil",
			fields{
				monitoringClient: nil,
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			config.ClusterUsage{},
		},
		{
			"case 2 test failed to get result",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"test\":\"test\"}", errors.New("failed to get data")
						}),
				})
				return patches
			},
			config.ClusterUsage{},
		},
		{
			"case 3 test failed to unmarshal data",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"test\":\"test\"", nil
						}),
				})
				return patches
			},
			config.ClusterUsage{},
		},
		{
			"case 4 test success to get data",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{\"ClusterID\":\"1\"}", nil
						}),
				})
				return patches
			},
			config.ClusterUsage{
				ClusterID: "1",
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		h := &Handler{
			tickerTime:             tt.fields.tickerTime,
			stopCh:                 tt.fields.stopCh,
			monitoringClient:       tt.fields.monitoringClient,
			clusters:               tt.fields.clusters,
			refreshCacheFunc:       tt.fields.refreshCacheFunc,
			refreshClusterFunc:     tt.fields.refreshClusterFunc,
			checkFunctionMetaExist: tt.fields.checkFunctionMetaExist,
		}
		if got := h.getClusterResourceUsageFromRedis(tt.args.clusterID); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("getClusterResourceUsageFromRedis() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()
	}
}

func TestHandler_RegisterRefreshCaeMetricsCacheFunc(t *testing.T) {
	h := Handler{}
	h.RegisterRefreshCaeMetricsCacheFunc(func(resourceMetricCache map[resourceusage.
		ClusterID]map[resourceusage.FuncSvcID]map[resourceusage.PodName]caemetrics.List) {
	})

	if h.refreshCaeMetricsCacheFunc == nil {
		t.Errorf("failed to register refresh cae metrics cache func")
	}

	caeMetricsResources := initAllCaeMetricsResources()
	caeMetricsResources.pushData("funcSvc1", "podA", caemetrics.List{})
	if len(caeMetricsResources) == 0 {
		t.Errorf("failed to test push data")
	}

	h.RegisterCheckFunctionMetaExist(func(funcSvcID string) bool {
		return true
	})

	h.changeUploadCaeMetricsData(map[resourceusage.NodeName]UploadCaeMetricsData{
		"node1": {
			Metrics: map[resourceusage.FuncKey]map[resourceusage.PodName]caemetrics.List{
				"funcA": {
					"podA": {
						{CPUUsageRate: 100},
					},
				},
			},
			UploadTime: time.Now().Unix(),
		},
	})
}

func TestHandler_getCaeMetricsFromRedis(t *testing.T) {
	type fields struct {
		tickerTime       int64
		stopCh           <-chan struct{}
		monitoringClient *redisclient.Client
		clusters         []string
		refreshCacheFunc func(resourceMetricCache map[resourceusage.ClusterID]map[resourceusage.
					FuncSvcID]map[resourceusage.PodName]caemetrics.List)
		checkFunctionMetaExist func(funcSvcID string) bool
		refreshClusterFunc     func(map[string]config.ClusterUsage)
	}
	type args struct {
		clusterID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		patches utils.PatchesFunc
		want    map[resourceusage.NodeName]UploadCaeMetricsData
	}{
		{
			"test monitoringClient is nil",
			fields{monitoringClient: nil},
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			map[resourceusage.NodeName]UploadCaeMetricsData{},
		},
		{
			"test get data error",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							return &redis.MapStringStringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.MapStringStringCmd{}), "Result",
						func(_ *redis.MapStringStringCmd) (map[string]string, error) {
							return map[string]string{}, errors.New("failed to get result")
						}),
				})
				return patches
			},
			map[resourceusage.NodeName]UploadCaeMetricsData{},
		},
		{
			"test get data",
			fields{
				monitoringClient: &redisclient.Client{},
			},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HGetAll",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							return &redis.MapStringStringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.MapStringStringCmd{}), "Result",
						func(_ *redis.MapStringStringCmd) (map[string]string, error) {
							resultMap := map[string]string{}
							resultMap["a"] = "{\"metrics\":{\"funcA\":{\"podA\":[{\"cpuUsage\":0.3}]}},\"uploadTime\":1698407137}"
							return resultMap, nil
						}),
				})
				return patches
			},
			map[resourceusage.NodeName]UploadCaeMetricsData{
				"a": {
					Metrics: map[resourceusage.FuncKey]map[resourceusage.PodName]caemetrics.List{
						"funcA": {
							"podA": caemetrics.List{
								{
									CPUUsage: 0.3,
								},
							},
						},
					},
					UploadTime: 1698407137,
				},
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patches()
		h := &Handler{
			tickerTime:             tt.fields.tickerTime,
			stopCh:                 tt.fields.stopCh,
			monitoringClient:       tt.fields.monitoringClient,
			clusters:               tt.fields.clusters,
			checkFunctionMetaExist: tt.fields.checkFunctionMetaExist,
		}

		if got := h.getCaeMetricsFromRedis(tt.args.clusterID); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("getCaeMetricsFromRedis() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()
	}
}
