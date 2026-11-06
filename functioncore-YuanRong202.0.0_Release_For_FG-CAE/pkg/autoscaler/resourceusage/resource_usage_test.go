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
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/resourceusage"
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
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringStringMapCmd {
							return &redis.StringStringMapCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringStringMapCmd{}), "Result",
						func(_ *redis.StringStringMapCmd) (map[string]string, error) {
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
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringStringMapCmd {
							return &redis.StringStringMapCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringStringMapCmd{}), "Result",
						func(_ *redis.StringStringMapCmd) (map[string]string, error) {
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
		checkFunctionMetaExist func(funcSvcID string) bool
		refreshClusterFunc     func(map[string]config.ClusterUsage)
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
				tickerTime:             tt.fields.tickerTime,
				stopCh:                 tt.fields.stopCh,
				monitoringClient:       tt.fields.monitoringClient,
				clusters:               tt.fields.clusters,
				refreshCacheFunc:       tt.fields.refreshCacheFunc,
				checkFunctionMetaExist: tt.fields.checkFunctionMetaExist,
				refreshClusterFunc:     tt.fields.refreshClusterFunc,
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
