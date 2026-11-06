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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/common/types/functiontypes"
)

func TestGetFuncAvailableCluster(t *testing.T) {
	t.Run("no available cluster", func(t *testing.T) {
		patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return nil
			})
		defer patch.Reset()
		assert.Equal(t, 0, len(GetFuncAvailableCluster()))
	})
	t.Run("one available cluster", func(t *testing.T) {
		patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{"cluster1": {}}
			})
		defer patch.Reset()
		assert.Equal(t, []string{"cluster1"}, GetFuncAvailableCluster())
	})
	t.Run("three available cluster", func(t *testing.T) {
		patch := gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
			func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
				return map[string]clustermgr.Cluster{"cluster1": {}, "cluster2": {}, "cluster3": {}}
			})
		defer patch.Reset()
		assert.Equal(t, []string{"cluster1", "cluster2", "cluster3"}, GetFuncAvailableCluster())
	})
}

func TestGetMaxInstanceCount(t *testing.T) {
	tests := []struct {
		name     string
		funcMeta *functiontypes.FunctionEtcdInfo
		want     int64
	}{
		{
			name: "default max instance count",
			funcMeta: &functiontypes.FunctionEtcdInfo{
				InstanceMetaData: functiontypes.InstanceMetaData{
					MaxInstance: -1,
				},
			},
			want: defaultMaxInstanceNum,
		},
		{
			name: "custom max instance count",
			funcMeta: &functiontypes.FunctionEtcdInfo{
				InstanceMetaData: functiontypes.InstanceMetaData{
					MaxInstance: 10,
				},
			},
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetMaxInstanceCount(tt.funcMeta)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestIntersectArray(t *testing.T) {
	var tests = []struct {
		a    []string
		b    []string
		want []string
	}{
		{[]string{"1", "2", "3"}, []string{"2", "3", "4"}, []string{"2", "3"}},
		{[]string{"1", "2", "3"}, []string{"4", "5", "6"}, nil},
		{[]string{"1", "2", "3"}, []string{"1", "2", "3"}, []string{"1", "2", "3"}},
		{[]string{}, []string{"1", "2", "3"}, nil},
		{[]string{"1", "2", "3"}, []string{}, nil},
		{[]string{}, []string{}, nil},
	}

	for _, test := range tests {
		got := IntersectArray(test.a, test.b)
		assert.Equal(t, test.want, got)
	}
}

func TestGetFunctionInfoByInstanceKey(t *testing.T) {
	key := "/instances/business/yrk/cluster/cluster1/tenant/tenant1/function/functionName/version/latest"
	functionInfo := getFunctionInfoByInstanceKey(key)
	assert.Equal(t, "yrk", functionInfo.Business)
	assert.Equal(t, "tenant1", functionInfo.Tenant)
	assert.Equal(t, "functionName", functionInfo.FuncName)
	assert.Equal(t, "latest", functionInfo.Version)
}

func TestGetFuncHashClusterID(t *testing.T) {
	availableClusters := []string{"cluster1", "cluster2", "cluster3", "cluster4"}
	functionVersionURN := "urn:function:3.0"
	result1 := getFuncHashClusterID(availableClusters, functionVersionURN)
	result2 := getFuncHashClusterID(availableClusters, functionVersionURN)
	if result1 != result2 {
		t.Errorf("Expected result1 equals result2")
	}
}

func TestCrNameByInstanceKey(t *testing.T) {
	instanceKey := "/instances/business/yrk/cluster/cluster01/tenant/123456/function/0@default@funcName/version/latest"
	expected := "yrk1234-0@default@funcName-latest-3639237522"
	assert.Equal(t, expected, CrNameByInstanceKey(instanceKey))
}

func TestDifferenceArray(t *testing.T) {
	tests := []struct {
		a    []string
		b    []string
		want []string
	}{
		{
			a:    []string{"a", "b", "c"},
			b:    []string{"b", "c", "d"},
			want: []string{"d"},
		},
		{
			a:    []string{"a", "b", "c"},
			b:    []string{"d", "e", "f"},
			want: []string{"d", "e", "f"},
		},
		{
			a:    []string{"a", "b", "c"},
			b:    []string{"a", "b", "c"},
			want: nil,
		},
		{
			a:    []string{},
			b:    []string{"a", "b", "c"},
			want: []string{"a", "b", "c"},
		},
		{
			a:    []string{"a", "b", "c"},
			b:    []string{},
			want: nil,
		},
	}

	for _, tt := range tests {
		got := differenceArray(tt.a, tt.b)
		assert.Equal(t, tt.want, got)
	}
}
