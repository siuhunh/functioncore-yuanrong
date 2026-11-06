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
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
)

func TestInitClusterStatusCache(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
		opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
		}}, nil
	})
	patch = gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{}}
	})
	defer patch.Reset()
	type args struct{}
	tests := []struct {
		name string
		args args
	}{
		{"case 1", args{}},
	}
	ClusterStatusCache = ClusterStatus{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitClusterStatusCache()
			assert.NotNil(t, ClusterStatusCache.ClusterStatusMap)
		})
	}
}

func TestInitClusterStatusCache_Error(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
		opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{
			{Key: []byte("cce-cluster-yuanrong01"), Value: []byte("{\"cce-cluster-yuanrong01\":{\"state\":\"running\",\"state_time\":1703560438}}")},
		}}, nil
	})
	patch.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{}
	})
	patch.ApplyFunc(time.Sleep, func(_ time.Duration) {})
	defer patch.Reset()
	type args struct{}
	tests := []struct {
		name string
		args args
	}{
		{"case 1", args{}},
	}
	ClusterStatusCache = ClusterStatus{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitClusterStatusCache()
			assert.Nil(t, ClusterStatusCache.ClusterStatusMap)
		})
	}
}

func TestInitClusterStatusCache_ETCDError(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
		opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
		return nil, errors.New("TestError")
	})
	patch = gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
		return &etcd3.EtcdClient{Client: &clientv3.Client{}}
	})
	patch.ApplyFunc(time.Sleep, func(_ time.Duration) {})
	defer patch.Reset()
	type args struct{}
	tests := []struct {
		name string
		args args
	}{
		{"case 1", args{}},
	}
	ClusterStatusCache = ClusterStatus{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			InitClusterStatusCache()
			assert.Nil(t, ClusterStatusCache.ClusterStatusMap)
		})
	}
}

func TestIsCurrentClustersStatusRunningAndFlowouting(t *testing.T) {
	test := []struct {
		name      string
		clusterID string
		wanted1   bool
		wanted2   bool
	}{
		{
			"isRunning",
			"runningCluster",
			true,
			false,
		},
		{
			"isRunning-empty",
			"runningCluster-empty",
			true,
			false,
		},
		{
			"noRunning",
			"not_runningCluster",
			false,
			true,
		},
	}
	ClusterStatusCache.ClusterStatusMap = map[string]ClusterFlowState{}
	ClusterStatusCache.ClusterStatusMap["not_runningCluster"] = ClusterFlowState{State: ClusterFlowOutingStatus}
	ClusterStatusCache.ClusterStatusMap["runningCluster"] = ClusterFlowState{State: ClusterRunningStatus}
	ClusterStatusCache.ClusterStatusMap["runningCluster-empty"] = ClusterFlowState{State: ""}
	for _, tt := range test {
		clusterID = tt.clusterID
		if IsCurrentClustersStatusRunning() != tt.wanted1 {
			t.Errorf("IsCurrentClustersStatusRunning %s error, wanted %v", tt.name, tt.wanted1)
		}
		if IsCurrentClustersStatusFlowouting() != tt.wanted2 {
			t.Errorf("IsCurrentClustersStatusFlowouting %s error, wanted %v", tt.name, tt.wanted2)
		}
	}
}

func TestClusterStatus_UpdateClusterStatusMap(t *testing.T) {
	type fields struct {
		ClusterStatusMap     map[string]ClusterFlowState
		DefaultClusterAmount int
		Mutex                sync.Mutex
	}
	type args struct {
		clusterStatusMap map[string]ClusterFlowState
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "UpdateClusterStatusMap",
			fields: fields{
				ClusterStatusMap:     nil,
				DefaultClusterAmount: 0,
				Mutex:                sync.Mutex{},
			},
			args: args{clusterStatusMap: map[string]ClusterFlowState{"test": {State: ClusterRunningStatus}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ct := &ClusterStatus{
				ClusterStatusMap:     tt.fields.ClusterStatusMap,
				DefaultClusterAmount: tt.fields.DefaultClusterAmount,
				Mutex:                tt.fields.Mutex,
			}
			ct.UpdateClusterStatusMap(tt.args.clusterStatusMap)
			if ct.ClusterStatusMap == nil {
				t.Errorf("failed to UpdateClusterStatusMap")
			}
		})
	}
}
