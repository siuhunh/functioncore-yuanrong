/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package funcmetamanager

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	testFuncKey = "7e186a/0@base@testresourcepython36768/latest"
)

var (
	testFuncMeta = &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
	}
)

func TestGetFuncMetaFromCache(t *testing.T) {
	convey.Convey("TestGetFuncMetaFromCache", t, func() {
		manager := CreateManager(nil)
		manager.UpdateFuncMeta(testFuncKey, testFuncMeta)
		meta, err := manager.GetFuncMeta(testFuncKey)
		convey.So(err, convey.ShouldBeNil)
		convey.So(meta.InstanceMetaData.ConcurrentNum, convey.ShouldEqual, 1)

		manager.DeleteFuncMeta(testFuncKey, 0)
		meta, err = manager.GetFuncMeta(testFuncKey)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func mockMetaFetcher(funcKey string) (*types.FunctionMetaInfo, error) {
	return testFuncMeta, nil
}

func TestGetFuncMetaFromRemoteDatabase(t *testing.T) {
	convey.Convey("TestGetFuncMetaFromRemoteDatabase", t, func() {
		manager := CreateManager(mockMetaFetcher)
		meta, err := manager.GetFuncMeta(testFuncKey)
		convey.So(err, convey.ShouldBeNil)
		convey.So(meta.InstanceMetaData.ConcurrentNum, convey.ShouldEqual, 1)
	})
}

func TestManagerUpdateDeleteMetaFirst(t *testing.T) {
	funcKey := "123456/0@base@hello/latest"
	metaData := &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 10,
		},
		Source: etcd3.Meta,
	}
	caeMetaData := &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
		Source: etcd3.CAEMeta,
	}

	tests := []struct {
		name   string
		source etcd3.EtcdType
		info   *types.FunctionMetaInfo
		event  int // 0 add 1 delete

		expectedInCache bool
		expectedNum     int
	}{
		{
			name:   "meta add event",
			source: etcd3.Meta,
			info:   metaData,

			expectedInCache: true,
			expectedNum:     10,
		},
		{
			name:   "cae meta add, skip",
			source: etcd3.CAEMeta,
			info:   caeMetaData,

			expectedInCache: true,
			expectedNum:     10,
		},
		{
			name:   "cae meta delete, skip",
			source: etcd3.CAEMeta,
			event:  1,

			expectedInCache: true,
			expectedNum:     10,
		},
		{
			name:   "meta delete",
			source: etcd3.Meta,
			event:  1,

			expectedInCache: false,
		},
		{
			name:   "cae meta add",
			source: etcd3.CAEMeta,
			info:   caeMetaData,

			expectedInCache: true,
			expectedNum:     1,
		},
		{
			name:   "cae meta delete",
			source: etcd3.CAEMeta,
			event:  1,

			expectedInCache: false,
		},
	}

	manager := CreateManager(nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.event == 0 {
				manager.UpdateFuncMeta(funcKey, tt.info)
			} else {
				manager.DeleteFuncMeta(funcKey, tt.source)
			}

			data, err := manager.GetFuncMetaFromCache(funcKey)
			if tt.expectedInCache {
				assert.Nil(t, err, tt.name)
				assert.Equal(t, tt.expectedNum, data.InstanceMetaData.ConcurrentNum, tt.name)
			} else {
				assert.NotNil(t, err, tt.name)
			}
		})
	}
}
