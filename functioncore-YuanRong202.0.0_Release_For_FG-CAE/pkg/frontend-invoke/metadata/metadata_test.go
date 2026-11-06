/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types/functiontypes"
)

func TestProcessUpdate(t *testing.T) {
	funcMetaData := FunctionMetaData{
		ResourceMetaData: functiontypes.ResourceMetaData{
			CPU:    300,
			Memory: 128,
		},
	}
	validValue, _ := json.Marshal(funcMetaData)
	tests := []struct {
		name        string
		functionKey string
		value       []byte
		exceptError bool
	}{
		{
			name:        "update success",
			functionKey: "/sn/functions/business/business/tenant/tenantID/function/functionName/version/version",
			value:       validValue,
			exceptError: false,
		},
		{
			name:        "functionKey is incorrect",
			functionKey: "/sn/functions/business/business/tenant/tenantID/",
			value:       validValue,
			exceptError: true,
		},
		{
			name:        "content that can't unmarshal",
			functionKey: "/sn/functions/business/business/tenant/tenantID/function/functionName/version/version",
			value:       nil,
			exceptError: true,
		},
	}
	for _, test := range tests {
		actualErr := ProcessUpdate(test.functionKey, test.value, etcd3.Meta)
		metaDataLock.Lock()
		delete(metaData, test.functionKey)
		metaDataLock.Unlock()
		if test.exceptError {
			assert.Error(t, actualErr, test.name)
		} else {
			assert.Nil(t, actualErr, test.name)
		}
	}
}

func TestProcessDelete(t *testing.T) {
	tests := []struct {
		name          string
		functionKey   string
		exceptedError error
	}{
		{
			name:          "delete success",
			functionKey:   "/sn/functions/business/business/tenant/tenantID/function/functionName/version/version",
			exceptedError: nil,
		},
		{
			name:          "functionKey is incorrect",
			functionKey:   "/sn/functions/business/business/tenant/tenantID/",
			exceptedError: errors.New("incorrect etcdKey length"),
		},
	}
	for _, test := range tests {
		actualErr := ProcessDelete(test.functionKey, etcd3.Meta)
		assert.Equal(t, test.exceptedError, actualErr, test.name)
	}
}

func TestLoadMetaData(t *testing.T) {
	convey.Convey("test: load meta data", t, func() {
		cacheOk := combineFunctionKey("cacheOk", "cacheOk", "cacheOk")
		metaDataLock.Lock()
		metaData[cacheOk] = &FunctionMetaData{
			ResourceMetaData: functiontypes.ResourceMetaData{
				CPU:    100,
				Memory: 100,
			},
		}
		metaDataLock.Unlock()
		etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
		errKey := fmt.Sprintf(constants.SilentFuncKey, "testErr", "testErr", "testErr")
		notExistKey := fmt.Sprintf(constants.SilentFuncKey, "testNotExist", "testNotExist", "testNotExist")
		unmarshalFailedKey := fmt.Sprintf(constants.SilentFuncKey,
			"testUnmarshalFailed", "testUnmarshalFailed", "testUnmarshalFailed")
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return etcdClient
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse", func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
				if key == errKey {
					return nil, errors.New("test err")
				}
				if key == notExistKey {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{},
					}, nil
				}
				if key == unmarshalFailedKey {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Key:   []byte("test ok"),
								Value: []byte("test unmarshal failed"),
							},
						},
					}, nil
				}
				return &clientv3.GetResponse{
					Kvs: []*mvccpb.KeyValue{
						{
							Key:   []byte("test ok"),
							Value: []byte(`{"resourceMetaData":{"cpu":100,"memory":100}}`),
						},
					},
				}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		// test: the cache exists and the data is correct
		testMeta, exist := LoadMetaData("cacheOk", "cacheOk", "cacheOk")
		convey.So(exist, convey.ShouldBeTrue)
		convey.So(testMeta.ResourceMetaData.CPU, convey.ShouldEqual, 100)
		// test: an error occurred when accessing etcd
		testMeta, exist = LoadMetaData("testErr", "testErr", "testErr")
		convey.So(exist, convey.ShouldBeFalse)
		convey.So(testMeta, convey.ShouldBeNil)
		// test: no data exists in the cache and ETCD
		testMeta, exist = LoadMetaData("testNotExist", "testNotExist", "testNotExist")
		convey.So(exist, convey.ShouldBeFalse)
		convey.So(testMeta, convey.ShouldBeNil)
		// test: data exists in etcd, but the data is abnormal
		testMeta, exist = LoadMetaData("testUnmarshalFailed", "testUnmarshalFailed", "testUnmarshalFailed")
		convey.So(exist, convey.ShouldBeFalse)
		convey.So(testMeta, convey.ShouldBeNil)
		// test: no data exists in the cache, but etcd exists and the data is correct
		testMeta, exist = LoadMetaData("testOK", "testOK", "testOK")
		convey.So(exist, convey.ShouldBeTrue)
		convey.So(testMeta.ResourceMetaData.CPU, convey.ShouldEqual, 100)
	})
}

func TestProcessUpdateDeleteMetaFirst(t *testing.T) {
	funcKey := "/sn/functions/business/yrk/tenant/123456/function/0@default@hello/version/latest"
	mapKey := "123456/0@default@hello/latest"
	metaVal := []byte(`{"instanceMetaData": {"concurrentNum": 10}}`)
	caeMetaVal := []byte(`{"instanceMetaData": {"concurrentNum": 1}}`)

	tests := []struct {
		name   string
		source etcd3.EtcdType
		val    []byte
		event  int // 0 add 1 delete

		expectedInCache bool
		expectedNum     int
	}{
		{
			name:   "meta add event",
			source: etcd3.Meta,
			val:    metaVal,

			expectedInCache: true,
			expectedNum:     10,
		},
		{
			name:   "cae meta add, skip",
			source: etcd3.CAEMeta,
			val:    caeMetaVal,

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
			val:    caeMetaVal,

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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.event == 0 {
				ProcessUpdate(funcKey, tt.val, tt.source)
			} else {
				ProcessDelete(funcKey, tt.source)
			}

			metaDataLock.Lock()
			val, ok := metaData[mapKey]
			metaDataLock.Unlock()

			assert.Equal(t, tt.expectedInCache, ok, tt.name)
			if tt.expectedInCache {
				assert.Equal(t, tt.expectedNum, val.InstanceMetaData.ConcurrentNum, tt.name)
			}
		})
	}
}
