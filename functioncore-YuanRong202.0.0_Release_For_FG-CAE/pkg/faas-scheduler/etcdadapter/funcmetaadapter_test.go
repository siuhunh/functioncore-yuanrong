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

package etcdadapter

import (
	"encoding/json"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
)

const (
	testFuncKey = "7e186a/0@base@testresourcepython36768/latest"
)

func TestFetchFuncMeta(t *testing.T) {
	convey.Convey("TestFetchFuncMeta", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})

		var value string
		patch.ApplyFunc((*etcd3.EtcdClient).GetValues,
			func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{value}, nil
			})

		patch.ApplyFunc((*etcd3.EtcdClient).GetEtcdStatusLostContact,
			func(e *etcd3.EtcdClient) bool {
				return true
			})

		defer func() {
			patch.Reset()
		}()

		value = ""
		_, err := FetchFuncMeta(testFuncKey)
		convey.So(err, convey.ShouldNotBeNil)

		invalidFuncMeta := *testFuncMeta
		invalidFuncMeta.FuncMetaData.FunctionVersionURN = ""
		valueBytes, _ := json.Marshal(&invalidFuncMeta)
		value = string(valueBytes)
		_, err = FetchFuncMeta(testFuncKey)
		convey.So(err, convey.ShouldNotBeNil)

		invalidFuncMeta = *testFuncMeta
		invalidFuncMeta.InstanceMetaData.ConcurrentNum = 0
		valueBytes, _ = json.Marshal(invalidFuncMeta)
		value = string(valueBytes)
		_, err = FetchFuncMeta(testFuncKey)
		// parse function will not return error when concurrent num is 0
		convey.So(err, convey.ShouldBeNil)

		valueBytes, _ = json.Marshal(testFuncMeta)
		value = string(valueBytes)
		info, err := FetchFuncMeta(testFuncKey)
		convey.So(err, convey.ShouldBeNil)
		convey.So(info.InstanceMetaData.ConcurrentNum, convey.ShouldEqual, 1)
	})
}
