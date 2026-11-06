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

// Package etcd3 implements crud and watch operations based etcd clientv3
package etcd3

import (
	"context"
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/client/v3"

	"functioncore/test/utils"
)

// TestEtcdClient_Grant -
func TestEtcdClient_Grant(t *testing.T) {
	convey.Convey("test: grant", t, func() {
		client := &EtcdClient{
			Client: &clientv3.Client{
				Lease: utils.FakeEtcdLease{},
			},
		}
		id, err := client.Grant(CreateEtcdCtxInfo(context.Background()), 10)
		convey.So(id, convey.ShouldEqual, 1)
		convey.So(err, convey.ShouldBeNil)
	})
}

// TestEtcdClient_KeepAliveOnce -
func TestEtcdClient_KeepAliveOnce(t *testing.T) {
	convey.Convey("test: keepAliveOnce", t, func() {
		client := &EtcdClient{
			Client: &clientv3.Client{
				Lease: utils.FakeEtcdLease{},
			},
		}
		err := client.KeepAliveOnce(CreateEtcdCtxInfo(context.Background()), 1)
		convey.So(err, convey.ShouldBeNil)
	})
}

// TestEtcdClient_KeepAliveOnce -
func TestEtcdClient_Revoke(t *testing.T) {
	convey.Convey("test: revoke", t, func() {
		client := &EtcdClient{
			Client: &clientv3.Client{
				Lease: utils.FakeEtcdLease{},
			},
		}
		err := client.Revoke(CreateEtcdCtxInfo(context.Background()), 1)
		convey.So(err, convey.ShouldBeNil)
	})
}
