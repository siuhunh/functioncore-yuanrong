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

// Package stateinsevent To handle state routing events
package stateinsevent

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/util"
)

// TestIsStateInstance -
func TestIsStateInstance(t *testing.T) {
	convey.Convey("test: is state instance", t, func() {
		// is not state key
		convey.So(IsStateInstance("/sn/workers/business/yrk/tenant/0/function/function-task/"), convey.ShouldBeFalse)
		// is invalid state key
		convey.So(IsStateInstance("/sn/stateworkers/business"), convey.ShouldBeFalse)
		// is valid state key
		convey.So(IsStateInstance(
			"/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/statetest"),
			convey.ShouldBeTrue)
	})
}

// TestProcessStateInsEvent_Add -
func TestProcessStateInsEvent_Add(t *testing.T) {
	convey.Convey("test: state instance add event", t, func() {
		testKey := "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/statetest"
		convey.Convey("test: invalid event", func() {
			event1 := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   testKey,
				Value: []byte(`{"statetest,instance"}`),
			}
			err := ProcessStateInsEvent(event1)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("test: failed to query etcd, err", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return nil, errors.New("test err")
			})
			patch.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			defer patch.Reset()
			event1 := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   testKey,
				Value: []byte(`{"statetest":"instance"}`),
			}
			err := ProcessStateInsEvent(event1)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("test: failed to query etcd, not found", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return nil, nil
			})
			patch.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			defer patch.Reset()
			event1 := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   testKey,
				Value: []byte(`{"statetest":"instance"}`),
			}
			err := ProcessStateInsEvent(event1)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("test: invalid task info", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{`{"nodeIP","10.10.10.10"}`}, nil
			})
			patch.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			defer patch.Reset()
			event1 := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   testKey,
				Value: []byte(`{"statetest":"instance"}`),
			}
			err := ProcessStateInsEvent(event1)
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("test: succeed to process add event", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc((*etcd3.EtcdClient).GetValues, func(_ *etcd3.EtcdClient,
				ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
				return []string{`{"nodeIP":"10.10.10.10","nodePort":"22423"}`}, nil
			})
			patch.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			patch.ApplyFunc(util.GetGlobalClient, func() *fasthttp.Client {
				return new(fasthttp.Client)
			})
			defer patch.Reset()
			event1 := &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   testKey,
				Value: []byte(`{"statetest":"instance"}`),
			}
			err := ProcessStateInsEvent(event1)
			convey.So(err, convey.ShouldBeNil)
			ins, ok := functionlb.FunctionLB.GetStateIns().WorkerInstancesConn.Load("statetest")
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(ins.(*functionlb.WorkerInstanceConnection).Host, convey.ShouldEqual, "10.10.10.10")
			convey.So(ins.(*functionlb.WorkerInstanceConnection).Port, convey.ShouldEqual, "22423")
		})
	})
}

// TestProcessStateInsEvent_Del -
func TestProcessStateInsEvent_Del(t *testing.T) {
	convey.Convey("test: state instance del event", t, func() {
		testKey := "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/statetest"
		event1 := &etcd3.Event{
			Type:  etcd3.DELETE,
			Key:   testKey,
			Value: []byte(`{"statetest":"instance"}`),
		}
		err := ProcessStateInsEvent(event1)
		convey.So(err, convey.ShouldBeNil)
	})
}
