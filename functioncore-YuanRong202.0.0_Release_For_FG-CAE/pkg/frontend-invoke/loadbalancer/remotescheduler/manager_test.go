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

package remotescheduler

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/grpcconn"
)

var (
	testSchedulerKey  = "faas-scheduler-6b758c8b74-5zdwv"
	testSchedulerInfo = &commontypes.FaaSServiceInfo{
		InstanceID: testSchedulerKey,
		IP:         "127.0.0.1",
		Port:       "8889",
	}
)

type mockRPCClient struct {
	scheduler   schedulerapi.Scheduler
	retainError error
}

func (c *mockRPCClient) Start() {}

func (c *mockRPCClient) Stop() {}

func (c *mockRPCClient) IsEnable() bool {
	return true
}

func (c *mockRPCClient) ServerKey() string {
	return ""
}

func (c *mockRPCClient) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	rsp, err := c.scheduler.AcquireInstance(req)
	return rsp, err
}

func (c *mockRPCClient) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	err := c.scheduler.ReleaseInstance(req)
	return err
}

func (c *mockRPCClient) RetainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	if c.retainError != nil {
		return nil, c.retainError
	}
	rsp, err := c.scheduler.RetainInstance(req)
	return rsp, err
}

func TestAddScheduler(t *testing.T) {
	convey.Convey("TestAddScheduler", t, func() {
		constant.InitDefaultConfig()
		constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
		}
		common.SetRPCClientCreator(grpcconn.CreateGrpcClient)
		CreateInstance()

		GetInstance().AddScheduler(testSchedulerKey, testSchedulerInfo)
		client := GetInstance().GetScheduler("")
		convey.So(client, convey.ShouldNotBeNil)

		GetInstance().AddScheduler(testSchedulerKey, testSchedulerInfo)

		GetInstance().DeleteScheduler(testSchedulerKey, testSchedulerInfo)
		client = GetInstance().GetScheduler("")
		convey.So(client, convey.ShouldBeNil)

		GetInstance().DeleteScheduler(testSchedulerKey, testSchedulerInfo)
	})
}

func TestGetStatefulScheduler(t *testing.T) {
	CreateInstance()
	manager := GetInstance()
	client1 := &mockRPCClient{}
	manager.stateful["scheduler001"] = client1
	client3 := &mockRPCClient{}
	manager.stateful["scheduler003"] = client3
	convey.Convey("TestGetStatefulScheduler ok", t, func() {
		gomonkey.ApplyMethod(reflect.TypeOf(client1), "IsEnable",
			func(c *mockRPCClient) bool { return true })
		scheduler := manager.GetStatefulScheduler("scheduler001")
		convey.So(scheduler, convey.ShouldNotBeNil)
	})
	convey.Convey("TestGetStatefulScheduler not found", t, func() {
		scheduler := manager.GetStatefulScheduler("scheduler002")
		convey.So(scheduler, convey.ShouldBeNil)
	})
	convey.Convey("TestGetStatefulScheduler not enable", t, func() {
		gomonkey.ApplyMethod(reflect.TypeOf(client3), "IsEnable",
			func(c *mockRPCClient) bool { return false })
		scheduler := manager.GetStatefulScheduler("scheduler003")
		convey.So(scheduler, convey.ShouldBeNil)
	})
}
