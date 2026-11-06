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

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/grpcconn"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
)

var (
	testSchedulerKey  = "faas-scheduler-6b758c8b74-5zdwv"
	testSchedulerInfo = &commontypes.FaaSServiceInfo{
		InstanceID: testSchedulerKey,
		IP:         "127.0.0.1",
		Port:       "8889",
	}
)

func TestProcessSchedulerEvent(t *testing.T) {
	convey.Convey("TestProcessSchedulerEvent", t, func() {
		constant.InitDefaultConfig()
		constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
		}

		common.SetRPCClientCreator(grpcconn.CreateGrpcClient)
		remotescheduler.CreateInstance()
		SetSchedulerInsHandler(remotescheduler.GetInstance())
		value, _ := json.Marshal(testSchedulerInfo)

		addEvent := &etcd3.Event{
			Type:  etcd3.PUT,
			Value: value,
		}
		ProcessSchedulerEvent(addEvent)
		scheduler := remotescheduler.GetInstance().GetScheduler("")
		convey.So(scheduler, convey.ShouldNotBeNil)

		deleteEvent := &etcd3.Event{
			Type:      etcd3.DELETE,
			PrevValue: value,
		}
		ProcessSchedulerEvent(deleteEvent)
		scheduler = remotescheduler.GetInstance().GetScheduler("")
		convey.So(scheduler, convey.ShouldBeNil)

		errorEvent := &etcd3.Event{
			Type: etcd3.ERROR,
		}
		err := ProcessSchedulerEvent(errorEvent)
		convey.So(err, convey.ShouldNotBeNil)

		syncEvent := &etcd3.Event{
			Type: etcd3.SYNCED,
		}
		err = ProcessSchedulerEvent(syncEvent)
		convey.So(err, convey.ShouldNotBeNil)

		invalidAddEvent := &etcd3.Event{
			Type:  etcd3.PUT,
			Value: []byte{1},
		}
		err = ProcessSchedulerEvent(invalidAddEvent)
		convey.So(err, convey.ShouldNotBeNil)

		invalidDeleteEvent := &etcd3.Event{
			Type:  etcd3.PUT,
			Value: []byte{1},
		}
		err = ProcessSchedulerEvent(invalidDeleteEvent)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestIsFaaSScheduler(t *testing.T) {
	convey.Convey("TestIsFaaSScheduler", t, func() {
		key := "/sn/faas-scheduler/instances/cluster001/7.218.100.25/faas-scheduler-59ddbc4b75-8xdjf"
		convey.So(IsFaaSScheduler(key), convey.ShouldBeTrue)

		invalidKey := "/sn/faas/instances/cluster001/7.218.100.25/faas-59ddbc4b75-8xdjf"
		convey.So(IsFaaSScheduler(invalidKey), convey.ShouldBeFalse)
	})
}
