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

package funcinsmanager

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/etcdadapter/etcd"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/scheduler/schedulermanager"
	"functioncore/pkg/faas-scheduler/types"
)

var (
	testFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	testInstanceKey    = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"

	testWorkerInstance = &types.WorkerInstance{
		InstanceID: testInstanceKey,
		Applier:    "worker-manager",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		CPU:        300,
		Memory:     128,
	}
)

type mockObserver struct {
	instances map[string]*common.Instance
}

func (observer *mockObserver) OnObserved(instance *common.Instance, eventType common.InstanceEventType) {
	if eventType == common.AddInstance {
		observer.instances[instance.InstanceKey] = instance
	} else if eventType == common.RecoverInstance {
		return
	} else {
		delete(observer.instances, instance.InstanceKey)
	}
}

func TestAddInstance(t *testing.T) {
	convey.Convey("TestAddInstance", t, func() {
		manager := CreateManager(&schedulermanager.Manager{})
		observer := &mockObserver{
			instances: map[string]*common.Instance{},
		}
		manager.AddInstance(testFuncKeyWithRes, testWorkerInstance)
		manager.AddObserver(testFuncKeyWithRes, observer)
		convey.So(len(observer.instances), convey.ShouldEqual, 1)

		instanceA := *testWorkerInstance
		instanceA.InstanceID = testInstanceKey + "-1"
		manager.AddInstance(testFuncKeyWithRes, &instanceA)
		convey.So(len(observer.instances), convey.ShouldEqual, 2)

		processesStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
		processesStatusMap[0] = &schedulerapi.ProcessStatus{
			ProcessorID:  0,
			LeaseVersion: 10,
			RestThread:   9,
		}
		instancesStatusMap := make(map[string]*schedulerapi.InstancesStatus)
		instancesStatusMap[testInstanceKey+"-1"] = &schedulerapi.InstancesStatus{
			ProcessesStatusMap: processesStatusMap,
		}
		req := &schedulerapi.CheckInstanceReq{
			FuncKeyWithRes:     testFuncKeyWithRes,
			InstancesStatusMap: instancesStatusMap,
		}
		manager.RecoverInstance(testFuncKeyWithRes, req)
		convey.So(len(observer.instances), convey.ShouldEqual, 2)

		manager.DeleteInstance(testFuncKeyWithRes, testWorkerInstance)
		convey.So(len(observer.instances), convey.ShouldEqual, 1)

		instanceB := *testWorkerInstance
		instanceB.InstanceID = testInstanceKey + "-2"
		manager.DeleteInstance(testFuncKeyWithRes, &instanceB)
		convey.So(len(observer.instances), convey.ShouldEqual, 1)

		manager.DeleteObserver(testFuncKeyWithRes)
		manager.DeleteInstance(testFuncKeyWithRes, &instanceA)
		convey.So(len(observer.instances), convey.ShouldEqual, 1)
	})
}

func Test_createScheduler(t *testing.T) {
	convey.Convey("Test_createScheduler ok", t, func() {
		manager := schedulermanager.CreateManager()
		mgr := &Manager{
			instanceCaches:   map[string]*instanceCache{},
			schedulermanager: manager,
		}
		cache := mgr.getOrCreateInstanceCache("funcKeyWithRes")
		convey.So(cache.observer, convey.ShouldBeNil)
		config.FConfig.InstanceLeaseConfig = &config.InstanceLeaseConfig{LeaseCheckInterval: 10}
		sch, _ := mgr.schedulermanager.CreateScheduler("funcKeyWithRes", &types.FunctionMetaInfo{}, mgr.AddObserver, true)
		convey.So(cache.observer, convey.ShouldNotBeNil)
		convey.So(sch, convey.ShouldNotBeNil)
	})
}

func Test_AddInstance(t *testing.T) {
	convey.Convey("Test_createScheduler ok", t, func() {
		manager := schedulermanager.CreateManager()
		mgr := &Manager{
			instanceCaches:   map[string]*instanceCache{},
			schedulermanager: manager,
		}
		cache := mgr.getOrCreateInstanceCache("funcKeyWithRes")
		convey.So(cache.observer, convey.ShouldBeNil)
		config.FConfig.InstanceLeaseConfig = &config.InstanceLeaseConfig{LeaseCheckInterval: 10}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd.FetchFuncMetaFromEtcd,
				func(key string, etcdType etcd3.EtcdType) (*types.FunctionMetaInfo, error) {
					return &types.FunctionMetaInfo{
						FuncMetaData:     types.FuncMetaData{BusinessType: "CAE"},
						ExtendedMetaData: types.ExtendedMetaData{EnableWebsocket: true},
					}, nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		mgr.AddInstance("funcKeyWithRes", &types.WorkerInstance{
			BusinessType:    "CAE",
			EnableWebsocket: true,
			InstanceID:      "InstanceID",
		})
		cache = mgr.getOrCreateInstanceCache("funcKeyWithRes")
		convey.So(cache.observer, convey.ShouldNotBeNil)
	})
}
