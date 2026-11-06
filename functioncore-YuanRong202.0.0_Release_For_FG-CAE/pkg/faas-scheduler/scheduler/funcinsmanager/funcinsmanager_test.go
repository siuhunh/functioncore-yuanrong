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
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
	"github.com/smartystreets/goconvey/convey"
	"testing"
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
	} else {
		delete(observer.instances, instance.InstanceKey)
	}
}

func TestAddInstance(t *testing.T) {
	convey.Convey("TestAddInstance", t, func() {
		manager := CreateManager()
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
