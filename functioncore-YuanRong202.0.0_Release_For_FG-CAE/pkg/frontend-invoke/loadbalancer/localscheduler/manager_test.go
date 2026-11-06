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

package localscheduler

import (
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/instance"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	testFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	testInstanceKey    = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
)

func TestAddInstance(t *testing.T) {
	convey.Convey("TestAddInstance", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "CAE",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		constant.InitDefaultConfig()
		stopCh := make(chan struct{})
		manager := CreateManager(testFuncKeyWithRes, stopCh)
		nodeIP1 := "127.1.1.1"
		manager.AddInstance(testInstanceKey + util.WorkerInstanceAZSep + nodeIP1)
		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus(nodeIP1, true)
		chosenKey := manager.NextFromTotalPool("")
		convey.So(chosenKey, convey.ShouldEqual, testInstanceKey)

		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus(nodeIP1, false)
		chosenKey2 := manager.NextFromTotalPool("")
		convey.So(chosenKey2, convey.ShouldEqual, "")
		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus(nodeIP1, true)
		chosenKey = manager.NextFromTotalPool("")
		convey.So(chosenKey, convey.ShouldEqual, testInstanceKey)

		manager.Release("", manager.Done(""), 0)
		manager.Release(chosenKey, manager.Done(chosenKey), 0)

		manager.DeleteInstance(testInstanceKey)
		chosenKey = manager.NextFromTotalPool("")
		convey.So(chosenKey, convey.ShouldEqual, "")
	})
}

func TestSelectCAEInstance(t *testing.T) {
	convey.Convey("TestSelectCAEInstance", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(metadata.LoadMetaData, func(funcName, tenantID, version string) (*metadata.FunctionMetaData, bool) {
				data := &metadata.FunctionMetaData{
					FuncMetaDataParts: functiontypes.FuncMetaDataParts{
						BusinessType: "CAE",
					},
				}
				return data, true
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		stopCh := make(chan struct{})
		manager := CreateManager(testFuncKeyWithRes, stopCh)
		for i := 0; i < 10; i++ {
			nodeIP := "127.0.1." + strconv.Itoa(i%2)
			instanceKey := testInstanceKey + strconv.Itoa(i)
			manager.AddInstance(instanceKey + util.WorkerInstanceAZSep + nodeIP)
		}
		var chosenIns instance.Instance
		var instancesMap1 = make(map[string]int)
		for i := 0; i < 10; i++ {
			chosenIns1 := manager.selectCAEInstance(chosenIns)
			if value, ok := instancesMap1[chosenIns1.GetInstanceKey()]; ok {
				instancesMap1[chosenIns1.GetInstanceKey()] = value + 1
			} else {
				instancesMap1[chosenIns1.GetInstanceKey()] = 1
			}
			convey.So(chosenIns1, convey.ShouldNotBeNil)
			time.Sleep(1 * time.Millisecond)
		}
		convey.So(len(instancesMap1), convey.ShouldBeGreaterThan, 2)
		convey.So(len(instancesMap1), convey.ShouldBeLessThan, 11)

		var instancesMap2 = make(map[string]int)
		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus("127.0.1.0", true)
		for i := 0; i < 10; i++ {
			chosenIns1 := manager.selectCAEInstance(chosenIns)
			if value, ok := instancesMap2[chosenIns1.GetInstanceKey()]; ok {
				instancesMap2[chosenIns1.GetInstanceKey()] = value + 1
			} else {
				instancesMap2[chosenIns1.GetInstanceKey()] = 1
			}
			convey.So(chosenIns1, convey.ShouldNotBeNil)
			convey.So(chosenIns1.GetNodeIP(), convey.ShouldEqual, "127.0.1.0")
		}
		convey.So(len(instancesMap2), convey.ShouldEqual, 5)

		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus("127.0.1.0", false)
		for i := 0; i < 10; i++ {
			chosenIns1 := manager.selectCAEInstance(chosenIns)
			convey.So(chosenIns1, convey.ShouldNotBeNil)
			convey.So(chosenIns1.GetNodeIP(), convey.ShouldEqual, "127.0.1.1")
		}

		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus("127.0.1.1", false)
		for i := 0; i < 10; i++ {
			chosenIns1 := manager.selectCAEInstance(chosenIns)
			convey.So(chosenIns1, convey.ShouldBeNil)
		}

		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus("127.0.1.0", true)
		instance.GetWorkerInstanceManager().AddWorkerInstanceStatus("127.0.1.1", true)
		var instancesMap3 = make(map[string]int)
		for i := 0; i < 100; i++ {
			chosenIns1 := manager.selectCAEInstance(chosenIns)
			if value, ok := instancesMap3[chosenIns1.GetInstanceKey()]; ok {
				instancesMap3[chosenIns1.GetInstanceKey()] = value + 1
			} else {
				instancesMap3[chosenIns1.GetInstanceKey()] = 1
			}
			convey.So(chosenIns1, convey.ShouldNotBeNil)
		}
		convey.So(len(instancesMap3), convey.ShouldEqual, 10)
	})
}
