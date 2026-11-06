/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package willdeleteworker

import (
	mockutils "functioncore/test/utils"
	"github.com/agiledragon/gomonkey/v2"
	"reflect"
	"sync"
	"testing"

	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"github.com/smartystreets/goconvey/convey"
)

func TestGetCache(t *testing.T) {
	convey.Convey("get cache should not be nil", t, func() {
		cache := GetCache()
		convey.ShouldNotBeNil(cache)
	})
}

func TestCache_AddWillDeleteWorkerInstances(t *testing.T) {
	convey.Convey("three worker instances added successfully", t, func() {
		funcSvnID := "yrk111"
		cache := GetCache()
		instances := []v1alpha1.WorkerInstance{
			{InstanceID: "111111111111"},
			{InstanceID: "222222222222"},
			{InstanceID: "333333333333"},
		}
		cache.AddWillDeleteWorkerInstances(funcSvnID, instances)
		tmpInstances := cache.GetWillDeleteWorkerInstances(funcSvnID)
		convey.ShouldEqual(len(instances), len(tmpInstances))
	})
}

func TestCache_DeleteWillDeleteWorkerInstance(t *testing.T) {
	convey.Convey("success to delete one worker instance", t, func() {
		funcSvnID := "yrk222"
		cache := GetCache()
		instances := []v1alpha1.WorkerInstance{
			{InstanceID: "444444444444"},
		}
		cache.AddWillDeleteWorkerInstances(funcSvnID, instances)
		tmpInstances := cache.GetWillDeleteWorkerInstances(funcSvnID)
		convey.ShouldEqual(len(instances), len(tmpInstances))

		cache.DeleteWillDeleteWorkerInstance(funcSvnID, "444444444444")
		tmpInstances = cache.GetWillDeleteWorkerInstances(funcSvnID)
		convey.ShouldEqual(len(instances), 0)
	})
}

func TestCache_GetWillDeleteWorkerInstancesAndDelete(t *testing.T) {
	convey.Convey("three worker instances added successfully", t, func() {
		funcSvnID := "yrk111"
		cache := GetCache()
		instances := []v1alpha1.WorkerInstance{
			{InstanceID: "111111111111"},
			{InstanceID: "222222222222"},
			{InstanceID: "333333333333"},
		}
		cache.AddWillDeleteWorkerInstances(funcSvnID, instances)
		tmpInstances := cache.GetWillDeleteWorkerInstancesAndDelete(funcSvnID)
		convey.ShouldEqual(len(instances), len(tmpInstances))
		tmpInstances = cache.GetWillDeleteWorkerInstances(funcSvnID)
		convey.ShouldEqual(len(tmpInstances), 0)
	})
}

func TestCache_DeleteFuncWillDeleteWorker(t *testing.T) {
	w := Cache{
		functions: sync.Map{},
	}

	patches := mockutils.InitPatchSlice()
	patches.Append(
		mockutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "LoadAndDelete",
				func(_ *sync.Map, key any) (value any, loaded bool) {
					return nil, true
				}),
		})

	w.DeleteFuncWillDeleteWorker("")
	patches.ResetAll()
}
