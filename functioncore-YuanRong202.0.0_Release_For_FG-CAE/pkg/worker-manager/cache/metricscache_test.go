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

package cache

import (
	"strconv"
	"sync"
	"testing"

	"functioncore/pkg/common/metrics"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
	"github.com/smartystreets/goconvey/convey"
)

const (
	functionName = "0@state@test-yr-stateful-java-012-java-caller"
	podName      = "pool27-1200-1280-fusion-7cf6d8477b-qqb5s"
	tenantID     = "7e1ad6a6-cc5c-44fa-bd54-25873f72a86a"
	version      = "latest"
	functionKey  = "7e1ad6a6-cc5c-44fa-bd54-25873f72a86a@@0@state@test-yr-stateful-java-012-java-caller@latest"
)

func TestHandleFunctionName(t *testing.T) {
	convey.Convey("TestHandleFunctionName", t, func() {
		funcName, memSize := handleFunctionName(functionName, podName)
		convey.So(funcName, convey.ShouldEqual, "state@test-yr-stateful-java-012-java-caller")
		convey.So(memSize, convey.ShouldEqual, "1280")
	})
}

func TestMetricsCacheCrud(t *testing.T) {
	convey.Convey("TestMetricsCacheCrud", t, func() {
		backupMetricsCache := MetricsCache
		defer func() {
			MetricsCache = backupMetricsCache
		}()
		MetricsCache = &metricsCache{
			functionInstanceCurrentCount: make(map[string]int, initCacheSize),
			ReserveInstanceTimestamp:     make(map[string]ReserveInstanceInfo, initCacheSize),
		}

		funcInfo := &metrics.FunctionInfo{
			TenantID:     tenantID,
			FunctionName: functionName,
			Version:      version,
		}

		// add count
		MetricsCache.UpdateFuncInstanceCurrentCount(funcInfo, 1)
		MetricsCache.lock.RLock()
		countMap := MetricsCache.functionInstanceCurrentCount
		MetricsCache.lock.RUnlock()
		count, ok := countMap[functionKey]
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(count, convey.ShouldEqual, 1)

		// increase count
		IncFuncInstanceCurrentCount(functionKey)
		MetricsCache.lock.RLock()
		countMap = MetricsCache.functionInstanceCurrentCount
		MetricsCache.lock.RUnlock()
		count, ok = countMap[functionKey]
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(count, convey.ShouldEqual, 2)

		// clean function key
		functionService := &faasfunction.FunctionService{
			Spec: faasfunction.FunctionServiceSpec{
				Configuration: faasfunction.ConfigurationSpec{
					TenantID:        tenantID,
					FunctionName:    functionName,
					FunctionVersion: version,
				},
			},
		}
		MetricsCache.ClearByFunctionService(functionService)

		MetricsCache.lock.RLock()
		countMap = MetricsCache.functionInstanceCurrentCount
		MetricsCache.lock.RUnlock()
		_, ok = countMap[functionKey]
		convey.So(ok, convey.ShouldBeFalse)
	})
}

// TestAddReserveInstanceTimestamp -
func TestAddReserveInstanceTimestamp(t *testing.T) {
	convey.Convey("TestAddReserveInstanceTimestamp", t, func() {
		MetricsCache = &metricsCache{
			functionInstanceCurrentCount: make(map[string]int, initCacheSize),
			ReserveInstanceTimestamp: map[string]ReserveInstanceInfo{
				"pool22-300-128-fusion-test1-test1": {},
			},
		}
		defer func() {
			MetricsCache = &metricsCache{
				functionInstanceCurrentCount: make(map[string]int, initCacheSize),
				ReserveInstanceTimestamp:     make(map[string]ReserveInstanceInfo, initCacheSize),
			}
		}()
		AddReserveInstanceTimestamp(&metrics.FunctionInfo{}, "pool22-300-128-fusion-test1-test1", false)
		_, ok := MetricsCache.ReserveInstanceTimestamp["pool22-300-128-fusion-test1-test1"]
		convey.So(ok, convey.ShouldBeTrue)
		AddReserveInstanceTimestamp(&metrics.FunctionInfo{}, "pool22-300-128-fusion-test2-test2", false)
		_, ok = MetricsCache.ReserveInstanceTimestamp["pool22-300-128-fusion-test2-test2"]
		convey.So(ok, convey.ShouldBeFalse)
		AddReserveInstanceTimestamp(&metrics.FunctionInfo{
			FunctionName: "funcname3",
			TenantID:     "tenant3",
			Version:      "version3",
		}, "pool22-300-128-fusion-test3-test3", false)
		_, ok = MetricsCache.ReserveInstanceTimestamp["pool22-300-128-fusion-test3-test3"]
		convey.So(ok, convey.ShouldBeFalse)
		AddReserveInstanceTimestamp(&metrics.FunctionInfo{
			FunctionName: "0@service@funcname",
			TenantID:     "tenant4",
			Version:      "version4",
		}, "pool22-300-128-fusion-test4-test4", false)
		funcInfo, ok := MetricsCache.ReserveInstanceTimestamp["pool22-300-128-fusion-test4-test4"]
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(funcInfo.FunctionURN, convey.ShouldEqual, "tenant4@service@funcname@version4@128")
		convey.So(funcInfo.PodName, convey.ShouldEqual, "pool22-300-128-fusion-test4-test4")
		convey.So(funcInfo.FirstStart, convey.ShouldEqual, false)
	})
}

func TestMarshalFuncInsCurrentCount(t *testing.T) {
	loopNum := 1000
	incWaitGroup := &sync.WaitGroup{}
	incWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		s := strconv.Itoa(i)
		go func() {
			IncFuncInstanceCurrentCount(functionKey + s)
			incWaitGroup.Done()
		}()
	}
	marshalWaitGroup := &sync.WaitGroup{}
	marshalWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			_, err := MarshalFuncInsCurrentCount()
			if err != nil {
				t.Errorf("marshal error:%v want nil", err)
			}
			marshalWaitGroup.Done()
		}()
	}
	incWaitGroup.Wait()
	marshalWaitGroup.Wait()
}

func TestUpdateFuncInstanceCurrentCount(t *testing.T) {
	loopNum := 1000

	funcInfo := &metrics.FunctionInfo{
		FunctionName: "hello",
		TenantID:     "123456",
		Version:      "latest",
		ServiceID:    "7890123",
	}
	funcKey := tools.GetFuncKey(funcInfo.TenantID, funcInfo.ServiceID, funcInfo.FunctionName, funcInfo.Version)
	mockMetricsCache := &metricsCache{
		functionInstanceCurrentCount: map[string]int{funcKey: 1},
	}
	incWaitGroup := &sync.WaitGroup{}
	incWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockMetricsCache.IncFuncInstanceCurrentCount(funcKey)
			incWaitGroup.Done()
		}()
	}
	marshalWaitGroup := &sync.WaitGroup{}
	marshalWaitGroup.Add(loopNum)
	for i := 0; i < loopNum; i++ {
		go func() {
			mockMetricsCache.UpdateFuncInstanceCurrentCount(funcInfo, 1)
			marshalWaitGroup.Done()
		}()
	}
	// no panic means test case is ok
	incWaitGroup.Wait()
	marshalWaitGroup.Wait()
}

func TestCheckReserveInstanceExist(t *testing.T) {
	convey.Convey("TestCheckReserveInstanceExist", t, func() {
		MetricsCache = &metricsCache{
			functionInstanceCurrentCount: make(map[string]int, initCacheSize),
			ReserveInstanceTimestamp: map[string]ReserveInstanceInfo{
				"pool22-300-128-fusion-test1-test2": {DelSignal: true},
				"pool22-300-128-fusion-test1-test1": {},
			},
		}
		defer func() {
			MetricsCache = &metricsCache{
				functionInstanceCurrentCount: make(map[string]int, initCacheSize),
				ReserveInstanceTimestamp:     make(map[string]ReserveInstanceInfo, initCacheSize),
			}
		}()
		ok := CheckReserveInstanceExist("test")
		convey.So(ok, convey.ShouldBeFalse)

		ok = CheckReserveInstanceExist("pool22-300-128-fusion-test1-test2")
		convey.So(ok, convey.ShouldBeFalse)

		ok = CheckReserveInstanceExist("pool22-300-128-fusion-test1-test1")
		convey.So(ok, convey.ShouldBeTrue)
	})
}
