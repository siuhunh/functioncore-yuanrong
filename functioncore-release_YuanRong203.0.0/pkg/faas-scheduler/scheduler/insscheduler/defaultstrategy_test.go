/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package insscheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/faas-scheduler/scheduler/common"
)

func TestCheckInstance(t *testing.T) {
	strategy := defaultStrategy{}
	strategy.instances = make(map[string]*instance)

	strategy.checkInstance(&schedulerapi.CheckInstanceReq{})

	processesStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
	processesStatusMap[0] = &schedulerapi.ProcessStatus{
		ProcessorID:  0,
		LeaseVersion: 1,
		RestThread:   9,
	}
	instancesStatus := make(map[string]*schedulerapi.InstancesStatus)
	instancesStatus["instanceA"] = &schedulerapi.InstancesStatus{
		ProcessesStatusMap: processesStatusMap,
	}

	strategy.checkInstance(&schedulerapi.CheckInstanceReq{InstancesStatusMap: instancesStatus})
	assert.Equal(t, 0, len(strategy.instances))

	processorA := &processor{processorID: 0, leaseVersion: 2, restThread: 8}
	processors := make([]*processor, 1)
	processors[0] = processorA
	strategy.instances["instanceA"] = &instance{
		instanceKey: "instanceA",
		processors:  processors,
	}
	strategy.checkInstance(&schedulerapi.CheckInstanceReq{InstancesStatusMap: instancesStatus})
	assert.Equal(t, 1, len(strategy.instances))
	assert.Equal(t, 2, int(strategy.instances["instanceA"].processors[0].leaseVersion))
	assert.Equal(t, 8, int(strategy.instances["instanceA"].processors[0].restThread))

	processesStatusMap[0] = &schedulerapi.ProcessStatus{
		ProcessorID:  0,
		LeaseVersion: 5,
		RestThread:   7,
	}
	instancesStatus["instanceA"] = &schedulerapi.InstancesStatus{
		ProcessesStatusMap: processesStatusMap,
	}
	strategy.checkInstance(&schedulerapi.CheckInstanceReq{InstancesStatusMap: instancesStatus})
	assert.Equal(t, 1, len(strategy.instances))
	assert.Equal(t, 5, int(strategy.instances["instanceA"].processors[0].leaseVersion))
	assert.Equal(t, 7, strategy.instances["instanceA"].processors[0].restThread)
	assert.Equal(t, false, strategy.instances["instanceA"].processors[0].leased)
}

func TestReservedBalance(t *testing.T) {
	strategy := createDefaultStrategy(10, false, "testFunc")

	for i := 0; i < 10; i++ {
		strategy.addInstance(&common.Instance{
			InstanceKey: fmt.Sprintf("instance%d", i),
		}, reservedInstance)
	}

	count := make(map[string]int)

	for i := 0; i < 100000; i++ {
		resp, err := strategy.acquireInstance(&schedulerapi.AcquireInstanceReq{})
		assert.Nil(t, err)
		count[resp.InstanceKey]++
		err = strategy.releaseInstance(&schedulerapi.ReleaseInstanceReq{
			ProcessorID:  resp.ProcessorID,
			InstanceKey:  resp.InstanceKey,
			LeaseVersion: resp.LeaseVersion,
		})
		assert.Nil(t, err)
	}
	for k, v := range count {
		assert.True(t, v <= 10500 && v >= 9500, k)
	}
}

func TestOnDemandPriority(t *testing.T) {
	strategy := createDefaultStrategy(10, false, "testFunc")
	for i := 0; i < 10; i++ {
		strategy.addInstance(&common.Instance{
			InstanceKey: fmt.Sprintf("instance%d", i),
		}, normalInstance)
	}

	count := make(map[string]int)

	for i := 0; i < 100000; i++ {
		resp, err := strategy.acquireInstance(&schedulerapi.AcquireInstanceReq{})
		assert.Nil(t, err)
		count[resp.InstanceKey]++
		err = strategy.releaseInstance(&schedulerapi.ReleaseInstanceReq{
			ProcessorID:  resp.ProcessorID,
			InstanceKey:  resp.InstanceKey,
			LeaseVersion: resp.LeaseVersion,
		})
		assert.Nil(t, err)
	}
	assert.Equal(t, 1, len(count))
	assert.Equal(t, 100000, count["instance0"])
}

func TestMixedBalance(t *testing.T) {
	wg := sync.WaitGroup{}
	concurrency := 50

	wg.Add(concurrency)

	strategy := createDefaultStrategy(10, false, "testFunc")
	for i := 0; i < 3; i++ {
		strategy.addInstance(&common.Instance{
			InstanceKey: fmt.Sprintf("instance%d", i),
		}, reservedInstance)
	}
	for i := 3; i < 6; i++ {
		strategy.addInstance(&common.Instance{
			InstanceKey: fmt.Sprintf("instance%d", i),
		}, normalInstance)
	}

	strategyMutex := &sync.Mutex{}
	count := make(map[string]int)
	countMutex := &sync.Mutex{}

	for g := 0; g < concurrency; g++ {
		go func() {
			defer wg.Done()
			icount := make(map[string]int)
			for i := 0; i < 10000; i++ {
				strategyMutex.Lock()
				resp, err := strategy.acquireInstance(&schedulerapi.AcquireInstanceReq{})
				strategyMutex.Unlock()
				assert.Nil(t, err)
				icount[resp.InstanceKey]++

				strategyMutex.Lock()
				err = strategy.releaseInstance(&schedulerapi.ReleaseInstanceReq{
					ProcessorID:  resp.ProcessorID,
					InstanceKey:  resp.InstanceKey,
					LeaseVersion: resp.LeaseVersion,
				})
				strategyMutex.Unlock()
				assert.Nil(t, err)
			}

			countMutex.Lock()
			for k, v := range icount {
				count[k] += v
			}
			countMutex.Unlock()
		}()
	}
	wg.Wait()

	fmt.Println(count)
	for i := 3; i < 6; i++ {
		assert.True(t, count[fmt.Sprintf("instance%d", i)] < count[fmt.Sprintf("instance%d", i-1)] ||
			count[fmt.Sprintf("instance%d", i)] == 0)
	}
}

func Test_bridgeScheduleStrategy(t *testing.T) {
	convey.Convey("Test_defaultScheduleStrategy ok", t, func() {
		onDemand := make([]*instance, 0)
		processors1 := make([]*processor, 0)
		processors1 = append(processors1, &processor{
			restThread: 50,
		})
		processors2 := make([]*processor, 0)
		processors2 = append(processors2, &processor{
			restThread: 50,
		})
		onDemand = append(onDemand, &instance{
			instanceKey: "aprilyolies",
			processors:  processors1,
		})
		onDemand = append(onDemand, &instance{
			instanceKey: "johnson",
			processors:  processors2,
		})
		res, _ := bridgeScheduleStrategy(&defaultStrategy{
			rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
			onDemand: onDemand,
		})
		flag := false
		cur := res.InstanceKey
		for i := 0; i < 20; i++ {
			res, _ = bridgeScheduleStrategy(&defaultStrategy{
				rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
				onDemand: onDemand,
			})
			if res.InstanceKey != cur {
				flag = true
				break
			}
		}
		convey.So(flag, convey.ShouldBeTrue)
	})
	convey.Convey("Test_defaultScheduleStrategy err", t, func() {
		onDemand := make([]*instance, 0)
		onDemand = append(onDemand, &instance{
			instanceKey: "aprilyolies",
		})
		onDemand = append(onDemand, &instance{
			instanceKey: "johnson",
		})
		res, _ := bridgeScheduleStrategy(&defaultStrategy{
			rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
			onDemand: onDemand,
		})
		convey.So(res.Error, convey.ShouldNotBeNil)
	})
}
