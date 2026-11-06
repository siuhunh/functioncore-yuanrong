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

package testsuit

import (
	"strconv"
	"sync"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/service"
)

// TestSuit -
type TestSuit struct {
	funcMetaDatabase *FuncMetaDatabase
}

// CreateTestSuit -
func CreateTestSuit() *TestSuit {
	return &TestSuit{
		funcMetaDatabase: createFuncMetaDatabase(),
	}
}

// Setup -
func (suit *TestSuit) Setup() {
	config.InitDefaultConfig()
	suit.DisableLeaseCheck()
}

// DisableLeaseCheck -
func (suit *TestSuit) DisableLeaseCheck() {
	config.FConfig.InstanceLeaseConfig.LeaseSpan = 300000
}

func (suit *TestSuit) EnableLeaseCheck() {
	config.FConfig.InstanceLeaseConfig.LeaseSpan = 2000
}

func (suit *TestSuit) GetFuncMetaDatabase() *FuncMetaDatabase {
	return suit.funcMetaDatabase
}

func (suit *TestSuit) ConcurrentAddInstance(service service.SchedulerService, funcKeyWithRes string, instanceNum int) {
	wg := sync.WaitGroup{}
	for i := 0; i < instanceNum; i++ {
		instance := *TestWorkerInstance
		instance.InstanceID = instance.InstanceID + "-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			service.AddInstance(funcKeyWithRes, &instance)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (suit *TestSuit) ConcurrentDeleteInstance(service service.SchedulerService, funcKeyWithRes string, instanceNum int) {
	wg := sync.WaitGroup{}
	for i := 0; i < instanceNum; i++ {
		instance := *TestWorkerInstance
		instance.InstanceID = instance.InstanceID + "-" + strconv.Itoa(i)
		wg.Add(1)
		go func() {
			service.DeleteInstance(funcKeyWithRes, &instance)
			wg.Done()
		}()
	}
	wg.Wait()
}

func (suit *TestSuit) ConcurrentAcquireInstance(service service.SchedulerService,
	req *schedulerapi.AcquireInstanceReq, invokeTime int) []*schedulerapi.AcquireInstanceRsp {
	wg := sync.WaitGroup{}

	lock := sync.Mutex{}
	result := make([]*schedulerapi.AcquireInstanceRsp, 0)
	for i := 0; i < invokeTime; i++ {
		wg.Add(1)
		go func() {
			rsp, _ := service.AcquireInstance(req)

			lock.Lock()
			result = append(result, rsp)
			lock.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()
	return result
}

func (suit *TestSuit) CountUniqueProcessor(rspList []*schedulerapi.AcquireInstanceRsp) int {
	keyMap := make(map[string]int, 1)
	for _, rsp := range rspList {
		if rsp == nil || rsp.Error.ErrorCode != 0 {
			continue
		}
		key := rsp.InstanceKey + strconv.Itoa(int(rsp.ProcessorID))
		keyMap[key] = 1
	}
	return len(keyMap)
}
