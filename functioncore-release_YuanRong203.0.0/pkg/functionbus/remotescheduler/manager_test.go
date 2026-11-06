/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/loadbalance"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
)

var (
	schedulerKey  = "faas-scheduler-869c9d8df9-fkt5w"
	schedulerInfo = &commontypes.FaaSServiceInfo{
		InstanceID: schedulerKey,
		IP:         "10.28.88.205",
		Port:       "8889",
	}

	schedulerKeyA  = "faas-scheduler-869c9d8df9-fkt5a"
	schedulerInfoA = &commontypes.FaaSServiceInfo{
		InstanceID: schedulerKeyA,
		IP:         "10.28.88.205",
		Port:       "8889",
	}

	schedulerKeyB  = "faas-scheduler-869c9d8df9-fkt5b"
	schedulerInfoB = &commontypes.FaaSServiceInfo{
		InstanceID: schedulerKeyB,
		IP:         "10.28.88.205",
		Port:       "8889",
	}
)

func TestAddAndDeleteScheduler(t *testing.T) {
	convey.Convey("TestAddScheduler", t, func() {
		mockChan := make(chan struct{})
		CreateInstanceManager(mockChan)
		close(mockChan)

		GetInstanceManager().AddScheduler(schedulerKey, schedulerInfo)
		client := GetInstanceManager().getScheduler("")
		convey.So(client, convey.ShouldNotBeNil)
		convey.So(client.instanceID, convey.ShouldEqual, schedulerKey)

		GetInstanceManager().AddScheduler(schedulerKey, schedulerInfo)

		GetInstanceManager().DeleteScheduler(schedulerKey)
		client = GetInstanceManager().getScheduler("")
		convey.So(client, convey.ShouldBeNil)

		GetInstanceManager().DeleteScheduler(schedulerKey)
		client = GetInstanceManager().getScheduler("")
		convey.So(client, convey.ShouldBeNil)

		GetInstanceManager().AddScheduler(schedulerKeyA, schedulerInfoA)
		GetInstanceManager().AddScheduler(schedulerKeyB, schedulerInfoB)

		client = GetInstanceManager().getScheduler("funcA")
		convey.So(client, convey.ShouldNotBeNil)

		for i := 0; i < 100; i++ {
			clientA := GetInstanceManager().getScheduler("funcA")
			convey.So(clientA, convey.ShouldNotBeNil)
			convey.So(clientA.instanceID, convey.ShouldEqual, client.instanceID)
		}
		GetInstanceManager().DeleteScheduler(schedulerKeyA)
		GetInstanceManager().DeleteScheduler(schedulerKeyB)

		instanceManager.disable = true
		client = GetInstanceManager().getScheduler("")
		convey.So(client, convey.ShouldBeNil)
		GetInstanceManager().DeleteScheduler(schedulerKeyB)
		GetInstanceManager().AddScheduler(schedulerKeyA, schedulerInfoA)
		client = GetInstanceManager().getScheduler("funcA")
		convey.So(client, convey.ShouldBeNil)
	})
}

func TestAcquireInstanceAndReleaseInstance(t *testing.T) {
	funcKeyWithResA := "funcA"
	traceID := "trace001"

	convey.Convey("TestAcquireInstanceAndReleaseInstance", t, func() {
		convey.Convey("instanceManager disable", func() {
			_, key, _ := AcquireInstance(funcKeyWithResA, traceID)
			convey.So(key, convey.ShouldBeEmpty)
			ReleaseInstance(funcKeyWithResA, "", false, "")
		})
		convey.Convey("no scheduler", func() {
			mockChan := make(chan struct{})
			CreateInstanceManager(mockChan)
			close(mockChan)
			_, key, _ := AcquireInstance(funcKeyWithResA, traceID)
			instanceMap.Store("instanceA", "aa")
			ReleaseInstance(funcKeyWithResA, "instanceA", true, "")
			convey.So(key, convey.ShouldBeEmpty)
		})
		convey.Convey("AcquireInstance err", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*clientAdapter).AcquireInstance,
					func(c *clientAdapter, req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return nil, errors.New("AcquireInstance err")
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			mockChan := make(chan struct{})
			CreateInstanceManager(mockChan)
			close(mockChan)
			GetInstanceManager().AddScheduler(schedulerKey, schedulerInfo)
			_, key, _ := AcquireInstance(funcKeyWithResA, traceID)
			convey.So(key, convey.ShouldBeEmpty)
		})
		convey.Convey("AcquireInstance and ReleaseInstance case", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*clientAdapter).AcquireInstance,
					func(c *clientAdapter, req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
						return &schedulerapi.AcquireInstanceRsp{InstanceKey: req.TraceID, LeaseVersion: 1}, nil
					}),
				gomonkey.ApplyFunc((*clientAdapter).ReleaseInstance,
					func(c *clientAdapter, req *schedulerapi.ReleaseInstanceReq) error {
						return nil
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()

			// case1: firstCall with no instance cache
			_, instance1, _ := AcquireInstance(funcKeyWithResA, "instanceA")
			convey.So(instance1, convey.ShouldEqual, "instanceA&&0")
			ReleaseInstance(funcKeyWithResA, "instanceA&&0", false, "")

			// case2: secondCall with instance cache
			_, instance2, _ := AcquireInstance(funcKeyWithResA, "instanceB")
			convey.So(instance2, convey.ShouldEqual, "instanceA&&0")

			// case3: current call with no instance cache
			_, instance3, _ := AcquireInstance(funcKeyWithResA, "instanceB")
			convey.So(instance3, convey.ShouldEqual, "instanceB&&0")
			_, instance4, _ := AcquireInstance(funcKeyWithResA, "instanceC")
			convey.So(instance4, convey.ShouldEqual, "instanceC&&0")

			// case4: call with instance cache
			ReleaseInstance(funcKeyWithResA, "instanceA&&0", true, "")
			ReleaseInstance(funcKeyWithResA, "instanceA&&0", true, "")
			ReleaseInstance(funcKeyWithResA, "instanceB&&0", true, "")
			ReleaseInstance(funcKeyWithResA, "instanceC&&0", false, "")
			_, instance5, _ := AcquireInstance(funcKeyWithResA, "instanceC")
			convey.So(instance5, convey.ShouldEqual, "instanceC&&0")

			// case5: release more instance in cache
			AcquireInstance(funcKeyWithResA, "instanceA")
			// case4: call with instance cache
			ReleaseInstance(funcKeyWithResA, "instanceA&&0", false, "")
			ReleaseInstance(funcKeyWithResA, "instanceC&&0", false, "")
			idleInstanceMapLock.Lock()
			convey.So(len(idleInstanceMap[funcKeyWithResA]), convey.ShouldEqual, 2)
			idleInstanceMapLock.Unlock()

			// case6: Acquire instance repeatedly with cache for the same instance
			_, instance6, _ := AcquireInstance(funcKeyWithResA, "instanceD")
			convey.So(instance6, convey.ShouldEqual, "instanceC&&0")
			ReleaseInstance(funcKeyWithResA, "instanceC&&0", false, "")
			_, instance7, _ := AcquireInstance(funcKeyWithResA, "instanceD")
			convey.So(instance7, convey.ShouldEqual, "instanceC&&0")
			ReleaseInstance(funcKeyWithResA, "instanceC&&0", false, "")
			convey.So(len(idleInstanceMap[funcKeyWithResA]), convey.ShouldEqual, 2)
			instanceMap.Delete("instanceC&&0")
			_, instance8, _ := AcquireInstance(funcKeyWithResA, "instanceD")
			convey.So(instance8, convey.ShouldEqual, "instanceA&&0")
			ReleaseInstance(funcKeyWithResA, "instanceA&&0", false, "")
			convey.So(len(idleInstanceMap[funcKeyWithResA]), convey.ShouldEqual, 1)

			// case7: invalid release
			ReleaseInstance(funcKeyWithResA, "instanceC&&0", false, "")
			instanceMap.Store("instanceA", "aa")
			ReleaseInstance(funcKeyWithResA, "instanceA", true, "")
			instanceMap.Store("instanceA&&kk", "aa")
			ReleaseInstance(funcKeyWithResA, "instanceA&&kk", true, "")
			instanceMap.Store("instanceF&&0", "aa")
			// case7: invalid release
			ReleaseInstance(funcKeyWithResA, "instanceD&&0", false, "")
			convey.So(len(idleInstanceMap[funcKeyWithResA]), convey.ShouldEqual, 1)
		})
	})

}

func TestScheduledTask(t *testing.T) {
	convey.Convey("TestScheduledTask", t, func() {
		mockChan := make(chan struct{})
		common.FConfig.RemoteSchedulerConfig = &common.RemoteSchedulerConfig{
			Disable: false, AcquireInstanceTimeout: 3, InstanceReleaseTime: 3, CheckInstancePeriod: 3,
			InstanceLeaseTime: 3,
		}
		CreateInstanceManager(mockChan)
		time.Sleep(5 * time.Second)
		close(mockChan)
	})
}

func TestRenewInstancesLeaseScheduledTask(t *testing.T) {
	convey.Convey("TestRenewInstancesLeaseScheduledTask", t, func() {
		convey.Convey("renew instance lease while not idle", func() {
			checkResult := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
				gomonkey.ApplyFunc((*clientAdapter).RetainInstance,
					func(c *clientAdapter, req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
						checkResult = 1
						return &schedulerapi.RetainInstanceRsp{}, nil
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			status := &instanceStatus{
				funcKeyWithRes: funcKeyWithRes,
				instanceID:     instanceID,
				nodeIP:         "10.28.88.205",
				processID:      0,
				leaseVersion:   1,
				restThread:     1,
				lastUsedTime:   time.Now(),
				idleFlag:       false,
			}
			key := fmt.Sprintf("%s%s%v", instanceID, InstanceSplicer, status.processID)
			instanceMap.Store(key, status)

			renewInstancesLease("test")
			time.Sleep(10 * time.Millisecond)
			convey.So(checkResult, convey.ShouldEqual, 1)
		})
		convey.Convey("renew instance lease while not idle, but lease fail", func() {
			checkResult := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			status := &instanceStatus{
				funcKeyWithRes: funcKeyWithRes,
				instanceID:     instanceID,
				nodeIP:         "10.28.88.205",
				processID:      0,
				leaseVersion:   1,
				restThread:     1,
				lastUsedTime:   time.Now(),
				idleFlag:       false,
			}
			key := fmt.Sprintf("%s%s%v", instanceID, InstanceSplicer, status.processID)
			instanceMap.Store(key, status)

			renewInstancesLease("test")
			time.Sleep(10 * time.Millisecond)
			convey.So(checkResult, convey.ShouldEqual, 0)
		})
		convey.Convey("renew instance lease while dile too long", func() {
			checkResult := 2
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
				gomonkey.ApplyFunc((*clientAdapter).ReleaseInstance,
					func(c *clientAdapter, req *schedulerapi.ReleaseInstanceReq) error {
						checkResult = 2
						return nil
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			status := &instanceStatus{
				funcKeyWithRes: funcKeyWithRes,
				instanceID:     instanceID,
				nodeIP:         "10.28.88.205",
				processID:      0,
				leaseVersion:   1,
				restThread:     1,
				lastUsedTime:   time.Now(),
				idleFlag:       true,
			}
			key := fmt.Sprintf("%s%s%v", instanceID, InstanceSplicer, status.processID)
			instanceMap.Store(key, status)

			time.Sleep(4 * time.Second)
			renewInstancesLease("test")
			time.Sleep(10 * time.Millisecond)
			convey.So(checkResult, convey.ShouldEqual, 2)
		})
		convey.Convey("renew instance lease while not idle but no long", func() {
			checkResult := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
				gomonkey.ApplyFunc((*clientAdapter).RetainInstance,
					func(c *clientAdapter, req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
						checkResult = 1
						return &schedulerapi.RetainInstanceRsp{}, nil
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			status := &instanceStatus{
				funcKeyWithRes: funcKeyWithRes,
				instanceID:     instanceID,
				nodeIP:         "10.28.88.205",
				processID:      0,
				leaseVersion:   1,
				restThread:     1,
				lastUsedTime:   time.Now(),
				idleFlag:       true,
			}
			key := fmt.Sprintf("%s%s%v", instanceID, InstanceSplicer, status.processID)
			instanceMap.Store(key, status)

			renewInstancesLease("test")
			time.Sleep(10 * time.Millisecond)
			convey.So(checkResult, convey.ShouldEqual, 1)
		})
	})
}

func TestRemoveIdleInstance(t *testing.T) {
	convey.Convey("TestRemoveIdleInstance", t, func() {
		convey.Convey("test remove idle instance while only one", func() {
			status := &instanceStatus{
				funcKeyWithRes: funcKeyWithRes,
				instanceID:     instanceID,
				nodeIP:         "10.28.88.205",
				processID:      0,
				leaseVersion:   1,
				restThread:     1,
				lastUsedTime:   time.Now(),
				idleFlag:       true,
			}
			key := fmt.Sprintf("%s%s%v", instanceID, InstanceSplicer, status.processID)
			list := []string{key}
			idleInstanceMap[funcKeyWithRes] = list
			length := len(idleInstanceMap[funcKeyWithRes])
			convey.So(length, convey.ShouldEqual, 1)

			removeIdleInstance(status, key)
			length = len(idleInstanceMap[funcKeyWithRes])
			convey.So(len(idleInstanceMap[funcKeyWithRes]), convey.ShouldEqual, 0)
		})
		convey.Convey("test remove idle instance while two", func() {
			status := &instanceStatus{
				funcKeyWithRes: funcKeyWithRes,
				instanceID:     instanceID,
				nodeIP:         "10.28.88.205",
				processID:      0,
				leaseVersion:   1,
				restThread:     1,
				lastUsedTime:   time.Now(),
				idleFlag:       true,
			}
			key := fmt.Sprintf("%s%s%v", instanceID, InstanceSplicer, status.processID)
			list := []string{key, "other"}
			idleInstanceMap[funcKeyWithRes] = list
			length := len(idleInstanceMap[funcKeyWithRes])
			convey.So(length, convey.ShouldEqual, 2)

			removeIdleInstance(status, key)
			length = len(idleInstanceMap[funcKeyWithRes])
			convey.So(length, convey.ShouldEqual, 1)
		})
	})
}

func TestCheckInstanceLease(t *testing.T) {
	convey.Convey("TestRenewInstancesLeaseScheduledTask", t, func() {
		convey.Convey("check instance successfully", func() {
			checkResult := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
				gomonkey.ApplyFunc((*clientAdapter).CheckInstance,
					func(c *clientAdapter, req *schedulerapi.CheckInstanceReq) error {
						funcKey := funcKeyWithRes + "0"
						if funcKey == req.FuncKeyWithRes && len(req.InstancesStatusMap) != 2 {
							return nil
						}
						if funcKey == req.FuncKeyWithRes &&
							len(req.InstancesStatusMap[instanceID+"0"].ProcessesStatusMap) != 2 {
							return nil
						}
						checkResult = 1
						return nil
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			instanceRecords =
				schedulerapi.RecoverInstancesRes{InstancesRes: make(map[string]*schedulerapi.CheckInstanceReq)}
			createInstance(funcKeyWithRes+"0", instanceID+"0", 0)
			createInstance(funcKeyWithRes+"0", instanceID+"0", 1)
			createInstance(funcKeyWithRes+"1", instanceID+"1", 2)
			createInstance(funcKeyWithRes+"0", instanceID+"2", 3)

			checkInstanceLease("test")
			convey.So(checkResult, convey.ShouldEqual, 1)

			deleteProcessorRecord(funcKeyWithRes+"0", instanceID+"0", 0)
			deleteProcessorRecord(funcKeyWithRes+"1", instanceID+"1", 2)
			processorRecoredLength := len(instanceRecords.InstancesRes)
			convey.So(processorRecoredLength, convey.ShouldEqual, 1)
			instancesStatusMapLength := len(instanceRecords.InstancesRes[funcKeyWithRes+"0"].InstancesStatusMap)
			convey.So(instancesStatusMapLength, convey.ShouldEqual, 2)
		})
		convey.Convey("check instance failed", func() {
			checkResult := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()
			createInstance(funcKeyWithRes+"0", instanceID+"0", 0)

			checkInstanceLease("test")
			convey.So(checkResult, convey.ShouldEqual, 0)
		})
	})
}

func createInstance(funcKey string, instanceId string, processorId uint32) {
	status := &instanceStatus{
		funcKeyWithRes: funcKey,
		instanceID:     instanceId,
		nodeIP:         "10.28.88.205",
		processID:      processorId,
		leaseVersion:   1,
		restThread:     1,
		lastUsedTime:   time.Now(),
		idleFlag:       false,
	}
	processStatus := &schedulerapi.ProcessStatus{ProcessorID: status.processID,
		RestThread: status.restThread, LeaseVersion: status.leaseVersion}
	createOrUpdateInstancesRecord(status.funcKeyWithRes, status.instanceID, processStatus)
	key := fmt.Sprintf("%s%s%v", status.instanceID, InstanceSplicer, status.processID)
	instanceMap.Store(key, status)
}

func TestRemoteScheduler(t *testing.T) {
	client := mockClient{}
	convey.Convey("TestRemoteScheduler", t, func() {
		convey.Convey("TestRemoteScheduler", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*Manager).getScheduler,
					func(c *Manager, funcKeyWithRes string) *clientAdapter {
						return &clientAdapter{}
					}),
				gomonkey.ApplyFunc((*clientAdapter).AcquireInstance, func(c *clientAdapter,
					req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
					return client.AcquireInstance(req)
				}),
				gomonkey.ApplyFunc((*clientAdapter).ReleaseInstance,
					func(c *clientAdapter, req *schedulerapi.ReleaseInstanceReq) error {
						return client.ReleaseInstance(req)
					}),
				gomonkey.ApplyFunc((*clientAdapter).RetainInstance, func(c *clientAdapter,
					req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
					return client.RetainInstance(req)
				}),
			}
			defer func() {
				for i := range patches {
					patches[i].Reset()
				}
			}()

			funcKeyWithResource := "7e1ad6a6cc5c44fabd5425873f72a86a" +
				"/0@base@testcaeinvokecangjieconcurrency200-cj/latest/1800-2048"

			instanceManager = &Manager{
				lb: loadbalance.NewConsistentHashWithVirtualNode(virtualNodeNum),
			}
			stopCh := make(chan struct{})
			go renewInstancesLeaseScheduledTask(stopCh)

			for i := 0; i < 100; i++ {
				go func(i int) {
					_, key, _ := AcquireInstance(funcKeyWithResource, fmt.Sprintf("traceID%d", i))
					time.Sleep(100 * time.Millisecond)
					ReleaseInstance(funcKeyWithResource, key, false, fmt.Sprintf("traceID%d", i))
				}(i)
			}
			time.Sleep(1000 * time.Millisecond)
			var result []string
			unique := make(map[string]bool)
			instanceMap.Range(func(key, value interface{}) bool {
				parts := strings.SplitN(key.(string), "&&", 2)
				if !unique[parts[0]] {
					unique[parts[0]] = true
					result = append(result, parts[0])
				}
				return true
			})
			fmt.Printf("instanceMap pod count: %d\n", len(result))
			fmt.Printf("idleInstanceMap count: %d\n", len(idleInstanceMap[funcKeyWithResource]))
			convey.So(len(result), convey.ShouldBeLessThan, 30)

			concurrent := int32(100)
			for i := 0; i < 10000; i++ {
				for {
					if atomic.LoadInt32(&concurrent) <= 0 {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					break
				}
				if i == 100 {
					// Simulate traffic fluctuations and expand the number of instances
					go func() {
						for i := 0; i < 100; i++ {
							go func(i int) {
								_, key, _ := AcquireInstance(funcKeyWithResource, fmt.Sprintf("traceID%d", i))
								time.Sleep(10 * time.Millisecond)
								ReleaseInstance(funcKeyWithResource, key, false, fmt.Sprintf("traceID%d", i))
							}(i)
						}
					}()
				}
				atomic.AddInt32(&concurrent, -1)
				go func(i int) {
					_, key, _ := AcquireInstance(funcKeyWithResource, fmt.Sprintf("traceID%d", i))
					rand.Seed(time.Now().UnixNano())
					randomMs := rand.Intn(100) + 1
					delay := time.Duration(randomMs) * time.Millisecond
					time.Sleep(delay)
					ReleaseInstance(funcKeyWithResource, key, false, fmt.Sprintf("traceID%d", i))
					atomic.AddInt32(&concurrent, 1)
				}(i)
			}
			var result2 []string
			unique2 := make(map[string]bool)
			instanceMap.Range(func(key, value interface{}) bool {
				parts := strings.SplitN(key.(string), "&&", 2)
				if !unique2[parts[0]] {
					unique2[parts[0]] = true
					result2 = append(result2, parts[0])
				}
				return true
			})
			fmt.Printf("instanceMap pod count: %d\n", len(result2))
			fmt.Printf("idleInstanceMap count: %d\n", len(idleInstanceMap[funcKeyWithResource]))
			convey.So(len(result), convey.ShouldBeLessThan, 30)
			close(stopCh)
		})
	})
}

type mockClient struct {
}

type mockInstance struct {
	instanceKey  string
	instanceIP   string
	instancePort string
	nodeIP       string
	nodePort     string
	processors   []*mockProcessor
	// used to store extra processor after decrease the concurrency
	extraProcessorsMap map[uint32]*mockProcessor
	idleProcessorNum   int
	applier            string
	recovered          bool
	concurrency        int
}

type mockProcessor struct {
	processorID    uint32
	threadNum      int
	restThread     int
	leased         bool
	leaseVersion   uint64
	leaseStartTime time.Time
}

var (
	instancesList []*mockInstance
	instancesMap  = make(map[string]*mockInstance)
	lock          sync.Mutex
)

func (service *mockClient) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	lock.Lock()
	defer lock.Unlock()
	return acquireInstance(), nil
}

func acquireInstance() *schedulerapi.AcquireInstanceRsp {
	for i := range instancesList {
		processor := instancesList[i].acquireProcessor()
		if processor != nil {
			return &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  instancesList[i].instanceKey,
				ProcessorID:  processor.processorID,
				LeaseVersion: processor.leaseVersion,
				ThreadNum:    uint32(processor.threadNum),
				RestThread:   uint32(processor.restThread),
				NodeIP:       instancesList[i].nodeIP,
			}
		}
	}
	ins := createMockInstance()
	instancesList = append(instancesList, ins)
	instancesMap[ins.instanceKey] = ins
	processor := instancesList[len(instancesList)-1].acquireProcessor()
	if processor != nil {
		return &schedulerapi.AcquireInstanceRsp{
			InstanceKey:  instancesList[len(instancesList)-1].instanceKey,
			ProcessorID:  processor.processorID,
			LeaseVersion: processor.leaseVersion,
			ThreadNum:    uint32(processor.threadNum),
			RestThread:   uint32(processor.restThread),
			NodeIP:       instancesList[len(instancesList)-1].nodeIP,
		}
	}
	return nil
}

func createMockInstance() *mockInstance {
	instance := &mockInstance{
		instanceKey:      fmt.Sprintf("pool30-1800-2048-fusion-d555957d6-%s", randStr(5)),
		instanceIP:       "127.0.0.1",
		instancePort:     "8080",
		processors:       make([]*mockProcessor, 0),
		idleProcessorNum: 4,
		concurrency:      4,
		nodeIP:           "127.0.0.1",
		nodePort:         "8080",
	}
	instance.initProcessors(4)
	return instance
}

func (instance *mockInstance) initProcessors(concurrency int) {
	instance.processors = make([]*mockProcessor, 0)
	averageNum := concurrency / 10
	processorNum := 0
	if averageNum == 0 {
		averageNum = 1
		processorNum = concurrency
	} else {
		processorNum = 10
	}
	for i := 0; i < processorNum; i++ {
		p := &mockProcessor{
			processorID:  uint32(i),
			threadNum:    averageNum,
			restThread:   averageNum,
			leaseVersion: 1,
		}
		if i == 0 {
			p.threadNum += concurrency - averageNum*processorNum
			p.restThread = p.threadNum
		}
		instance.processors = append(instance.processors, p)
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStr(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Int63()%int64(len(letters))]
	}
	return string(b)
}

func (instance *mockInstance) acquireProcessor() *mockProcessor {
	for _, processor := range instance.processors {
		if processor.leased {
			continue
		}
		processor.restThread--
		if processor.restThread > 0 {
			return processor
		}
		processor.leased = true
		processor.leaseStartTime = time.Now()
		instance.idleProcessorNum--
		return processor
	}
	return nil
}

func (service *mockClient) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	lock.Lock()
	defer lock.Unlock()
	ins, ok := instancesMap[req.InstanceKey]
	if !ok {
		return errors.New("instance not exist")
	}
	_, err := ins.releaseProcessor(req.ProcessorID, req.LeaseVersion)
	return err
}

func (instance *mockInstance) releaseProcessor(processorID uint32, leaseVersion uint64) (int, error) {
	if int(processorID) >= len(instance.processors) {
		return -1, errors.New("invalid processorID")
	}
	processor := instance.processors[processorID]
	if processor.leaseVersion != leaseVersion {
		return -1, fmt.Errorf("leaseVersion %d is not valid, valid leaseVersion %d",
			leaseVersion, processor.leaseVersion)
	}
	processor.restThread++
	processor.leased = false
	processor.leaseVersion++
	instance.idleProcessorNum++
	return instance.idleProcessorNum, nil
}

func (service *mockClient) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
	return nil
}

func (service *mockClient) RetainInstance(req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	return &schedulerapi.RetainInstanceRsp{}, nil
}
