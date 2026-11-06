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

// Package insscheduler contains function instance scheduler logic
package insscheduler

import (
	"errors"
	"math/rand"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/scheduler/common"
)

type defaultStrategy struct {
	instances        map[string]*instance
	reserved         []*instance
	onDemand         []*instance
	concurrency      int
	funcKeyWithRes   string
	rnd              *rand.Rand
	scheduleStrategy func(strategy *defaultStrategy) (*schedulerapi.AcquireInstanceRsp, error)
}

func createDefaultStrategy(concurrency int, isBridge bool, funcKeyWithRes string) strategy {
	ds := &defaultStrategy{
		instances:        map[string]*instance{},
		reserved:         []*instance{},
		onDemand:         []*instance{},
		concurrency:      concurrency,
		funcKeyWithRes:   funcKeyWithRes,
		rnd:              rand.New(rand.NewSource(time.Now().UnixNano())),
		scheduleStrategy: defaultScheduleStrategy,
	}
	if isBridge {
		log.GetLogger().Infof("use bridgeScheduleStrategy for %s", funcKeyWithRes)
		ds.scheduleStrategy = bridgeScheduleStrategy
	}
	return ds
}

func (strategy *defaultStrategy) addInstance(newInstance *common.Instance, t instanceType) *instance {
	instanceKey := newInstance.InstanceKey
	instance, ok := strategy.instances[instanceKey]
	if ok {
		log.GetLogger().Infof("instance %s already exist", instanceKey)
		return instance
	}
	instance = createInstance(newInstance, strategy.concurrency, false)
	if t == reservedInstance {
		strategy.reserved = append(strategy.reserved, instance)
	} else {
		strategy.onDemand = append(strategy.onDemand, instance)
	}

	strategy.instances[instanceKey] = instance
	return instance
}

func (strategy *defaultStrategy) deleteInstance(instanceKey string) {
	delete(strategy.instances, instanceKey)
	for i, instance := range strategy.reserved {
		if instance.instanceKey != instanceKey {
			continue
		}
		strategy.reserved = append(strategy.reserved[:i], strategy.reserved[i+1:]...)
		return
	}
	for i, instance := range strategy.onDemand {
		if instance.instanceKey != instanceKey {
			continue
		}
		strategy.onDemand = append(strategy.onDemand[:i], strategy.onDemand[i+1:]...)
		return
	}
}

func acquireInstance(instances []*instance) *schedulerapi.AcquireInstanceRsp {
	for i := range instances {
		processor := instances[i].acquireProcessor()
		if processor != nil {
			return &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  instances[i].instanceKey,
				ProcessorID:  processor.processorID,
				LeaseVersion: processor.leaseVersion,
				ThreadNum:    uint32(processor.threadNum),
				RestThread:   uint32(processor.restThread),
				NodeIP:       instances[i].nodeIP,
			}
		}
	}
	return nil
}

func (strategy *defaultStrategy) acquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	return strategy.scheduleStrategy(strategy)
}

func defaultScheduleStrategy(strategy *defaultStrategy) (*schedulerapi.AcquireInstanceRsp, error) {
	strategy.rnd.Shuffle(len(strategy.reserved), func(i, j int) {
		strategy.reserved[i], strategy.reserved[j] = strategy.reserved[j], strategy.reserved[i]
	})
	if rsp := acquireInstance(strategy.reserved); rsp != nil {
		return rsp, nil
	}
	if rsp := acquireInstance(strategy.onDemand); rsp != nil {
		return rsp, nil
	}
	return &schedulerapi.AcquireInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: snerror.NoIdleInstanceErrorCode,
		},
	}, nil
}

func bridgeScheduleStrategy(strategy *defaultStrategy) (*schedulerapi.AcquireInstanceRsp, error) {
	strategy.rnd.Shuffle(len(strategy.onDemand), func(i, j int) {
		strategy.onDemand[i], strategy.onDemand[j] = strategy.onDemand[j], strategy.onDemand[i]
	})
	if rsp := acquireInstance(strategy.onDemand); rsp != nil {
		return rsp, nil
	}
	return &schedulerapi.AcquireInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: snerror.NoIdleInstanceErrorCode,
		},
	}, nil
}

func (strategy *defaultStrategy) releaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	instance, ok := strategy.instances[req.InstanceKey]
	if !ok {
		return errors.New("instance not exist")
	}
	_, err := instance.releaseProcessor(req.ProcessorID, req.LeaseVersion)
	return err
}

func (strategy *defaultStrategy) checkInstance(req *schedulerapi.CheckInstanceReq) {
	if req == nil || req.InstancesStatusMap == nil {
		log.GetLogger().Warnf("checkInstanceReq invalid")
		return
	}
	for instanceKey, instancesStatus := range req.InstancesStatusMap {
		ins, ok := strategy.instances[instanceKey]
		if !ok {
			log.GetLogger().Warnf("failed to check instance %s for instance not exist", instanceKey)
			continue
		}
		resetProcessStatus(instancesStatus.ProcessesStatusMap, ins)
	}
}

func resetProcessStatus(processMap map[uint32]*schedulerapi.ProcessStatus, ins *instance) {
	if ins == nil || processMap == nil {
		return
	}
	for processID, processStatus := range processMap {
		if int(processID) >= len(ins.processors) {
			log.GetLogger().Warnf("failed to check instance %s for invalid processorID %d",
				ins.instanceKey, processID)
			continue
		}
		processorOrigin := ins.processors[processID]
		if processStatus.LeaseVersion <= processorOrigin.leaseVersion {
			continue
		}
		processorOrigin.leaseVersion = processStatus.LeaseVersion
		if processorOrigin.restThread == int(processStatus.RestThread) {
			log.GetLogger().Debugf("succeed to reset instance %s, processID %d, restThread %d, "+
				"LeaseVersion %d", ins.instanceKey, processID, processorOrigin.restThread, processorOrigin.leaseVersion)
			continue
		}
		// origin processor restThread is 0, set to the processStatus's restThread, idleProcessorNum++
		if processorOrigin.restThread == 0 {
			ins.idleProcessorNum++
		}
		processorOrigin.restThread = int(processStatus.RestThread)
		if processorOrigin.restThread > 0 {
			processorOrigin.leased = false
			log.GetLogger().Infof("succeed to reset instance %s, processID %d, restThread %d, "+
				"LeaseVersion %d", ins.instanceKey, processID, processorOrigin.restThread, processorOrigin.leaseVersion)
			continue
		}
		// origin processor restThread not 0, set to 0, idleProcessorNum--
		processorOrigin.leased = true
		processorOrigin.leaseStartTime = time.Now()
		ins.idleProcessorNum--
		log.GetLogger().Infof("succeed to reset instance %s, processID %d, restThread %d, "+
			"LeaseVersion %d", ins.instanceKey, processID, processorOrigin.restThread, processorOrigin.leaseVersion)
	}

}

func (strategy *defaultStrategy) retainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	instance, ok := strategy.instances[req.InstanceKey]
	if !ok {
		return &schedulerapi.RetainInstanceRsp{
			Error: schedulerapi.Error{
				ErrorCode: snerror.InstanceNotExistErrorCode,
			},
		}, nil
	}

	err := instance.retainInstance(req.ProcessorID, req.LeaseVersion)
	if err != nil {
		return &schedulerapi.RetainInstanceRsp{
			Error: schedulerapi.Error{
				ErrorCode:    snerror.InstanceInternalError,
				ErrorMessage: err.Error(),
			},
		}, nil
	}

	return &schedulerapi.RetainInstanceRsp{}, nil
}

func (strategy *defaultStrategy) checkLease() {
	for _, instance := range strategy.instances {
		now := time.Now()
		instance.checkLease(now)
	}
}

func (strategy *defaultStrategy) updateConcurrency(concurrency int) {
	strategy.concurrency = concurrency
	for _, instance := range strategy.instances {
		instance.updateConcurrency(concurrency)
	}
}

func (strategy *defaultStrategy) countInstance() int {
	return len(strategy.instances)
}

// useless for normal function instance
func (strategy *defaultStrategy) recoverAllConnStatus(funcKeyWithRes string) {
	return
}
