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
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/faas-scheduler/scheduler/common"
)

type defaultStrategy struct {
	instances      map[string]*instance
	instanceOrders []*instance
	concurrency    int
	funcKeyWithRes string
}

func createDefaultStrategy(concurrency int, funcKeyWithRes string) strategy {
	return &defaultStrategy{
		instances:      map[string]*instance{},
		instanceOrders: []*instance{},
		concurrency:    concurrency,
		funcKeyWithRes: funcKeyWithRes,
	}
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
		strategy.instanceOrders = append(strategy.instanceOrders, nil)
		copy(strategy.instanceOrders[1:], strategy.instanceOrders[0:])
		strategy.instanceOrders[0] = instance
	} else {
		strategy.instanceOrders = append(strategy.instanceOrders, instance)
	}

	strategy.instances[instanceKey] = instance
	return instance
}

func (strategy *defaultStrategy) deleteInstance(instanceKey string) {
	delete(strategy.instances, instanceKey)
	for i, instance := range strategy.instanceOrders {
		if instance.instanceKey != instanceKey {
			continue
		}
		strategy.instanceOrders = append(strategy.instanceOrders[:i], strategy.instanceOrders[i+1:]...)
		break
	}
}

func (strategy *defaultStrategy) acquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	for _, instance := range strategy.instanceOrders {
		processor := instance.acquireProcessor()
		if processor != nil {
			return &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  instance.instanceKey,
				ProcessorID:  processor.processorID,
				LeaseVersion: processor.leaseVersion,
				ThreadNum:    uint32(processor.threadNum),
			}, nil
		}
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
