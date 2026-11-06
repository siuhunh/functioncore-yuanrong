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

// Package insscheduler contains function instance scheduler logic
package insscheduler

import (
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/utils"
)

const uintConvertorBase = 10

type centralizedStrategy struct {
	instances         map[string]*instance
	instanceOrders    []*instance
	concurrency       int
	scalingOutNum     uint64
	nextMockIndex     uint64
	funcKeyWithRes    string
	businessType      string
	enableRemoteDebug bool
}

func createCentralizedStrategy(concurrency int, funcKeyWithRes, businessType string, enableRemoteDebug bool) strategy {
	return &centralizedStrategy{
		instances:         map[string]*instance{},
		instanceOrders:    []*instance{},
		concurrency:       concurrency,
		scalingOutNum:     0,
		nextMockIndex:     0,
		funcKeyWithRes:    funcKeyWithRes,
		businessType:      businessType,
		enableRemoteDebug: enableRemoteDebug,
	}
}

func (strategy *centralizedStrategy) addInstance(instance *common.Instance, t instanceType) *instance {
	// placeholder instance provider: when scaling out function instance, generate a placeholder instance in cache
	if instance.InstanceKey == common.InstancePlaceHolderPrefix {
		return strategy.addPlaceHolderInstance()
	}

	if instance.InstanceKey == "" || instance.PodIP == "" || instance.Applier == "" {
		log.GetLogger().Errorf("failed to add instance for function %s, instance %v", strategy.funcKeyWithRes, instance)
		return nil
	}
	instanceKey := instance.InstanceKey
	log.GetLogger().Infof("instance key: %s | instance ip: %s | applier: %s | strategy: centralized strategy "+
		"| start to add instance to cache", instance.InstanceKey, instance.PodIP, instance.Applier)
	curInstance, ok := strategy.instances[instanceKey]
	if ok {
		log.GetLogger().Infof("instance %s already exist", instanceKey)
		return curInstance
	}
	switch t {
	case reservedInstance:
		newInstance := createInstance(instance, strategy.concurrency, true)
		strategy.instanceOrders = append(strategy.instanceOrders, nil)
		copy(strategy.instanceOrders[1:], strategy.instanceOrders[0:])
		strategy.instanceOrders[0] = newInstance
		strategy.instances[instanceKey] = newInstance
		return newInstance
	case websocketInstance:
		log.GetLogger().Debugf("start to add websocket function instance")
		// happy path: consume existing placeholder instance from instanceOrders array
		// placeholder instance consumer: when etcd event came, consume a placeholder instance in cache
		if strategy.scalingOutNum > 0 {
			targetIns := strategy.removePlaceHolderInstance(instance)
			if targetIns != nil {
				return targetIns
			}
			log.GetLogger().Warnf("no placeholder can be replaced when scalingOutNum is zero")
		}
		log.GetLogger().Warnf("new instance key: %s | no placeholder instances can be replaced",
			instance.InstanceKey)
		// bad path: if placeholder instances scaling out timeout, it will remove itself from instance array
		// when etcd event arrives, create a new instance and append to the end of instance array
		// start a goroutine to scale in the instance if not used
		newInstance := createInstance(instance, strategy.concurrency, true)
		strategy.instanceOrders = append(strategy.instanceOrders, newInstance)
		strategy.instances[instanceKey] = newInstance
		go strategy.instanceScaleInCountdown("", newInstance.instanceKey, newInstance.stopScaleInChan)
		return newInstance
	default:
		// should not reach default
		return nil
	}
}

func (strategy *centralizedStrategy) addPlaceHolderInstance() *instance {
	instanceKey := common.InstancePlaceHolderPrefix + strconv.FormatUint(strategy.nextMockIndex, uintConvertorBase)
	strategy.scalingOutNum++
	strategy.nextMockIndex++
	newInstance, ok := strategy.instances[instanceKey]
	log.GetLogger().Infof("placeholder instance key: %s | current scaling out num: %d | next mock index: %d | "+
		"create placeholder function instance", instanceKey, strategy.scalingOutNum, strategy.nextMockIndex)
	if ok {
		log.GetLogger().Errorf("instance %s already exist when creating placeholder instance", instanceKey)
		return newInstance
	}
	mockInstance := createInstance(&common.Instance{InstanceKey: instanceKey}, strategy.concurrency, true)
	strategy.instanceOrders = append(strategy.instanceOrders, mockInstance)
	strategy.instances[instanceKey] = mockInstance
	// hold the first concurrency for the AcquireInstance request which triggered scaling out
	mockInstance.processors[0].leased = true
	mockInstance.idleProcessorNum--
	mockInstance.processors[0].isWebSocket = true
	return mockInstance
}

func (strategy *centralizedStrategy) removePlaceHolderInstance(newInstance *common.Instance) *instance {
	targetKey := ""
	for _, ins := range strategy.instanceOrders {
		if utils.IsPlaceHolderInstance(ins.instanceKey) {
			targetKey = ins.instanceKey
			break
		}
	}
	log.GetLogger().Infof("new instance key: %s | removed mock key: %s | current scaling out num: %d | next "+
		"mock index: %d | succeed to remove placeholder function instance",
		newInstance.InstanceKey, targetKey, strategy.scalingOutNum, strategy.nextMockIndex)
	if targetKey == "" {
		strategy.nextMockIndex = 0
		strategy.scalingOutNum = 0
		return nil
	}
	mockInstance := strategy.instances[targetKey]
	delete(strategy.instances, targetKey)
	strategy.instances[newInstance.InstanceKey] = mockInstance
	mockInstance.instanceKey = newInstance.InstanceKey
	mockInstance.instanceIP = newInstance.PodIP
	mockInstance.instancePort = newInstance.PodPort
	mockInstance.nodeIP = newInstance.NodeIP
	mockInstance.nodePort = newInstance.NodePort
	mockInstance.notifyAllWaitingRequest()
	strategy.scalingOutNum--
	if strategy.scalingOutNum == 0 {
		strategy.nextMockIndex = 0
	}
	return mockInstance
}

func (strategy *centralizedStrategy) deleteInstance(instanceKey string) {
	curInstance, ok := strategy.instances[instanceKey]
	delete(strategy.instances, instanceKey)
	for i, insInInstanceOrders := range strategy.instanceOrders {
		if insInInstanceOrders.instanceKey != instanceKey {
			continue
		}
		strategy.instanceOrders = append(strategy.instanceOrders[:i], strategy.instanceOrders[i+1:]...)
		break
	}
	// placeholder instance consumer: if the placeholder instance can not be replaced within sometime,
	// scheduler will set countdown and remove the placeholder instance
	if ok && utils.IsPlaceHolderInstance(instanceKey) {
		strategy.scalingOutNum--
		if strategy.scalingOutNum == 0 {
			strategy.nextMockIndex = 0
		}
		curInstance.notifyAllWaitingRequestScaleOutFailed()
	}
}

func (strategy *centralizedStrategy) acquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	var rsp *schedulerapi.AcquireInstanceRsp
	rsp = strategy.acquireWebsocketInstance(req)
	if rsp == nil {
		return nil, errors.New("acquire function instance error")
	}
	// return directly because no idle instance left
	if rsp.Error.ErrorCode == snerror.NoIdleInstanceErrorCode {
		return rsp, nil
	}
	// if the elastic instance is used again(busy processor number 0 -> 1), close the scale in countdown
	// rsp.InstanceKey must exit if ErrorCode is not NoIdleInstanceErrorCode
	ins, ok := strategy.instances[rsp.InstanceKey]
	if ok && !utils.IsPlaceHolderInstance(rsp.InstanceKey) && // use placeholder instance
		ins.applier != constants.WorkerManagerApplier &&
		ins.idleProcessorNum == strategy.concurrency-1 {
		ins.stopScaleInChan <- struct{}{}
	}
	return rsp, nil
}

func (strategy *centralizedStrategy) acquireRemoteDebugIns() *schedulerapi.AcquireInstanceRsp {
	if len(strategy.instanceOrders) == 0 {
		return &schedulerapi.AcquireInstanceRsp{
			Error: schedulerapi.Error{
				ErrorCode: snerror.RemoteDebugInsNotReadyErrorCode,
			},
		}
	}
	if len(strategy.instanceOrders) > 1 {
		return &schedulerapi.AcquireInstanceRsp{
			Error: schedulerapi.Error{
				ErrorCode: snerror.MoreThanOneInstanceErrorCode,
			},
		}
	}
	// remote debug only have one instance
	ins := strategy.instanceOrders[0]
	return &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  ins.instanceKey,
		ProcessorID:  0,
		InstanceIP:   ins.instanceIP,
		InstancePort: common.WorkerDefaultWebSocketPort,
		NodeIP:       ins.nodeIP,
		NodePort:     ins.nodePort,
	}
}

func (strategy *centralizedStrategy) acquireWebsocketInstance(
	req *schedulerapi.AcquireInstanceReq) *schedulerapi.AcquireInstanceRsp {
	if strategy.enableRemoteDebug && commonUtils.IsCAEFunc(strategy.businessType) {
		return strategy.acquireRemoteDebugIns()
	}
	for _, curInstance := range strategy.instanceOrders {
		processor, isPlaceHolder := curInstance.acquireWebSocketProcessor()
		if processor != nil {
			rsp := &schedulerapi.AcquireInstanceRsp{
				InstanceKey:  curInstance.instanceKey,
				ProcessorID:  processor.processorID,
				InstanceIP:   curInstance.instanceIP,
				InstancePort: common.WorkerDefaultWebSocketPort,
				NodeIP:       curInstance.nodeIP,
				NodePort:     curInstance.nodePort,
			}
			if isPlaceHolder {
				rsp.Error = schedulerapi.Error{
					ErrorCode: snerror.GetPlaceHolderInstanceError,
				}
				rsp.PlaceHolderChan = curInstance.websocketReadyChan
			}
			log.GetLogger().Infof("traceID: %s | acquiredIns: %s | processorID: %d | isRecovered: %t | acquired "+
				"wsInstance", req.TraceID, curInstance.instanceKey, processor.processorID, curInstance.recovered)
			return rsp
		}
	}
	return &schedulerapi.AcquireInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: snerror.NoIdleInstanceErrorCode,
		},
	}
}

func (strategy *centralizedStrategy) releaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	targetInstance, ok := strategy.instances[req.InstanceKey]
	if !ok {
		return errors.New("instance not exist")
	}
	var idleInstanceNum int
	var err error
	log.GetLogger().Infof("traceID: %s | funcKey: %s | instanceKey: %s | isWebSocket: %t | "+
		"start to release processor", req.TraceID, req.FuncKeyWithRes, req.InstanceKey, req.IsWebSocketReq)
	if req.IsWebSocketReq {
		idleInstanceNum, err = targetInstance.releaseWebSocketProcessor(req.ProcessorID)
	} else {
		idleInstanceNum, err = targetInstance.releaseProcessor(req.ProcessorID, req.LeaseVersion)
	}
	if err != nil {
		return err
	}
	// skip to scale in placeholder and reserved instance
	if !utils.IsPlaceHolderInstance(targetInstance.instanceKey) &&
		strategy.instances[req.InstanceKey].applier != constants.WorkerManagerApplier &&
		idleInstanceNum == strategy.concurrency {
		// move the target instance which may be scaled in to the end of the order array
		// reduce the probability of reusing
		for i, insInInstanceOrders := range strategy.instanceOrders {
			if insInInstanceOrders.instanceKey != req.InstanceKey {
				continue
			}
			strategy.instanceOrders = append(strategy.instanceOrders[:i], strategy.instanceOrders[i+1:]...)
			strategy.instanceOrders = append(strategy.instanceOrders, targetInstance)
			break
		}
		log.GetLogger().Infof("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | start to set countdown "+
			"for scaling in", req.TraceID, req.FuncKeyWithRes, req.InstanceKey)
		go strategy.instanceScaleInCountdown(req.TraceID, targetInstance.instanceKey, targetInstance.stopScaleInChan)
	}
	return nil
}

// unused for websocket function instance
func (strategy *centralizedStrategy) retainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	return nil, errors.New("centralized strategy does not support retainInstance yet")
}

func (strategy *centralizedStrategy) checkLease() {
	for _, instance := range strategy.instances {
		instance.checkLease(time.Now())
	}
}

func (strategy *centralizedStrategy) updateConcurrency(concurrency int) {
	strategy.concurrency = concurrency
	for _, instance := range strategy.instances {
		instance.updateCentralizedConcurrency(concurrency)
	}
}

func (strategy *centralizedStrategy) countInstance() int {
	return len(strategy.instances)
}

func (strategy *centralizedStrategy) instanceScaleInCountdown(traceID, instanceKey string, stopChan chan struct{}) {
	log.GetLogger().Infof("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | start to countdown for "+
		"scaling in", traceID, strategy.funcKeyWithRes, instanceKey)
	select {
	case <-stopChan:
		log.GetLogger().Infof("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | instance is used again "+
			"and stop counting down for scaling in", traceID, strategy.funcKeyWithRes, instanceKey)
		return
	case <-time.After(time.Minute):
		utils.ScaleInInstance(instanceKey, strategy.funcKeyWithRes, traceID)
	}
}

func (strategy *centralizedStrategy) recoverAllConnStatus(funcKeyWithRes string) {
	var needDeletedIns []*instance
	for _, ins := range strategy.instances {
		traceID := uuid.New().String()
		total, ws, err := utils.SendCountWebSocketConRequestToWorker(ins.instanceIP, common.WorkerServerPort, traceID)
		if err != nil {
			log.GetLogger().Errorf("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | failed to get request "+
				"counter for worker", traceID, funcKeyWithRes, ins.instanceKey)
			needDeletedIns = append(needDeletedIns, ins)
		}
		if int(total) > ins.concurrency {
			log.GetLogger().Warnf("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | total used request "+
				"number is larger than original concurrency", traceID, funcKeyWithRes, ins.instanceKey)
			total = uint32(ins.concurrency)
		}
		if total > 0 {
			ins.recovered = true
			ins.idleProcessorNum -= int(total)
			if ins.idleProcessorNum < 0 {
				log.GetLogger().Warnf("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | total: %d | concurrency: "+
					"%d | failed to get request counter for worker, because the total conn number is large than "+
					"concurrency", traceID, funcKeyWithRes, ins.instanceKey, total, ins.concurrency)
				ins.idleProcessorNum = 0
			}
			ins.stopScaleInChan <- struct{}{}
			log.GetLogger().Infof("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | stop scale in",
				traceID, funcKeyWithRes, ins.instanceKey)
		}
		log.GetLogger().Infof("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | total: %d | ws: %d | "+
			"succeed to get request counter for worker", traceID, funcKeyWithRes, ins.instanceKey, total, ws)
	}
	for _, ins := range needDeletedIns {
		strategy.deleteInstance(ins.instanceKey)
	}
}
