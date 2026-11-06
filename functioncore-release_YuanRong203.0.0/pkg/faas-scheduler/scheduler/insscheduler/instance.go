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
	"fmt"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/utils"
)

type processor struct {
	processorID     uint32
	threadNum       int
	restThread      int
	leased          bool
	leaseVersion    uint64
	leaseStartTime  time.Time
	isWebSocket     bool
	rdispatcherIP   string
	rdispatcherPort string
}

type instance struct {
	instanceKey  string
	instanceIP   string
	instancePort string
	nodeIP       string
	nodePort     string
	processors   []*processor
	// used to store extra processor after decrease the concurrency
	extraProcessorsMap map[uint32]*processor
	idleProcessorNum   int
	applier            string
	recovered          bool
	concurrency        int
	stopScaleInChan    chan struct{}
	websocketReadyChan chan *schedulerapi.WebsocketInsInfo
}

const (
	maxProcessorNum                         = 10
	maxWebsocketFunctionInstanceConcurrency = 200
)

func createInstance(newInstance *common.Instance, concurrency int, isWebSocketProcessor bool) *instance {
	instance := &instance{
		instanceKey:        newInstance.InstanceKey,
		instanceIP:         newInstance.PodIP,
		instancePort:       newInstance.PodPort,
		processors:         make([]*processor, 0),
		websocketReadyChan: make(chan *schedulerapi.WebsocketInsInfo, concurrency),
		idleProcessorNum:   concurrency,
		concurrency:        concurrency,
		applier:            newInstance.Applier,
		nodeIP:             newInstance.NodeIP,
		nodePort:           newInstance.NodePort,
		stopScaleInChan:    make(chan struct{}, 1),
		recovered:          false,
		extraProcessorsMap: make(map[uint32]*processor),
	}
	if isWebSocketProcessor {
		instance.initWebSocketProcessors(concurrency)
	} else {
		instance.initProcessors(concurrency)
	}
	return instance
}

func (instance *instance) initWebSocketProcessors(concurrency int) {
	instance.processors = make([]*processor, 0)
	if concurrency > maxWebsocketFunctionInstanceConcurrency {
		concurrency = maxWebsocketFunctionInstanceConcurrency
	}
	for i := 0; i < concurrency; i++ {
		wsProcessor := &processor{
			processorID:  uint32(i),
			leaseVersion: 1,
		}
		instance.processors = append(instance.processors, wsProcessor)
	}
}

func (instance *instance) initProcessors(concurrency int) {
	instance.processors = make([]*processor, 0)
	averageNum := concurrency / maxProcessorNum
	processorNum := 0
	if averageNum == 0 {
		averageNum = 1
		processorNum = concurrency
	} else {
		processorNum = maxProcessorNum
	}
	for i := 0; i < processorNum; i++ {
		p := &processor{
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

func (instance *instance) updateConcurrency(concurrency int) {
	instance.initProcessors(concurrency)
}

func (instance *instance) updateCentralizedConcurrency(concurrency int) {
	if concurrency > instance.concurrency {
		for i := instance.concurrency + 1; i <= concurrency; i++ {
			p := &processor{
				processorID:  uint32(i - 1),
				leaseVersion: 1,
			}
			instance.processors = append(instance.processors, p)
		}
		instance.idleProcessorNum += concurrency - instance.concurrency
		instance.concurrency = concurrency
		return
	}
	newProcessorArr := instance.processors[0:concurrency]
	extraProcessorArr := instance.processors[concurrency:]
	newIdleNum := concurrency
	for _, p := range newProcessorArr {
		if p.leased {
			newIdleNum--
		}
	}
	for _, p := range extraProcessorArr {
		if p.leased {
			instance.extraProcessorsMap[p.processorID] = p
		}
	}
	instance.processors = newProcessorArr
	instance.idleProcessorNum = newIdleNum
	instance.concurrency = concurrency
}

func (instance *instance) acquireProcessor() *processor {
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

func (instance *instance) releaseProcessor(processorID uint32, leaseVersion uint64) (int, error) {
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

func (instance *instance) retainInstance(processorID uint32, leaseVersion uint64) error {
	if int(processorID) >= len(instance.processors) {
		return errors.New("invalid processorID")
	}
	processor := instance.processors[processorID]
	if processor.leaseVersion != leaseVersion {
		return fmt.Errorf("leaseVersion %d is not valid, valid leaseVersion %d",
			leaseVersion, processor.leaseVersion)
	}

	if !processor.leased {
		return errors.New("processor is not leased")
	}

	processor.leaseStartTime = time.Now()
	return nil
}

func (instance *instance) checkLease(now time.Time) {
	leaseSpan := time.Duration(config.FConfig.InstanceLeaseConfig.LeaseSpan) * time.Millisecond
	for _, processor := range instance.processors {
		if !processor.leased || processor.isWebSocket {
			continue
		}
		if now.Sub(processor.leaseStartTime) > leaseSpan {
			processorKey := urnutils.GetProcessorKey(
				instance.instanceKey, processor.processorID, processor.leaseVersion)
			log.GetLogger().Warnf("processorKey %s lease time out",
				processorKey)
			processor.leased = false
			processor.restThread++
			processor.leaseVersion++
			instance.idleProcessorNum++
		}
	}
}

// acquireWebSocketProcessor return available processor and flag reveal if the processor is placeholder
func (instance *instance) acquireWebSocketProcessor() (*processor, bool) {
	if instance.recovered {
		if instance.idleProcessorNum > 0 && instance.idleProcessorNum < instance.concurrency {
			instance.idleProcessorNum--
			// after recovery the instance must not be placeholder
			return instance.processors[instance.idleProcessorNum], false
		}
		return nil, false
	}
	for _, curProcessor := range instance.processors {
		log.GetLogger().Debugf("acquiring websocket processor %v", curProcessor)
		if curProcessor.leased {
			continue
		}
		curProcessor.leased = true
		curProcessor.isWebSocket = true
		instance.idleProcessorNum--
		return curProcessor, utils.IsPlaceHolderInstance(instance.instanceKey)
	}
	return nil, false
}

func (instance *instance) releaseWebSocketProcessor(processorID uint32) (int, error) {
	if instance.recovered {
		instance.idleProcessorNum++
		if instance.idleProcessorNum == instance.concurrency {
			instance.recovered = false
			log.GetLogger().Infof("instanceKey: %s | converted to NormalMode from Recovered Mode", instance.instanceKey)
		}
		return instance.idleProcessorNum, nil
	}
	if int(processorID) >= len(instance.processors) {
		// delete extra processor generated by updating concurrency which less than old concurrency
		_, ok := instance.extraProcessorsMap[processorID]
		if ok {
			delete(instance.extraProcessorsMap, processorID)
			return instance.idleProcessorNum, nil
		}
		return -1, errors.New("invalid processorID")
	}
	processor := instance.processors[processorID]
	if !processor.leased {
		return -1, errors.New("trying to release unreleased processor and the processor has been released before")
	}
	processor.leased = false
	processor.isWebSocket = false
	instance.idleProcessorNum++
	return instance.idleProcessorNum, nil
}

func (instance *instance) notifyAllWaitingRequest() {
	log.GetLogger().Debugf("instanceKey: %s | start to notify all requests waiting for scaling out",
		instance.instanceKey)
	for _, processor := range instance.processors {
		if processor.leased {
			log.GetLogger().Debugf("instanceKey: %s | processorID: %d | start to notify all requests waiting "+
				"for scaling out", instance.instanceKey, processor.processorID)
			instance.websocketReadyChan <- &schedulerapi.WebsocketInsInfo{
				PodPort:     instance.instancePort,
				PodIP:       instance.instanceIP,
				PodName:     instance.instanceKey,
				ProcessorID: processor.processorID,
				NodeIP:      instance.nodeIP,
				NodePort:    instance.nodePort,
			}
		}
	}
}

func (instance *instance) notifyAllWaitingRequestScaleOutFailed() {
	log.GetLogger().Debugf("instanceKey: %s | scale out failed then notify all requests waiting for scaling out",
		instance.instanceKey)
	for _, processor := range instance.processors {
		if processor.leased {
			log.GetLogger().Debugf("instanceKey: %s | processorID: %d | scale out failed then notify all "+
				"requests waiting for scaling out", instance.instanceKey, processor.processorID)
			instance.websocketReadyChan <- &schedulerapi.WebsocketInsInfo{
				Error: snerror.ScalingOutFunctionInstanceError,
			}
		}
	}
}
