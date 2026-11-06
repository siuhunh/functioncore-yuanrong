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

// Package instance contains loadbalance instance logic
package instance

import (
	"strings"
	"sync/atomic"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	defaultChanCapacity     = 20
	functionTaskKeySliceNum = 2
	instanceKeySliceNum     = 3
)

type defaultInstance struct {
	processors     map[int]*common.Processor
	disabledChan   chan *common.Processor
	insRetainer    retainer
	instanceKey    string
	funcKeyWithRes string
	requestNum     int
	threadNum      int
	nodeIP         string
}

// GetNodeIP -
func (ins *defaultInstance) GetNodeIP() string {
	return ins.nodeIP
}

func createDefaultInstance(funcKeyWithRes string, instanceKey string) *defaultInstance {
	instanceKeySlice := strings.Split(instanceKey, util.WorkerInstanceAZSep)
	nodeIP := ""
	if len(instanceKeySlice) == instanceKeySliceNum {
		nodeIP = instanceKeySlice[2]
		instanceKey = instanceKeySlice[0] + util.WorkerInstanceAZSep + instanceKeySlice[1]
	}
	ins := &defaultInstance{
		processors:     map[int]*common.Processor{},
		instanceKey:    instanceKey,
		funcKeyWithRes: funcKeyWithRes,
		disabledChan:   make(chan *common.Processor, defaultChanCapacity),
		nodeIP:         nodeIP,
	}
	ins.insRetainer = createRetainer(funcKeyWithRes, instanceKey, ins.notify)
	return ins
}

// GetInstanceKey -
func (ins *defaultInstance) GetInstanceKey() string {
	return ins.instanceKey
}

// AddProcessor -
func (ins *defaultInstance) AddProcessor(p *common.Processor) error {
	oldProcessor, ok := ins.processors[p.ProcessorID]
	if ok {
		if oldProcessor.LeaseVersion == p.LeaseVersion {
			log.GetLogger().Warnf("instance %s version %d already exist", ins.instanceKey, p.LeaseVersion)
			ins.processors[p.ProcessorID] = p
			ins.insRetainer.addProcessor(p)
			return nil
		}
		ins.threadNum -= oldProcessor.ThreadNum
	}
	ins.processors[p.ProcessorID] = p
	ins.threadNum += p.ThreadNum
	ins.insRetainer.addProcessor(p)

	return nil
}

// TrySchedule -
func (ins *defaultInstance) TrySchedule(force bool) bool {
	if force {
		ins.requestNum++
		return true
	}

	ins.checkDisabledProcessor()
	if ins.requestNum < ins.threadNum {
		ins.requestNum++
		return true
	}
	return false
}

// Done -
func (ins *defaultInstance) Done() *common.Processor {
	if ins.requestNum == 0 {
		return nil
	}
	ins.requestNum--
	return ins.tryReleaseProcessor()
}

func (ins *defaultInstance) tryReleaseProcessor() *common.Processor {
	for _, processor := range ins.processors {
		if ins.threadNum <= ins.requestNum {
			break
		}
		processorKey := urnutils.GetProcessorKey(ins.instanceKey,
			uint32(processor.ProcessorID), processor.LeaseVersion)
		log.GetLogger().Debugf("FuncKeyWithRes %s processorKey %s release",
			ins.funcKeyWithRes, processorKey)
		ins.remoteProcessor(processor)
		return processor
	}
	return nil
}

// CountRequestNum -
func (ins *defaultInstance) CountRequestNum() int {
	return ins.requestNum
}

// RetainLease -
func (ins *defaultInstance) RetainLease() {
	ins.insRetainer.retainLease()
}

func (ins *defaultInstance) notify(p *common.Processor) {
	ins.disabledChan <- p
}

func (ins *defaultInstance) checkDisabledProcessor() {
	select {
	case processor, ok := <-ins.disabledChan:
		if !ok {
			return
		}
		ins.removeDisabledProcessor(processor)
		for i := 0; i < len(ins.disabledChan); i++ {
			ins.removeDisabledProcessor(<-ins.disabledChan)
		}
	default:
		return
	}
}

func (ins *defaultInstance) removeDisabledProcessor(disabledProcessor *common.Processor) {
	p, ok := ins.processors[disabledProcessor.ProcessorID]
	if !ok {
		return
	}
	disable := atomic.LoadUint32(&p.Disable)
	if disable == 0 {
		return
	}

	ins.remoteProcessor(p)
}

func (ins *defaultInstance) remoteProcessor(p *common.Processor) {
	ins.threadNum -= p.ThreadNum
	delete(ins.processors, p.ProcessorID)

	atomic.StoreUint32(&p.Disable, 1)
	ins.insRetainer.deleteProcessor(p)
}
