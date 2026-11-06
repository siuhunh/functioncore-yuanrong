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

package instance

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/util"
)

const funcKeyWithResSliceNum = 4

type defaultRetainer struct {
	funcKeyWithRes string
	funcKey        string
	tenantID       string
	instanceKey    string
	processors     sync.Map
	notifier       notify
}

func createDefaultRetainer(funcKeyWithRes string, instanceKey string, notifier notify) retainer {
	tenantID, funcKey := util.ParseFuncKeyWithRes(funcKeyWithRes)
	return &defaultRetainer{
		funcKeyWithRes: funcKeyWithRes,
		instanceKey:    instanceKey,
		processors:     sync.Map{},
		notifier:       notifier,
		tenantID:       tenantID,
		funcKey:        funcKey,
	}
}

func (r *defaultRetainer) addProcessor(p *common.Processor) {
	r.processors.Store(p.ProcessorID, p)
}

func (r *defaultRetainer) deleteProcessor(p *common.Processor) {
	processorIf, ok := r.processors.Load(p.ProcessorID)
	if !ok {
		return
	}

	processor, ok := processorIf.(*common.Processor)
	if !ok {
		return
	}
	disable := atomic.LoadUint32(&processor.Disable)
	if disable == 0 {
		return
	}
	r.processors.Delete(p.ProcessorID)
}

func (r *defaultRetainer) retainLease() {
	r.processors.Range(func(k, v interface{}) bool {
		p, ok := v.(*common.Processor)
		if !ok {
			return true
		}

		disable := atomic.LoadUint32(&p.Disable)
		if disable == disabled {
			return true
		}

		common.GetPoolInstance().Submit(func() {
			r.retainLeaseTask(p)
		})
		return true
	})
}

func (r *defaultRetainer) retainLeaseTask(p *common.Processor) {
	processorKey := urnutils.GetProcessorKey(
		r.instanceKey, uint32(p.ProcessorID), p.LeaseVersion)

	traceID := uuid.New().String()
	err := r.isProcessorDisabled(traceID, p)
	if err != nil {
		log.GetLogger().Warnf("failed to retain lease,processorKey %s traceID %s err %s",
			processorKey, traceID, err.Error())
		atomic.StoreUint32(&p.Disable, disabled)
		r.notifier(p)
		return
	}
	log.GetLogger().Debugf("success to retain lease, processorKey %s traceID %s",
		processorKey, traceID)
}

func (r *defaultRetainer) isProcessorDisabled(
	traceID string, p *common.Processor) error {
	var client common.RPCClient
	useStatefulClient := false
	if target := remotescheduler.GetWhitelist().Query(r.funcKey, r.tenantID); target != "" {
		useStatefulClient = true
		client = remotescheduler.GetInstance().GetStatefulScheduler(target)
	} else {
		client = remotescheduler.GetInstance().GetScheduler(r.funcKeyWithRes)
	}

	if client == nil {
		return errors.New("faas-scheduler client not exist")
	}
	log.GetLogger().Debugf("use stateful scheduler %t, used faas-scheduler %s, traceid %s",
		useStatefulClient, client.ServerKey(), traceID)
	rsp, err := client.RetainInstance(&schedulerapi.RetainInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: r.funcKeyWithRes,
		InstanceKey:    r.instanceKey,
		ProcessorID:    uint32(p.ProcessorID),
		LeaseVersion:   p.LeaseVersion,
	})
	if err != nil {
		return err
	}
	if rsp.Error.ErrorCode != 0 {
		return fmt.Errorf("faas-scheduler return code %d msg %s", rsp.Error.ErrorCode, rsp.Error.ErrorMessage)
	}
	return nil
}
