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
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/util"
)

type defaultReleaser struct {
	funcKeyWithRes string
	funcKey        string
	tenantID       string
}

func createDefaultReleaser(funcKeyWithRes string) Releaser {
	tenantID, funcKey := util.ParseFuncKeyWithRes(funcKeyWithRes)
	return &defaultReleaser{
		funcKeyWithRes: funcKeyWithRes,
		tenantID:       tenantID,
		funcKey:        funcKey,
	}
}

// ReleaseInstance -
func (releaser *defaultReleaser) ReleaseInstance(
	req *schedulerapi.ReleaseInstanceReq) {
	releaser.releaseInstanceTask(req)
}

// ReleaseInstanceTask -
func (releaser *defaultReleaser) releaseInstanceTask(
	req *schedulerapi.ReleaseInstanceReq) {
	processorKey := urnutils.GetProcessorKey(
		req.InstanceKey, req.ProcessorID, req.LeaseVersion)

	var client common.RPCClient
	useStatefulClient := false
	if target := remotescheduler.GetWhitelist().Query(releaser.funcKey, releaser.tenantID); target != "" {
		useStatefulClient = true
		client = remotescheduler.GetInstance().GetStatefulScheduler(target)
	} else {
		client = remotescheduler.GetInstance().GetScheduler(req.FuncKeyWithRes)
	}

	if client == nil {
		log.GetLogger().Warnf("failed to release instance, no faas-scheduler find,"+
			" traceID %s funcKeyWithRes %s processorKey %s",
			req.TraceID, urnutils.AnonymizeTenantKey(releaser.funcKeyWithRes), processorKey)
		return
	}
	log.GetLogger().Debugf("use stateful scheduler %t, used faas-scheduler %s", useStatefulClient, client.ServerKey())
	err := client.ReleaseInstance(req)
	if err != nil {
		log.GetLogger().Warnf("failed to release instance, err %s, traceID %s funcKeyWithRes %s processorKey %s",
			err.Error(), req.TraceID, urnutils.AnonymizeTenantKey(releaser.funcKeyWithRes), processorKey)
		return
	}
	log.GetLogger().Debugf("success to ReleaseInstance"+
		" traceID %s funcKeyWithRes %s processorKey %s",
		req.TraceID, urnutils.AnonymizeTenantKey(releaser.funcKeyWithRes), processorKey)
}
