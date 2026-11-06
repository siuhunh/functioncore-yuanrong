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
	"functioncore/pkg/common/api/schedulerapi"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
)

type instanceType int

const (
	reservedInstance     = 0
	normalInstance       = 1
	websocketInstance    = 2
	caeWebSocketInstance = 3
)

type strategy interface {
	addInstance(instance *common.Instance, t instanceType) *instance
	deleteInstance(instanceKey string)

	acquireInstance(req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error)
	releaseInstance(req *schedulerapi.ReleaseInstanceReq) error
	checkInstance(req *schedulerapi.CheckInstanceReq)
	retainInstance(req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error)

	checkLease()
	updateConcurrency(concurrency int)
	countInstance() int
	recoverAllConnStatus(funcKeyWithRes string)
}

func createStrategy(funcMeta *types.FunctionMetaInfo, funcKeyWithRes string) strategy {
	if commonUtils.IsWebSocketFunc(funcMeta.FuncMetaData.BusinessType,
		funcMeta.FuncMetaData.EnableRemoteDebug, funcMeta.ExtendedMetaData.EnableWebsocket) {
		return createCentralizedStrategy(funcMeta.InstanceMetaData.ConcurrentNum, funcKeyWithRes,
			funcMeta.FuncMetaData.BusinessType, funcMeta.FuncMetaData.EnableRemoteDebug,
			funcMeta.ExtendedMetaData.EnableWebsocket)
	}
	return createDefaultStrategy(funcMeta.InstanceMetaData.ConcurrentNum,
		funcMeta.FuncMetaData.IsBridgeFunction, funcKeyWithRes)
}
