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

// Package service contains scheduler domain service
package service

import (
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/faas-scheduler/scheduler/common"
	"functioncore/pkg/faas-scheduler/types"
)

// CreateService create scheduler service
func CreateService(fetcher common.FuncMetaFetcher) (SchedulerService, error) {
	return createDefaultService(fetcher)
}

// SchedulerService scheduler service interface
type SchedulerService interface {
	FuncInsHandler
	FuncMetaHandler
	schedulerapi.Scheduler
}

// FuncInsHandler function instance handler
type FuncInsHandler interface {
	AddInstance(funcKeyWithRes string, info *types.WorkerInstance)
	DeleteInstance(funcKeyWithRes string, info *types.WorkerInstance)
}

// FuncMetaHandler function metadata handler
type FuncMetaHandler interface {
	UpdateFuncMeta(funcKey string, info *types.FunctionMetaInfo)
	DeleteFuncMeta(funcKey string, source etcd3.EtcdType)
}
