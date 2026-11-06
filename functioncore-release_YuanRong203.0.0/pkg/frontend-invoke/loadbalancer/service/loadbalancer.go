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

package service

import (
	"functioncore/pkg/common/loadbalance"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
)

// InstanceSource indicate scheduled instance source
type InstanceSource int

const (
	// AcquiredPool instance from acquired pool
	AcquiredPool = 0
	// RemoteScheduler instance from remote scheduler
	RemoteScheduler = 1
	// TotalPool instance from all instance pool
	TotalPool = 2
)

// ScheduleResult -
type ScheduleResult struct {
	InstanceKey    string
	InstanceSource InstanceSource
}

// LoadBalancer -
type LoadBalancer interface {
	loadbalance.LoadBalance
}

// SchedulerInsHandler -
type SchedulerInsHandler interface {
	AddScheduler(key string, info *types.FaaSServiceInfo)
	DeleteScheduler(key string, info *types.FaaSServiceInfo)
}

// CreateLoadBalancer -
func CreateLoadBalancer(funcKeyWithRes string, concurrency int, isLongRun, isCaeFunc bool) LoadBalancer {
	log.GetLogger().Infof("CreateLoadBalancer funcKeyWithRes %s, concurrency %d, isLongRun: %t, isCaeFunc: %t",
		funcKeyWithRes, concurrency, isLongRun, isCaeFunc)
	return createDefaultLoadBalancer(funcKeyWithRes, concurrency, isLongRun, isCaeFunc)
}
