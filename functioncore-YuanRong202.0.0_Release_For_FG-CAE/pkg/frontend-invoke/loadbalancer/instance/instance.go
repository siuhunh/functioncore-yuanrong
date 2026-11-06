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
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
)

// Instance -
// note: instance do not support concurrency invoke method
// instance manager must have lock protect
type Instance interface {
	AddProcessor(p *common.Processor) error
	CountRequestNum() int
	TrySchedule(force bool) bool
	Done() *common.Processor
	RetainLease()
	GetInstanceKey() string
	GetNodeIP() string
}

// CreateInstance -
func CreateInstance(funcKeyWithRes string, instanceKey string) Instance {
	return createDefaultInstance(funcKeyWithRes, instanceKey)
}

// Releaser instance release,return instance to faas-scheduler
// note: ReleaseInstance is async method
type Releaser interface {
	ReleaseInstance(req *schedulerapi.ReleaseInstanceReq)
}

// CreateReleaser -
func CreateReleaser(funcKeyWithRes string) Releaser {
	return createDefaultReleaser(funcKeyWithRes)
}
