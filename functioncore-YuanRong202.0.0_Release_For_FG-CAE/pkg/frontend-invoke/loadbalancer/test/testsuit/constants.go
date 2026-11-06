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

// Package testsuit contains test tool for loadbalancer api test
package testsuit

import (
	"functioncore/pkg/common/loadbalance"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	// TestFuncKey -
	TestFuncKey = "7e186a/0@base@testresourcepython36768/latest"
	// TestFuncKeyWithRes -
	TestFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	// TestInstanceKey -
	TestInstanceKey = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	// TestSchedulerKey -
	TestSchedulerKey = "faas-scheduler-6b758c8b74-5zdwv"
)

var (
	// TestFuncMeta -
	TestFuncMeta = &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
	}

	// TestWorkerInstance -
	TestWorkerInstance = &types.WorkerInstance{
		InstanceID: TestInstanceKey,
		Applier:    "worker-manager",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		CPU:        300,
		Memory:     128,
	}

	// TestSchedulerInfo -
	TestSchedulerInfo = &commontypes.FaaSServiceInfo{
		InstanceID: TestSchedulerKey,
	}

	TestNextReq = &loadbalance.Request{
		Name:    "",
		TraceID: "1234",
	}
)
