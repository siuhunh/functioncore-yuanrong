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

// Package testsuit contains test tools for ease test
package testsuit

import (
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	// TestFuncKey -
	TestFuncKey = "7e186a/0@base@testresourcepython36768/latest"
	// TestFuncKeyWithRes -
	TestFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	// TestInstanceKey -
	TestInstanceKey = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	// TraceID -
	TraceID = "1234"
	// TestInstanceKeyNormal -
	TestInstanceKeyNormal = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9-normal"
)

var (
	// TestWebSocketFuncMeta -
	TestWebSocketFuncMeta = &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
		FuncMetaData: types.FuncMetaData{
			FunctionVersionURN: "",
			BusinessType:       constants.BusinessTypeWebSocket,
		},
	}
	// TestWebSocketFuncMetaWithConcurrencyMoreThanOne -
	TestWebSocketFuncMetaWithConcurrencyMoreThanOne = &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 3,
		},
		FuncMetaData: types.FuncMetaData{
			FunctionVersionURN: "",
			BusinessType:       constants.BusinessTypeWebSocket,
		},
	}
	// TestFuncMeta -
	TestFuncMeta = &types.FunctionMetaInfo{
		InstanceMetaData: types.InstanceMetaData{
			ConcurrentNum: 1,
		},
	}
	// TestAcquireInstanceReq -
	TestAcquireInstanceReq = &schedulerapi.AcquireInstanceReq{
		TraceID:        TraceID,
		FuncKeyWithRes: TestFuncKeyWithRes,
	}
	// TestWebSocketAcquireInstanceReq -
	TestWebSocketAcquireInstanceReq = &schedulerapi.AcquireInstanceReq{
		TraceID:        TraceID,
		FuncKeyWithRes: TestFuncKeyWithRes,
		ReqType:        schedulerapi.WebSocketRequest,
		Timeout:        "5s",
	}
	// TestAcquireInstanceRsp -
	TestAcquireInstanceRsp = &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  TestInstanceKey,
		ProcessorID:  0,
		LeaseVersion: 1,
		ThreadNum:    1,
	}
	// TestWebSocketReleaseInstanceReq -
	TestWebSocketReleaseInstanceReq = &schedulerapi.ReleaseInstanceReq{
		TraceID:        TraceID,
		FuncKeyWithRes: TestFuncKeyWithRes,
		IsWebSocketReq: true,
		InstanceKey:    TestInstanceKeyNormal,
	}
	// TestWorkerInstance -
	TestWorkerInstance = &types.WorkerInstance{
		InstanceID: TestInstanceKey,
		Applier:    "worker-manager",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		PodIP:      "10.24.78.6",
		PodPort:    "8766",
		CPU:        300,
		Memory:     128,
	}
	// TestNormalWorkerInstance -
	TestNormalWorkerInstance = &types.WorkerInstance{
		InstanceID: TestInstanceKeyNormal,
		Applier:    "functiontask-123",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		PodIP:      "10.24.78.6",
		PodPort:    "8766",
		CPU:        300,
		Memory:     128,
	}
	// TestNormalWebSocketWorkerInstance -
	TestNormalWebSocketWorkerInstance = &types.WorkerInstance{
		InstanceID: TestInstanceKeyNormal,
		Applier:    "faas-scheduler",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		PodIP:      "10.24.78.6",
		PodPort:    "8766",
		CPU:        300,
		Memory:     128,
	}
	// TestReservedWebSocketWorkerInstance -
	TestReservedWebSocketWorkerInstance = &types.WorkerInstance{
		InstanceID: TestInstanceKeyNormal,
		Applier:    "worker-manager",
		NodeIP:     "127.0.0.1",
		NodePort:   "22423",
		PodIP:      "10.24.78.6",
		PodPort:    "8766",
		CPU:        300,
		Memory:     128,
	}
	// TestReleaseInstanceReq -
	TestReleaseInstanceReq = &schedulerapi.ReleaseInstanceReq{
		TraceID:        TraceID,
		FuncKeyWithRes: TestFuncKeyWithRes,
		InstanceKey:    TestInstanceKey,
		ProcessorID:    0,
		LeaseVersion:   1,
	}
	// TestRetainInstanceReq -
	TestRetainInstanceReq = &schedulerapi.RetainInstanceReq{
		TraceID:        TraceID,
		FuncKeyWithRes: TestFuncKeyWithRes,
		InstanceKey:    TestInstanceKey,
		ProcessorID:    0,
		LeaseVersion:   1,
	}
	// TestAcquireNormalInstanceRsp -
	TestAcquireNormalInstanceRsp = &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  TestInstanceKeyNormal,
		ProcessorID:  0,
		LeaseVersion: 1,
		ThreadNum:    1,
	}
)
