/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

// Package frontend of function invoke
package frontend

import (
	"os"
	"strconv"
	"time"

	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/model"
)

const (
	pressureLimitTimeEnv               = "BACKPRESSURE_TIMEOUT"
	customPressureLimitTimeEnv         = "CUSTOM_BACKPRESSURE_TIMEOUT"
	backPressureLimitTime              = int64(60)
	defaultCustomBackPressureLimitTime = int64(300)
	minBackPressureLimitTime           = int64(60)
)

var (
	busTimeout                  int64
	customBackPressureLimitTime int64
)

func init() {
	busTimeout = backPressureLimitTime
	customBackPressureLimitTime = defaultCustomBackPressureLimitTime
	limitTimeStr := os.Getenv(pressureLimitTimeEnv)
	if limitTimeStr != "" {
		if val, err := strconv.ParseInt(limitTimeStr, base, bitSize); err == nil && val >= minBackPressureLimitTime {
			busTimeout = val
		}
	}
	customLimitTimeStr := os.Getenv(customPressureLimitTimeEnv)
	if customLimitTimeStr != "" {
		if val, err := strconv.ParseInt(customLimitTimeStr, base, bitSize); err == nil &&
			val >= defaultCustomBackPressureLimitTime {
			customBackPressureLimitTime = val
		}
	}
}

func setHeader(reqInfo *model.FunctionRequestInfo, req *fhttp.Request,
	funcVersionURN string, insResource types.InstanceResource) {
	// process request
	req.Header.Set(constants.HeaderInvokeURN, funcVersionURN)
	if req.Header.Peek(constants.HeaderLogType) == nil {
		req.Header.Set(constants.HeaderLogType, constant.DefaultLogFlag)
	}

	req.Header.Set(constants.HeaderCPUSize, insResource.CPU)
	req.Header.Set(constants.HeaderMemorySize, insResource.Memory)

	setRequestDeadline(reqInfo)
}

// getInsResource get instance resource info from header or meta data
func getInsResource(reqInfo *model.FunctionRequestInfo, req *fhttp.Request) types.InstanceResource {
	metaInfo, ok := metadata.LoadMetaData(reqInfo.Name, reqInfo.TenantID, reqInfo.Version)
	if ok && utils.IsCAEFunc(metaInfo.FuncMetaDataParts.BusinessType) {
		return types.InstanceResource{
			CPU:    "0",
			Memory: "0",
		}
	}
	insResource := types.InstanceResource{
		CPU:    string(req.Header.Peek(constants.HeaderCPUSize)),
		Memory: string(req.Header.Peek(constants.HeaderMemorySize)),
	}
	if insResource.CPU == "" && insResource.Memory == "" {
		if !ok {
			log.GetLogger().Warnf("failed to get resource info")
			return types.InstanceResource{}
		}
		insResource.CPU, insResource.Memory = getCPUAndMemory(&metaInfo.ResourceMetaData)
	}
	return insResource
}

func getRequestDeadline(reqInfo *model.FunctionRequestInfo) time.Duration {
	var timeout time.Duration
	var backPressureTimeout time.Duration
	metaInfo, ok := metadata.LoadMetaData(reqInfo.Name, reqInfo.TenantID, reqInfo.Version)
	if !ok || metaInfo.FuncMetaDataParts.IsCustomContainer {
		timeout = time.Duration(constant.FConfig.HTTPConfig.WorkerInstanceReadTimeOut) * time.Second
		backPressureTimeout = time.Duration(customBackPressureLimitTime) * time.Second
	} else {
		funcTimeout := metaInfo.FuncMetaDataParts.Timeout
		if funcTimeout > constants.GetSyncRequestTimeout() {
			funcTimeout = constants.GetSyncRequestTimeout()
		}
		timeout = time.Duration(constant.FConfig.E2EMaxDelayTime+
			metaInfo.ExtendedMetaData.Initializer.InitializerTimeout+funcTimeout) * time.Second
		backPressureTimeout = time.Duration(busTimeout+
			metaInfo.ExtendedMetaData.Initializer.InitializerTimeout) * time.Second
	}
	reqInfo.BackPressureDeadline = time.Now().Add(backPressureTimeout)
	reqInfo.Deadline = time.Now().Add(timeout)
	return timeout
}

func setRequestDeadline(reqInfo *model.FunctionRequestInfo) {
	reqInfo.Deadline = time.Now().Add(getRequestDeadline(reqInfo))
}

func getCPUAndMemory(metaResource *functiontypes.ResourceMetaData) (string, string) {
	return strconv.Itoa(metaResource.CPU), strconv.Itoa(metaResource.Memory)
}
