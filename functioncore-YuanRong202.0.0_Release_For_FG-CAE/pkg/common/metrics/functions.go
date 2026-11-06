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

// Package metrics extracts public functions of metrics
package metrics

import (
	"fmt"
	"strings"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

const (
	// FunctionNameLen a function name with serviceID looks like "0-serviceID-funcName"
	FunctionNameLen = 3

	// ServiceIDIndex serviceID is the second element in a function name
	ServiceIDIndex = 1
)

// LoadFuncInfo loads function info by functionKey
func LoadFuncInfo(functionKey string) (*FunctionInfo, bool) {
	functionContent, ok := functionDataMap.Load(functionKey)
	if !ok {
		return nil, false
	}
	functionInfo, ok := functionContent.(*FunctionInfo)
	if !ok {
		log.GetLogger().Errorf("cannot assert the type of functionContent %+v", functionContent)
		return nil, false
	}
	return functionInfo, true
}

// FunctionInfoProcess composes function info by request info
func FunctionInfoProcess(requestInfo *FunctionInfo) *FunctionInfo {
	serviceID := GetServiceIDFromFuncName(requestInfo.FunctionName)
	functionKey := fmt.Sprintf("%s-%s-%s", requestInfo.FunctionName, requestInfo.TenantID,
		requestInfo.Version)
	functionInfo, ok := LoadFuncInfo(functionKey)
	if !ok {
		log.GetLogger().Warnf("failed to load function info of %s-%s-%s", requestInfo.FunctionName,
			urnutils.Anonymize(requestInfo.TenantID), requestInfo.Version)
		functionInfo = &FunctionInfo{
			FunctionName: requestInfo.FunctionName,
			TenantID:     requestInfo.TenantID,
			BusinessID:   requestInfo.BusinessID,
			Version:      requestInfo.Version,
			ServiceID:    serviceID,
			CreateTime:   "N/A",
			RevisionID:   "N/A",
		}
	}
	return functionInfo
}

// GetServiceIDFromFuncName a function name with serviceID looks like "0-serviceID-funcName"
func GetServiceIDFromFuncName(funcName string) string {
	fields := strings.Split(funcName, urnutils.Separator)
	if len(fields) < FunctionNameLen {
		log.GetLogger().Warnf("funcName %s doesn't contain a serviceID", funcName)
		return ""
	}
	return fields[ServiceIDIndex]
}
