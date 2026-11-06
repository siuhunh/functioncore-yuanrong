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

// Package utils gets function services from URNs
package utils

import (
	"fmt"
	"strings"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types/bridgetypes"
)

// An example of a worker URN:
// /sn/workers/business/iot/tenant/j0f4413f7b4b4c33be576d432f7ee085/function/functest/version/$latest/cn-north-1a/cn-north-1a-#-ws-j0f4413f7b-functest-faaslatest-deployment-55b5f9dcb7-r2dsv
// An example of functionURN:
// "functionURN": "sn:cn:yrk:12345678901234561234567890123456:function:0@default@lp-pytest"
const (
	// URNIndexZero URN index 0
	URNIndexZero = iota
	// URNIndexOne URN index 1
	URNIndexOne
	// URNIndexTwo URN index 2
	URNIndexTwo
	// URNIndexThree URN index 3
	URNIndexThree
	// URNIndexFour URN index 4
	URNIndexFour
	// URNIndexFive URN index 5
	URNIndexFive
	// URNIndexSix URN index 6
	URNIndexSix
	// URNIndexSeven URN index 7
	URNIndexSeven
	// URNIndexEight URN index 8
	URNIndexEight
	// URNIndexNine URN index 9
	URNIndexNine
	// URNIndexTen URN index 10
	URNIndexTen
	// URNIndexEleven URN index 11
	URNIndexEleven
	// URNIndexTwelve URN index 12
	URNIndexTwelve
	// URNIndexThirteen URN index 13
	URNIndexThirteen

	k8sLabelLen = 63
	otherStrLen = 4
)

// FunctionInfo defines Function Info
type FunctionInfo struct {
	Business string
	Tenant   string
	FuncName string
	Version  string
}

// GetStateIDByPath /sn/bridgemanager/business/yrk/tenant/0/function/${complexFuncName}/version/latest/defaultaz/${stateID}
func GetStateIDByPath(path string) string {
	keyFields := strings.Split(path, "/")
	if len(keyFields) != URNIndexThirteen {
		return ""
	}
	return keyFields[URNIndexTwelve]
}

// GetPodNameByPath /sn/bridgeclient/business/yrk/tenant/0/function/${complexFuncName}/version/latest/defaultaz/defaultaz-#-${podName}
func GetPodNameByPath(path string) string {
	keyFields := strings.Split(path, "-#-")
	if len(keyFields) != URNIndexTwo {
		return ""
	}
	return keyFields[URNIndexOne]
}

// GetTenantIDByWorkersPath /sn/bridgeclient/business/yrk/tenant/0/function/${complexFuncName}/version/latest/defaultaz/defaultaz-#-${podName}
func GetTenantIDByWorkersPath(path string) string {
	keyFields := strings.Split(path, "/")
	if len(keyFields) < URNIndexSeven {
		log.GetLogger().Warnf("[bridgeplugin] format of function path: %s is invalid", path)
		return ""
	}
	return keyFields[URNIndexSix]
}

// GetFuncNameByWorkersPath /sn/bridgeclient/business/yrk/tenant/0/function/${complexFuncName}/version/latest/defaultaz/defaultaz-#-${podName}
func GetFuncNameByWorkersPath(path string) string {
	keyFields := strings.Split(path, "/")
	if len(keyFields) != URNIndexThirteen {
		log.GetLogger().Warnf("[bridgeplugin] format of path: %s is invalid", path)
		return ""
	}
	functionFields := strings.Split(keyFields[URNIndexEight], "@")
	if len(functionFields) <= URNIndexOne {
		log.GetLogger().Warnf("[bridgeplugin] format of function name path: %s is invalid", path)
		return ""
	}
	return functionFields[len(functionFields)-1]
}

// GetFuncURNByWorkersPath /sn/workers/business/yrk/tenant/${tenantID}/function/${complexFuncName}/version/latest/defaultaz/defaultaz-#-${podName}
func GetFuncURNByWorkersPath(path string) string {
	urnFormat := "sn:cn:yrk:%s:function:%s"
	keyFields := strings.Split(path, "/")
	if len(keyFields) != URNIndexThirteen {
		log.GetLogger().Warnf("[bridgeplugin] format of path: %s is invalid", path)
		return ""
	}
	return fmt.Sprintf(urnFormat, keyFields[URNIndexSix], keyFields[URNIndexEight])
}

// NewFunctionURN generate function urn by tenantID, serviceID and funcName
func NewFunctionURN(tenantID string, serviceID string, funcName string) string {
	urnFormat := "sn:cn:yrk:%s:function:%s@%s"
	return fmt.Sprintf(urnFormat, tenantID, serviceID, funcName)
}

// GetFunctionInfoByURN parse bridgeFunc info by urn
func GetFunctionInfoByURN(bridgeFuncUrn string) (string, string, string) {
	keyFields := strings.Split(bridgeFuncUrn, ":")
	if len(keyFields) < URNIndexSix {
		return "", "", ""
	}
	tenantID := keyFields[URNIndexThree]
	funcKeyFields := strings.Split(keyFields[URNIndexFive], "@")
	if len(funcKeyFields) < URNIndexThree {
		return "", "", ""
	}
	serviceID := funcKeyFields[URNIndexZero] + "@" + funcKeyFields[URNIndexOne]
	bridgeFuncName := funcKeyFields[URNIndexTwo]
	return tenantID, serviceID, bridgeFuncName
}

// GetTenantIDFromURN get tenantID by function urn
func GetTenantIDFromURN(functionURN string) string {
	keyFields := strings.Split(functionURN, ":")
	if len(keyFields) != URNIndexSix {
		log.GetLogger().Warnf("[bridgeplugin] format of function URN: %s is invalid", functionURN)
		return ""
	}
	return keyFields[URNIndexThree]
}

// GetComplexFuncNameFromURN get complex function name by function urn
func GetComplexFuncNameFromURN(functionURN string) string {
	keyFields := strings.Split(functionURN, ":")
	if len(keyFields) != URNIndexSix {
		log.GetLogger().Warnf("[bridgeplugin] format of function URN: %s is invalid", functionURN)
		return ""
	}
	return keyFields[URNIndexFive]
}

// NewComplexFuncName generate complex function urn by serviceID and function name
func NewComplexFuncName(serviceID string, funcName string) string {
	cfnFormat := "%s@%s"
	return fmt.Sprintf(cfnFormat, serviceID, funcName)
}

// GetClientCacheInfoKey generate client instance cache key
func GetClientCacheInfoKey(client *bridgetypes.ClientInstance) string {
	return fmt.Sprintf("%s_%s_%s", client.PodName, client.FuncInfo.TenantID, client.FuncInfo.BridgeFuncName)
}
