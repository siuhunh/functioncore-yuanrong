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

// Package bridgefunc -
package bridgefunc

import (
	"strings"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/core-sdk/common"
	"functioncore/pkg/core-sdk/communication"
)

// ApplyStateKey get stateKey
func ApplyStateKey(clientInstance bridgetypes.ClientInstance, traceID string) (string, bool, snerror.SNError) {
	if clientInstance.FuncInfo == nil {
		log.GetLogger().Errorf("traceID: %s, err: %s", traceID, common.BridgeFuncParamErrMsg)
		return "", false, snerror.New(common.BridgeFuncParamErrCode, common.BridgeFuncParamErrMsg)
	}

	funcName := clientInstance.FuncInfo.BridgeFuncName
	tenantID := clientInstance.FuncInfo.TenantID
	serviceID := clientInstance.FuncInfo.ServiceID
	log.GetLogger().Debugf("start to invoke ApplyStateKey, funcName: %s, tenantID: %s, serviceID: %s, traceID: %s",
		funcName, tenantID, serviceID, traceID)
	// check the format of funcName
	if !(common.FuncNameMatch.MatchString(funcName) || common.FuncNameAliasMatch.MatchString(funcName)) {
		log.GetLogger().Errorf("invoke ApplyStateKey error: funcName is invalid, funcName: %s, traceID: %s",
			funcName, traceID)
		return "", false, snerror.New(common.BridgeFuncParamErrCode, "funcName invalid")
	}
	strs := strings.Split(funcName, ":")

	// combine function name with sn service id
	complexFuncName := urnutils.NewComplexFuncName(serviceID, strs[0]).String()

	invokeResponse, err := communication.ApplyStateKeyByHTTP(complexFuncName, clientInstance, traceID)
	if err != nil {
		log.GetLogger().Errorf("ApplyStateKey error (http response is error): %s, funcName: %s, tenantID: %s, "+
			"serviceID: %s, traceID: %s", err.Error(), funcName, tenantID, serviceID, traceID)
		return "", false, err
	}
	log.GetLogger().Debugf("finished invoke ApplyStateKey, stateKey:%s, IsNewStateKeyScenario:%s, funcName: %s, "+
		"tenantID: %s, serviceID: %s, traceID:%s", invokeResponse.StateKey, invokeResponse.IsNewStateKeyScenario,
		funcName, tenantID, serviceID, traceID)
	return invokeResponse.StateKey, invokeResponse.IsNewStateKeyScenario, nil
}

// Release send request to bridgeManager and get a flag identify whether to terminate
func Release(clientInstance bridgetypes.ClientInstance, traceID string) (bool, snerror.SNError) {
	if clientInstance.FuncInfo == nil {
		log.GetLogger().Errorf("traceID: %s, error: %s", traceID, common.BridgeFuncParamErrMsg)
		return false, snerror.New(common.BridgeFuncParamErrCode, common.BridgeFuncParamErrMsg)
	}

	funcName := clientInstance.FuncInfo.BridgeFuncName
	tenantID := clientInstance.FuncInfo.TenantID
	serviceID := clientInstance.FuncInfo.ServiceID
	log.GetLogger().Debugf("start to invoke Release, funcName: %s, tenantID: %s, serviceID: %s, traceID: %s",
		funcName, tenantID, serviceID, traceID)
	// check the format of funcName
	if !(common.FuncNameMatch.MatchString(funcName) || common.FuncNameAliasMatch.MatchString(funcName)) {
		log.GetLogger().Errorf("invoke Release error: funcName is invalid, funcName:%s, traceID: %s", funcName,
			traceID)
		return false, snerror.New(common.BridgeFuncParamErrCode, "funcName invalid")
	}
	strs := strings.Split(funcName, ":")

	// combine function name with sn service id
	complexFuncName := urnutils.NewComplexFuncName(serviceID, strs[0]).String()

	invokeResponse, err := communication.ReleaseByHTTP(complexFuncName, clientInstance, traceID)
	if err != nil {
		log.GetLogger().Errorf("Release error (http response is error): %s, funcName: %s, tenantID: %s, "+
			"serviceID: %s, traceID: %s ", err.Error(), funcName, tenantID, serviceID, traceID)
		return false, err
	}
	log.GetLogger().Debugf("finished invoke Release, funcName: %s, tenantID: %s, serviceID: %s, traceID: %s",
		funcName, tenantID, serviceID, traceID)
	return invokeResponse.IsTerminate, nil
}
