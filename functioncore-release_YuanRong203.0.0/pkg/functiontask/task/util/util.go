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

// Package util defines some utils for function task.
package util

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/servicediscovery"
	busUtils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

const (
	delay                    = 3 * time.Minute
	defaultTypeKey           = "stateworkers"
	defaultBusinessID        = "yrk"
	defaultVersion           = "$latest"
	maxRedisRetry            = 5
	defaultRetryDelay        = 3
	defaultFuncNameSeparator = "@"
	lengthWithVersion        = 2
	funcUrnLength            = 7
	businessIDIndex          = 2
	funcIndex                = 0
	versionIndex             = 1
)

// DelayExit will exit after delay, caller ensures that err is not nil.
func DelayExit(module string, err error) {
	log.GetLogger().Errorf("panic, module: %s, err: %s", module, err.Error())
	time.Sleep(delay)
}

// IsExceedMaxNums check if nums of stateID exceed MaxNumsOfStateID
func IsExceedMaxNums(nums int) bool {
	return nums >= config.Conf.MaxNumsOfStateID
}

// ErrInfoOfTooManyState get error info of too many state
func ErrInfoOfTooManyState(nums int) string {
	return "number of stateID (" + strconv.Itoa(nums) + ") reaches or exceeds " +
		"maximum limit (" + strconv.Itoa(config.Conf.MaxNumsOfStateID) + ")"
}

// GenStateIDPattern generate stateID prefix pattern string
func GenStateIDPattern(tenantID, serviceID string) string {
	return metadata.Gen(tenantID, serviceID, metadata.StateIDPrefix, "*")
}

// HasExceedNumsError return error if nums of stateID in serviceID exceed maxNumsOfStateID
func HasExceedNumsError(client stateclient.StateClient, tenantID, serviceID string) error {
	nums, err := client.GetNumsByServiceID(GenStateIDPattern(tenantID, serviceID))
	if err != nil {
		return err
	}
	if IsExceedMaxNums(nums) {
		return errors.New(ErrInfoOfTooManyState(nums))
	}

	return nil
}

// GetStateETCDPath get etcd path
func GetStateETCDPath(stateID string) string {
	var key etcdkey.EtcdKey
	key = &etcdkey.StateWorkersKey{
		TypeKey:    defaultTypeKey,
		BusinessID: defaultBusinessID,
		// tenant ID is same as bus proxy for 0
		TenantID: "0",
		Function: coreconfig.NameOfFunctionTask,
		Version:  defaultVersion,
		Zone:     utils.AzEnv(),
		StateID:  stateID,
	}
	return key.String()
}

// DeleteStateInRedis delete state in redis
func DeleteStateInRedis(tenantID, serviceID, functionName, stateID string) snerror.SNError {
	var err snerror.SNError
	var hDelCount int
	stateHashKey := metadata.GeneStateHashKey(tenantID, serviceID, functionName)
	for i := 0; i < maxRedisRetry; i++ {
		hDelCount, err = stateclient.HDel(stateHashKey, stateID)
		if err == nil {
			break
		}
		time.Sleep(defaultRetryDelay * time.Second)
		log.GetLogger().Warnf("failed to delete state in redis with stateID: %s, this is %v time to retry",
			stateID, i+1)
	}

	if err != nil {
		log.GetLogger().Errorf("failed to delete state in redis, error (http response is error): %s,"+
			" stateID: %s", err.Error(), stateID)
		return err
	}
	log.GetLogger().Infof("finished deleting state in redis, stateID: %s, stateHashKey: %s, HDel count: %v",
		stateID, stateHashKey, hDelCount)
	return nil
}

// SaveStateInRedis save state in redis
func SaveStateInRedis(tenantID, serviceID, functionName, stateID, stateKey string) snerror.SNError {
	var err snerror.SNError

	stateHashKey := metadata.GeneStateHashKey(tenantID, serviceID, functionName)
	for i := 0; i < maxRedisRetry; i++ {
		_, err = stateclient.HSetNX(stateHashKey, stateID, stateKey)
		if err == nil {
			break
		}
		time.Sleep(defaultRetryDelay * time.Second)
		log.GetLogger().Warnf("failed to set state in redis with stateID: %s, this is %v time to retry",
			stateID, i+1)
	}

	if err != nil {
		log.GetLogger().Errorf("failed to set state in redis, error : %s,"+
			" stateID: %s", err.Error(), stateID)
		return err
	}
	log.GetLogger().Infof("finished set state in redis, stateID: %s, stateHashKey: %s", stateID, stateHashKey)
	return nil
}

// ParseFuncName -
func ParseFuncName(serviceID string, funcName string) (string, string) {
	result := strings.Split(funcName, ":")
	if len(result) == lengthWithVersion {
		return serviceID + defaultFuncNameSeparator + result[0], result[1]
	}
	return serviceID + defaultFuncNameSeparator + funcName, "latest"
}

// GetBusinessID -
func GetBusinessID(tenantID, functionName, version string) string {
	log.GetLogger().Debugf("start to get businessID with tenantID: %s, functionName: %s, version: %s",
		tenantID, functionName, version)
	info, err := servicediscovery.GetRegistry().GetFunctionMetadata(tenantID + "/" + functionName + "/" + version)
	if err != nil {
		return defaultBusinessID
	}
	funcUrn := strings.Split(info.FuncMetaData.FunctionVersionURN, ":")
	if len(funcUrn) != funcUrnLength {
		return defaultBusinessID
	}
	if funcUrn[businessIDIndex] == "" {
		return defaultBusinessID
	}
	return funcUrn[businessIDIndex]
}

// SplitFuncNameAndVersion -
func SplitFuncNameAndVersion(funcName string) (string, string) {
	funcNameWithVersion := strings.Split(funcName, ":")
	if len(funcNameWithVersion) != lengthWithVersion {
		return funcName, "latest"
	}
	functionName, functionVersion := funcNameWithVersion[funcIndex], funcNameWithVersion[versionIndex]
	if functionVersion == "" {
		functionVersion = "latest"
	}
	return functionName, functionVersion
}

// GetRealFuncName get real funcName
func GetRealFuncName(funcName, version, serviceID, tenantID string) (string, bool) {
	if version == "" {
		version = urnutils.DefaultVersion
	}
	isAlias := busUtils.IsAliasForFuncBranch(version)
	if isAlias {
		version = version[urnutils.BranchAliasPrefix:]
		fullName := fmt.Sprintf("%s@%s", serviceID, funcName)
		routeVersion := busUtils.GetRouteFuncVersion(tenantID, fullName, version)
		if routeVersion == "" {
			info, err := servicediscovery.GetRegistry().GetFunctionMetadata(
				tenantID + "/" + fullName + "/" + urnutils.DefaultVersion)
			if err == nil && info.FuncMetaData.IsBridgeFunction {
				// if we can not find the version of alias for bridge function, we can invoke latest
				return funcName, true
			}
			log.GetLogger().Errorf("routeVersion is empty, funcName is %s, version is %s, serviceID is %s, tenantID is %s",
				funcName, version, serviceID, tenantID)
			return "", false
		}
		version = routeVersion
	}
	if version == urnutils.DefaultVersion {
		return funcName, true
	}
	return fmt.Sprintf("%s:%s", funcName, version), true
}
