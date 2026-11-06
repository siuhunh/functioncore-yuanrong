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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

const (
	// SuccessFlag of a request
	SuccessFlag = "0"
	// FailureFlag of a request
	FailureFlag = "1"

	// EventKeySeparator is the separator of event key elements from etcd
	EventKeySeparator = "/"

	// FunctionEtcdKeyLen is 10 + 1 as what we get from Split() is longer than the actual by 1
	FunctionEtcdKeyLen = 11
)

const (
	businessIndex = 4 + 2*iota
	tenantIndex
	funcNameIndex
	versionIndex
)

var (
	functionDataMap = sync.Map{}
)

// FunctionInfo is only used for function info of metrics
type FunctionInfo struct {
	FunctionName string
	TenantID     string
	BusinessID   string
	Version      string
	ServiceID    string
	CreateTime   string
	RevisionID   string
}

// CreateFunctionMetrics creates related information of a function when it is created in etcd
func CreateFunctionMetrics(eventKey string, eventValue []byte) error {
	keyStrings := strings.Split(eventKey, EventKeySeparator)
	if len(keyStrings) != FunctionEtcdKeyLen {
		log.GetLogger().Errorf("the etcdKey length (%d) is incorrect", len(keyStrings))
		return errors.New("incorrect etcdKey length")
	}

	eventValueMap := make(map[string]interface{}, 0)
	err := json.Unmarshal(eventValue, &eventValueMap)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal the etcd event value")
		return err
	}
	funcMetaData, funcMetaDataOK := eventValueMap["funcMetaData"].(map[string]interface{})
	if !funcMetaDataOK {
		log.GetLogger().Errorf("failed to change the type of funcMetaData")
		return errors.New("cannot change the type of funcMetaData")
	}

	revisionID, revisionOK := funcMetaData["revisionId"].(string)
	createTime, createOK := funcMetaData["created"].(string)
	if !revisionOK || !createOK {
		log.GetLogger().Errorf("failed to change the type of revisionID or createTime")
		return errors.New("cannot change the type of revisionID or createTime")
	}

	functionContent := &FunctionInfo{
		FunctionName: keyStrings[funcNameIndex],
		TenantID:     keyStrings[tenantIndex],
		BusinessID:   keyStrings[businessIndex],
		Version:      keyStrings[versionIndex],
		ServiceID:    GetServiceID(keyStrings[funcNameIndex]),
		CreateTime:   createTime,
		RevisionID:   revisionID,
	}
	functionKey := fmt.Sprintf("%s-%s-%s", functionContent.FunctionName, functionContent.TenantID,
		functionContent.Version)
	log.GetLogger().Debugf("create function metrics, function name: %s, tenantID: %s, functionVersion: %s",
		functionContent.FunctionName, urnutils.AnonymizeTenantKey(functionContent.TenantID), functionContent.Version)
	functionDataMap.Store(functionKey, functionContent)
	return nil
}

// GetServiceID splices the ServiceID of a function
func GetServiceID(funcName string) string {
	c := &urnutils.ComplexFuncName{}
	c.ParseFrom(funcName)
	return c.ServiceID
}

// FetchFunctionInfo is used to get functionInfo and functionKey
func FetchFunctionInfo(eventKey string) (*FunctionInfo, string, error) {
	keyStrings := strings.Split(eventKey, EventKeySeparator)
	if len(keyStrings) != FunctionEtcdKeyLen {
		log.GetLogger().Errorf("the etcdKey length (%d) is incorrect", len(keyStrings))
		return nil, "", errors.New("incorrect etcdKey length")
	}
	tenantID := keyStrings[tenantIndex]
	funcName := keyStrings[funcNameIndex]
	version := keyStrings[versionIndex]
	serviceID := GetServiceID(keyStrings[funcNameIndex])
	log.GetLogger().Debugf("start to delete metrics for function: %s-%s-%s:%s",
		urnutils.Anonymize(tenantID), serviceID, funcName, version)
	functionKey := fmt.Sprintf("%s-%s-%s", funcName, tenantID, version)
	functionContent, ok := functionDataMap.Load(functionKey)
	if !ok {
		log.GetLogger().Errorf("cannot find the stored info of function %s-%s-%s in this component", funcName,
			urnutils.Anonymize(tenantID), version)
		return nil, functionKey, fmt.Errorf("cannot find the stored info of function %s in this component",
			functionKey)
	}
	functionInfo, ok := functionContent.(*FunctionInfo)
	if !ok {
		log.GetLogger().Errorf("cannot assert the type of functionContent %+v", functionContent)
		return nil, functionKey, errors.New("failure of functionContent type assertion")
	}
	return functionInfo, functionKey, nil
}
