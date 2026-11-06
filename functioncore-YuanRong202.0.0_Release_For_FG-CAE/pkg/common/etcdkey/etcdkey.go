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

// Package etcdkey contains etcd key definition and tools
package etcdkey

import (
	"fmt"
	"strings"

	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
)

const (
	keyFormat                   = "/sn/%s/business/%s/tenant/%s/function/%s/version/%s/%s/%s"
	keySeparator                = "/"
	stateWorkerLen              = 13
	instanceKeyLent             = 13
	functionMetadataTenantIndex = 4
	tenantMetadataTenantIndex   = 6

	defaultStateWorkerTypeKey = "stateworkers"
	defaultTaskTypeKey        = "workers"
	defaultBusinessID         = "yrk"
	defaultVersion            = "$latest"
	// NameOfFunctionTask -
	NameOfFunctionTask = "function-task"

	// StateRoutePrefix -
	StateRoutePrefix = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/"
	// FunctionTaskRoutePrefix -
	FunctionTaskRoutePrefix = "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/"
)

// Index of element in etcd key
const (
	prefixIndex = iota + 1
	typeIndex
	businessIDKey
	businessIDIndex
	tenantIDKey
	tenantIDIndex
	functionKey
	functionIndex
	versionKey
	versionIndex
	zoneIndex
	keyIndex
)

// EtcdKey etcd key interface definition
type EtcdKey interface {
	String() string
	ParseFrom(key string) error
}

// StateWorkersKey state workers key
type StateWorkersKey struct {
	TypeKey    string
	BusinessID string
	TenantID   string
	Function   string
	Version    string
	Zone       string
	StateID    string
}

// String serialize state workers key struct to string
func (s *StateWorkersKey) String() string {
	return fmt.Sprintf(keyFormat, s.TypeKey, s.BusinessID, s.TenantID, s.Function, s.Version, s.Zone, s.StateID)
}

// ParseFrom parse string to state workers key struct
func (s *StateWorkersKey) ParseFrom(key string) error {
	elements := strings.Split(key, keySeparator)
	if len(elements) != stateWorkerLen {
		return fmt.Errorf("failed to parse etcd key from %s", key)
	}
	s.TypeKey = elements[keyIndex]
	s.BusinessID = elements[businessIDIndex]
	s.TenantID = elements[tenantIDIndex]
	s.Function = elements[functionIndex]
	s.Version = elements[versionIndex]
	s.Zone = elements[zoneIndex]
	s.StateID = elements[keyIndex]

	return nil
}

// WorkerInstanceKey is the etcd key path of worker instance
type WorkerInstanceKey struct {
	TypeKey    string
	BusinessID string
	TenantID   string
	Function   string
	Version    string
	Zone       string
	Instance   string
}

// String serialize worker instance key struct to string
func (w *WorkerInstanceKey) String() string {
	return fmt.Sprintf(keyFormat, w.TypeKey, w.BusinessID, w.TenantID, w.Function, w.Version, w.Zone, w.Instance)
}

// ParseFrom parse string to worker instance key struct
func (w *WorkerInstanceKey) ParseFrom(key string) error {
	elements := strings.Split(key, keySeparator)
	if len(elements) != instanceKeyLent {
		return fmt.Errorf("failed to parse etcd key from %s", key)
	}
	w.TypeKey = elements[typeIndex]
	w.BusinessID = elements[businessIDIndex]
	w.TenantID = elements[tenantIDIndex]
	w.Function = elements[functionIndex]
	w.Version = elements[versionIndex]
	w.Zone = elements[zoneIndex]
	w.Instance = elements[keyIndex]
	return nil
}

// AnonymizeTenantCommonEtcdKey Anonymize tenant info in common etcd key
// /sn/functions/business/yrk/tenant/8e08d5cc0ad34032bba8d636040a278c/function/0-test1-addone/version/$latest
func AnonymizeTenantCommonEtcdKey(etcdKey string) string {
	elements := strings.Split(etcdKey, keySeparator)
	if len(elements) <= tenantIDIndex {
		return etcdKey
	}
	elements[tenantIDIndex] = urnutils.Anonymize(elements[tenantIDIndex])
	return strings.Join(elements, keySeparator)
}

// AnonymizeTenantFunctionMetadataEtcdKey Anonymize tenant info in function metadata etcd key
// /repo/FunctionVersion/business/tenant/funcName/version/
func AnonymizeTenantFunctionMetadataEtcdKey(etcdKey string) string {
	elements := strings.Split(etcdKey, keySeparator)
	if len(elements) <= functionMetadataTenantIndex {
		return etcdKey
	}
	elements[functionMetadataTenantIndex] = urnutils.Anonymize(elements[functionMetadataTenantIndex])
	return strings.Join(elements, keySeparator)
}

// AnonymizeTenantMetadataEtcdKey Anonymize tenant info in tenant metadata etcd key
// /sn/quota/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/instancemetadata
func AnonymizeTenantMetadataEtcdKey(etcdKey string) string {
	elements := strings.Split(etcdKey, keySeparator)
	if len(elements) <= tenantMetadataTenantIndex {
		return etcdKey
	}
	elements[tenantMetadataTenantIndex] = urnutils.Anonymize(elements[tenantMetadataTenantIndex])
	return strings.Join(elements, keySeparator)
}

// GetStateETCDPath get etcd path
func GetStateETCDPath(stateID string) string {
	var key EtcdKey
	key = &StateWorkersKey{
		TypeKey:    defaultStateWorkerTypeKey,
		BusinessID: defaultBusinessID,
		// tenant ID is same as bus proxy for 0
		TenantID: "0",
		Function: NameOfFunctionTask,
		Version:  defaultVersion,
		Zone:     utils.AzEnv(),
		StateID:  stateID,
	}
	return key.String()
}

// GetTaskETCDPath -
func GetTaskETCDPath(instanceID string) string {
	var key EtcdKey
	key = &WorkerInstanceKey{
		TypeKey:    defaultTaskTypeKey,
		BusinessID: defaultBusinessID,
		// tenant ID is same as bus proxy for 0
		TenantID: "0",
		Function: NameOfFunctionTask,
		Version:  defaultVersion,
		Instance: instanceID,
		Zone:     utils.AzEnv(),
	}
	return key.String()
}
