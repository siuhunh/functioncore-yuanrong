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

// Package etcdadapter contains etcd interaction
package etcdadapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/faas-scheduler/types"
)

const (
	validFuncPathCount   = 11
	defaultConcurrentNum = 100000
)

var (
	funcMetaHandler service.FuncMetaHandler
)

// CreateFuncMetaObserver -
func CreateFuncMetaObserver(handler service.FuncMetaHandler) Observer {
	funcMetaHandler = handler
	return &EtcdObserver{
		EtcdSelection: etcd3.Meta,
		WatchPath:     config.WatchPathPrefixWithFunction,
		Filter:        funcPathFilter,
		UpdateHandler: updateFuncMeta,
		DeleteHandler: deleteFuncMeta,
	}
}

// CreateCAEFuncMetaObserver -
func CreateCAEFuncMetaObserver() Observer {
	return &EtcdObserver{
		EtcdSelection: etcd3.CAEMeta,
		WatchPath:     config.WatchPathPrefixWithFunction,
		Filter:        funcPathFilter,
		UpdateHandler: updateFuncMeta,
		DeleteHandler: deleteFuncMeta,
	}
}

// FuncPathFilter filters function path
// /sn/functions/business/yrk/tenant/i1fe5394/function/0-ttl-testfuncsnjavastatettl001/version/$latest
// |seg1|  seg2 |  seg3 |seg4| seg5 |  seg6  |   seg7 |  seg8                          |seg9  | seg10 |
func funcPathFilter(path string) bool {
	items := strings.Split(path, "/")
	if len(items) != validFuncPathCount {
		return false
	}
	if items[2] != "functions" || items[7] != "function" || items[9] != "version" {
		return false
	}
	return true
}

func parseFuncMeta(value []byte) (string, *types.FunctionMetaInfo, error) {
	info := types.FunctionMetaInfo{}
	err := json.Unmarshal(value, &info)
	if err != nil {
		return "", nil, err
	}
	funcVersionURN, err := urnutils.GetFunctionInfo(info.FuncMetaData.FunctionVersionURN)
	if err != nil {
		return "", nil, err
	}
	if info.InstanceMetaData.ConcurrentNum < 0 {
		return "", nil, fmt.Errorf("invalid ConcurrentNum %d", info.InstanceMetaData.ConcurrentNum)
	} else if info.InstanceMetaData.ConcurrentNum == 0 {
		info.InstanceMetaData.ConcurrentNum = defaultConcurrentNum
	}
	if funcVersionURN.Version == "" {
		return "", nil, errors.New("no version in FunctionVersionURN")
	}

	funcKey := urnutils.GetFuncKey(funcVersionURN.TenantID, funcVersionURN.Name, funcVersionURN.Version)
	return funcKey, &info, nil
}

func updateFuncMeta(ev *etcd3.Event) {
	funcKey, info, err := parseFuncMeta(ev.Value)
	if err != nil {
		log.GetLogger().Errorf("failed to parseFuncMeta of %s, err %s", ev.Key, err.Error())
		return
	}
	info.Source = ev.Source
	funcMetaHandler.UpdateFuncMeta(funcKey, info)
}

func deleteFuncMeta(ev *etcd3.Event) {
	items := strings.Split(ev.Key, "/")
	if len(items) != validFuncPathCount {
		return
	}
	funcKey := urnutils.GetFuncKey(items[6], items[8], items[10])
	log.GetLogger().Infof("delete funcMeta, funcKey %s", urnutils.AnonymizeTenantKey(funcKey))
	funcMetaHandler.DeleteFuncMeta(funcKey, ev.Source)
}

// FetchFuncMeta -
func FetchFuncMeta(funcKey string) (*types.FunctionMetaInfo, error) {
	tenantID, funcName, funcVersion := urnutils.ParseFuncKey(funcKey)
	funcStorageKey := config.SilentWatchPathPrefixWithFunction + "/" + tenantID + "/function/" + funcName +
		"/version/" + funcVersion
	resp, err := getValueFromEtcd(funcStorageKey, etcd3.Meta)
	if err != nil {
		return nil, err
	}

	_, info, err := parseFuncMeta(resp)
	if err != nil {
		return nil, err
	}
	info.Source = etcd3.Meta
	return info, nil
}
