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
	"strconv"
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/config"
	"functioncore/pkg/faas-scheduler/scheduler/service"
	"functioncore/pkg/faas-scheduler/types"
	"functioncore/pkg/faas-scheduler/utils"
)

var (
	funcInsHandler service.FuncInsHandler
)

const (
	validPodPathCount = 13
)

// CreateFuncInsObserver -
func CreateFuncInsObserver(handler service.FuncInsHandler) Observer {
	funcInsHandler = handler
	return &EtcdObserver{
		EtcdSelection: etcd3.Router,
		WatchPath:     config.WatchPathPrefixWithWorkers,
		Filter:        workerPathFilter,
		SyncHandler:   recoverInstance,
		UpdateHandler: updateInstance,
		DeleteHandler: deleteInstance,
	}
}

// WorkerPathFilter filters worker path
// /sn/workers/business/yrk/tenant/i1fe5394/function/app-storage-list/version/$latest/defaultaz/default-#-yh
// |    seg1    | seg2  |  seg3 |seg4| seg5 |  seg6  |  seg7  |    seg8        | seg9  | seg10 |  seg11  |   seg12
func workerPathFilter(path string) bool {
	items := strings.Split(path, "/")
	if len(items) != validPodPathCount {
		return false
	}

	if items[2] != "workers" || items[7] != "function" || items[9] != "version" {
		return false
	}
	return true
}

func updateInstance(ev *etcd3.Event) {
	funcKeyWithRes, instance, err := parseInstanceEvent(ev.Key, ev.Value)
	if err != nil {
		log.GetLogger().Errorf("failed to parseInstanceEvent, err %s", err)
		return
	}
	if instance.InstanceID != "" {
		log.GetLogger().Infof("updateIns | %s | %s", urnutils.FuncKeyWithResToShort(funcKeyWithRes),
			urnutils.ExtractPodName(instance.InstanceID))
	}
	funcInsHandler.AddInstance(funcKeyWithRes, instance)
}

func recoverInstance() {
	instancesRes := utils.GetRecoverInstances()
	if len(instancesRes.InstancesRes) == 0 {
		return
	}
	for funcKeyWithRes, instancesStatus := range instancesRes.InstancesRes {
		log.GetLogger().Infof("start to recoverInstance with funcKeyWithRes %s", funcKeyWithRes)
		go funcInsHandler.RecoverInstance(funcKeyWithRes, instancesStatus)
	}
	utils.ResetRecoverInstances()
}

func deleteInstance(ev *etcd3.Event) {
	funcKeyWithRes, instance, err := parseInstanceEvent(ev.Key, ev.PrevValue)
	if err != nil {
		log.GetLogger().Errorf("failed to parseInstanceEvent, err %s", err)
		return
	}
	log.GetLogger().Infof("deleteInstance funcKeyWithRes %s, instanceID %s",
		urnutils.AnonymizeTenantKey(funcKeyWithRes), instance.InstanceID)
	funcInsHandler.DeleteInstance(funcKeyWithRes, instance)
}

func parseInstanceEvent(key string, value []byte) (string, *types.WorkerInstance, error) {
	instance := types.WorkerInstance{}
	err := json.Unmarshal(value, &instance)
	if err != nil {
		return "", nil, err
	}
	var instanceResource commontypes.InstanceResource

	instanceResource.CPU = strconv.Itoa(instance.CPU)
	instanceResource.Memory = strconv.Itoa(instance.Memory)

	instanceKey := etcdkey.WorkerInstanceKey{}
	if err := instanceKey.ParseFrom(key); err != nil {
		return "", nil, errors.New("invalid worker instance key")
	}
	instance.InstanceID = instanceKey.Instance
	funcKey := urnutils.GetFuncKey(instanceKey.TenantID, instanceKey.Function, instanceKey.Version)
	funcKeyWithRes := urnutils.GetFuncKeyWithResource(funcKey, instanceResource.CPU, instanceResource.Memory)
	return funcKeyWithRes, &instance, nil
}
