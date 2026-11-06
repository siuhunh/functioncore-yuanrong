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

// Package frontend of function instance manager
package frontend

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/util"
)

// URNInstance contains the URNInstance Info
type URNInstance struct {
	urn            string
	instance       string
	status         string
	instanceID     string
	tenantID       string
	deployNode     string
	err            error
	insResource    types.InstanceResource
	funcKeyWithRes string
	willDelete     bool
}

type instanceInfo struct {
	urn        string
	instanceID string
	tenantID   string
	funcKey    string
}

const (
	workerInstanceStatusReady = "ready"
	workerInstanceAZSep       = "-#-"
)

const (
	// wantedLenOfSplit the wanted length of the split of the instanceID
	wantedLenOfSplit = 2
)

func processInstanceEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		err := processAddEvent(event)
		if err != nil {
			return err
		}
	case etcd3.DELETE:
		err := processDeleteEvent(event)
		if err != nil {
			return err
		}
	case etcd3.ERROR:
		log.GetLogger().Warnf("etcd error event: %s", event.Value)
	default:
		log.GetLogger().Warnf("unsupported event: %s", event.Value)
	}
	return nil
}

func processAddEvent(event *etcd3.Event) error {
	urnInstance := extractURNInstance(event)
	urn, instance, status, instanceID, tenantID, insResource, err := urnInstance.urn, urnInstance.instance,
		urnInstance.status, urnInstance.instanceID, urnInstance.tenantID, urnInstance.insResource, urnInstance.err
	if err != nil {
		return err
	}

	az := util.ExtractAz(instanceID)

	if tenantID == "0" {
		urnInstance.deployNode = urnInstance.instanceID
		az = constant.NativeAz
		log.GetLogger().Infof("the URN %s with tenantID 0 is adding an instance with az %s", urn, az)
	}
	workerInstanceKey := instance + urnutils.TenantProductSplitStr + instanceID
	// Status of instance is not ready, Remove it
	if status != workerInstanceStatusReady {
		err = functionlb.FunctionLB.DeleteWorkerInstance(
			az, urn, workerInstanceKey, insResource)
		if err != nil {
			return err
		}
		log.GetLogger().Infof("delete instance %s(az %s) URN %s, status %s", instance, az,
			urnutils.AnonymizeTenantURN(urn), status)
		return nil
	}
	// the worker will be deleted, Remove it
	if urnInstance.willDelete {
		err = functionlb.FunctionLB.DeleteWorkerInstance(
			az, urn, workerInstanceKey, insResource)
		if err != nil {
			return err
		}
		log.GetLogger().Infof("delete instance %s(az %s) URN %s, status %s, %s instance tag will be deleted",
			instance, az, urnutils.AnonymizeTenantURN(urn), status, workerInstanceKey)
		return nil
	}

	log.GetLogger().Debugf("add worker/proxy instance %s from ETCD", workerInstanceKey)

	instanceInfo := functionlb.InstanceInfo{
		Az:                az,
		Urn:               urn,
		WorkerInstanceKey: workerInstanceKey,
		FuncKeyWithRes:    urnInstance.funcKeyWithRes,
		InsResource:       insResource,
	}
	err = functionlb.FunctionLB.AddWorkerInstance(instanceInfo)
	if err != nil {
		return err
	}
	log.GetLogger().Infof("addIns | %s | %s | %s | %s", instance, urnutils.ExtractPodName(instanceID), az,
		urnutils.FuncURNtoShort(urn))
	return nil
}

func processDeleteEvent(event *etcd3.Event) error {
	urnInstance := extractURNInstance(event)
	urn, instance, instanceID, tenantID, insReasource, err := urnInstance.urn, urnInstance.instance,
		urnInstance.instanceID, urnInstance.tenantID, urnInstance.insResource, urnInstance.err
	if err != nil {
		return err
	}

	az := util.ExtractAz(instanceID)
	if tenantID == "0" {
		az = constant.NativeAz
		log.GetLogger().Infof("the URN %s with tenantID 0 is deleting an instance with az %s", urn, az)
	}

	workerInstanceKey := instance + urnutils.TenantProductSplitStr + instanceID
	err = functionlb.FunctionLB.DeleteWorkerInstance(az, urn, workerInstanceKey, insReasource)
	if err != nil {
		return err
	}
	log.GetLogger().Infof("removeIns | %s | %s | %s | %s", instance, urnutils.ExtractPodName(instanceID), az,
		urnutils.FuncURNtoShort(urn))
	return nil
}

// key: /sn/workers/business/<businessID>/tenant/<tenantID>/function/<functionname>/version/<version>/<az>/<instanceID>
func isWorkerInstance(key string) bool {
	instance := etcdkey.WorkerInstanceKey{}
	if err := instance.ParseFrom(key); err != nil {
		log.GetLogger().Errorf("invalid worker instance key: %s", key)
		return false
	}
	return true
}

func buildURN(business, tenant, function, version string) string {
	if business == "" || tenant == "" || function == "" || version == "" {
		return ""
	}
	return fmt.Sprintf("%s:%s:%s:%s:%s:%s:%s", urnutils.DefaultURNProductID, urnutils.DefaultURNRegion,
		business, tenant, urnutils.DefaultURNFuncSign, function, version)
}

func buildInstanceInfoFromInstanceKey(key string) (*instanceInfo, error) {
	instanceKey := etcdkey.WorkerInstanceKey{}
	if err := instanceKey.ParseFrom(key); err != nil {
		return nil, errors.New("invalid worker instance key")
	}
	funcKey := urnutils.GetFuncKey(instanceKey.TenantID, instanceKey.Function, instanceKey.Version)
	return &instanceInfo{
		urn:        buildURN(instanceKey.BusinessID, instanceKey.TenantID, instanceKey.Function, instanceKey.Version),
		instanceID: instanceKey.Instance,
		tenantID:   instanceKey.TenantID,
		funcKey:    funcKey,
	}, nil
}

func extractURNInstance(event *etcd3.Event) URNInstance {
	var urnInstance URNInstance
	instanceInfo, err := buildInstanceInfoFromInstanceKey(event.Key)
	if err != nil {
		urnInstance.err = err
		return urnInstance
	}

	instance := &workerInstance{}
	if event.Value == nil || len(event.Value) == 0 {
		err := json.Unmarshal(event.PrevValue, instance)
		if err != nil {
			urnInstance.err = err
			return urnInstance
		}
	} else {
		err := json.Unmarshal(event.Value, instance)
		if err != nil {
			urnInstance.err = err
			return urnInstance
		}
	}

	urnInstance.urn = instanceInfo.urn
	if instanceInfo.tenantID != "0" {
		urnInstance.insResource = types.InstanceResource{
			CPU:    strconv.Itoa(instance.CPU),
			Memory: strconv.Itoa(instance.Memory),
		}
	}
	if instance.Owner != "" {
		// instance is "worker"
		urnInstance.instance = instance.Owner + ":" + instance.NodePort
	} else {
		if instanceInfo.tenantID != "0" {
			urnInstance.err = fmt.Errorf("ownerip of worker instance %s is empty", event.Key)
			return urnInstance
		}
		// instance is "functionTask"
		// "functionTask" is also stored in local table cache
		urnInstance.instance = instance.NodeIP + ":" + instance.NodePort
	}
	urnInstance.status = instance.Status
	urnInstance.deployNode = instance.NodeIP
	urnInstance.instanceID = instanceInfo.instanceID
	urnInstance.tenantID = instanceInfo.tenantID
	urnInstance.funcKeyWithRes = urnutils.GetFuncKeyWithResource(instanceInfo.funcKey,
		urnInstance.insResource.CPU, urnInstance.insResource.Memory)
	urnInstance.err = nil
	urnInstance.willDelete = instance.WillDelete
	log.GetLogger().Debugf("deployed address of %s@%s is %s",
		urnInstance.instanceID, urnInstance.instance, instance.NodeIP)

	return urnInstance
}
