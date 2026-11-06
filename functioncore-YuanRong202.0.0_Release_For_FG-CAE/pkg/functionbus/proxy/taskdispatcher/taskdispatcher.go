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

// Package taskdispatcher routing for state task requests and stateless task requests
package taskdispatcher

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

// TaskDispatcher task request dispatcher
type TaskDispatcher struct {
	registry *servicediscovery.Registry
}

// CreateTaskDispatcher function to create TaskDispatcher
func CreateTaskDispatcher(registry *servicediscovery.Registry) *TaskDispatcher {
	return &TaskDispatcher{registry: registry}
}

// Request new request coming
func (td *TaskDispatcher) Request(req *types.DispatcherRequest, dst *common.FunctionProperty) error {
	log.GetLogger().Debugf("taskDispatcher request, funcName:%s, stateID:%s, futureID:%s",
		dst.FunctionName, dst.StateID, dst.FutureID)
	// task request with instanceID should be forwarded to localTask
	if dst.InstanceID == internal.HostName {
		req.Position.InstanceID = internal.HostName
		req.Position.NodeID = internal.SelfNodeIP
		req.Response <- &types.TCPResponse{ErrorCode: proxyerrors.ForwardCode}
		return nil
	}
	// state task request should be forwarded according to state route result
	if dst.StateID != "" {
		td.stateReqToTask(req, dst)
		return nil
	}
	// stateless task request should be forwarded to localTask
	req.Position = types.Position{
		InstanceID: internal.HostName,
		NodeID:     internal.SelfNodeIP,
	}
	req.Response <- &types.TCPResponse{
		ErrorCode: proxyerrors.ForwardCode,
	}
	return nil
}

func (td *TaskDispatcher) stateReqToTask(req *types.DispatcherRequest, dst *common.FunctionProperty) {
	ip, instanceID, err := td.scheduleState(dst.TenantID, dst.FunctionName, dst.FunctionVersion, dst.StateID)
	if err != nil {
		log.GetLogger().Errorf("failed to schedule a state: %s, traceID %s", err, req.TraceID)
		req.Response <- &types.TCPResponse{
			ErrorCode:    proxyerrors.ScheduleErrCode,
			ErrorMessage: proxyerrors.ScheduleErr,
		}
		return
	}
	req.Position = types.Position{
		InstanceID: instanceID,
		NodeID:     ip,
	}
	req.Request.StateID = dst.StateID
	req.Response <- &types.TCPResponse{
		ErrorCode: proxyerrors.ForwardCode,
		RequestID: req.Request.RequestID,
	}
	return
}

func (td *TaskDispatcher) scheduleState(tenantID, funcName, funcVersion, stateID string) (string, string, error) {
	// first find state path in local cache
	if funcVersion == "latest" {
		funcVersion = common.VersionLatest
	}
	scheduleRes, scheduleSuccess := td._scheduleRequest(tenantID, funcName, funcVersion, stateID)
	if scheduleSuccess {
		ip, port, instance := getScheduleRes(scheduleRes)
		log.GetLogger().Debugf("schedule results of state call are: %s, %s, %s, %s, success result: %s, %s, %s",
			urnutils.Anonymize(tenantID), funcName, funcVersion, stateID, ip, port, instance)
		return ip, instance, nil
	}
	stateKey := common.WatchPathPrefixWithStateWorkers + "/" + tenantID + "/function/" + funcName + "/version/" +
		funcVersion + "/" + commonutils.AzEnv() + "/" + stateID
	// second if can't find state path in local cache, find it in etcd
	log.GetLogger().Debugf("schedule state key is: %s", stateKey)
	instance, err := utils.GetStateValue(stateKey, stateID)
	if err != nil {
		log.GetLogger().Errorf("failed to find state %s: %s", stateID, err)
		return "", "", err
	}
	podKey := common.WatchPathPrefixWithWorkers + "/" + tenantID + "/function/" + funcName + "/version/" +
		funcVersion + "/" + commonutils.AzEnv() + "/" + instance
	ip, _, err := utils.GetPodValue(podKey)
	log.GetLogger().Debugf("podKey: %s and ip: %s", podKey, ip)
	if err != nil {
		log.GetLogger().Errorf("state %s in task %s, but failed to find task info: %s", stateID, instance, err)
		return "", "", err
	}
	return ip, instance, nil
}

func getScheduleRes(scheduleRes []string) (string, string, string) {
	if len(scheduleRes) != 3 { // 3 the len of scheduleRes
		return "", "", ""
	}
	return scheduleRes[0], scheduleRes[1], scheduleRes[2] // 0 ip, 1 port, 2 instance
}

func (td *TaskDispatcher) _scheduleRequest(tenantID, functionName, functionVersion, stateID string) ([]string, bool) {
	functionKey := utils.GetFuncKey(tenantID, functionName, functionVersion)
	var ip, port, instanceID string
	if td.registry == nil {
		return []string{ip, port, instanceID}, false
	}
	td.registry.ProvidersMu.RLock()
	defer td.registry.ProvidersMu.RUnlock()

	if _, ok := td.registry.Providers[functionKey]; !ok && stateID == "" {
		return []string{ip, port, instanceID}, false
	}
	instanceIf, ok := td.registry.States.Load(stateID)
	if !ok {
		return []string{ip, port, instanceID}, false
	}
	instance, ok := instanceIf.(string)
	if !ok {
		log.GetLogger().Errorf("failed to assert instance type")
	}
	nodeInfo, exist := td.registry.Providers[functionKey][instance]
	if exist {
		ip = nodeInfo.NodeIP
		port = nodeInfo.P2pPort
		return []string{ip, port, instance}, true
	}
	return []string{ip, port, instance}, false
}
