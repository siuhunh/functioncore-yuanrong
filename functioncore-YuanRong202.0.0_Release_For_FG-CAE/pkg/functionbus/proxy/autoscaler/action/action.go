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

// Package action interacts with worker-manager
package action

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/caemetrics"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/resourceusage"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

const (
	workerManagerDeployURL = "/worker-manager/v1/functions/sn:cn:yrk:%s:function:%s:%s/worker"
	workerManagerDeleteURL = "/worker-manager/v1/functions/worker/delete"
	statusOKCode           = 150200
	nodeLabelKey           = "kubernetes.io/hostname"
	nodeLabelDefaultLength = 8
	scaleDownTimeout       = 30 * time.Second
)

// GetWorkerSuccessResponse define
type GetWorkerSuccessResponse struct {
	Worker   *Worker         `json:"worker"`
	Instance *types.Instance `json:"instance"`
	Code     int             `json:"code"`
	Message  string          `json:"message"`
}

// Worker define a worker
type Worker struct {
	Instances       []*types.Instance `json:"instances"`
	FunctionName    string            `json:"functionname"`
	FunctionVersion string            `json:"functionversion"`
	Tenant          string            `json:"tenant"`
	Business        string            `json:"business"`
}

// DeployParam Deploy function param
type DeployParam struct {
	FuncName        string // deployed function name
	Applier         string // applier instance id
	DeployNode      string // deploy node id
	Business        string // business
	TenantID        string // tenant id
	Version         string // version
	OwnerIP         string // owner IP
	TraceID         string // trace id
	TriggerFlag     string // This is a trigger request flag.
	StateID         string
	OwnedWorkerView types.WorkersView // proxy owned worker view
	CPU             int
	Memory          int
}

// DeleteParam Delete function param
type DeleteParam struct {
	InstanceID         string `json:"instance_id"`
	FuncName           string `json:"function_name"`
	FuncVersion        string `json:"function_version"`
	BusinessID         string `json:"business_id"`
	TenantID           string `json:"tenant_id"`
	Applier            string `json:"applier"`
	IsBrokenConnection bool   `json:"broken_connection_status"`
}

// DeleteWorkerResponse is response to delete worker
type DeleteWorkerResponse struct {
	Code     int    `json:"code"`
	Reserved bool   `json:"reserved"`
	Message  string `json:"message"`
}

// ScaleUp -
func ScaleUp(scaleUpParam common.ScaleUpParam) (*types.Instance, uint32, string) {
	log.GetLogger().Debugf("start to scale up function: %s, traceID: %s, nodeLabel: %s, stateID: %s, cpu: %s, "+
		"memory: %s timeout: %s", urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID,
		scaleUpParam.NodeLabel, scaleUpParam.StateID, scaleUpParam.InsResource.CPU, scaleUpParam.InsResource.Memory,
		scaleUpParam.Timeout)
	ctx, cancel := context.WithTimeout(wmhealth.GetContext(), scaleUpParam.Timeout)
	defer cancel()
	request := makeScaleUpRequest(ctx, scaleUpParam)
	resp, err := utils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Errorf("send request to worker manager error: %s, functionKey: %s, traceID: %s",
			err.Error(), urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID)
		return nil, proxyerrors.ScaleUpRequestErrCode, proxyerrors.ScaleUpRequestErrMsg
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("failed to get response body: %s, functionKey: %s, traceID: %s", err.Error(),
			urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID)
		return nil, proxyerrors.ScaleUpRequestErrCode, proxyerrors.ScaleUpRequestErrMsg
	}

	scaleUpResp := GetWorkerSuccessResponse{}
	if err = json.Unmarshal(respBody, &scaleUpResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal scaleUp response: %s, functionKey: %s, traceID: %s, body %s",
			err, urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID, string(respBody))
		return nil, proxyerrors.ScaleUpRequestErrCode, proxyerrors.ScaleUpRequestErrMsg
	}

	if resp.StatusCode != http.StatusOK {
		return nil, uint32(scaleUpResp.Code), scaleUpResp.Message
	}

	if scaleUpResp.Code == snerror.InnerRuntimeInitTimeoutCode && scaleUpResp.Worker != nil {
		log.GetLogger().Infof("deploy functionKey %s instance: %v traceID: %s with code %d msg %s",
			urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpResp.Instance, scaleUpParam.TraceID,
			scaleUpResp.Code, scaleUpResp.Message)
		return scaleUpResp.Instance, uint32(scaleUpResp.Code), scaleUpResp.Message
	}

	if scaleUpResp.Code != statusOKCode || scaleUpResp.Worker == nil {
		log.GetLogger().Warnf("deploy response code %d, functionKey: %s, traceID: %s", scaleUpResp.Code,
			urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID)
		if scaleUpResp.Code == 0 {
			return nil, proxyerrors.ScaleUpRequestErrCode, scaleUpResp.Message
		}
		return nil, uint32(scaleUpResp.Code), scaleUpResp.Message
	}
	log.GetLogger().Infof("deploy functionKey %s instance: %v traceID: %s successfully",
		urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpResp.Instance, scaleUpParam.TraceID)
	return scaleUpResp.Instance, 0, ""
}

var localNodeLabel = parseLocalNodeLabel()

func parseLocalNodeLabel() string {
	data, err := json.Marshal(makeLocalNodeLabel())
	if err != nil {
		log.GetLogger().Errorf("failed to marshal nodeLabelList: %s", err.Error())
		return ""
	}
	return string(data)
}

func makeScaleUpRequest(ctx context.Context, scaleUpParam common.ScaleUpParam) *http.Request {
	tenantID, functionName, functionVersion := utils.ParseFuncKey(scaleUpParam.FunctionKey)
	requestURI := wmhealth.GetWorkerManagerBaseURL() +
		fmt.Sprintf(workerManagerDeployURL, tenantID, functionName, functionVersion)

	var forceDeploy = true
	if scaleUpParam.NodeLabel == "" {
		scaleUpParam.NodeLabel = localNodeLabel
		forceDeploy = false
	}

	cpu, memory, err := getResourceInfo(scaleUpParam.InsResource)
	if err != nil {
		// when the CPU and memory are 0, the workermanager queries the CPU and memory from the CR.
		log.GetLogger().Debugf("failed to convert the type info of instanceResource. err: %s", err.Error())
	}
	param := DeployParam{
		FuncName:   functionName,
		Applier:    internal.HostName,
		DeployNode: nodeID,
		Business:   common.InternalScaleBusinessID,
		TenantID:   tenantID,
		Version:    functionVersion,
		OwnerIP:    internal.SelfNodeIP,
		StateID:    scaleUpParam.StateID,
		OwnedWorkerView: types.WorkersView{OwnedNum: scaleUpParam.OwnedWorkerNum,
			CurrentWorkerNum: scaleUpParam.CurrentWorkerNum, ScalingWorkersNum: scaleUpParam.ScalingWorkersNum},
		CPU:    cpu,
		Memory: memory,
	}

	data, err := json.Marshal(param)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal scaleUp request: %s", err.Error())
		return nil
	}

	request, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("action failed when make scaleUp request, err %s", err.Error())
		return nil
	}
	utils.FillInWorkerManagerRequestHeaders(request)
	log.GetLogger().Debugf("succeeded to sign the authorization of function: %s, traceID: %s",
		urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID)
	request.Header.Set(constants.HeaderTraceID, scaleUpParam.TraceID)
	request.Header.Set(constants.HeaderNodeLabel, scaleUpParam.NodeLabel)
	request.Header.Set(constants.HeaderForceDeploy, strconv.FormatBool(forceDeploy))
	request.Header.Set(constants.HeaderTraceParent, scaleUpParam.ChildTraceParent)

	return request
}

func getResourceInfo(insResource *types.InstanceResource) (int, int, error) {
	cpu, memory, err := convertResourceToInt(insResource)
	if err == nil {
		return cpu, memory, nil
	}
	log.GetLogger().Debugf("request don't carry CPU/memory information. assign zero value")
	return 0, 0, err
}

func convertResourceToInt(insResource *types.InstanceResource) (int, int, error) {
	cpu, err := strconv.Atoi(insResource.CPU)
	if err != nil {
		return 0, 0, err
	}
	memory, err := strconv.Atoi(insResource.Memory)
	if err != nil {
		return 0, 0, err
	}
	return cpu, memory, nil
}

var nodeID = os.Getenv("NODE_ID")

func makeLocalNodeLabel() []map[string]string {
	nodeLabelList := make([]map[string]string, 0, nodeLabelDefaultLength)
	localHost := make(map[string]string, nodeLabelDefaultLength)
	localHost[nodeLabelKey] = nodeID
	nodeLabelList = append(nodeLabelList, localHost)
	return nodeLabelList
}

// ScaleDown sends request to workerManager scale down instance.
// first return value is whether the instance scaled down or no need to retry
func ScaleDown(instanceID, functionKey string, isBrokenConnection bool) (bool, uint32, string) {
	log.GetLogger().Infof("start to scaleDown instance: %s", instanceID)
	ctx, cancel := context.WithTimeout(wmhealth.GetContext(), scaleDownTimeout)
	request := makeScaleDownRequest(ctx, instanceID, functionKey, isBrokenConnection)
	resp, err := utils.GetWorkerManagerClient().Do(request)
	cancel()
	if err != nil {
		log.GetLogger().Errorf("send scaleDown request to worker manager error: %s, functionKey: %s",
			err.Error(), urnutils.AnonymizeTenantKey(functionKey))
		// when workerManager is inaccessible, busProxy send scale down request failed.
		// worker instance is exited and can not be scale down.
		addToRetryList(instanceID, functionKey, isBrokenConnection)
		return false, proxyerrors.SendHTTPErrCode, proxyerrors.SendHTTPErr
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("failed to get response body: %s, functionKey: %s", err.Error(),
			urnutils.AnonymizeTenantKey(functionKey))
		return true, proxyerrors.ScaleDownRequestErrCode, proxyerrors.ScaleDownRequestErrMsg
	}

	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Warnf("failed to unmarshal scaleDown response: %s, functionKey: %s", err,
				urnutils.AnonymizeTenantKey(functionKey))
			return true, proxyerrors.ScaleDownRequestErrCode, proxyerrors.ScaleDownRequestErrMsg
		}
		return true, uint32(snErr.Code), snErr.Message
	}

	scaleDownResp := DeleteWorkerResponse{}
	if err := json.Unmarshal(respBody, &scaleDownResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal scaleDown response: %s, functionKey: %s", err,
			urnutils.AnonymizeTenantKey(functionKey))
		return true, proxyerrors.ScaleDownRequestErrCode, proxyerrors.ScaleDownRequestErrMsg
	}

	if scaleDownResp.Code != http.StatusOK {
		log.GetLogger().Warnf("scale down functionKey %s response code %d, response message %s",
			urnutils.AnonymizeTenantKey(functionKey), scaleDownResp.Code, scaleDownResp.Message)
		if scaleDownResp.Code == 0 {
			return true, proxyerrors.ScaleDownRequestErrCode, scaleDownResp.Message
		}
		return true, uint32(scaleDownResp.Code), scaleDownResp.Message
	}
	if scaleDownResp.Code == http.StatusOK && scaleDownResp.Reserved == false {
		internal.DeleteOwnedInstances(instanceID)
		resourceusage.GetNodeResourceUsageInstance().RemoveByInstance(functionKey, instanceID)
		caemetrics.GetCaeMetricsHandler().RemoveByInstance(functionKey, instanceID)
	}
	log.GetLogger().Infof("scale down functionKey %s instanceID %s and reserved result is %t",
		instanceID, urnutils.AnonymizeTenantKey(functionKey), scaleDownResp.Reserved)

	return true, 0, ""
}

func makeScaleDownRequest(ctx context.Context, instanceID, functionKey string, isBrokenConnection bool) *http.Request {
	tenantID, functionName, functionVersion := utils.ParseFuncKey(functionKey)
	requestURI := wmhealth.GetWorkerManagerBaseURL() + workerManagerDeleteURL

	param := DeleteParam{
		InstanceID:         instanceID,
		FuncName:           functionName,
		FuncVersion:        functionVersion,
		BusinessID:         common.InternalScaleBusinessID,
		TenantID:           tenantID,
		Applier:            internal.HostName,
		IsBrokenConnection: isBrokenConnection,
	}

	data, err := json.Marshal(param)
	if err != nil {
		log.GetLogger().Errorf("marshal error when make scaleUp request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("error when make scaleUp request: %s", err.Error())
		return nil
	}

	utils.FillInWorkerManagerRequestHeaders(request)
	return request
}
