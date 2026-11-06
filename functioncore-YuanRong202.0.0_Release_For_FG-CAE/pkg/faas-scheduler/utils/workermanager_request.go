/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
)

const (
	workerManagerDeployURL = "/worker-manager/v1/functions/sn:cn:yrk:%s:function:%s:%s/worker"
	workerManagerDeleteURL = "/worker-manager/v1/functions/worker/delete"
	statusOKCode           = 150200
	scaleDownTimeout       = 30 * time.Second
	funcKeyLen             = 3
)

var nodeID = os.Getenv("NODE_IP")

const hostName = "faas-scheduler"

// ScaleOutParam scale up param
type ScaleOutParam struct {
	InsResource *types.InstanceResource
	TraceID     string
	FunctionKey string
	NodeLabel   string
	Timeout     time.Duration
}

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

func makeScaleOutParam(req *schedulerapi.AcquireInstanceReq) (*ScaleOutParam, string) {
	funcKeyWithoutRes, cpu, memory := urnutils.GetFuncKeyAndResFromFuncKeyWithResource(req.FuncKeyWithRes)
	anonymizedFuncKeyWithRes := urnutils.AnonymizeTenantKey(funcKeyWithoutRes)
	requestTimeout, err := time.ParseDuration(req.Timeout)
	if err != nil {
		log.GetLogger().Warnf("traceID: %s | FuncKeyWithRes: %s | failed to parse duration: %s",
			req.TraceID, anonymizedFuncKeyWithRes, err.Error())
		requestTimeout = defaultRequestWorkerManagerImageTimeout
	}
	return &ScaleOutParam{
		InsResource: &types.InstanceResource{
			CPU:    cpu,
			Memory: memory,
		},
		TraceID:     req.TraceID,
		FunctionKey: funcKeyWithoutRes,
		Timeout:     requestTimeout,
	}, anonymizedFuncKeyWithRes
}

// ScaleOutInstance send scale out req to worker manager. e.g. websocket function instance
func ScaleOutInstance(req *schedulerapi.AcquireInstanceReq) error {
	log.GetLogger().Infof("traceID: %s | FuncKeyWithRes: %s | timeout: %s | start to scale out",
		req.TraceID, req.FuncKeyWithRes, req.Timeout)
	scaleOutParam, anonymizedFuncKeyWithRes := makeScaleOutParam(req)
	if scaleOutParam == nil {
		return errors.New("makeScaleOutParamFailed")
	}
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, scaleOutParam.Timeout)
	defer cancel()
	request := makeScaleOutRequest(ctx, scaleOutParam)
	if request == nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to make scale out request",
			scaleOutParam.TraceID, anonymizedFuncKeyWithRes)
		return errors.New("makeScaleOutRequestFailed")
	}
	resp, err := GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to send request to worker manager: %s",
			scaleOutParam.TraceID, anonymizedFuncKeyWithRes, err.Error())
		return err
	}
	return handleScaleOutResponseFromWorkerManager(resp, scaleOutParam, anonymizedFuncKeyWithRes)
}

// ScaleInInstance sends request to workerManager scale down instance.
// the returned value means whether the instance has been scaled in successfully
func ScaleInInstance(instanceID, functionKey, traceID string) {
	anonymizedFuncKey := urnutils.AnonymizeTenantKey(functionKey)
	log.GetLogger().Infof("traceID: %s | FuncKeyWithRes: %s | instanceID: %s | start to scale in instance",
		traceID, anonymizedFuncKey, instanceID)
	ctx := context.TODO()
	ctx, cancel := context.WithTimeout(ctx, scaleDownTimeout)
	defer cancel()
	request := makeScaleInRequest(ctx, instanceID, functionKey)
	if request == nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to make scale out request",
			traceID, anonymizedFuncKey)
		return
	}
	resp, err := GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | error: %s | sent http request error and "+
			"failed to send scaleIn request to worker manager", traceID, anonymizedFuncKey, err.Error())
		return
	}
	handleScaleInResponseFromWorkerManager(instanceID, functionKey, traceID, resp)
}

func makeScaleOutRequest(ctx context.Context, scaleUpParam *ScaleOutParam) *http.Request {
	tenantID, functionName, functionVersion := parseFuncKey(scaleUpParam.FunctionKey)
	requestURI := GetWorkerManagerBaseURL() +
		fmt.Sprintf(workerManagerDeployURL, tenantID, functionName, functionVersion)
	cpu, memory, err := getResourceInfo(scaleUpParam.InsResource)
	if err != nil {
		// when the CPU and memory are 0, the worker-manager queries the CPU and memory from the CR.
		log.GetLogger().Debugf("failed to convert the type info of instanceResource. err: %s", err.Error())
		return nil
	}
	param := DeployParam{
		FuncName:   functionName,
		Applier:    hostName,
		DeployNode: nodeID,
		Business:   "yrk",
		TenantID:   tenantID,
		Version:    functionVersion,
		OwnerIP:    nodeID,
		CPU:        cpu,
		Memory:     memory,
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
	FillInWorkerManagerRequestHeaders(request)
	log.GetLogger().Debugf("succeeded to sign the authorization of function: %s, traceID: %s",
		urnutils.AnonymizeTenantKey(scaleUpParam.FunctionKey), scaleUpParam.TraceID)
	request.Header.Set(constants.HeaderTraceID, scaleUpParam.TraceID)
	request.Header.Set(constants.HeaderForceDeploy, strconv.FormatBool(false))
	return request
}

func makeScaleInRequest(ctx context.Context, instanceID, functionKey string) *http.Request {
	tenantID, functionName, functionVersion := parseFuncKey(functionKey)
	requestURI := GetWorkerManagerBaseURL() + workerManagerDeleteURL
	param := DeleteParam{
		InstanceID:  instanceID,
		FuncName:    functionName,
		FuncVersion: functionVersion,
		BusinessID:  "yrk",
		TenantID:    tenantID,
		Applier:     hostName,
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
	FillInWorkerManagerRequestHeaders(request)
	return request
}

func handleScaleOutResponseFromWorkerManager(resp *http.Response,
	scaleOutParam *ScaleOutParam, anonymizedFuncKeyWithRes string) error {
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to get response body: %s",
			scaleOutParam.TraceID, anonymizedFuncKeyWithRes, err.Error())
		return err
	}
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to unmarshal scale out "+
				"response: %s", scaleOutParam.TraceID, anonymizedFuncKeyWithRes, err.Error())
			return err
		}
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | errCode: %d | errMessage: %s | failed to "+
			"scale out function instance", scaleOutParam.TraceID, anonymizedFuncKeyWithRes, snErr.Code, snErr.Message)
		return snerror.New(snErr.Code, snErr.Message)
	}
	scaleOutResp := GetWorkerSuccessResponse{}
	if err = json.Unmarshal(respBody, &scaleOutResp); err != nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to unmarshal scaleUp response: %s",
			scaleOutParam.TraceID, anonymizedFuncKeyWithRes, err.Error())
		return err
	}
	if scaleOutResp.Code != statusOKCode || scaleOutResp.Worker == nil {
		log.GetLogger().Warnf("traceID: %s | FuncKeyWithRes: %s | responseCode: %d | responseMessage: %s | "+
			"worker manager returned error", scaleOutParam.TraceID, anonymizedFuncKeyWithRes,
			scaleOutResp.Code, scaleOutResp.Message)
		return snerror.New(scaleOutResp.Code, scaleOutResp.Message)
	}
	log.GetLogger().Infof("traceID: %s | funcKeyWithRes: %s | instanceKey: %s | succeed to scale out "+
		"function instance", scaleOutParam.TraceID, anonymizedFuncKeyWithRes, scaleOutResp.Instance.InstanceID)
	return nil
}

func handleScaleInResponseFromWorkerManager(instanceID, functionKey, traceID string, resp *http.Response) {
	anonymizedFuncKey := urnutils.AnonymizeTenantKey(functionKey)
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | error: %s | failed to get response body",
			traceID, anonymizedFuncKey, err.Error())
		return
	}
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | error: %s | failed to unmarshal scale "+
				"in bad response", traceID, anonymizedFuncKey, err.Error())
			return
		}
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | responseCode: %d | responseMessage: %s | "+
			"worker manager returned bad response", traceID, anonymizedFuncKey, snErr.Code, snErr.Message)
		return
	}
	scaleDownResp := DeleteWorkerResponse{}
	if err := json.Unmarshal(respBody, &scaleDownResp); err != nil {
		log.GetLogger().Errorf("traceID: %s | FuncKeyWithRes: %s | failed to unmarshal scale in response: "+
			"DeleteWorkerResponse", traceID, anonymizedFuncKey)
		return
	}
	if scaleDownResp.Code != http.StatusOK {
		log.GetLogger().Warnf("traceID: %s | FuncKeyWithRes: %s | responseCode: %d | responseMessage: %s | "+
			"worker manager returned error", traceID, anonymizedFuncKey, scaleDownResp.Code, scaleDownResp.Message)
		return
	}
	log.GetLogger().Infof("traceID: %s | FuncKeyWithRes: %s | instanceID: %s | succeed to scale in instance",
		traceID, anonymizedFuncKey, instanceID)
	return
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

// parse funcKey with format "tenantID/funcName/funcVersion" or "tenantID/funcName/funcVersion/CPU-memory"
func parseFuncKey(funcKey string) (string, string, string) {
	funcInfo := strings.Split(funcKey, "/")
	if len(funcInfo) < funcKeyLen {
		return "", "", ""
	}
	return funcInfo[0], funcInfo[1], funcInfo[2]
}
