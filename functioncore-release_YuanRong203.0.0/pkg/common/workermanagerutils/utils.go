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

// Package workermanagerutils is common utils of querying instances from worker-manager
package workermanagerutils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
)

const (
	queryInstancesURL                             = "/worker-manager/v1/functions/instances"
	workerManagerNodePort                         = "30007"
	appID                                         = "ondemand"
	dialTimeout                                   = 3 * time.Second
	idleConnTimeout                               = 90 * time.Second
	connKeepAlive                                 = 30 * time.Second
	maxIdleConns                                  = 100
	defaultRequestWorkerManagerInstanceNumTimeout = 10 * time.Second
	getMethod                                     = "GET"
)

const (
	// StatusOKCode -
	StatusOKCode = 150200
	// ScaleDownTimeout -
	ScaleDownTimeout = 30 * time.Second
)

var (
	instanceOnce       sync.Once
	instanceHTTPClient *http.Client

	// ErrReachMaxInstance - scale up error of reach max instance
	ErrReachMaxInstance = errors.New("this cluster reach max instance")
	// ErrReachMaxOnDemandInstance - scale up error of reach on demand instance
	ErrReachMaxOnDemandInstance = errors.New("this cluster reach on demand max instance")
	// ErrFunctionDisabled - scale up error of function disabled
	ErrFunctionDisabled = errors.New("this cluster function disabled")
)

// QueryInstancesFromWmByIP query instances from worker-manger
func QueryInstancesFromWmByIP(funcSvc string, nodeIP string, enableHTTPS bool,
	auth *localauth.AuthConfig) (types.QueryInstanceResponse, error) {
	u, err := url.Parse(fmt.Sprintf("%s%s", GetWorkerManagerBaseURLByIP(enableHTTPS, nodeIP), queryInstancesURL))
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to parse url, %s", err.Error())
	}
	params := url.Values{}
	params.Add(constants.FuncSvcID, funcSvc)

	req, err := http.NewRequest(getMethod, u.String(), nil)
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to create request, %s", err.Error())
	}
	req.URL.RawQuery = params.Encode()
	FillInWorkerManagerRequestHeaders(req, auth)
	resp, err := GetHTTPClient(enableHTTPS).Do(req)
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to send request to worker-manager, %s",
			err.Error())
	}
	defer closeResponse(resp)
	if resp != nil && resp.StatusCode != http.StatusOK {
		if resp.Body != nil {
			if respBody, err := ioutil.ReadAll(resp.Body); err == nil {
				return types.QueryInstanceResponse{}, fmt.Errorf("resp code: %d, resp body: %s",
					resp.StatusCode, respBody)
			}
		}
		return types.QueryInstanceResponse{}, fmt.Errorf("resp code: %d", resp.StatusCode)
	}

	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to read resp body, %s", err.Error())
	}
	var queryInsResp types.QueryInstanceResponse
	if err = json.Unmarshal(respBytes, &queryInsResp); err != nil {
		return types.QueryInstanceResponse{}, fmt.Errorf("failed to unmarshal resp body, %s", err.Error())
	}
	return queryInsResp, nil
}

// GetWorkerManagerBaseURLByIP get worker-manager base url
func GetWorkerManagerBaseURLByIP(enableHTTPS bool, targetIP string) string {
	return tls.GetURLScheme(enableHTTPS) + "://" + targetIP + ":" + workerManagerNodePort
}

// FillInWorkerManagerRequestHeaders contains authorization, source and so on
func FillInWorkerManagerRequestHeaders(request *http.Request, auth *localauth.AuthConfig) {
	authorization, timestamp := localauth.SignLocally(auth.AKey, auth.SKey, appID, auth.Duration)
	request.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	request.Header.Set(constants.Authorization, authorization)
	request.Header.Set(constants.HeaderEventSourceID, appID)
	request.Header.Set(constants.HeaderCallType, "active")
}

// GetHTTPClient -
func GetHTTPClient(enableHTTPS bool) *http.Client {
	instanceOnce.Do(func() {
		transport := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: connKeepAlive,
			}).DialContext,
			MaxIdleConns:        maxIdleConns,
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     idleConnTimeout,
			TLSHandshakeTimeout: dialTimeout,
		}
		instanceHTTPClient = &http.Client{
			Timeout:   defaultRequestWorkerManagerInstanceNumTimeout,
			Transport: transport,
		}
		if enableHTTPS {
			transport.TLSClientConfig = tls.GetClientTLSConfig()
		}
	})
	return instanceHTTPClient
}

func closeResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		log.GetLogger().Errorf("failed to close response body, err: %s", err.Error())
	}
}

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

// HandleResp -
func HandleResp(functionSvcID string, respBody []byte, traceID string) (GetWorkerSuccessResponse,
	*types.Instance, error) {
	scaleUpResp := GetWorkerSuccessResponse{}
	if err := json.Unmarshal(respBody, &scaleUpResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal scaleUp response: %s, functionSvcID: %s, traceID: %s",
			err, functionSvcID, traceID)
		return GetWorkerSuccessResponse{}, nil, err
	}

	if scaleUpResp.Code == snerror.ReachMaxInstancesCode {
		log.GetLogger().Warnf("deploy response this cluster reach max instance, functionSvcID: %s, traceID: %s",
			functionSvcID, traceID)
		return GetWorkerSuccessResponse{}, nil, ErrReachMaxInstance
	}

	if scaleUpResp.Code == snerror.ReachMaxOnDemandInstancesPerTenant {
		log.GetLogger().Warnf("deploy response this cluster reach max on demand instance, functionSvcID: %s, traceID: %s",
			functionSvcID, traceID)
		return GetWorkerSuccessResponse{}, nil, ErrReachMaxOnDemandInstance
	}

	if scaleUpResp.Code == snerror.FunctionIsDisabled {
		log.GetLogger().Warnf("deploy response this cluster function disabled, functionSvcID: %s, traceID: %s",
			functionSvcID, traceID)
		return GetWorkerSuccessResponse{}, nil, ErrFunctionDisabled
	}

	if scaleUpResp.Code != StatusOKCode || scaleUpResp.Worker == nil {
		log.GetLogger().Warnf("deploy response code %d, functionSvcID: %s, traceID: %s", scaleUpResp.Code,
			functionSvcID, traceID)
		return GetWorkerSuccessResponse{}, nil, fmt.Errorf(scaleUpResp.Message)
	}
	return scaleUpResp, nil, nil
}
