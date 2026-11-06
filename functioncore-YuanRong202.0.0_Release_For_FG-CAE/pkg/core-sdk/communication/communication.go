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

// Package communication implements function of http connection
package communication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/types/bridgetypes"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/core-sdk/common"
	configs "functioncore/pkg/core-sdk/config"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/worker-manager/utils"
)

var (
	addr       = fmt.Sprintf("state-manager.%s.svc.cluster.local:8087", commonUtils.SysNameSpace())
	httpClient *http.Client
	once       sync.Once
)

const (
	appID                = "coresdk"
	reqTimeout           = 10 * time.Second
	defaultRetryTimes    = 5
	defaultRetryInterval = time.Duration(200) * time.Millisecond
)

// FuncInfo keeps function information in request message
type FuncInfo struct {
	FunctionName  string                          `json:"functionName,omitempty"`
	StateID       string                          `json:"stateID"`
	TenantID      string                          `json:"tenantID,omitempty"`
	ProxyFunction string                          `json:"proxyFunction,omitempty"`
	TraceID       string                          `json:"traceID,omitempty"`
	Creator       string                          `json:"creator,omitempty"`
	Version       string                          `json:"version,omitempty"`
	BusinessID    string                          `json:"businessID,omitempty"`
	ServiceID     string                          `json:"serviceID,omitempty"`
	FuncName      string                          `json:"funcName,omitempty"`
	CreateOptions *commontypes.StateCreateOptions `json:"createOptions,omitempty"`
}

type requestParam struct {
	Ctx  context.Context
	Body []byte
	Path string
}

// CreateStateResponse keeps response message when creating state
type CreateStateResponse struct {
	Code    int    `json:"code"`
	StateID string `json:"stateID,omitempty"`
	NodeID  string `json:"nodeID,omitempty"`
	Err     string `json:"error,omitempty"`
}

// DeleteStateResponse keeps response message when deleting state
type DeleteStateResponse struct {
	Code int    `json:"code"`
	Err  string `json:"error,omitempty"`
}

// InvokeFuncResponse define
type InvokeFuncResponse struct {
	StateKey              string `json:"stateKey"`
	Code                  int    `json:"code"`
	Message               string `json:"message"`
	IsTerminate           bool   `json:"isTerminate"`
	IsNewStateKeyScenario bool   `json:"isNewStateKeyScenario"`
}

// CreateStateByHTTP create state of function by http
func CreateStateByHTTP(funcName string, stateID string, config types.CreateStateConfig) (string, string,
	snerror.SNError) {
	body := FuncInfo{
		FunctionName:  funcName,
		StateID:       stateID,
		TenantID:      config.TenantID,
		ProxyFunction: config.ProxyFunction,
		TraceID:       config.TraceID,
		Creator:       config.Creator,
		ServiceID:     config.SnServiceID,
		FuncName:      config.FuncName,
		BusinessID:    config.BusinessID,
		Version:       config.Version,
		CreateOptions: config.CreateOptions,
	}
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		log.GetLogger().Errorf("error when marshal state request body, stateID: %s, traceID: error: %s",
			stateID, config.TraceID, marshalErr.Error())
		return "", "", snerror.New(common.InternalErrCode, marshalErr.Error())
	}
	path := ""
	if configs.FConfigs.HTTPSConfig.HTTPSEnable {
		path = "https://" + addr + "/createstate"
	} else {
		path = "http://" + addr + "/createstate"
	}

	reqParam := requestParam{
		Ctx:  context.Background(),
		Body: data,
		Path: path,
	}
	return doCreateRequest(reqParam, config.TraceID)
}

// DeleteStateByHTTP delete state mapping by http
func DeleteStateByHTTP(stateID string, tenantID, proxyFunction string) snerror.SNError {
	body := FuncInfo{
		StateID:       stateID,
		TenantID:      tenantID,
		ProxyFunction: proxyFunction,
	}
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		return snerror.New(common.InternalErrCode, marshalErr.Error())
	}

	path := ""
	if configs.FConfigs.HTTPSConfig.HTTPSEnable {
		path = "https://" + addr + "/deletestate"
	} else {
		path = "http://" + addr + "/deletestate"
	}
	req, err := makePostRequest(requestParam{
		Ctx:  context.Background(),
		Body: data,
		Path: path,
	})

	if err != nil {
		log.GetLogger().Errorf("error when make post request, stateID: %s, error: %s", stateID, err.Error())
		return snerror.New(common.InternalErrCode, err.Error())
	}

	return doDeleteRequest(req)
}

func doCreateRequest(reqParam requestParam, traceID string) (string, string, snerror.SNError) {
	resp, err := retryDoHTTPRequest(reqParam, traceID)
	if err != nil {
		log.GetLogger().Errorf("doing fasthttp state request err: %s, with traceID %s", err.Error(), traceID)
		return "", "", snerror.New(common.StateClientErrCode, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		result, err := getErrorResponse(resp)
		if err == nil {
			log.GetLogger().Errorf("getErrorResponse, message: %s, status code: %d, traceID: %s",
				result.Message, result.Code, traceID)
			if result.Code == snerror.TenantStateNumExceedLimit {
				return "", "", snerror.New(snerror.TenantStateNumExceedLimit, result.Message)
			}
		} else {
			log.GetLogger().Errorf("getErrorResponse error, traceID: %s, error is %s", traceID, err.Error())
		}

		return "", "", snerror.New(common.InternalErrCode, "http response status is not ok")
	}

	var result CreateStateResponse
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.GetLogger().Errorf("err when read create state response body, traceID: %s, error: %s", traceID, err.Error())
		return "", "", snerror.New(common.InternalErrCode, err.Error())
	}
	unmarshalErr := json.Unmarshal(respBody, &result)
	if unmarshalErr != nil {
		log.GetLogger().Errorf("err when unmarshal state response body, traceID: %s, error: %s",
			traceID, unmarshalErr.Error())
		return "", "", snerror.New(common.InternalErrCode, unmarshalErr.Error())
	}

	return result.StateID, result.NodeID, nil
}

func retryDoHTTPRequest(reqParam requestParam, traceID string) (*http.Response, error) {
	if httpClient == nil {
		return nil, snerror.New(common.StateClientErrCode, common.HTTPClientErrMsg)
	}
	var err error
	var resp *http.Response
	var req *http.Request

	for i := 1; i <= defaultRetryTimes; i++ {
		req, err = makePostRequest(reqParam)
		if err != nil {
			log.GetLogger().Errorf("error when make post request, traceID: %s, error: %s", traceID, err.Error())
			return resp, err
		}
		resp, err = httpClient.Do(req)
		if err != nil && (strings.Contains(err.Error(), "EOF") ||
			strings.Contains(err.Error(), "cannot assign requested address")) {
			log.GetLogger().Warnf("doing fasthttp state request err: %s, with traceID %s, request times: %d",
				err.Error(), traceID, i)
			time.Sleep(defaultRetryInterval)
		} else {
			break
		}
	}
	return resp, err
}

func getErrorResponse(resp *http.Response) (snerror.BadResponse, error) {
	var result snerror.BadResponse
	if resp.Body != nil {
		defer resp.Body.Close()
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return snerror.BadResponse{}, snerror.New(common.InternalErrCode, err.Error())
		}
		unmarshalErr := json.Unmarshal(respBody, &result)
		if unmarshalErr != nil {
			return snerror.BadResponse{}, snerror.New(common.InternalErrCode, unmarshalErr.Error())
		}

		return result, nil
	}

	return snerror.BadResponse{}, snerror.New(common.InternalErrCode, "http response status is not ok, resp body is nil")
}

func doDeleteRequest(req *http.Request) snerror.SNError {
	if httpClient == nil {
		log.GetLogger().Errorf("failed to get state, error: stateClient is nil")
		return snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return snerror.New(common.InternalErrCode, err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("http response status is not ok, status code: %d", resp.StatusCode)
		return snerror.New(common.InternalErrCode, "http response status is not ok")
	}

	var result DeleteStateResponse
	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("err when read delete state response body: %s", err.Error())
		return snerror.New(common.InternalErrCode, err.Error())
	}
	unmarshalErr := json.Unmarshal(respBody, &result)
	if unmarshalErr != nil {
		return snerror.New(common.InternalErrCode, unmarshalErr.Error())
	}

	return nil
}

func makePostRequest(reqParam requestParam) (*http.Request, error) {
	request, err := http.NewRequest("POST", reqParam.Path, bytes.NewBuffer(reqParam.Body))
	if err != nil {
		log.GetLogger().Errorf("failed to make stateID request, err %s", err.Error())
		return nil, err
	}
	request.Header.Set(constants.HeaderEventSourceID, appID)
	request.Header.Set(constants.HeaderCallType, "active")
	request.Header.Set(constants.ContentType, constants.ApplicationJSON)
	authorization, timestamp := localauth.SignLocally(configs.FConfigs.LocalAuth.AKey,
		configs.FConfigs.LocalAuth.SKey, utils.AppID, configs.FConfigs.LocalAuth.Duration)
	request.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	request.Header.Set(constants.Authorization, authorization)
	return request, nil
}

// ApplyStateKeyByHTTP get bridgeFunc stateKey by http
func ApplyStateKeyByHTTP(funcName string, clientInstance bridgetypes.ClientInstance, traceID string) (
	InvokeFuncResponse, snerror.SNError) {
	log.GetLogger().Debugf("start ApplyStateKeyByHTTP, traceID: %s, funcName: %s", traceID, funcName)
	if clientInstance.FuncInfo == nil {
		log.GetLogger().Errorf("param clientInstance.FuncInfo of func ApplyStateKeyByHTTP is nil, traceID: %s, "+
			"funcName: %s", traceID, funcName)
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, "param clientInstance.FuncInfo "+
			"of func ApplyStateKeyByHTTP is nil")
	}

	body := bridgetypes.FuncRequestInfo{
		TraceID:        traceID,
		FunctionName:   funcName,
		ClientInstance: clientInstance,
	}
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		log.GetLogger().Errorf("error when marshal state request body: %s, traceID:%s, funcName:%s",
			marshalErr.Error(), traceID, funcName)
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, marshalErr.Error())
	}
	path := ""
	if configs.FConfigs.HTTPSConfig.HTTPSEnable {
		path = "https://" + addr + "/applyStateKey"
	} else {
		path = "http://" + addr + "/applyStateKey"
	}
	reqParam := requestParam{
		Ctx:  context.Background(),
		Body: data,
		Path: path,
	}
	log.GetLogger().Debugf("request path is %s, traceID: %s, funcName: %s", path, traceID, funcName)
	return doCreateFuncRequest(reqParam, traceID)
}

func doCreateFuncRequest(param requestParam, traceID string) (InvokeFuncResponse, snerror.SNError) {
	resp, err := retryDoHTTPRequest(param, traceID)
	if err != nil {
		log.GetLogger().Errorf("doing fasthttp state request err: %s, with traceID %s", err.Error(), traceID)
		return InvokeFuncResponse{}, snerror.New(common.StateClientErrCode, err.Error())
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("http response status is not ok, status code: %d, traceID: %s", resp.StatusCode,
			traceID)
		result, err := getErrorResponse(resp)
		if err == nil && result.Code == snerror.BridgeManagerFailCode {
			log.GetLogger().Errorf("%s, status code: %d", result.Message, result.Code)
			return InvokeFuncResponse{}, snerror.New(snerror.BridgeManagerFailCode,
				fmt.Sprintf(snerror.ErrText(snerror.BridgeManagerFailCode), result.Message))
		}
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, "http response status is not ok")
	}

	var result InvokeFuncResponse
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.GetLogger().Errorf("err when read create state response body: %s, traceID: %s", err.Error(), traceID)
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, err.Error())
	}
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		log.GetLogger().Errorf("err when unmarshal state response body: %s, traceID: %s", err.Error(),
			traceID)
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, err.Error())
	}

	return result, nil
}

// ReleaseByHTTP send request to bridgeManager and get a flag identify whether to terminate
func ReleaseByHTTP(funcName string, clientInstance bridgetypes.ClientInstance, traceID string) (
	InvokeFuncResponse, snerror.SNError) {
	log.GetLogger().Debugf("start ReleaseByHTTP, traceID: %s, funcName: %s", traceID, funcName)
	if clientInstance.FuncInfo == nil {
		log.GetLogger().Errorf("param clientInstance.FuncInfo of func ReleaseByHTTP is nil, traceID: %s, "+
			"funcName: %s", traceID, funcName)
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, "param clientInstance.FuncInfo "+
			"of func ReleaseByHTTP is nil")
	}

	body := bridgetypes.FuncRequestInfo{
		TraceID:        traceID,
		FunctionName:   funcName,
		ClientInstance: clientInstance,
	}
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		log.GetLogger().Errorf("error when marshal state request body: %s, traceID: %s, funcName: %s",
			marshalErr.Error(), traceID, funcName)
		return InvokeFuncResponse{}, snerror.New(common.InternalErrCode, marshalErr.Error())
	}
	path := ""
	if configs.FConfigs.HTTPSConfig.HTTPSEnable {
		path = "https://" + addr + "/release"
	} else {
		path = "http://" + addr + "/release"
	}

	reqParam := requestParam{
		Ctx:  context.Background(),
		Body: data,
		Path: path,
	}
	log.GetLogger().Debugf("request path is %s, traceID: %s, funcName: %s", path, traceID, funcName)
	return doCreateFuncRequest(reqParam, traceID)
}

// InitHTTPClient init http client
func InitHTTPClient() error {
	httpClient = &http.Client{Timeout: reqTimeout}
	if configs.FConfigs.HTTPSConfig.HTTPSEnable {
		transport, transportOk := http.DefaultTransport.(*http.Transport)
		if !transportOk {
			log.GetLogger().Errorf("failed to assert https default transport")
			err := errors.New("failed to assert https default transport")
			return err
		}
		transport.TLSClientConfig = tls.GetClientTLSConfig()
		httpClient.Transport = transport
	}
	return nil
}
