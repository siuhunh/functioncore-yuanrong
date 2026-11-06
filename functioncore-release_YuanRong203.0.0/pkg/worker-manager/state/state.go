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

// Package state is about stateID and the request to state-managaer
package state

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/metrics"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

const (
	appID              = "coresdk"
	workerManagerAppID = "worker-manager"
	reqTimeout         = 6 * time.Second
	defaultRetryDelay  = 5
	stateManagerReqURL = "state-manager.%s.svc.cluster.local:8087/deletestate"
	schemaHTTP         = "http://"
	schemaHTTPS        = "https://"
	internalErrCode    = 220500
	internalErrMsg     = "internal error"
	stateClientErrCode = 220007
	stateClientErrMsg  = "state client is nil"
)

var (
	httpClient *http.Client
	schema     = schemaHTTP
)

// FuncInfo keeps function information in request message
type FuncInfo struct {
	FunctionName   string `json:"functionName,omitempty"`
	StateID        string `json:"stateID"`
	TenantID       string `json:"tenantID,omitempty"`
	ServiceID      string `json:"serviceID,omitempty"`
	ProxyFunction  string `json:"proxyFunction,omitempty"`
	TraceID        string `json:"traceID,omitempty"`
	FunctionDelete bool   `json:"functionDelete,omitempty"`
}

// InvokeFuncResponse define
type InvokeFuncResponse struct {
	StateKey              string `json:"stateKey"`
	Code                  int    `json:"code"`
	Message               string `json:"message"`
	IsTerminate           bool   `json:"isTerminate"`
	IsNewStateKeyScenario bool   `json:"isNewStateKeyScenario"`
}

// DeleteStateResponse keeps response message when deleting state
type DeleteStateResponse struct {
	Code int    `json:"code"`
	Err  string `json:"error,omitempty"`
}

// SendDeleteStateReq -
func SendDeleteStateReq(stateID string, funcInfo metrics.FunctionInfo) {
	for {
		err := deleteStateByStateManager(stateID, funcInfo)
		if err == nil {
			break
		}
		log.GetLogger().Warnf("failed to delete state with stateID: %s, keep retrying", stateID)
		time.Sleep(defaultRetryDelay * time.Second)
	}
	log.GetLogger().Infof("finished deleting State from state-manager, stateID: %s", stateID)
	return
}

func deleteStateByStateManager(stateID string, funcInfo metrics.FunctionInfo) snerror.SNError {
	log.GetLogger().Infof("Start to delete state by state manager, funcName is %s, version is %s, stateID is %s",
		funcInfo.FunctionName, funcInfo.Version, stateID)
	funcName := funcInfo.FunctionName
	if funcInfo.Version != "latest" {
		funcName = fmt.Sprintf("%s:%s", funcName, funcInfo.Version)
	}
	body := FuncInfo{
		StateID:        stateID,
		TenantID:       funcInfo.TenantID,
		ProxyFunction:  "function-task",
		FunctionName:   funcName,
		ServiceID:      funcInfo.ServiceID,
		FunctionDelete: true,
	}
	data, marshalErr := json.Marshal(body)
	if marshalErr != nil {
		return snerror.New(internalErrCode, marshalErr.Error())
	}

	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to initialize worker-manager http client while getting config, err: %s", err.Error())
		return snerror.New(internalErrCode, internalErrMsg)
	}

	path := schema + fmt.Sprintf(stateManagerReqURL, os.Getenv(constants.PodNamespaceEnvKey))

	request, err := http.NewRequest("POST", path, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("failed to make stateID request, err %s", err.Error())
		return snerror.New(internalErrCode, internalErrMsg)
	}

	request.Header.Set(constants.HeaderEventSourceID, appID)
	request.Header.Set(constants.HeaderCallType, "active")
	request.Header.Set(constants.ContentType, constants.ApplicationJSON)
	authorization, timestamp := localauth.SignLocally(conf.LocalAuth.AKey,
		conf.LocalAuth.SKey, utils.AppID, conf.LocalAuth.Duration)
	request.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	request.Header.Set(constants.Authorization, authorization)

	return doDeleteRequest(request)
}

func doDeleteRequest(req *http.Request) snerror.SNError {
	if httpClient == nil {
		log.GetLogger().Errorf("failed to get state, error: stateClient is nil")
		return snerror.New(stateClientErrCode, stateClientErrMsg)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return snerror.New(internalErrCode, err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("http response status is not ok, status code: %d", resp.StatusCode)
		return snerror.New(internalErrCode, "http response status is not ok")
	}

	var result DeleteStateResponse
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.GetLogger().Errorf("err when read delete state response body: %s", err.Error())
		return snerror.New(internalErrCode, err.Error())
	}
	unmarshalErr := json.Unmarshal(respBody, &result)
	if unmarshalErr != nil {
		return snerror.New(internalErrCode, unmarshalErr.Error())
	}
	return nil
}

// InitHTTPClient init http client
func InitHTTPClient() error {
	httpClient = &http.Client{Timeout: reqTimeout}
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to initialize worker-manager http client while getting config, err: %s", err.Error())
		return err
	}

	if conf.HTTPSConfig.HTTPSEnable {
		transport, transportOk := http.DefaultTransport.(*http.Transport)
		if !transportOk {
			log.GetLogger().Errorf("failed to assert https default transport")
			return errors.New("failed to assert https default transport")
		}
		transport.TLSClientConfig = tls.GetClientTLSConfig()
		httpClient.Transport = transport
		schema = schemaHTTPS
	}
	return nil
}
