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

// Package bridgefunc -
package bridgefunc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager"
	"functioncore/pkg/state-manager/constant"
	"functioncore/pkg/state-manager/server"
	"functioncore/pkg/state-manager/utils"
)

var (
	requestHandler *bridgemanager.BridgeHandler
	once           sync.Once
)

// InvokeResponse define
type InvokeResponse struct {
	StateKey              string `json:"stateKey"`
	Code                  int    `json:"code"`
	Message               string `json:"message"`
	IsTerminate           bool   `json:"isTerminate"`
	IsNewStateKeyScenario bool   `json:"isNewStateKeyScenario"`
}

// RegisterRouter register router
func RegisterRouter() {
	handleParamGetBridgeFunc := &server.HandleParam{
		HandleFunc: func(g *gin.Context) {
			ApplyStateKey(g.Writer, g.Request)
		},
		Path:   "/applyStateKey",
		Method: "POST",
	}

	server.RegisterRouter(handleParamGetBridgeFunc, "applyStateKey")

	handleParamReleaseBridge := &server.HandleParam{
		HandleFunc: func(g *gin.Context) {
			release(g.Writer, g.Request)
		},
		Path:   "/release",
		Method: "POST",
	}

	server.RegisterRouter(handleParamReleaseBridge, "release")
}

// ApplyStateKey send request to bridgeManager and get stateKey
func ApplyStateKey(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Debugf("begin to ApplyStateKey")
	startTime := time.Now()

	err := utils.AuthCheck(r)
	if err != nil {
		log.GetLogger().Errorf("failed to check auth: %s", err.Error())
		utils.ErrorResponse(w, snerror.RequestAuthCheckFail, fmt.Sprintf("failed to check auth, err:%s",
			err.Error()))
		return
	}

	funcRequestInfo, isReturn := parseRequest(w, r)
	if isReturn {
		return
	}

	if !checkParamValid(w, funcRequestInfo) {
		return
	}

	clientInstance := funcRequestInfo.ClientInstance
	applyStateKeyFuncLogger := log.GetLogger().With(zap.String("funcName", clientInstance.FuncInfo.BridgeFuncName),
		zap.String("tenantID", clientInstance.FuncInfo.TenantID), zap.String("serviceID",
			clientInstance.FuncInfo.ServiceID), zap.String("traceID", funcRequestInfo.TraceID))

	if !utils.IsStateful(funcRequestInfo.FunctionName) {
		applyStateKeyFuncLogger.Errorf("the function name(%s) don't have sn service id", clientInstance.FuncInfo.
			BridgeFuncName)
		InvokeErrorResponse(w, snerror.FunctionNotStateful, "", applyStateKeyFuncLogger)
		return
	}

	var requestHandler *bridgemanager.BridgeHandler
	if requestHandler, err = GetRequestHandler(); err != nil || requestHandler == nil {
		applyStateKeyFuncLogger.Errorf("get bridgeManager handler error, err:", err.Error())
		InvokeErrorResponse(w, snerror.BridgeManagerFailCode, err.Error(), applyStateKeyFuncLogger)
		return
	}

	var stateKey string
	isNewStateKeyScenario := false
	if stateKey, isNewStateKeyScenario, err = requestHandler.ApplyStateKey(&clientInstance,
		funcRequestInfo.TraceID); err != nil {
		applyStateKeyFuncLogger.Errorf("failed to get bridgeFunc stateKey error, error :%s", err.Error())
		InvokeErrorResponse(w, snerror.BridgeManagerFailCode, err.Error(), applyStateKeyFuncLogger)
		return
	}

	if err = invokeSuccessResponse(w, stateKey, isNewStateKeyScenario, applyStateKeyFuncLogger); err != nil {
		applyStateKeyFuncLogger.Errorf("successResponse error:%s", err.Error())
	}
	applyStateKeyFuncLogger.Debugf("invoke ApplyStateKey successfully, cost time: %v", time.Now().Sub(startTime))
	return
}

func parseRequest(w http.ResponseWriter, r *http.Request) (*bgtypes.FuncRequestInfo, bool) {
	funcRequestInfo, err := parseFuncRequest(r)
	if err != nil {
		log.GetLogger().Errorf("failed to parse request, error: %s", err.Error())
		errs := utils.ErrorResponse(w, constant.RequestParamInvalid, constant.CodeText[constant.RequestParamInvalid])
		if errs != nil {
			log.GetLogger().Errorf("errorResponse error: %s", errs.Error())
		}
		return nil, true
	}
	return funcRequestInfo, false
}

func checkParamValid(w http.ResponseWriter, funcRequestInfo *bgtypes.FuncRequestInfo) bool {
	if funcRequestInfo == nil || funcRequestInfo.ClientInstance.FuncInfo == nil {
		log.GetLogger().Errorf("param bridgeFuncRequestInfo or bridgeFuncRequestInfo.ClientInstance.FuncInfo " +
			"is nil")
		utils.ErrorResponse(w, constant.InternalServerError, "param bridgeFuncRequestInfo or bridgeFuncRequestInfo."+
			"ClientInstance.FuncInfo is nil")
		return false
	}
	return true
}

// release send release request to bridgeManager
func release(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Debugf("begin to release")
	startTime := time.Now()

	err := utils.AuthCheck(r)
	if err != nil {
		log.GetLogger().Errorf("failed to check auth: %s", err.Error())
		utils.ErrorResponse(w, snerror.RequestAuthCheckFail, fmt.Sprintf("failed to check auth, err:%s",
			err.Error()))
		return
	}

	funcRequestInfo, isReturn := parseRequest(w, r)
	if isReturn {
		return
	}

	if !checkParamValid(w, funcRequestInfo) {
		return
	}

	clientInstance := funcRequestInfo.ClientInstance
	releaseLogger := log.GetLogger().With(zap.String("funcName", clientInstance.FuncInfo.BridgeFuncName),
		zap.String("tenantID", clientInstance.FuncInfo.TenantID), zap.String("serviceID", clientInstance.
			FuncInfo.ServiceID), zap.String("traceID", funcRequestInfo.TraceID))

	if !utils.IsStateful(funcRequestInfo.FunctionName) {
		releaseLogger.Errorf("the function name(%s) don't have sn service id", clientInstance.FuncInfo.
			ServiceID)
		InvokeErrorResponse(w, snerror.FunctionNotStateful, fmt.Sprintf("the function name(%s) don't have sn "+
			"service id", clientInstance.FuncInfo.ServiceID), releaseLogger)
		return
	}

	var requestHandler *bridgemanager.BridgeHandler
	if requestHandler, err = GetRequestHandler(); err != nil || requestHandler == nil {
		releaseLogger.Errorf("get bridgeManager handler error, err:", err.Error())
		InvokeErrorResponse(w, snerror.BridgeManagerFailCode, err.Error(), releaseLogger)
		return
	}

	var isTerminate bool
	if isTerminate, err = requestHandler.ReleaseBridge(&clientInstance, funcRequestInfo.TraceID); err != nil {
		releaseLogger.Errorf("failed to release bridge, error: %s", err.Error())
		InvokeErrorResponse(w, snerror.BridgeManagerFailCode, err.Error(), releaseLogger)
		return
	}

	if err = ReleaseSuccessResponse(w, releaseLogger, isTerminate); err != nil {
		releaseLogger.Errorf("successResponse error:%s", err.Error())
	}

	releaseLogger.Debugf("invoke releaseBridge successfully with funcName: %s and tenantID: %s, "+
		"serviceID:%s, cost time: %v", clientInstance.FuncInfo.BridgeFuncName, clientInstance.FuncInfo.TenantID,
		clientInstance.FuncInfo.ServiceID, time.Now().Sub(startTime))
	return
}

// parse http request to internal FuncRequestInfo
func parseFuncRequest(r *http.Request) (*bgtypes.FuncRequestInfo, error) {
	reqConf := bgtypes.FuncRequestInfo{}
	res, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read request body")
		return nil, err
	}
	err = json.Unmarshal(res, &reqConf)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal request info")
		return nil, err
	}

	return &reqConf, nil
}

// InvokeErrorResponse -
func InvokeErrorResponse(w http.ResponseWriter, code int, message string, logger log.FormatLogger) {
	if message == "" {
		message = constant.CodeText[code]
	}
	err := utils.ErrorResponse(w, code, message)
	if err != nil {
		logger.Errorf("errorResponse error: %s", err.Error())
	}
}

func invokeSuccessResponse(w http.ResponseWriter, stateKey string, isNewStateKeyScenario bool,
	logger log.FormatLogger) error {
	w.WriteHeader(http.StatusOK)
	resp := InvokeResponse{
		Code:                  http.StatusOK,
		StateKey:              stateKey,
		IsNewStateKeyScenario: isNewStateKeyScenario,
	}

	logger.Infof("succeed to invoke func, stateKey:%s, IsNewStateKeyScenario:%t", stateKey, isNewStateKeyScenario)
	respData, err := json.Marshal(resp)
	if err != nil {
		logger.Errorf("function json marshal response error:%s", err.Error())
		return err
	}
	_, err = w.Write(respData)
	if err != nil {
		logger.Errorf("function write response error: %s", err.Error())
		return err
	}

	return nil
}

// ReleaseSuccessResponse -
func ReleaseSuccessResponse(w http.ResponseWriter, logger log.FormatLogger, isTerminate bool) error {
	w.WriteHeader(http.StatusOK)
	resp := InvokeResponse{
		Code:        http.StatusOK,
		IsTerminate: isTerminate,
	}

	respData, err := json.Marshal(resp)
	if err != nil {
		logger.Errorf("function json marshal response error:%s", err.Error())
		return err
	}
	_, err = w.Write(respData)
	if err != nil {
		logger.Errorf("function write response error: %s", err.Error())
		return err
	}

	return nil
}

// Initialize init instance manager
func Initialize() error {
	_, err := GetRequestHandler()
	return err
}

// GetRequestHandler get a state handler
func GetRequestHandler() (*bridgemanager.BridgeHandler, error) {
	var err error
	once.Do(func() {
		if requestHandler == nil {
			bridgeHandler, bgHandlerErr := bridgemanager.Instance()
			if bgHandlerErr != nil {
				err = bgHandlerErr
				return
			}

			requestHandler = bridgeHandler
		}
	})
	if err != nil {
		log.GetLogger().Errorf("in GetRequestHandler error: %s", err.Error())
		return nil, err
	}
	return requestHandler, nil
}
