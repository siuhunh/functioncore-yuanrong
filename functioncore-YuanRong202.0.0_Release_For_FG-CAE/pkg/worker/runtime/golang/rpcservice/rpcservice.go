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

// Package rpcservice -
package rpcservice

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/rpc"
	"strings"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/runtime"
)

const (
	rpcRemoteHealthCheckMethodName = "Function.HealthCheck"
	rpcRemoteInvokeMethodName      = "Function.Invoke"
	timeoutErrorMessage            = "call invoke timeout"
	invokeErrorMessage             = "call invoke error"
	assertFailureMessage           = "failed to assert resp from call.reply"
)

var (
	rpcService *RPCService
)

// RPCService provide rpc client
type RPCService struct {
	client *rpc.Client
}

// InvokeResponse define response struct
type InvokeResponse struct {
	StatusCode int
	Payload    []byte
}

// InvokeRequest define response struct
type InvokeRequest struct {
	Payload []byte
	Header  http.Header
}

// HealthCheckRequest define health check request
type HealthCheckRequest struct {
}

// HealthCheckResponse define health check response
type HealthCheckResponse struct {
}

func newRPCService(remoteAddress string) *RPCService {
	client, err := rpc.DialHTTP("tcp", remoteAddress)
	if err != nil {
		log.GetLogger().Warnf("dialHTTPFail | %s | %s", remoteAddress, err.Error())
		return nil
	}
	return &RPCService{
		client: client,
	}
}

// RenewRPCService -
func RenewRPCService(remoteAddress string) *RPCService {
	rpcService = newRPCService(remoteAddress)
	return rpcService
}

// GetRPCService -
func GetRPCService(remoteAddress string) *RPCService {
	if rpcService == nil {
		rpcService = newRPCService(remoteAddress)
	}
	return rpcService
}

// HealthCheck -
func (service *RPCService) HealthCheck() error {
	err := service.client.Call(rpcRemoteHealthCheckMethodName, &HealthCheckRequest{}, &HealthCheckResponse{})
	if err != nil {
		log.GetLogger().Errorf("failed to call healthCheck, %s", err.Error())
		return err
	}
	return nil
}

// Invoke is used to send invoke requests to the go runtime.
func (service *RPCService) Invoke(req *InvokeRequest) *rpc.Call {
	resp := &InvokeResponse{}
	done := make(chan *rpc.Call, 1)
	call := service.client.Go(rpcRemoteInvokeMethodName, req, resp, done)

	call.Args = nil
	return call
}

// WaitResp is used to wait for the response message of the runtime.
func WaitResp(timeout time.Duration, resp *InvokeResponse, call *rpc.Call) snerror.SNError {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		return snerror.New(snerror.RuntimeInvokeTimeoutCode, fmt.Sprintf(snerror.ErrText(snerror.
			RuntimeInvokeTimeoutCode), timeout))
	case _, ok := <-runtime.ProcessExitSignal:
		if !ok {
			log.GetLogger().Infof("failed to call invoke method, the runtime process exit")
			return snerror.NewSNError(snerror.RuntimeProcessExit, "failed to call invoke method, the runtime "+
				"process exit")
		}
	case callResult, ok := <-call.Done:
		if !ok {
			log.GetLogger().Error("failed to call invoke method, the done channel was closed.")
			return snerror.New(snerror.FuncInsExceptionCode, invokeErrorMessage)
		}
		if callResult == nil {
			log.GetLogger().Errorf("failed to call invoke method, " +
				"the callResult returned by the done channel was null.")
			return snerror.New(snerror.FuncInsExceptionCode, invokeErrorMessage)
		}
		if callResult.Error != nil {
			log.GetLogger().Errorf("failed to call invoke method, %s.", callResult.Error.Error())
			if isInvokeError(callResult.Error) {
				return snerror.NewSNError(snerror.FunctionExceptionCode, callResult.Error.Error())
			}
			return snerror.New(snerror.FuncInsExceptionCode, callResult.Error.Error())
		}
		respTemp, ok := callResult.Reply.(*InvokeResponse)
		if !ok {
			log.GetLogger().Errorf(assertFailureMessage)
			return snerror.New(snerror.FuncInsExceptionCode, assertFailureMessage)
		}
		*resp = *respTemp
	}
	return nil
}

func isInvokeError(err error) bool {
	if !json.Valid([]byte(err.Error())) {
		return false
	}
	if strings.Contains(err.Error(), "errorMessage") {
		return true
	}
	return false
}
