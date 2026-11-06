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

// Package utils contains converter between pb and struct
package utils

import (
	"strconv"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/common"
)

const (
	base    = 10
	bitSize = 64
)

// ToCallRequestPb converts CallRequest to pb.CallRequest
func ToCallRequestPb(request *types.TCPRequest) *pb.CallRequest {
	return &pb.CallRequest{
		LogType:         request.LogType,
		RawData:         request.RawData,
		RequestID:       request.RequestID,
		UserData:        request.UserData,
		CPU:             request.CPU,
		Memory:          request.Memory,
		DirectRequest:   request.DirectRequest,
		RetryFlag:       request.RetryFlag,
		Concurrency:     request.Concurrency,
		PersistResponse: request.PersistResponse,
		Extra:           request.Extra,
		WorkerID:        request.WorkerID,
		IsStateFul:      request.IsStateFul,
		MetricsErrSaved: request.MetricsErrSaved,
	}
}

// FromCallRequestPb converts CallRequest from pb.CallRequest
func FromCallRequestPb(request *pb.CallRequest) *types.TCPRequest {
	return &types.TCPRequest{
		LogType:         request.LogType,
		RawData:         request.RawData,
		RequestID:       request.RequestID,
		Concurrency:     request.Concurrency,
		UserData:        request.UserData,
		CPU:             request.CPU,
		Memory:          request.Memory,
		RetryFlag:       request.RetryFlag,
		PersistResponse: request.PersistResponse,
		DirectRequest:   request.DirectRequest,
		Extra:           request.Extra,
		WorkerID:        request.WorkerID,
		IsStateFul:      request.IsStateFul,
		MetricsErrSaved: request.MetricsErrSaved,
	}
}

// ToFunctionPropertyPb converts FunctionProperty to pb.FunctionProperty
func ToFunctionPropertyPb(property *common.FunctionProperty) *pb.FunctionProperty {
	return &pb.FunctionProperty{
		TenantID:        property.TenantID,
		FunctionName:    property.FunctionName,
		FunctionVersion: property.FunctionVersion,
		InstanceID:      property.InstanceID,
		StateID:         property.StateID,
		FutureID:        property.FutureID,
	}
}

// FromFunctionPropertyPb converts FunctionProperty from pb.FunctionProperty
func FromFunctionPropertyPb(property *pb.FunctionProperty) *common.FunctionProperty {
	return &common.FunctionProperty{
		TenantID:        property.TenantID,
		FunctionName:    property.FunctionName,
		FunctionVersion: property.FunctionVersion,
		InstanceID:      property.InstanceID,
		StateID:         property.StateID,
		FutureID:        property.FutureID,
	}
}

// ToCallResponsePb converts CallResponse to pb.CallResponse
func ToCallResponsePb(resp *types.TCPResponse) *pb.CallResponse {
	return &pb.CallResponse{
		RawData:      resp.RawData,
		Logs:         resp.Logs,
		RequestID:    resp.RequestID,
		ErrorCode:    resp.ErrorCode,
		ErrorMessage: resp.ErrorMessage,
		DcCallStack:  resp.DcCallStack,
		Summary:      resp.Summary,
		Labels:       resp.Labels,
	}
}

// FromCallResponsePb converts CallResponse from pb.CallResponse
func FromCallResponsePb(resp *pb.CallResponse) *types.TCPResponse {
	return &types.TCPResponse{
		RawData:      resp.RawData,
		RequestID:    resp.RequestID,
		ErrorCode:    resp.ErrorCode,
		ErrorMessage: resp.ErrorMessage,
		Logs:         resp.Logs,
		DcCallStack:  resp.DcCallStack,
		Summary:      resp.Summary,
		Labels:       resp.Labels,
	}
}

// ToPosixCallRequestPb converts CallRequest to poxix.CallRequest
func ToPosixCallRequestPb(traceID string, request *types.TCPRequest,
	property *common.FunctionProperty) *pb.PosixCallRequest {
	return &pb.PosixCallRequest{
		Args: []*pb.Arg{
			&pb.Arg{Value: request.RawData},
		},
		TraceID:              traceID,
		LogType:              request.LogType,
		RequestID:            strconv.FormatUint(request.RequestID, base),
		UserData:             request.UserData,
		CPU:                  request.CPU,
		Memory:               request.Memory,
		RetryFlag:            request.RetryFlag,
		Concurrency:          request.Concurrency,
		PersistResponse:      request.PersistResponse,
		DirectRequest:        request.DirectRequest,
		Extra:                request.Extra,
		WorkerID:             request.WorkerID,
		IsStateFul:           request.IsStateFul,
		SpecInvokeInstanceID: request.InstanceID,
		TenantID:             property.TenantID,
		Function:             property.FunctionName,
		FunctionVersion:      property.FunctionVersion,
		InstanceID:           property.InstanceID,
		StateID:              property.StateID,
		FutureID:             property.FutureID,
		MetricsErrSaved:      request.MetricsErrSaved,
	}
}

// FromPosixCallRequestPb converts CallRequest from pb.PosixCallRequest
func FromPosixCallRequestPb(request *pb.PosixCallRequest) *types.TCPRequest {
	curRequestID, err := StringToUint64WithRequestID(request.RequestID)
	if err != nil {
		log.GetLogger().Warnf("failed to convert requestID %s to uint64, err: %s", request.RequestID, err.Error())
	}
	var rawData []byte
	if len(request.Args) > 0 {
		rawData = request.Args[0].Value
	}
	return &types.TCPRequest{
		LogType:         request.LogType,
		RawData:         rawData,
		RequestID:       curRequestID,
		UserData:        request.UserData,
		CPU:             request.CPU,
		Memory:          request.Memory,
		Concurrency:     request.Concurrency,
		RetryFlag:       request.RetryFlag,
		PersistResponse: request.PersistResponse,
		DirectRequest:   request.DirectRequest,
		Extra:           request.Extra,
		WorkerID:        request.WorkerID,
		IsStateFul:      request.IsStateFul,
		InstanceID:      request.SpecInvokeInstanceID,
	}
}

// GetFuncPropertyFromPosixPb get FunctionProperty from pb.PosixCallRequest
func GetFuncPropertyFromPosixPb(request *pb.PosixCallRequest) *common.FunctionProperty {
	return &common.FunctionProperty{
		TenantID:        request.TenantID,
		FunctionName:    request.Function,
		FunctionVersion: request.FunctionVersion,
		InstanceID:      request.InstanceID,
		StateID:         request.StateID,
		FutureID:        request.FutureID,
	}
}

// ToCallResultPb converts CallResult to pb.PosixCallResult
func ToCallResultPb(resp *types.TCPResponse) *pb.PosixCallResult {
	return &pb.PosixCallResult{
		RawData:         resp.RawData,
		Logs:            resp.Logs,
		RequestID:       strconv.FormatUint(resp.RequestID, base),
		ErrorCode:       resp.ErrorCode,
		ErrorMessage:    resp.ErrorMessage,
		DcCallStack:     resp.DcCallStack,
		Summary:         resp.Summary,
		Labels:          resp.Labels,
		MetricsErrSaved: resp.MetricsErrSaved,
		CffHTTPResponse: resp.CffHTTPResponse,
	}
}

// FromCallResultPb converts CallResult from pb.CallResult
func FromCallResultPb(result *pb.PosixCallResult) (*types.TCPResponse, error) {
	curRequestID, err := StringToUint64WithRequestID(result.RequestID)
	if err != nil {
		return nil, err
	}
	return &types.TCPResponse{
		RawData:         result.RawData,
		Logs:            result.Logs,
		RequestID:       curRequestID,
		ErrorCode:       result.ErrorCode,
		ErrorMessage:    result.ErrorMessage,
		DcCallStack:     result.DcCallStack,
		Summary:         result.Summary,
		Labels:          result.Labels,
		MetricsErrSaved: result.MetricsErrSaved,
		CffHTTPResponse: result.CffHTTPResponse,
	}, nil
}

// StringToUint64WithRequestID converts RequestID string to Uint64
func StringToUint64WithRequestID(requestID string) (uint64, error) {
	curRequestID, err := strconv.ParseUint(requestID, base, bitSize)
	if err != nil {
		return 0, err
	}
	return curRequestID, nil
}
