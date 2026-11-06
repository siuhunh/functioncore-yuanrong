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
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/common"
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
		Concurrency:     request.Concurrency,
		RetryFlag:       request.RetryFlag,
		PersistResponse: request.PersistResponse,
		DirectRequest:   request.DirectRequest,
		Extra:           request.Extra,
		WorkerID:        request.WorkerID,
		IsStateFul:      request.IsStateFul,
	}
}

// FromCallRequestPb converts CallRequest from pb.CallRequest
func FromCallRequestPb(request *pb.CallRequest) *types.TCPRequest {
	return &types.TCPRequest{
		LogType:         request.LogType,
		RawData:         request.RawData,
		RequestID:       request.RequestID,
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
		RawData:         resp.RawData,
		Logs:            resp.Logs,
		RequestID:       resp.RequestID,
		ErrorCode:       resp.ErrorCode,
		ErrorMessage:    resp.ErrorMessage,
		DcCallStack:     resp.DcCallStack,
		Summary:         resp.Summary,
		Labels:          resp.Labels,
		CffHTTPResponse: resp.CffHTTPResponse,
	}
}

// FromCallResponsePb converts CallResponse from pb.CallResponse
func FromCallResponsePb(resp *pb.CallResponse) *types.TCPResponse {
	return &types.TCPResponse{
		RawData:         resp.RawData,
		Logs:            resp.Logs,
		RequestID:       resp.RequestID,
		ErrorCode:       resp.ErrorCode,
		ErrorMessage:    resp.ErrorMessage,
		DcCallStack:     resp.DcCallStack,
		Summary:         resp.Summary,
		Labels:          resp.Labels,
		CffHTTPResponse: resp.CffHTTPResponse,
	}
}
