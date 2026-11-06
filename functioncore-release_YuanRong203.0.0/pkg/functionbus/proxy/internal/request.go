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

// Package internal contains internal structs and variables
package internal

import (
	"encoding/json"
	"fmt"
	"net/http"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

// HandleReq  handle request function type
type HandleReq func(traceID string,
	request *commontypes.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse

// HandleCancel handle cancel function type
type HandleCancel func(traceID string, dst *common.CancelProperty) *commontypes.TCPResponse

// HandleReport handle report function type
type HandleReport func(traceID string, dst *common.CancelProperty) *commontypes.TCPResponse

// BuildReqResult build request result
func BuildReqResult(message *commontypes.TCPResponse) types.Result {
	var data []byte
	var err error
	if data, err = proto.Marshal(&pb.Message{
		Type:         pb.Message_CALL_RESPONSE,
		CallResponse: utils.ToCallResponsePb(message),
	}); err != nil {
		log.GetLogger().Errorf("failed to marshal response: %s", err)
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  "marshal result error",
		}
	}
	return types.Result{
		RspData: data,
	}
}

// BuildPosixReqResult build request result
func BuildPosixReqResult(message *commontypes.TCPResponse) types.Result {
	var data []byte
	var err error
	if data, err = proto.Marshal(&pb.Message{
		Type:            pb.Message_POSIX_CALL_RESPONSE,
		PosixCallResult: utils.ToCallResultPb(message),
	}); err != nil {
		log.GetLogger().Errorf("failed to marshal posix response: %s", err)
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  "marshal result error",
		}
	}
	return types.Result{
		RspData: data,
	}
}

// BuildCallRequest build call request
func BuildCallRequest(msg *pb.Message_CallMessage) (*commontypes.TCPRequest, *common.FunctionProperty, error) {
	if msg.Dst.FunctionVersion == "" && msg.Dst.TenantID != common.BusProxyTenantID {
		var err error
		msg.Dst.FunctionName, msg.Dst.FunctionVersion, err = ConvertFuncName(msg.Dst.TenantID,
			msg.Dst.FunctionName, msg.Dst.StateID)
		if err != nil {
			log.GetLogger().Errorf("failed to handle functionInfo: %s, traceID: %s", err, msg.TraceID)
			return nil, nil, err
		}
	}

	busCallReq := &commontypes.TCPRequest{
		StateID:         msg.Dst.StateID,
		LogType:         msg.Req.LogType,
		RawData:         msg.Req.RawData,
		RequestID:       msg.Req.RequestID,
		UserData:        msg.Req.UserData,
		CPU:             msg.Req.CPU,
		Memory:          msg.Req.Memory,
		Concurrency:     msg.Req.Concurrency,
		RetryFlag:       msg.Req.RetryFlag,
		PersistResponse: msg.Req.PersistResponse,
		DirectRequest:   msg.Req.DirectRequest,
		Extra:           msg.Req.Extra,
		WorkerID:        msg.Req.WorkerID,
		IsStateFul:      msg.Req.IsStateFul,
		MetricsErrSaved: msg.Req.MetricsErrSaved,
	}
	dst := &common.FunctionProperty{
		TenantID:        msg.Dst.TenantID,
		FunctionName:    msg.Dst.FunctionName,
		FunctionVersion: msg.Dst.FunctionVersion,
		InstanceID:      msg.Dst.InstanceID,
		StateID:         msg.Dst.StateID,
		FutureID:        msg.Dst.FutureID,
	}
	return busCallReq, dst, nil
}

// BuildPosixCallRequest build posix call request
func BuildPosixCallRequest(msg *pb.PosixCallRequest) (*commontypes.TCPRequest, *common.FunctionProperty, error) {
	if msg.FunctionVersion == "" && msg.TenantID != common.BusProxyTenantID {
		var err error
		msg.Function, msg.FunctionVersion, err = ConvertFuncName(msg.TenantID,
			msg.Function, msg.StateID)
		if err != nil {
			log.GetLogger().Errorf("failed to handle functionInfo: %s, traceID: %s", err, msg.TraceID)
			return nil, nil, err
		}
	}
	curRequestID, err := utils.StringToUint64WithRequestID(msg.RequestID)
	if err != nil {
		log.GetLogger().Errorf("failed to convert requestID: %s, traceID: %s, "+
			"err: %s", msg.RequestID, msg.TraceID, err.Error())
		return nil, nil, err
	}
	var rawData []byte
	if len(msg.Args) > 0 {
		rawData = msg.Args[0].Value
	}
	busCallReq := &commontypes.TCPRequest{
		StateID:         msg.StateID,
		LogType:         msg.LogType,
		RawData:         rawData,
		RequestID:       curRequestID,
		UserData:        msg.UserData,
		CPU:             msg.CPU,
		Memory:          msg.Memory,
		Concurrency:     msg.Concurrency,
		RetryFlag:       msg.RetryFlag,
		PersistResponse: msg.PersistResponse,
		DirectRequest:   msg.DirectRequest,
		Extra:           msg.Extra,
		WorkerID:        msg.WorkerID,
		IsStateFul:      msg.IsStateFul,
		InstanceID:      msg.SpecInvokeInstanceID,
		MetricsErrSaved: msg.MetricsErrSaved,
	}
	dst := &common.FunctionProperty{
		TenantID:        msg.TenantID,
		FunctionName:    msg.Function,
		FunctionVersion: msg.FunctionVersion,
		InstanceID:      msg.InstanceID,
		StateID:         msg.StateID,
		FutureID:        msg.FutureID,
	}
	return busCallReq, dst, nil
}

// BuildCancelResult build result from basic response
func BuildCancelResult(msg *commontypes.TCPResponse) types.Result {
	cancelData, err := proto.Marshal(&pb.Message{
		Type: pb.Message_CANCEL_RESPONSE,
		CancelResponse: &pb.Message_CancelResponse{
			ErrorCode:    msg.ErrorCode,
			ErrorMessage: msg.ErrorMessage,
		},
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal cancel response: %s", err)
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  "marshal result error",
		}
	}
	return types.Result{
		RspData: cancelData,
	}
}

// BuildRecoverResult build result from basic response
func BuildRecoverResult(msg *commontypes.TCPResponse) types.Result {
	recoverData, err := proto.Marshal(&pb.Message{
		Type: pb.Message_RECOVER_RESPONSE,
		RecoverResponse: &pb.Message_RecoverResponse{
			ErrorCode:    msg.ErrorCode,
			ErrorMessage: msg.ErrorMessage,
			RawData:      msg.RawData,
		},
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal recover response: %s", err)
		return types.Result{
			ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg:  "marshal result error",
		}
	}
	return types.Result{
		RspData: recoverData,
	}
}

// ConvertFuncName for functionName like "function1", functionName = function1;
// for functionName like "function1:alias", functionName = function1, functionVersion is routed version
func ConvertFuncName(tenantID, functionInfo string, stateID string) (string, string, error) {
	var err error
	functionName, functionVersion := utils.GetFuncNameAndVersion(functionInfo)
	if functionName == "" {
		err = fmt.Errorf("empty FunctionName")
	}
	if functionVersion == "" {
		functionVersion = "latest"
	}

	// Check the field is alias or version number
	var isAlias bool
	isAlias = utils.IsAliasForFuncBranch(functionVersion)
	if isAlias {
		// remove "!" from the beginning of the alias
		functionVersion = functionVersion[urnutils.BranchAliasPrefix:]
	}

	// for aliases function, get the route version
	if isAlias {
		routeVersion := utils.GetRouteFuncVersion(tenantID, functionName, functionVersion)
		if routeVersion == "" {
			info, getMetadataErr := servicediscovery.GetRegistry().GetFunctionMetadata(
				tenantID + "/" + functionName + "/" + urnutils.DefaultVersion)
			// if we can not find the version of alias for bridge function, we can invoke latest
			if getMetadataErr != nil || !info.FuncMetaData.IsBridgeFunction {
				err = fmt.Errorf("empty routeVersion, FuncInfo: %s:%s", functionName, functionVersion)
			} else {
				routeVersion = urnutils.DefaultVersion
			}
		}
		log.GetLogger().Infof("succeed to get alias function, functionName: %s, functionVersion/alias: %s, "+
			"routeversion: %s", functionName, functionVersion, routeVersion)
		functionVersion = routeVersion
	}
	return functionName, functionVersion, err
}

type errorResponse struct {
	Code    uint32 `json:"code"`
	Message string `json:"message"`
}

// ConvertErrorToHTTPResponseBody convert internal error to http response body
func ConvertErrorToHTTPResponseBody(errCode uint32, errMessage string) []byte {
	res, err := json.Marshal(errorResponse{
		Code:    errCode,
		Message: errMessage,
	})
	if err != nil {
		return []byte(err.Error())
	}
	return res
}

// ConvertHTTPResponseStatusCode convert internal errCode to http status code
func ConvertHTTPResponseStatusCode(errCode uint32) int {
	if errCode == proxyerrors.StateOKCode {
		return http.StatusOK
	}
	return http.StatusInternalServerError
}
