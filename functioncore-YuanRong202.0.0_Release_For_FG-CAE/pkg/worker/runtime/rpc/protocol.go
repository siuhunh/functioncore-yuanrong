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

package rpc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/rpc/pb"
	"functioncore/pkg/worker/utils"
	"functioncore/pkg/worker/websocket"
)

const funcDigestLength = 32

var nodeIP = os.Getenv("HOST_IP")

// DataProtocol -
type DataProtocol interface {
	BuildLoadRequest(c *config.Configuration, instanceFuncName string, traceID string,
		extra map[string]string) ([]byte, error)
	ParseLoadResponse(rsp []byte) *runtime.Error
	BuildInitializerRequest(traceID string, extra map[string]string) ([]byte, error)
	ParseInitializerResponse(response []byte) *runtime.Error
	BuildRestoreHookRequest(traceID string, restoreHook config.RestoreHook) ([]byte, error)
	ParseRestoreHookResponse(response []byte) *runtime.Error
	ParseLog(l *functionlog.FunctionLog, message []byte) (string, string, error)
	BuildInvokeRequest(id string, message runtime.InvokeRequest) ([]byte, error)
	ParseInvokeResponse(result *runtime.InvokeResponse, payload []byte)
	BuildPreStopRequest(traceID string, extra map[string]string) ([]byte, error)
	ParsePreStopResponse(response []byte) *runtime.Error
	ParseRuntimeLog(l *functionlog.FunctionLog, message []byte) (string, error)
	BuildUpdateAgencyRequest(traceID string, extra map[string]string) ([]byte, error)
}

func setTimestamp(l *functionlog.FunctionLog, timestamp string) {
	// log time has contained location info
	// parse runtime log time directly by using layout, such as 2022-03-23 10:28:12.4480000001+08:00
	ti, err := time.Parse(functionlog.RuntimeLogFormat, timestamp)
	if err == nil {
		l.NanoTime = ti.UTC().Format(functionlog.DateTimeLayoutWithZeroPaddedNanos)
		l.Time = ti.UTC().Format(functionlog.FuncLogTimeFormat)
	} else {
		now := time.Now()
		l.Time = now.UTC().Format(functionlog.FuncLogTimeFormat)
		l.NanoTime = now.UTC().Format(functionlog.DateTimeLayoutWithZeroPaddedNanos)
		l.IsStdLog = true
	}
}

// JSONProtocol -
type JSONProtocol struct{}

// BuildLoadRequest -
func (j *JSONProtocol) BuildLoadRequest(c *config.Configuration, instanceFuncName string,
	traceID string, extra map[string]string) ([]byte, error) {
	envs := fetchEnvFromConfig(c)
	request := LoadRequest{
		Entry:            c.EntryFile,
		FuncName:         c.HandlerName,
		FuncEnvs:         envs.FuncEnvs,
		RuntimeEnvs:      envs.RuntimeEnvs,
		StorageInfo:      getStorageInfo(c),
		InstanceFuncName: instanceFuncName,
		TraceID:          traceID,
	}
	req, err := json.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal load request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// ParseLoadResponse -
func (j *JSONProtocol) ParseLoadResponse(rsp []byte) *runtime.Error {
	var loadResponse LoadResponse
	if err := json.Unmarshal(rsp, &loadResponse); err != nil {
		log.GetLogger().Errorf("failed to unmarshal load response: %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if loadResponse.Error != nil && loadResponse.Error.Code != 0 {
		log.GetLogger().Errorf("failed to load function: %s", loadResponse.Error.Message)
		return &runtime.Error{
			Code:    loadResponse.Error.Code,
			Message: loadResponse.Error.Message,
		}
	}
	return nil
}

// BuildInitializerRequest -
func (j *JSONProtocol) BuildInitializerRequest(traceID string, extra map[string]string) ([]byte, error) {
	request := InitializerRequest{
		TraceID: traceID,
		Extra:   extra,
	}
	req, err := json.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to json marshal initializer request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// ParseInitializerResponse -
func (j *JSONProtocol) ParseInitializerResponse(response []byte) *runtime.Error {
	var rsp InitializerResponse
	if err := json.Unmarshal(response, &rsp); err != nil {
		log.GetLogger().Errorf("failed to process initializer request, %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if rsp.Error != nil {
		return &runtime.Error{
			Code:    rsp.Error.Code,
			Message: rsp.Error.Message,
		}
	}
	return nil
}

// BuildRestoreHookRequest -
func (j *JSONProtocol) BuildRestoreHookRequest(traceID string, restoreHook config.RestoreHook) ([]byte, error) {
	return nil, errors.New("restore hook not implemented")
}

// ParseRestoreHookResponse -
func (j *JSONProtocol) ParseRestoreHookResponse(response []byte) *runtime.Error {
	return &runtime.Error{
		Code:    snerror.WorkerInternalErrorCode,
		Message: "restore hook not implemented",
	}
}

// ParseLog -
func (j *JSONProtocol) ParseLog(l *functionlog.FunctionLog, message []byte) (string, string, error) {
	var funcLog = &FunctionLog{}
	var traceID string
	if err := json.Unmarshal(message, funcLog); err != nil {
		log.GetLogger().Errorf("failed to unmarshal function log: %s", err.Error())
		return "", "", err
	}
	l.Level = funcLog.Level
	l.Message.WriteString(funcLog.Content)
	buildTelemetryFunctionEvent(funcLog.Content, funcLog.Timestamp)
	setTimestamp(l, funcLog.Timestamp)
	if l.IsStdLog {
		traceID, _ = functionlog.ParseUserLog(funcLog.Content)
	}
	return funcLog.InvokeID, traceID, nil
}

// ParseRuntimeLog -
func (j *JSONProtocol) ParseRuntimeLog(l *functionlog.FunctionLog, message []byte) (string, error) {
	return "", nil
}

// BuildInvokeRequest -
func (j *JSONProtocol) BuildInvokeRequest(id string, message runtime.InvokeRequest) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	if err := binary.Write(buf, binary.BigEndian, uint32(len(message.Event))); err != nil {
		log.GetLogger().Errorf("failed to build InvokeRequest: %s", err.Error())
		return nil, err
	}
	buf.Write(message.Event)
	request := &InvokeRequest{
		TraceID:  message.TraceID,
		InvokeID: id,
		Extra:    message.Extra,
		FutureID: message.FutureID,
	}
	req, err := json.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to json invoke request: %s", err.Error())
		return nil, err
	}
	buf.Write(req)
	return buf.Bytes(), nil
}

// ParseInvokeResponse -
func (j *JSONProtocol) ParseInvokeResponse(result *runtime.InvokeResponse, payload []byte) {
	if len(payload) < uint32Len {
		return
	}
	resLength := binary.BigEndian.Uint32(payload[0:uint32Len])
	var res []byte
	if resLength > 0 {
		res = payload[uint32Len : uint32Len+resLength]
	}
	var rsp InvokeResponse
	if err := json.Unmarshal(payload[uint32Len+resLength:], &rsp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal invoke response %s", err.Error())
		result.Error = &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
		return
	}

	if rsp.Error != nil && rsp.Error.Code != 0 {
		// Get error code or result from rpc response
		result.Error = &runtime.Error{
			Code:    rsp.Error.Code,
			Message: rsp.Error.Message,
		}
		if len(res) != 0 {
			// Return response instead of error message if not empty
			result.Error.Message = string(res)
		}
		return
	}
	if len(res) != 0 {
		result.Result = res
	}
}

// BuildPreStopRequest -
func (j *JSONProtocol) BuildPreStopRequest(traceID string, extra map[string]string) ([]byte, error) {
	request := PreStopRequest{
		TraceID: traceID,
		Extra:   extra,
	}
	req, err := json.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to json marshal preStop request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// BuildUpdateAgencyRequest -
func (j *JSONProtocol) BuildUpdateAgencyRequest(traceID string, extra map[string]string) ([]byte, error) {
	request := PreStopRequest{
		TraceID: traceID,
		Extra:   extra,
	}
	req, err := json.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to json marshal agency update request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// ParsePreStopResponse -
func (j *JSONProtocol) ParsePreStopResponse(response []byte) *runtime.Error {
	var rsp PreStopResponse
	if err := json.Unmarshal(response, &rsp); err != nil {
		log.GetLogger().Errorf("failed to process preStop response, %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if rsp.Error != nil {
		return &runtime.Error{
			Code:    rsp.Error.Code,
			Message: rsp.Error.Message,
		}
	}
	return nil
}

// ProtoProtocol -
type ProtoProtocol struct{}

// BuildLoadRequest -
func (p *ProtoProtocol) BuildLoadRequest(c *config.Configuration, instanceFuncName string,
	traceID string, extra map[string]string) ([]byte, error) {
	envs := fetchEnvFromConfig(c)
	cpxSrcFuncName := (&urnutils.ComplexFuncName{}).ParseFrom(utils.LocalFuncURN.Name)
	ServiceID := cpxSrcFuncName.GetSvcIDWithPrefix()
	userData := map[string]string{
		"SNServiceID": ServiceID,
		"SrcTenantID": utils.LocalFuncURN.TenantID,
	}
	funcDigest := ""
	enableBusClient := utils.IsDirectFunc(c.Language)
	if len(c.CodeSha512) > funcDigestLength {
		funcDigest = c.CodeSha512[0 : funcDigestLength-1]
	} else {
		funcDigest = c.CodeSha512
	}
	request := &pb.LoadRequest{
		Entry:                c.EntryFile,
		FuncName:             c.HandlerName,
		FuncEnvs:             envs.FuncEnvs,
		RuntimeEnvs:          envs.RuntimeEnvs,
		StorageInfo:          getStorageInfo(c),
		InstanceFuncName:     instanceFuncName,
		RunningMode:          c.RunningMode,
		EnableClassIsolation: c.RuntimeConfig.EnableClassIsolation,
		TraceID:              traceID,
		UserData:             userData,
		Extra:                extra,
		NodeIP:               nodeIP,
		EnableBusClient:      enableBusClient,
		FuncDigest:           funcDigest,
		PodApplier:           runtime.PodApplier,
	}
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal load request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// ParseLoadResponse -
func (p *ProtoProtocol) ParseLoadResponse(rsp []byte) *runtime.Error {
	var loadResponse = &pb.LoadResponse{}
	if err := proto.Unmarshal(rsp, loadResponse); err != nil {
		log.GetLogger().Errorf("failed to unmarshal load response: %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if loadResponse.Error != nil && loadResponse.Error.Code != 0 {
		log.GetLogger().Errorf("failed to load function: %s", loadResponse.Error.Message)
		return &runtime.Error{
			Code:    loadResponse.GetError().GetCode(),
			Message: loadResponse.GetError().GetMessage(),
		}
	}
	return nil
}

// ParseInitializerResponse -
func (p *ProtoProtocol) ParseInitializerResponse(response []byte) *runtime.Error {
	// Unmarshal response and return
	var rsp = &pb.InitializerResponse{}
	if err := proto.Unmarshal(response, rsp); err != nil {
		log.GetLogger().Errorf("failed to process initializer request, %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if rsp.GetError() != nil {
		log.GetLogger().Warnf("initializer response error: %s", rsp.Error.Message)
		return &runtime.Error{
			Code:    rsp.GetError().GetCode(),
			Message: rsp.GetError().GetMessage(),
		}
	}
	return nil
}

// BuildInitializerRequest -
func (p *ProtoProtocol) BuildInitializerRequest(traceID string, extra map[string]string) ([]byte, error) {
	request := &pb.InitializerRequest{
		TraceID: traceID,
		Extra:   extra,
	}
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to proto marshal initializer request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// BuildRestoreHookRequest -
func (p *ProtoProtocol) BuildRestoreHookRequest(traceID string, restoreHook config.RestoreHook) ([]byte, error) {
	request := &pb.RestoreHookRequest{
		TraceID: traceID,
		Handler: restoreHook.Handler,
		Timeout: restoreHook.Timeout,
	}
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to proto marshal restore hook request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// ParseRestoreHookResponse -
func (p *ProtoProtocol) ParseRestoreHookResponse(response []byte) *runtime.Error {
	var rsp = &pb.RestoreHookResponse{}
	if err := proto.Unmarshal(response, rsp); err != nil {
		log.GetLogger().Errorf("failed to process restore hook request, %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if rsp.GetError() != nil {
		log.GetLogger().Errorf("restore hook response error: %s", rsp.Error.Message)
		return &runtime.Error{
			Code:    rsp.GetError().GetCode(),
			Message: rsp.GetError().GetMessage(),
		}
	}
	return nil
}

// ParseLog -
func (p *ProtoProtocol) ParseLog(l *functionlog.FunctionLog, message []byte) (string, string, error) {
	var funcLog = &pb.FunctionLog{}
	var traceID string
	if err := proto.Unmarshal(message, funcLog); err != nil {
		log.GetLogger().Errorf("failed to unmarshal function log: %s", err.Error())
		return "", "", err
	}
	l.Level = funcLog.Level
	l.Message.WriteString(funcLog.Content)
	buildTelemetryFunctionEvent(funcLog.Content, funcLog.Timestamp)
	setTimestamp(l, funcLog.Timestamp)
	if l.IsStdLog {
		traceID, _ = functionlog.ParseUserLog(funcLog.Content)
	}
	if utils.GetEnableRemoteDebug() {
		log.GetLogger().Debugf("it will send message to rdispatcher with %s", funcLog.String())
		// no block
		websocket.SendMsgToRDispatcher(funcLog.String())
	}
	return funcLog.InvokeID, traceID, nil
}

// ParseRuntimeLog -
func (p *ProtoProtocol) ParseRuntimeLog(l *functionlog.FunctionLog, message []byte) (string, error) {
	var funcLog = &pb.FunctionLog{}
	var traceID string
	if err := proto.Unmarshal(message, funcLog); err != nil {
		log.GetLogger().Errorf("failed to unmarshal function log: %s", err.Error())
		return "", err
	}
	l.Level = funcLog.Level
	l.Message.WriteString(funcLog.Content)
	setTimestamp(l, funcLog.Timestamp)
	l.IsFinish = funcLog.IsFinish
	l.IsStart = funcLog.IsStart
	l.TraceID = funcLog.TraceID
	l.Stage = funcLog.Stage
	l.LogType = funcLog.LogType
	l.Summary = funcLog.Summary
	l.ErrorCode = funcLog.ErrorCode
	l.LiveDataTraceID = funcLog.LiveDataTraceID
	if l.IsStdLog {
		traceID, _ = functionlog.ParseUserLog(funcLog.Content)
	}
	return traceID, nil
}

// BuildInvokeRequest -
func (p *ProtoProtocol) BuildInvokeRequest(id string, message runtime.InvokeRequest) ([]byte, error) {
	request := &pb.InvokeRequest{
		Event:    message.Event,
		TraceID:  message.TraceID,
		InvokeID: id,
		Extra:    message.Extra,
		FutureID: message.FutureID,
	}
	// Marshal invoke request and metaData
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal invoke request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// ParseInvokeResponse -
func (p *ProtoProtocol) ParseInvokeResponse(result *runtime.InvokeResponse, payload []byte) {
	// Unmarshal response and return
	var rsp = &pb.InvokeResponse{}
	if err := proto.Unmarshal(payload, rsp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal invoke response %s", err.Error())
		result.Error = &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
		return
	}
	// Get error code or result from rpc response
	if rsp.GetError() != nil {
		result.Error = &runtime.Error{
			Code:        rsp.GetError().GetCode(),
			Message:     rsp.GetError().GetMessage(),
			DcCallStack: rsp.GetError().GetDcCallStack(),
		}
		if rsp.GetResult() != nil {
			// Return response instead of error message if not empty
			result.Error.Message = string(rsp.GetResult())
		}
	} else {
		result.Result = rsp.GetResult()
		result.CffHTTPResponse = rsp.GetCffHTTPResponse()
	}
}

// ParsePreStopResponse -
func (p *ProtoProtocol) ParsePreStopResponse(response []byte) *runtime.Error {
	// Unmarshal response and return
	var rsp = &pb.PreStopResponse{}
	if err := proto.Unmarshal(response, rsp); err != nil {
		log.GetLogger().Errorf("failed to process preStop response, %s", err.Error())
		return &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: err.Error(),
		}
	}
	if rsp.GetError() != nil {
		log.GetLogger().Errorf("preStop response error: %s", rsp.Error.Message)
		return &runtime.Error{
			Code:    rsp.GetError().GetCode(),
			Message: rsp.GetError().GetMessage(),
		}
	}
	return nil
}

// BuildPreStopRequest -
func (p *ProtoProtocol) BuildPreStopRequest(traceID string, extra map[string]string) ([]byte, error) {
	request := &pb.PreStopRequest{
		TraceID: traceID,
		Extra:   extra,
	}
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to proto marshal preStop request: %s", err.Error())
		return nil, err
	}
	return req, nil
}

// BuildUpdateAgencyRequest -
func (p *ProtoProtocol) BuildUpdateAgencyRequest(traceID string, extra map[string]string) ([]byte, error) {
	request := &pb.UpdateAgencyRequest{
		TraceID: traceID,
		Extra:   extra,
	}
	req, err := proto.Marshal(request)
	if err != nil {
		log.GetLogger().Errorf("failed to proto marshal update agency request: %s", err.Error())
		return nil, err
	}
	return req, nil
}
