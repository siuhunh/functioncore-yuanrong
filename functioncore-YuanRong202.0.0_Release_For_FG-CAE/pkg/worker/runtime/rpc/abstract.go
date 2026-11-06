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

// Package rpc call rpc method in server
package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/common/urnutils"
	commUtils "functioncore/pkg/common/utils"
	rmUtils "functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionenv"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/rpc/pb"
	"functioncore/pkg/worker/startup"
	"functioncore/pkg/worker/telemetry"
	"functioncore/pkg/worker/utils"
)

var (
	loadOnce sync.Once

	maxLoadFunctionTimeout = 5 * time.Minute

	defaultMaxTimeout = 72 * time.Hour
)

const (
	// retry times of new tcp client in prestart period
	clientRetryTimes      = 100
	csharp                = "c#"
	uint32Len             = 4
	base                  = 10
	bitSize               = 64
	userFunctionLogPrefix = "[Function]"
	checkHealthTimeout    = 3
	updateAgencyTimeout   = 3
)

type creator struct {
	runtimeClient *Client
}

// RegisterRPCRuntime -
func RegisterRPCRuntime(version string) {
	runtime.Registry.Register(version, &creator{})
}

// NewRuntime -
func (c *creator) NewRuntime(configuration *config.Configuration, handlers runtime.Handlers,
) (runtime.Runtime, error) {
	rpcRuntimeIns := &rpcRuntime{}
	var err error
	if rpcRuntimeIns.AbstractRPC, err = NewAbstractRPC(configuration, handlers); err != nil {
		log.GetLogger().Errorf("failed to create abstract rpc: %s, runtime: %s",
			err.Error(), configuration.Language)
		return nil, err
	}
	startTime := time.Now()

	if err = rpcRuntimeIns.StartClient(c.runtimeClient); err != nil {
		return nil, fmt.Errorf("failed to start rpc client %s, runtime: %s",
			err.Error(), configuration.Language)
	}
	log.GetLogger().Debugf("cold start time %s, end start rpc client, runtime: %s",
		time.Now().Sub(startTime), configuration.Language)

	return rpcRuntimeIns, err
}

// Prestart connect runtime server
func (c *creator) Prestart(configuration *config.RuntimeConfig) error {
	var err error
	address := GetRuntimeAddress(configuration.RPCPort)
	log.GetLogger().Debugf("Prestart new runtimeClient")
	c.runtimeClient, err = NewClient(address, configuration.Timeout, ReconnectTimes(clientRetryTimes),
		ConnectType(configuration.Language, rmUtils.GetSocketType(configuration, configuration.Language)),
		CheckJavaxLanguage(configuration.Language))
	if err != nil {
		log.GetLogger().Warnf("connectRuntimeServerFail | %s", configuration.Language)
		return err
	}
	return nil
}

type rpcRuntime struct {
	*AbstractRPC
}

// AbstractRPC is abstract for rpc
type AbstractRPC struct {
	loadCtx    *invokectx.InvokeCtx
	initCtx    *invokectx.InvokeCtx
	restoreCtx *invokectx.InvokeCtx
	preStopCtx *invokectx.InvokeCtx
	rpcClient  *Client
	conf       *config.Configuration
	runtime.AbstractRuntime
	handlers     runtime.Handlers
	protocol     DataProtocol
	isDirectFunc bool
}

// GetConnInfo get conn local addr and remote addr info
func (r *AbstractRPC) GetConnInfo() string {
	return r.rpcClient.conn.GetConnInfo()
}

// NewAbstractRPC returns a rpc runtime
func NewAbstractRPC(conf *config.Configuration, handlers runtime.Handlers) (*AbstractRPC, error) {
	newAbstractRuntime, err := runtime.NewAbstractRuntime(conf)
	if err != nil {
		return nil, fmt.Errorf("new abstract runtime error: %s", err.Error())
	}

	if handlers.CallHandler == nil {
		return nil, errors.New("nil call handler")
	}

	if handlers.CancelHandler == nil {
		return nil, errors.New("nil cancel handler")
	}

	newAbstractRPC := &AbstractRPC{
		AbstractRuntime: *newAbstractRuntime,
		handlers:        handlers,
		protocol:        &ProtoProtocol{},
		conf:            conf,
		isDirectFunc:    utils.IsDirectFunc(conf.Language),
	}
	newAbstractRPC.Configuration.URN = conf.URN

	return newAbstractRPC, nil
}

// GetRuntimeAddress get address of runtime
func GetRuntimeAddress(port int32) string {
	return fmt.Sprintf("%s:%d", worker.LocalServerIP, port)
}

// StartClient starts RPC client
func (r *AbstractRPC) StartClient(runtimeClient *Client) error {
	if runtimeClient == nil {
		log.GetLogger().Infof("runtime is not connected")
		client, err := r.newClient()
		if err != nil {
			log.GetLogger().Errorf("failed to connect runtime: %s", err.Error())
			return err
		}
		r.rpcClient = client
	} else {
		if !runtimeClient.conn.IsBroken() {
			log.GetLogger().Infof("runtimeAlreadyConnected")
			r.rpcClient = runtimeClient
		} else {
			log.GetLogger().Info("the connection to runtime is broken. Try to reconnect.")
			client, err := r.newClient()
			if err != nil {
				log.GetLogger().Errorf("failed to reconnect runtime: %s", err.Error())
				return err
			}
			r.rpcClient = client
		}
	}

	r.rpcClient.SetProcessor(r)
	return nil
}

// StartupHealthz return rpc server startup health
func (r *AbstractRPC) StartupHealthz(path string, timeout int) (bool, error) {
	if r.rpcClient == nil {
		log.GetLogger().Errorf(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
		return false, errors.New(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
	}
	if timeout <= 0 {
		timeout = checkHealthTimeout
	}
	response, err := r.rpcClient.CheckStartupHeath(time.Duration(timeout) * time.Second)
	if err != nil {
		log.GetLogger().Errorf("failed to check startup health of runtime of %s", r.GetConnInfo())
		return false, err
	}

	if response.Status != "OK" {
		log.GetLogger().Errorf("runtime startup check is unhealthy of %s response %v", r.GetConnInfo(), response)
		return false, errors.New("failed to check startup health of runtime")
	}
	log.GetLogger().Debugf("succeed to check startup health of runtime")
	return true, nil
}

// Healthz return rpc server health
func (r *AbstractRPC) Healthz(path string, timeout int) (bool, error) {
	if r.rpcClient == nil {
		log.GetLogger().Errorf(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
		return false, errors.New(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
	}
	if timeout <= 0 {
		timeout = checkHealthTimeout
	}
	response, err := r.rpcClient.CheckHeath(time.Duration(timeout) * time.Second)
	if err != nil {
		log.GetLogger().Errorf("failed to check health of runtime of %s", r.GetConnInfo())
		return false, err
	}

	if response.Status != "OK" {
		log.GetLogger().Errorf("runtime is unhealthy of %s response %v", r.GetConnInfo(), response)
		return false, errors.New("failed to check health of runtime")
	}
	log.GetLogger().Debugf("succeed to check health of runtime")
	return true, nil
}

// ReportCustomMetrics report Custom Metrics
func (r *AbstractRPC) ReportCustomMetrics(timeout int) ([]byte, error) {
	if r.rpcClient == nil {
		log.GetLogger().Warnf(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
		return []byte{}, errors.New(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
	}
	response, err := r.rpcClient.ReportCustomMetrics(timeout)
	if err != nil {
		log.GetLogger().Warnf("failed to obtain custom report metrics %s", r.GetConnInfo())
		return []byte{}, err
	}

	return response, nil
}

// HotLoad call runtime process
func (r *AbstractRPC) HotLoad(ctx context.Context, c *config.Configuration) error {
	if c.RunningMode == constants.RunningModeRestore {
		return r.restoreSnapshot(ctx, c)
	}

	// Set invoke timeout
	invokeTimeout := time.Duration(r.Configuration.RuntimeConfig.Timeout) * time.Second
	r.rpcClient.SetInvokeTimeout(invokeTimeout)
	r.FuncLogger.SetLogLevel(c.RuntimeConfig.FuncLogLevel)
	_, instanceFuncName, err := urnutils.GetFunctionNameAndServiceName(utils.LocalFuncURN.Name)
	if err != nil {
		log.GetLogger().Warnf("failed to fetch localFunction name from urn: %s", err.Error())
	}
	traceID := commUtils.UniqueID()
	log.GetLogger().Infof("hotLoad | entry %s | handler %s | %s", c.EntryFile, c.HandlerName, traceID)
	loadFunctionTimeout := invokeTimeout
	if loadFunctionTimeout > maxLoadFunctionTimeout {
		loadFunctionTimeout = maxLoadFunctionTimeout
	}

	stage := constants.LoadStage
	if c.RunningMode == constants.RunningModeCheckpoint {
		stage = constants.CheckpointLoadStage
	}
	summarySpan := runtime.NewSummarySpan(stage, traceID, r.Configuration, loadFunctionTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	r.loadCtx = &ic
	defer func() {
		r.loadCtx = nil
		invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	}()
	logRecorder := r.FuncLogger.NewLogRecorder(ic, traceID, stage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)

	if !r.isDirectFunc {
		if runtime.PodApplier == constants.WorkerManagerApplier {
			logRecorder.FinishHeader(ic)
			defer logRecorder.FinishFooter(ic)
		} else {
			defer logRecorder.FinishCopy(ic)
		}
	}

	if strings.Contains(c.Language, csharp) {
		r.protocol = &JSONProtocol{}
	} else {
		r.protocol = &ProtoProtocol{}
	}

	if err = r.loadFunction(c, summarySpan, instanceFuncName, logRecorder, loadFunctionTimeout); err != nil {
		return err
	}

	// start extension
	if err := runtime.InitExtensionProcess(loadFunctionTimeout, r.Configuration, r.FuncLogger); err != nil {
		log.GetLogger().Errorf("failed to start extension process, error: %s", err.Error())
		return err
	}

	if c.RuntimeConfig.Initializer.InitializerHandler != "" && !r.isDirectFunc {
		initializerTimeout :=
			time.Duration(r.Configuration.RuntimeConfig.Initializer.InitializerTimeout) * time.Second
		r.rpcClient.SetInitializerTimeout(initializerTimeout)
		return r.processInitFunction(c)
	}
	return nil
}

func (r *AbstractRPC) restoreSnapshot(ctx context.Context, c *config.Configuration) error {
	if c.SnapshotConfig.RestoreHook.Handler != "" {
		log.GetLogger().Infof("run restore hook")
		restoreHookTimeout :=
			time.Duration(c.RestoreHook.Timeout) * time.Second
		r.rpcClient.SetRestoreHookTimeout(restoreHookTimeout)
		return r.processRestoreHook(c)
	}
	return nil
}

func (r *AbstractRPC) processRestoreHook(c *config.Configuration) error {
	traceID := commUtils.UniqueID()
	summarySpan := runtime.NewSummarySpan(constants.RestoreStage, traceID, r.Configuration,
		r.rpcClient.restoreHookTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	r.restoreCtx = &ic
	defer func() {
		r.restoreCtx = nil
		invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	}()
	logRecorder := r.FuncLogger.NewLogRecorder(ic, traceID, constants.RestoreStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	if runtime.PodApplier == constants.WorkerManagerApplier {
		logRecorder.FinishHeader(ic)
		defer logRecorder.FinishFooter(ic)
	} else {
		defer logRecorder.FinishCopy(ic)
	}
	// Send restore to runtime
	request, err := r.protocol.BuildRestoreHookRequest(traceID, c.RestoreHook)
	if err != nil {
		return err
	}

	if slice.ContainsString(telemetry.Config.Types, constants.TelemetryPlatformType, nil) {
		startEvent := telemetry.Event{
			Type:      constants.TelemetryPlatformType,
			Name:      constants.TelemetryRestoreStart,
			StartTime: time.Now().UTC().Format(functionlog.LogTimeFormat),
			TraceID:   traceID,
		}
		telemetry.SendEvent(startEvent)
	}
	rsp, err := r.rpcClient.ProcessRestoreHook(request)
	errorInfo := r.convertErrorFromRestoreHookResponse(err, rsp)
	summarySpan.Finish(errorInfo)
	buildRuntimeDoneAndReportEvent(constants.TelemetryRestoreRuntimeDone, constants.TelemetryRestoreReport,
		summarySpan, traceID)

	if errorInfo != nil {
		logs := logRecorder.MarshalAll()
		snErr := snerror.New(int(errorInfo.Code),
			startup.GenerateErrorString(logs, summarySpan.SummaryInfo, errorInfo))
		if commUtils.IsUserError(snErr) {
			runtime.RestoreDuration = summarySpan.SubDuration()
			if runtime.PodApplier == constants.WorkerManagerApplier {
				runtime.ConResource.SetRestoreSummary(summarySpan)
			}
			errorCode := snerror.FunctionRestoreHookFailCode
			if snErr.Code() == snerror.RuntimeInvokeTimeoutCode {
				errorCode = snerror.RuntimeRestoreHookTimeoutCode
				errorInfo.Message = fmt.Sprintf(snerror.ErrText(errorCode), r.rpcClient.restoreHookTimeout.String())
			}
			return snerror.New(errorCode, startup.GenerateErrorString(logs, summarySpan.SummaryInfo, errorInfo))
		}
		return snerror.New(int(errorInfo.Code), startup.GenerateErrorString(logs, summarySpan.SummaryInfo, errorInfo))
	}
	// The timeWindows can be added only after the restore is successful.
	runtime.ConResource.SetRestoreSummary(summarySpan)
	runtime.RestoreDuration = summarySpan.SubDuration()
	return nil
}

func (r *AbstractRPC) loadFunction(c *config.Configuration, summarySpan *runtime.Summary, instanceFuncName string,
	logRecorder *functionlog.LogRecorder, loadFunctionTimeout time.Duration) error {
	request, err := r.protocol.BuildLoadRequest(c, instanceFuncName, summarySpan.GetTraceID(),
		c.RuntimeConfig.Initializer.Extra)
	if err != nil {
		return err
	}
	rsp, err := r.rpcClient.LoadFunction(request, loadFunctionTimeout)
	errorInfo := r.convertErrorFromLoadResponse(err, rsp)
	summarySpan.Finish(errorInfo)
	if errorInfo != nil {
		snErr := snerror.New(int(errorInfo.Code),
			startup.GenerateErrorString(logRecorder.MarshalAll(), summarySpan.SummaryInfo, errorInfo))
		if commUtils.IsUserError(snErr) {
			runtime.StartupDuration = summarySpan.SubDuration()
			if runtime.PodApplier == constants.WorkerManagerApplier {
				runtime.ConResource.SetLoadSummary(summarySpan)
			}
		}
		return snErr
	}
	runtime.StartupDuration = summarySpan.SubDuration()
	runtime.ConResource.SetLoadSummary(summarySpan)
	log.GetLogger().Debugf("succeed to Load function in runtime")
	return nil
}

func fetchEnvFromConfig(c *config.Configuration) *utils.Environment {
	envs, err := functionenv.FetchEnvFromConfigWithHandlerErr(c)
	if err != nil {
		log.GetLogger().Warnf("failed to fetch environment from the configuration, error: %s", err.Error())
	}
	return envs
}

func (r *AbstractRPC) handleLoadFailure(err error) error {
	log.GetLogger().Errorf("failed to load function: %s", err.Error())
	if r.handlers.GetExceptionHandler != nil {
		if runtimeErr := r.handlers.GetExceptionHandler(); runtimeErr != nil {
			log.GetLogger().Errorf("load runtime error: %v", runtimeErr)
			return snerror.New(int(runtimeErr.Code), runtimeErr.Message)
		}
	}
	return err
}

func (r *AbstractRPC) processInitFunction(c *config.Configuration) error {
	if err := r.processInitializer(c); err != nil {
		log.GetLogger().Warnf("initFuncFail | %s", commUtils.ExtractErrMsg(err.Error()))
		return err
	}
	log.GetLogger().Infof("succeed to initializer function in runtime")
	return nil
}

// used in financial scenarios
func getStorageInfo(c *config.Configuration) []string {
	if c.HandlerName != "" {
		return nil
	}
	log.GetLogger().Info("handler name is empty, this is financial scenarios")
	return []string{c.StorageConfig.Addr}
}

func (r *AbstractRPC) getRuntimeExceptionSNError(err error, stage string) *runtime.Error {
	snErr, ok := err.(snerror.SNError)
	if ok && (snErr.Code() == snerror.InternalError || snErr.Code() == snerror.FuncInsExceptionCode) &&
		r.handlers.GetExceptionHandler != nil {
		if runtimeErr := r.handlers.GetExceptionHandler(); runtimeErr != nil {
			if stage == constants.InitializeStage && runtime.IsUserError(*runtimeErr) {
				runtimeErr.Message = snerror.FunctionInitFailErrPrefix + runtimeErr.Message
			}
			if stage == constants.RestoreStage && runtime.IsUserError(*runtimeErr) {
				runtimeErr.Message = snerror.FunctionRestoreFailErrPrefix + runtimeErr.Message
			}
			log.GetLogger().Warnf("get exception: %v", runtimeErr)
			return runtimeErr
		} else if snErr.Code() == snerror.FuncInsExceptionCode {
			msg := tcpmsg.BuildMessage(tcpmsg.KillRuntimeProcess, nil)
			err := runtime.RuntimeManagerClient.SendMessageWithNoResp(msg)
			if err != nil {
				log.GetLogger().Warnf("failed to kill runtime: %s", err.Error())
			}
			return &runtime.Error{
				Code:    snerror.FuncInsExceptionCode,
				Message: snerror.ErrText(snerror.FuncInsExceptionCode),
			}
		}
	}
	return nil
}

func (r *AbstractRPC) buildRuntimeError(err error, stage string) *runtime.Error {
	if snerr := r.getRuntimeExceptionSNError(err, stage); snerr != nil {
		return snerr
	}
	if sErr, ok := err.(snerror.SNError); ok {
		var timeout = ""
		runtimeErr := &runtime.Error{
			Code:    int32(sErr.Code()),
			Message: sErr.Error(),
		}
		if stage == constants.InvokeStage {
			timeout = r.rpcClient.invokeTimeout.String()
		}
		if stage == constants.InitializeStage {
			timeout = r.rpcClient.initializerTimeout.String()
		}
		if stage == constants.RestoreStage {
			timeout = r.rpcClient.restoreHookTimeout.String()
		}
		if sErr.Code() == snerror.RuntimeInvokeTimeoutCode {
			runtimeErr.Message = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), timeout)
			return runtimeErr
		}
		if sErr.Code() == snerror.RuntimeLoadTimeoutCode {
			runtimeErr.Message = fmt.Sprintf(snerror.ErrText(snerror.RuntimeLoadTimeoutCode), timeout)
			return runtimeErr
		}
		if stage == constants.InitializeStage {
			runtimeErr.Message = snerror.FunctionInitFailErrPrefix + sErr.Error()
		}
		if stage == constants.RestoreStage {
			runtimeErr.Message = snerror.FunctionRestoreFailErrPrefix + sErr.Error()
		}
		return runtimeErr
	}
	return &runtime.Error{
		Code:    snerror.WorkerInternalErrorCode,
		Message: err.Error(),
	}
}

func (r *AbstractRPC) convertErrorFromLoadResponse(err error,
	rsp []byte) *runtime.Error {
	if err != nil {
		log.GetLogger().Errorf("failed to load function: %s", err.Error())
		return r.buildRuntimeError(err, constants.InvokeStage)
	}
	return r.protocol.ParseLoadResponse(rsp)
}

func (r *AbstractRPC) convertErrorFromInitializerResponse(err error,
	response []byte) *runtime.Error {
	if err != nil {
		log.GetLogger().Warnf("processInitReqFail | %s", err.Error())
		return r.buildRuntimeError(err, constants.InitializeStage)
	}
	return r.protocol.ParseInitializerResponse(response)
}

func (r *AbstractRPC) convertErrorFromRestoreHookResponse(err error,
	response []byte) *runtime.Error {
	if err != nil {
		log.GetLogger().Errorf("failed to process restore request, %s", err.Error())
		return r.buildRuntimeError(err, constants.RestoreStage)
	}
	return r.protocol.ParseRestoreHookResponse(response)
}

func (r *AbstractRPC) setResultFromRsp(result *runtime.InvokeResponse, err error, payload []byte) {
	if err != nil {
		log.GetLogger().Warnf("failed to process invoke response: %s", err.Error())
		result.Error = r.buildRuntimeError(err, constants.InvokeStage)
		return
	}
	r.protocol.ParseInvokeResponse(result, payload)
}

// processInitializer process event of initializer
func (r *AbstractRPC) processInitializer(c *config.Configuration) error {
	var errorInfo *runtime.Error
	traceID := commUtils.UniqueID()
	stage := constants.InitializeStage
	if c.RunningMode == constants.RunningModeCheckpoint {
		stage = constants.CheckpointInitStage
	}
	summarySpan := runtime.NewSummarySpan(stage, traceID, r.Configuration,
		r.rpcClient.initializerTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	r.initCtx = &ic
	defer func() {
		r.initCtx = nil
		invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	}()
	logRecorder := r.FuncLogger.NewLogRecorder(ic, traceID, stage, functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	if runtime.PodApplier == constants.WorkerManagerApplier {
		logRecorder.FinishHeader(ic)
		defer logRecorder.FinishFooter(ic)
	} else {
		defer func() {
			if errorInfo != nil {
				logRecorder.Finish(ic)
				return
			}
			logRecorder.FinishCopy(ic)
		}()
	}
	// Send initializer to runtime
	request, err := r.protocol.BuildInitializerRequest(traceID, c.Initializer.Extra)
	if err != nil {
		return err
	}
	if slice.ContainsString(telemetry.Config.Types, constants.TelemetryPlatformType, nil) {
		startEvent := telemetry.Event{
			Type:      constants.TelemetryPlatformType,
			Name:      constants.TelemetryInitStart,
			StartTime: time.Now().UTC().Format(functionlog.LogTimeFormat),
			TraceID:   traceID,
		}
		telemetry.SendEvent(startEvent)
	}
	rsp, err := r.rpcClient.ProcessInitializer(request)
	errorInfo = r.convertErrorFromInitializerResponse(err, rsp)
	summarySpan.Finish(errorInfo)
	buildRuntimeDoneAndReportEvent(constants.TelemetryInitRuntimeDone, constants.TelemetryInitReport,
		summarySpan, traceID)
	if errorInfo != nil {
		logs := logRecorder.MarshalAll()
		snErr := snerror.New(int(errorInfo.Code),
			startup.GenerateErrorString(logs, summarySpan.SummaryInfo, errorInfo))
		if commUtils.IsUserError(snErr) {
			runtime.InitializerDuration = summarySpan.SubDuration()
			if runtime.PodApplier == constants.WorkerManagerApplier {
				runtime.ConResource.SetInitSummary(summarySpan)
			} else {
				runtime.ConResource.SetLoadSummary(nil)
			}
			errorCode := snerror.FunctionInitFailCode
			if snErr.Code() == snerror.RuntimeInvokeTimeoutCode {
				errorCode = snerror.RuntimeInitTimeoutCode
				errorInfo.Message = fmt.Sprintf(snerror.ErrText(errorCode), r.rpcClient.initializerTimeout.String())
			}
			errorInfo.Code = int32(errorCode)
			return snerror.New(errorCode, startup.GenerateErrorString(logs, summarySpan.SummaryInfo, errorInfo))
		}
		return snerror.New(int(errorInfo.Code), startup.GenerateErrorString(logs, summarySpan.SummaryInfo, errorInfo))
	}
	// The timeWindows can be added only after the initialization is successful.
	runtime.ConResource.SetInitSummary(summarySpan)
	runtime.InitializerDuration = summarySpan.SubDuration()
	return nil
}

// ProcessCancelEvent process cancel request from bus
func (r *AbstractRPC) ProcessCancelEvent(request *runtime.CancelRequest) *runtime.CancelResponse {
	response := &runtime.CancelResponse{
		Error: &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"cancel is not supported"),
		},
	}
	return response
}

func formatInvokeCtx(ic invokectx.InvokeCtx) string {
	return strconv.FormatInt(int64(ic.ID), base)
}

func (r *AbstractRPC) getInvokeCtx(s string) (invokectx.InvokeCtx, bool) {
	if s == functionlog.LoadInvokeID {
		if ic := r.loadCtx; ic != nil {
			return *ic, true
		}
		return invokectx.InvokeCtx{}, false
	}
	if s == functionlog.InitializerInvokeID {
		if ic := r.initCtx; ic != nil {
			return *ic, true
		}
		return invokectx.InvokeCtx{}, false
	}
	if s == functionlog.RestoreHookInvokeID {
		if ic := r.restoreCtx; ic != nil {
			return *ic, true
		}
		return invokectx.InvokeCtx{}, false
	}
	if s == functionlog.PreStopInvokeID {
		if ic := r.preStopCtx; ic != nil {
			return *ic, true
		}
		return invokectx.InvokeCtx{}, false
	}

	id, err := strconv.ParseUint(s, base, bitSize)
	if err != nil {
		log.GetLogger().Warnf("failed to Invoke ID transfer Bus ID, error: %s", err.Error())
		return invokectx.InvokeCtx{}, false
	}
	return invokectx.DefaultMetrics.GetInvokeCtx(id)
}

// SendProcessEvent send process event of invoking
func (r *AbstractRPC) SendProcessEvent(ic invokectx.InvokeCtx, message runtime.InvokeRequest,
	conf *config.Configuration) {
	request, err := r.protocol.BuildInvokeRequest(formatInvokeCtx(ic), message)
	if err != nil {
		result := runtime.InvokeResponse{}
		r.setResultFromRsp(&result, err, nil)
		go r.handlers.RecvProcessEventHandler(ic, result)
		return
	}
	r.rpcClient.SendInvoke(ic, request, r.getInvokeTimeout())
}

// RecvInvoke -
func (r *AbstractRPC) RecvInvoke(ic invokectx.InvokeCtx, rsp []byte, err error) {
	result := runtime.InvokeResponse{}
	r.setResultFromRsp(&result, err, rsp)
	go r.handlers.RecvProcessEventHandler(ic, result)
}

// ProcessCall process call message from runtime
func (r *AbstractRPC) ProcessCall(callRequest *pb.CallRequest) (*pb.CallResponse, error) {
	start := time.Now()
	callResponse := pb.CallResponse{}

	rsp, err := r.doRequest(context.Background(), callRequest)
	if err != nil {
		callResponse.Error = &pb.Error{
			Code:        err.Code,
			Message:     err.Message,
			DcCallStack: err.DcCallStack,
		}
	}
	callResponse.Result = rsp
	callResponse.CallID = callRequest.CallID

	log.GetLogger().Debugf("call function %s response, callID %s, traceID %s, cost %s",
		callRequest.Destination, callRequest.CallID, callRequest.TraceID, time.Since(start).String())
	return &callResponse, nil
}

// Request send request to function bus client
func (r *AbstractRPC) doRequest(ctx context.Context, message *pb.CallRequest) ([]byte, *runtime.Error) {
	cpxSrcFuncName := (&urnutils.ComplexFuncName{}).ParseFrom(utils.LocalFuncURN.Name)
	// Get tenantID of target
	request := &runtime.CallRequest{
		FunctionInfo: utils.FunctionInfo{
			TenantID:  utils.LocalFuncURN.TenantID,
			ServiceID: cpxSrcFuncName.GetSvcIDWithPrefix(),
			FuncName:  message.Destination,
			Version:   utils.LocalFuncURN.Version,
		},
		Event:    message.Event,
		Timeout:  message.Timeout,
		CallID:   message.CallID,
		InvokeID: message.InvokeID,
		TraceID:  message.TraceID,
		FutureID: message.FutureID,
	}
	if message.DestAttribute == constants.PlatformTenantID {
		request.TenantID = message.DestAttribute
	}

	if ic, ok := r.getInvokeCtx(message.InvokeID); ok {
		recorder := r.FuncLogger.GetLogRecorder(ic)
		request.LogType = recorder.LogOption()
	}

	// Get SN serviceID
	if request.ServiceID != "" && request.TenantID != constants.PlatformTenantID {
		request.FuncName = urnutils.NewComplexFuncName(cpxSrcFuncName.ServiceID, request.FuncName).String()
	}

	response := r.handlers.CallHandler(ctx, request)
	if response == nil {
		log.GetLogger().Errorf("call handler return nil, callID %s, traceID %s",
			message.CallID, message.TraceID)
		return nil, &runtime.Error{
			Code:    snerror.CallFunctionErrorCode,
			Message: snerror.ErrText(snerror.CallFunctionErrorCode),
		}
	}

	return response.Result, response.Error
}

// ProcessCancelRequest process cancel request from runtime and send it to bus
func (r *AbstractRPC) ProcessCancelRequest(request *pb.CancelRequest) (*pb.CancelResponse, error) {
	req := &runtime.CancelRequest{
		TraceID:   request.TraceID,
		FutureID:  request.FutureID,
		Force:     request.Force,
		Recursive: request.Recursive,
	}
	ctx := context.Background()
	res := r.handlers.CancelHandler(ctx, req)
	response := &pb.CancelResponse{
		CallID:  request.CallID,
		TraceID: request.TraceID,
	}
	if res == nil {
		log.GetLogger().Errorf("cancel handler return nil, traceID %s", req.TraceID)
		response.Error = &pb.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"cancel handler return nil"),
		}
	} else if res.Error != nil {
		response.Error = &pb.Error{
			Code:    res.Error.Code,
			Message: res.Error.Message,
		}
	}
	return response, nil
}

func handleStartLog(conf *config.Configuration, funcLog *functionlog.FunctionLog, isReversed bool) error {
	if funcLog.Stage == "invoke" && !isReversed {
		runtime.WriteLoadInitLog(funcLog.TraceID, funcLog.LiveDataTraceID)
	}
	if err := runtime.CreateRuntimeLoggerCtx(conf, funcLog, isReversed); err != nil {
		log.GetLogger().Errorf("failed to create runtime LoggerCtx, err: %s", err.Error())
		return err
	}
	return nil
}

func (r *AbstractRPC) handleFinishLog(conf *config.Configuration, funcLog *functionlog.FunctionLog,
	isReversed bool) error {
	var logs string
	summarySpan := runtime.NewSummarySpan(funcLog.Stage, funcLog.TraceID, conf, defaultMaxTimeout)
	summaryInfo := snerror.SummaryInfo{}
	err := json.Unmarshal([]byte(funcLog.Summary), &summaryInfo)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal summary info, err: %s, summaryStr: %s", err.Error(), funcLog.Summary)
	} else {
		summarySpan.SummaryInfo = summaryInfo
	}
	traceLogger, err := runtime.GetRuntimeLogRecorder(funcLog.TraceID)
	if err != nil || traceLogger.LogRecorder == nil || traceLogger.Info == nil {
		log.GetLogger().Errorf("failed to get runtime logRecorder")
		return err
	}
	traceLogger.LogRecorder.SetFooter(funcLog, summarySpan)
	traceID := funcLog.TraceID
	errCode := funcLog.ErrorCode
	summary := funcLog.Summary
	var isReportLog bool
	if funcLog.Stage == "invoke" && funcLog.LogType == "Tail" {
		logs = traceLogger.LogRecorder.MarshalAll()
		isReportLog = true
	}
	if funcLog.Stage == "invoke" || isReversed {
		traceLogger.LogRecorder.FinishFooter(traceLogger.Ic)
	} else {
		traceLogger.LogRecorder.FinishCopy(traceLogger.Ic)
	}
	if isReportLog {
		go func() {
			r.PushLogAndSummaryToBus(traceID, summary, logs, errCode)
		}()
	}
	return nil
}

// PushLogAndSummaryToBus -
func (r *AbstractRPC) PushLogAndSummaryToBus(traceID, summary, logs string, errorCode int32) {
	if r.handlers.ReportHandler != nil {
		ctx := context.Background()
		workerInfo := &connecttypes.WorkerReportInfo{
			TraceID:        traceID,
			ErrorCode:      errorCode,
			ErrorMessage:   snerror.ErrText(int(errorCode)),
			RuntimeSummary: summary,
			RuntimeLog:     logs,
		}
		err := r.handlers.ReportHandler(ctx, workerInfo)
		if err != nil {
			log.GetLogger().Errorf("log handler return nil, err %s", err.Error())
		}
	} else {
		log.GetLogger().Errorf("r.handlers.LogHandler is nil")
	}
}

func (r *AbstractRPC) handleTraceIDLog(funcLog *functionlog.FunctionLog) {
	info := functionlog.GetRuntimeInfo(funcLog.TraceID)
	if info != nil && info.ID != 0 {
		invokeCtxID := strconv.Itoa((int)(info.ID))
		if ic, ok := r.getInvokeCtx(invokeCtxID); ok {
			recorder := r.FuncLogger.GetLogRecorder(ic)
			if runtime.PodApplier != constants.WorkerManagerApplier &&
				(recorder.Stage() != constants.InvokeStage && recorder.Stage() != constants.ColdStartStage &&
					recorder.Stage() != constants.PreStopStage) {
				recorder.WriteLimit(funcLog)
				return
			}
			recorder.RealTimeWrite(funcLog)
			return
		}
	}
	r.FuncLogger.WriteDefaultLog(funcLog)
}

func (r *AbstractRPC) handleDuringLog(funcLog *functionlog.FunctionLog, traceID string) {
	if funcLog.IsStdLog && len(traceID) != 0 {
		r.FuncLogger.RangeLoggerWithTraceID(funcLog, traceID)
	} else {
		r.handleTraceIDLog(funcLog)
	}
}

// ProcessRuntimeLog process log from runtime
func (r *AbstractRPC) ProcessRuntimeLog(message []byte) error {
	funcLog := r.FuncLogger.AcquireLog()
	if funcLog == nil {
		return errors.New("failed to AcquireLog")
	}
	traceID, err := r.protocol.ParseRuntimeLog(funcLog, message)
	if err != nil {
		return err
	}
	isReversed := runtime.PodApplier == constants.WorkerManagerApplier
	if funcLog.IsStart {
		if err := handleStartLog(r.conf, funcLog, isReversed); err != nil {
			log.GetLogger().Warnf("failed to handleStartLog, err: %s", err.Error())
			return err
		}
		return nil
	}
	if funcLog.IsFinish {
		if err := r.handleFinishLog(r.conf, funcLog, isReversed); err != nil {
			log.GetLogger().Warnf("failed to handleFinishLog, err: %s", err.Error())
			return err
		}
		if err := runtime.DestroyRuntimeLoggerCtx(funcLog); err != nil {
			log.GetLogger().Errorf("failed to destroyRuntimeLoggerCtx, err: %s", err.Error())
			return err
		}
		return nil
	}
	r.handleDuringLog(funcLog, traceID)
	return nil
}

// ProcessLog process log from runtime
func (r *AbstractRPC) ProcessLog(message []byte) error {
	funcLog := r.FuncLogger.AcquireLog()
	invokeID, traceID, err := r.protocol.ParseLog(funcLog, message)
	if err != nil {
		return err
	}
	if funcLog.IsStdLog && len(traceID) != 0 {
		r.FuncLogger.RangeLoggerWithTraceID(funcLog, traceID)
	} else {
		if ic, ok := r.getInvokeCtx(invokeID); ok {
			recorder := r.FuncLogger.GetLogRecorder(ic)
			if runtime.PodApplier != constants.WorkerManagerApplier &&
				(recorder.Stage() != constants.InvokeStage && recorder.Stage() != constants.ColdStartStage &&
					recorder.Stage() != constants.PreStopStage) {
				recorder.WriteLimit(funcLog)
				return nil
			}
			recorder.RealTimeWrite(funcLog)
		} else {
			r.FuncLogger.WriteDefaultLog(funcLog)
		}
	}
	return nil
}

// ProcessMetrics process metrics message from runtime
func (r *AbstractRPC) ProcessMetrics(metrics *pb.Metrics) {
	return
}

func (r *AbstractRPC) getInvokeTimeout() time.Duration {
	timeout := r.rpcClient.invokeTimeout
	loadOnce.Do(func() {
		timeout = r.rpcClient.invokeTimeout - runtime.StartupDuration
	})
	return timeout
}

func buildRuntimeDoneAndReportEvent(runtimeDoneEvent string, reportEvent string,
	summarySpan *runtime.Summary, traceID string) {
	if !slice.ContainsString(telemetry.Config.Types, constants.TelemetryPlatformType, nil) {
		return
	}
	runtimeDoneAndReportEvent := telemetry.Event{
		Type:      constants.TelemetryPlatformType,
		Name:      runtimeDoneEvent,
		StartTime: time.Now().UTC().Format(functionlog.LogTimeFormat),
		TraceID:   traceID,
	}
	if summarySpan.GetErr() != nil {
		runtimeDoneAndReportEvent.Status = "fail"
	} else {
		runtimeDoneAndReportEvent.Status = "success"
	}
	telemetry.SendEvent(runtimeDoneAndReportEvent)

	metrics := telemetry.ReportMetrics{
		DurationMs:        summarySpan.SummaryInfo.Duration,
		BillingDurationMS: summarySpan.SummaryInfo.BillingDuration,
		MemorySize:        summarySpan.SummaryInfo.MemorySize,
		MemoryUsed:        summarySpan.SummaryInfo.MemoryUsed,
		PodName:           summarySpan.SummaryInfo.PodName,
	}
	runtimeDoneAndReportEvent.ReportMetrics = metrics
	runtimeDoneAndReportEvent.Name = reportEvent
	runtimeDoneAndReportEvent.StartTime = time.UnixMilli(time.Now().UnixMilli() -
		int64(metrics.BillingDurationMS)).UTC().Format(functionlog.LogTimeFormat)
	runtimeDoneAndReportEvent.EndTime = time.Now().UTC().Format(functionlog.LogTimeFormat)
	telemetry.SendEvent(runtimeDoneAndReportEvent)
}

func buildTelemetryFunctionEvent(message string, timestamp string) {
	if !slice.ContainsString(telemetry.Config.Types, constants.TelemetryFunctionType, nil) {
		return
	}
	if strings.Contains(message, userFunctionLogPrefix) {
		message = strings.TrimRight(message, "\n")
		split := strings.Split(message, userFunctionLogPrefix)
		event := telemetry.Event{
			Type: constants.TelemetryFunctionType,
			Name: constants.TelemetryFunctionType,
			Logs: strings.Join(split, ""),
		}
		ti, err := time.Parse(functionlog.RuntimeLogFormat, timestamp)
		if err == nil {
			event.StartTime = ti.UTC().Format(functionlog.LogTimeFormat)
		} else {
			event.StartTime = time.Now().UTC().Format(functionlog.LogTimeFormat)
		}
		telemetry.SendEvent(event)
	}
}

// PreStop preStop user function
func (r *AbstractRPC) PreStop() error {
	log.GetLogger().Infof("preStop function %s, timeout is %d", r.Configuration.RuntimeConfig.PreStop.Handler,
		r.Configuration.RuntimeConfig.PreStop.Timeout)
	if r.Configuration.RuntimeConfig.PreStop.Handler != "" {
		preStopTimeout := time.Duration(r.Configuration.RuntimeConfig.PreStop.Timeout) * time.Second
		r.rpcClient.SetPreStopTimeout(preStopTimeout)
		return r.processPreStopFunction()
	}
	return nil
}

// GetException get runtime exception error info
func (r *AbstractRPC) GetException(shouldWait bool) *runtime.Error {
	return nil
}

func (r *AbstractRPC) processPreStopFunction() error {
	if err := r.processPreStop(); err != nil {
		log.GetLogger().Errorf("failed to preStop the function, err: %s", err.Error())
		return err
	}
	log.GetLogger().Info("succeed to preStop function in runtime")
	return nil
}

// processPreStop process event of preStop
func (r *AbstractRPC) processPreStop() error {
	traceID := commUtils.UniqueID()
	stage := constants.PreStopStage
	summarySpan := runtime.NewSummarySpan(stage, traceID, r.Configuration,
		r.rpcClient.preStopTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	r.preStopCtx = &ic
	defer func() {
		r.preStopCtx = nil
		invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	}()
	logRecorder := r.FuncLogger.NewLogRecorder(ic, traceID, stage, functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	logRecorder.FinishHeader(ic)
	defer logRecorder.FinishFooter(ic)
	return r.requestPreStop(traceID, logRecorder, summarySpan)
}

func (r *AbstractRPC) convertErrorFromPreStopResponse(err error,
	response []byte) *runtime.Error {
	if err != nil {
		log.GetLogger().Errorf("failed to process preStop request, %s", err.Error())
		return r.buildRuntimeError(err, constants.PreStopStage)
	}
	return r.protocol.ParsePreStopResponse(response)
}

func (r *AbstractRPC) newClient() (*Client, error) {
	var client *Client
	var err error
	if r.Configuration.ReconnectTimes != 0 {
		client, err = NewClient(GetRuntimeAddress(r.Configuration.RPCPort), r.Configuration.RuntimeConfig.Timeout,
			ReconnectTimes(r.Configuration.ReconnectTimes), ConnectType(r.Configuration.Language,
				rmUtils.GetSocketType(&r.Configuration.RuntimeConfig, r.Configuration.Language)),
			CheckJavaxLanguage(r.Configuration.Language))
	} else {
		client, err = NewClient(GetRuntimeAddress(r.Configuration.RPCPort), r.Configuration.RuntimeConfig.Timeout,
			ConnectType(r.Configuration.Language,
				rmUtils.GetSocketType(&r.Configuration.RuntimeConfig, r.Configuration.Language)),
			CheckJavaxLanguage(r.Configuration.Language))
	}
	return client, err
}

func (r *AbstractRPC) requestPreStop(traceID string, logRecorder *functionlog.LogRecorder,
	summarySpan *runtime.Summary) error {
	var errorInfo *runtime.Error
	switch r.Configuration.PreStop.Type {
	case "command":
		// send preStop to runtime-manager
		msg := tcpmsg.BuildMessage(tcpmsg.ExecPreStopCommand, nil)
		resp := runtime.RuntimeManagerClient.SendMessageWithTimeout(msg, r.rpcClient.preStopTimeout)
		if resp.Response.Result != "" {
			r.writePreStopCommandOutput(resp.Response.Result, logRecorder, "INFO")
		}
		errorInfo = r.convertErrorFromRuntimeManagerResponse(resp)
		if errorInfo != nil {
			r.writePreStopCommandOutput(errorInfo.Message, logRecorder, "ERROR")
		}
	default:
		// send preStop to runtime
		extra := decryptUserAgencyToExtra()
		extra[constants.StateIDFlag] = r.Configuration.StateID
		extra[constants.IsClusterUpgrading] = r.Configuration.IsClusterUpgrading
		request, err := r.protocol.BuildPreStopRequest(traceID, extra)
		if err != nil {
			return err
		}
		rsp, err := r.rpcClient.ProcessPreStop(request)
		errorInfo = r.convertErrorFromPreStopResponse(err, rsp)
	}
	summarySpan.Finish(errorInfo)
	defer runtime.ConResource.SetPreStopSummary(summarySpan)

	if errorInfo != nil {
		log.GetLogger().Errorf("failed to preStop the function, code: %d, message: %s",
			errorInfo.Code, errorInfo.Message)
		return errors.New(errorInfo.Message)
	}
	return nil
}

// RequestUpdateAgency update agency request
func (r *AbstractRPC) RequestUpdateAgency() error {
	extra := decryptUserAgencyToExtra()
	traceID := commUtils.UniqueID()
	request, err := r.protocol.BuildUpdateAgencyRequest(traceID, extra)
	if err != nil {
		log.GetLogger().Errorf("failed to build update request: %s", err.Error())
		return err
	}
	_, err = r.rpcClient.ProcessUpdateAgency(request)
	if err != nil {
		log.GetLogger().Errorf("failed to process update request: %s", err.Error())
		return err
	}
	return nil
}

func (r *AbstractRPC) writePreStopCommandOutput(result string, recorder *functionlog.LogRecorder, logLevel string) {
	now := time.Now()
	preStopCommandLog := r.FuncLogger.AcquireLog()
	preStopCommandLog.Time = now.Format(functionlog.FuncLogTimeFormat)
	preStopCommandLog.NanoTime = now.Format(functionlog.NanoLogLayout)
	preStopCommandLog.Level = logLevel
	preStopCommandLog.IsStdLog = true
	preStopCommandLog.Message.WriteString(result)
	recorder.RealTimeWrite(preStopCommandLog)
}

func (r *AbstractRPC) convertErrorFromRuntimeManagerResponse(resp tcpmsg.RuntimeManagerRSP) *runtime.Error {
	runtimeErr := &runtime.Error{}
	if resp.Code > 0 {
		runtimeErr.Code = int32(resp.Code)
		runtimeErr.Message = resp.Err
		return runtimeErr
	}
	if resp.Err == "read response timeout" {
		runtimeErr.Code = snerror.RuntimeInvokeTimeoutCode
		runtimeErr.Message = fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode),
			r.rpcClient.preStopTimeout.String())
		return runtimeErr
	}
	if resp.Err != "" {
		runtimeErr.Code = snerror.IllegalArgumentErrorCode
		runtimeErr.Message = resp.Err
		return runtimeErr
	}
	return nil
}

func decryptUserAgencyToExtra() map[string]string {
	extra := make(map[string]string, 0)
	decryptToExtra(extra, constants.HeaderAccessKey)
	decryptToExtra(extra, constants.HeaderSecretKey)
	decryptToExtra(extra, constants.HeaderAuthToken)
	decryptToExtra(extra, constants.HeaderSecurityAccessKey)
	decryptToExtra(extra, constants.HeaderSecuritySecretKey)
	decryptToExtra(extra, constants.HeaderSecurityToken)
	return extra
}

func decryptToExtra(extra map[string]string, headerName string) {
	if extra == nil {
		return
	}
	if utils.GetUserAgency(headerName) != "" {
		decryptAgencyValue, err := localauth.Decrypt(utils.GetUserAgency(headerName))
		if err != nil {
			log.GetLogger().Warnf("failed to decrypt %", headerName)
			extra[headerName] = ""
		} else {
			extra[headerName] = string(decryptAgencyValue)
		}
		commUtils.ClearByteMemory(decryptAgencyValue)
	} else {
		extra[headerName] = ""
	}
}
