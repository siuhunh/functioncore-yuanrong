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

// Package v1 is go1.x runtime
package v1

import (
	"context"
	"fmt"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	commUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionenv"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/golang/rpcservice"
	"functioncore/pkg/worker/startup"
	"functioncore/pkg/worker/utils"
)

const (
	formatIntBase                 = 10
	formatInt64                   = 64
	megabyte                      = 1024 * 1024
	checkReadyInterval            = time.Duration(5) * time.Millisecond
	envRuntimeAPIADDR             = "RUNTIME_API_ADDR"
	envRuntimeMaxResponseBodySize = "RUNTIME_MAX_RESP_BODY_SIZE"
	headerSize                    = 9
	invokeReqNotifyChNum          = 100
)

type creator struct {
}

// recvMsgCall is used to notify the goroutine that receives the invoke request to start working.
type recvMsgCall struct {
	call    *rpc.Call
	ic      invokectx.InvokeCtx
	traceID string
}

type golangV1 struct {
	rpcService   *rpcservice.RPCService
	startupTimer *time.Timer

	startupTimeout time.Duration
	startupOnce    sync.Once
	serverAddress  string
	readyChan      chan struct{}

	maxResponseBodyLength int64
	invokeTimeout         time.Duration
	runtime.AbstractRuntime
	handlers runtime.Handlers

	invokeChan chan *recvMsgCall
}

// RegisterRuntime -
func RegisterRuntime() {
	runtime.Registry.Register(worker.GOV1RuntimeLanguage, &creator{})
}

// NewRuntime -
func (c *creator) NewRuntime(configuration *config.Configuration, handlers runtime.Handlers,
) (runtime.Runtime, error) {
	newAbstractRuntime, err := runtime.NewAbstractRuntime(configuration)
	if err != nil {
		return nil, fmt.Errorf("failed to new abstract runtime, %s", err.Error())
	}
	goConfig := configuration.RMConfig.RuntimeConfigs[worker.GOV1RuntimeLanguage]
	goRuntime := &golangV1{
		serverAddress:   fmt.Sprintf("%s:%d", worker.LocalServerIP, goConfig.Port),
		readyChan:       make(chan struct{}, 1),
		AbstractRuntime: *newAbstractRuntime,
		handlers:        handlers,
		invokeChan:      make(chan *recvMsgCall, invokeReqNotifyChNum),
	}
	goRuntime.rpcService = rpcservice.RenewRPCService(goRuntime.serverAddress)
	goRuntime.startupTimeout = time.Duration(configuration.Timeout) * time.Second

	if size, err := strconv.ParseInt(goConfig.Addition["maxResponseBodySize"],
		formatIntBase, formatInt64); err == nil {
		goRuntime.maxResponseBodyLength = size * megabyte
	}
	return goRuntime, nil
}

// ProcessCancelEvent -
func (g *golangV1) ProcessCancelEvent(message *runtime.CancelRequest) *runtime.CancelResponse {
	response := &runtime.CancelResponse{
		Error: &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"cancel is not supported"),
		},
	}
	return response
}

// HotLoad let the runtime load function
func (g *golangV1) HotLoad(ctx context.Context, configuration *config.Configuration) error {
	g.resetMaxBodySize(configuration)

	startLoadFunc := time.Now()
	go g.recvInvokeMsg(ctx)

	g.start(ctx)

	abstractRuntime, err := runtime.NewAbstractRuntime(configuration)
	if err != nil {
		return err
	}
	g.AbstractRuntime = *abstractRuntime
	g.invokeTimeout = time.Duration(g.Configuration.Timeout) * time.Second

	traceID := commUtils.UniqueID()
	log.GetLogger().Infof("golangV1 hotload, traceID: %s", traceID)

	summarySpan := runtime.NewSummarySpan(constants.LoadStage, traceID, g.Configuration, g.invokeTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	defer invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	logRecorder := g.FuncLogger.NewLogRecorder(ic, traceID, constants.LoadStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	if runtime.PodApplier == constants.WorkerManagerApplier {
		logRecorder.FinishHeader(ic)
		defer logRecorder.FinishFooter(ic)
	} else {
		defer logRecorder.FinishCopy(ic)
	}

	runtimeError := g.awaitReady(ctx)
	if runtimeError == nil {
		configuration.DurationInfo.LoadFuncDuration = time.Since(startLoadFunc)
	}

	summarySpan.Finish(runtimeError)

	// start extension
	if err := runtime.InitExtensionProcess(g.invokeTimeout, g.Configuration, g.FuncLogger); err != nil {
		return err
	}

	if runtimeError != nil {
		if runtime.PodApplier == constants.WorkerManagerApplier {
			runtime.ConResource.SetLoadSummary(summarySpan)
		}
		runtime.StartupDuration = summarySpan.SubDuration()
		return snerror.New(int(runtimeError.Code),
			startup.GenerateErrorString(logRecorder.MarshalAll(), summarySpan.SummaryInfo, runtimeError))
	}

	runtime.StartupDuration = summarySpan.SubDuration()
	runtime.ConResource.SetLoadSummary(summarySpan)
	return nil
}

func (g *golangV1) resetMaxBodySize(configuration *config.Configuration) {
	if configuration.MaxRequestBodySize != "" {
		if size, err := strconv.ParseInt(configuration.MaxRequestBodySize,
			formatIntBase, formatInt64); err == nil {
			g.maxResponseBodyLength = size * megabyte
			log.GetLogger().Infof("succeed to reset the max request body size: %d", g.maxResponseBodyLength)
		}
	}
}

// Healthz -
func (g *golangV1) Healthz(path string, timeout int) (bool, error) {
	return true, nil
}

// StartupHealthz return rpc server startup health
func (g *golangV1) StartupHealthz(path string, timeout int) (bool, error) {
	return true, nil
}

// ReportCustomMetrics -
func (g *golangV1) ReportCustomMetrics(timeout int) ([]byte, error) {
	return []byte{}, nil
}

func (g *golangV1) start(ctx context.Context) {
	g.startupTimer = time.NewTimer(g.startupTimeout)
	go g.checkReady(ctx)
}

func (g *golangV1) checkReady(ctx context.Context) {
	startRuntimeTimer := time.NewTimer(g.startupTimeout)
	defer startRuntimeTimer.Stop()
	var err error
	for {
		select {
		case <-startRuntimeTimer.C:
			if err != nil {
				log.GetLogger().Warnf("the go1.x runtime server started timeout, last error: %s", err.Error())
			}
		case <-ctx.Done():
			log.GetLogger().Errorf("the go1.x runtime server exited before ready")
			return
		default:
			rpcService := g.getRPCService()
			if rpcService == nil {
				continue
			}
			if err = rpcService.HealthCheck(); err == nil {
				close(g.readyChan)
				return
			}
			time.Sleep(checkReadyInterval)
		}
	}
}

func (g *golangV1) awaitReady(ctx context.Context) *runtime.Error {
	select {
	case <-g.startupTimer.C:
		log.GetLogger().Errorf("waiting go1.x runtime server ready timeout")
		return &runtime.Error{
			Code:    snerror.RuntimeInitTimeoutCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInitTimeoutCode), g.startupTimeout.String()),
		}
	case <-g.readyChan:
		log.GetLogger().Infof("the go1.x runtime server is ready")
		return nil
	case <-ctx.Done():
		log.GetLogger().Warnf("context canceled")
		if g.handlers.GetExceptionHandler != nil {
			if runtimeException := g.handlers.GetExceptionHandler(); runtimeException != nil {
				log.GetLogger().Warnf("runtime error: %v", runtimeException)
				return runtimeException
			}
		}
		return runtime.NewRuntimeError(snerror.RuntimeProcessExit, "")
	}
}

func (g *golangV1) getRPCService() *rpcservice.RPCService {
	return rpcservice.GetRPCService(g.serverAddress)
}

func (g *golangV1) doRequest(ic invokectx.InvokeCtx, request *runtime.InvokeRequest) {
	req := &rpcservice.InvokeRequest{
		Payload: request.Event,
		Header:  g.getFunctionHeader(request),
	}
	call := g.getRPCService().Invoke(req)

	g.invokeChan <- &recvMsgCall{
		call:    call,
		ic:      ic,
		traceID: request.TraceID,
	}
}

// recvInvokeMsg runs permanently until this runtime exits.
// It is used to start the goroutine for receiving the response after the invoke request is sent.
func (g *golangV1) recvInvokeMsg(c context.Context) {
	for {
		select {
		case <-c.Done():
			return
		case msgCall, ok := <-g.invokeChan:
			if !ok {
				log.GetLogger().Errorf("golang invokeChan closed")
				return
			}
			go func() {
				resp, err := g.processResp(msgCall.call, msgCall.traceID)
				g.recvInvoke(msgCall.ic, resp, err)
			}()
		}
	}
}

func (g *golangV1) processResp(call *rpc.Call, traceID string) ([]byte, *runtime.Error) {
	timeout := g.getInvokeTimer()
	resp := &rpcservice.InvokeResponse{}
	if err := rpcservice.WaitResp(timeout, resp, call); err != nil {
		log.GetLogger().Errorf("failed to request to go1.x runtime server, "+
			"error: %s, traceID: %s", err, traceID)
		if err.Code() == snerror.RuntimeInvokeTimeoutCode {
			return nil, &runtime.Error{
				Code:    int32(err.Code()),
				Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), g.invokeTimeout),
			}
		}
		if err.Code() == snerror.FunctionExceptionCode {
			return nil, &runtime.Error{
				Code:    int32(err.Code()),
				Message: err.Error(),
			}
		}
		if g.handlers.GetExceptionHandler != nil {
			if runtimeErr := g.handlers.GetExceptionHandler(); runtimeErr != nil {
				log.GetLogger().Warnf("runtime error: %v", runtimeErr)
				return nil, runtimeErr
			}
		}
		return nil, &runtime.Error{
			Code:    int32(err.Code()),
			Message: err.Error(),
		}
	}

	log.GetLogger().Infof("succeed to request to go1.x runtime server, status: %d, traceID: %s",
		resp.StatusCode, traceID)
	if int64(len(resp.Payload)) > g.maxResponseBodyLength {
		log.GetLogger().Errorf("The response payload is greater than maxResponseBodyLength: %d, "+
			", traceID: %s", g.maxResponseBodyLength, traceID)
		errorMessage := fmt.Sprintf(snerror.ErrText(snerror.IllegalArgumentErrorCode),
			len(resp.Payload), g.maxResponseBodyLength)
		return nil, &runtime.Error{
			Code:    snerror.IllegalArgumentErrorCode,
			Message: errorMessage,
		}
	}
	return resp.Payload, nil
}

func (g *golangV1) getFunctionHeader(request *runtime.InvokeRequest) http.Header {
	header := make(http.Header, headerSize)
	header.Set("X-CFF-Request-Id", request.TraceID)
	header.Set("X-CFF-Access-Key", request.Extra[constants.HeaderAccessKey])
	header.Set("X-CFF-Secret-key", request.Extra[constants.HeaderSecretKey])
	header.Set("X-CFF-Security-Access-Key", request.Extra[constants.HeaderSecurityAccessKey])
	header.Set("X-CFF-Security-Secret-Key", request.Extra[constants.HeaderSecuritySecretKey])
	header.Set("X-CFF-Workflow-Id", request.Extra[constants.HeaderWorkflowID])
	header.Set("X-CFF-Origin-Version-Tag", request.Extra[constants.HeaderOriginVersionTag])
	header.Set("X-CFF-Workflow-Run-Id", request.Extra[constants.HeaderWorkflowRunID])
	header.Set("X-CFF-Workflow-State-Id", request.Extra[constants.HeaderWorkflowStateID])
	header.Set("X-CFF-Auth-Token", request.Extra[constants.HeaderAuthToken])
	header.Set("X-CFF-Security-Token", request.Extra[constants.HeaderSecurityToken])
	header.Set("X-Stream-Enable", request.Extra[constants.HeaderStreamEnable])
	header.Set("X-Stream-Addr", request.Extra[constants.HeaderStreamAddr])
	header.Set("X-Stream-Run-Id", request.Extra[constants.HeaderStreamRunID])
	header.Set("X-Stream-Token", request.Extra[constants.HeaderStreamToken])
	return header
}

// SendProcessEvent process invoke event
func (g *golangV1) SendProcessEvent(ic invokectx.InvokeCtx, request runtime.InvokeRequest, conf *config.Configuration) {
	g.doRequest(ic, &request)
}

// recvInvoke is used to forward the received invoke response to bus.
func (g *golangV1) recvInvoke(ic invokectx.InvokeCtx, resp []byte, err *runtime.Error) {
	response := &runtime.InvokeResponse{}
	response.Result, response.Error = resp, err
	g.handlers.RecvProcessEventHandler(ic, *response)
}

func (g *golangV1) getInvokeTimer() time.Duration {
	timer := g.invokeTimeout
	if runtime.StartupDuration > 0 {
		g.startupOnce.Do(func() {
			timer = g.invokeTimeout - runtime.StartupDuration
		})
	}
	return timer
}

// RunProcess -
func RunProcess(uid, gid int, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
	deployPath, err := filepath.Abs(os.Getenv("DEPLOY_DIR"))
	if err != nil {
		log.GetLogger().Warnf("failed to get absolute path: %s", err.Error())
		return nil, err
	}
	bootstrap := filepath.Join(deployPath, conf.HandlerName)
	log.GetLogger().Infof("bootstrap path is %s", bootstrap)

	_, err = os.Stat(bootstrap)
	if err != nil && os.IsNotExist(err) {
		return nil, snerror.New(snerror.RuntimeBootstrapNotFound, snerror.ErrText(snerror.RuntimeBootstrapNotFound))
	}
	log.GetLogger().Infof("running go1.x runtime server: %v", bootstrap)

	cmd := generateCommand(bootstrap, conf, logReporter)
	if err = utils.SetCmdCredByID(cmd, uid, gid); err != nil {
		log.GetLogger().Errorf("failed to set cmd cred by id, %s", err.Error())
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		log.GetLogger().Errorf("failed to start the go1.x server, %s", err.Error())
		return nil, snerror.NewSNError(snerror.RuntimeProcessExit, err.Error())
	}

	return cmd.Process, nil
}

func generateCommand(bootstrap string, conf *config.Configuration, logReporter func([]byte)) *exec.Cmd {
	cmd := exec.Command(bootstrap)
	if env, err := functionenv.FetchEnvFromConfig(conf, &runtimemanagerrequest.RunProcessParams{}); err != nil {
		log.GetLogger().Errorf("failed to fetch environment from the configuration, error: %s",
			err.Error())
		cmd.Env = make([]string, 0, 0)
	} else {
		// the RuntimeEnvs is not include encrypted user environment variant
		// encrypted user environment variant need to get in context
		cmd.Env = env.RuntimeEnvs
		for idx := range cmd.Env {
			if strings.HasPrefix(cmd.Env[idx], worker.RuntimeEnvPrefix) {
				cmd.Env[idx] = cmd.Env[idx][len(worker.RuntimeEnvPrefix):]
			}
		}
		goXConfig := conf.RMConfig.RuntimeConfigs[worker.GOV1RuntimeLanguage]
		runtimeAPIADDR := fmt.Sprintf("%s:%d", worker.LocalServerIP, goXConfig.Port)
		cmd.Env = append(cmd.Env, envRuntimeAPIADDR+"="+runtimeAPIADDR)
		goConfig := conf.RMConfig.RuntimeConfigs[worker.GOV1RuntimeLanguage]
		if size, err := strconv.ParseInt(goConfig.Addition["maxResponseBodySize"],
			formatIntBase, formatInt64); err == nil {
			cmd.Env = append(cmd.Env, envRuntimeMaxResponseBodySize+"="+fmt.Sprintf("%d", size*megabyte))
		}
	}

	cmd.Dir = conf.RMConfig.RuntimeConfigs[worker.GOV1RuntimeLanguage].WorkerDir
	stdReader := functionlog.CreateSTDLogger()
	stdReader.RegisterLogCallback(logReporter)
	cmd.Stdout, cmd.Stderr = stdReader, stdReader
	return cmd
}

// PreStop preStop user function
func (g *golangV1) PreStop() error {
	return nil
}

// GetException get runtime exception error info
func (g *golangV1) GetException(shouldWait bool) *runtime.Error {
	return nil
}

// RequestUpdateAgency -
func (g *golangV1) RequestUpdateAgency() error {
	return nil
}

// NotifyRouterReady not implement
func (g *golangV1) NotifyRouterReady() error {
	return nil
}
