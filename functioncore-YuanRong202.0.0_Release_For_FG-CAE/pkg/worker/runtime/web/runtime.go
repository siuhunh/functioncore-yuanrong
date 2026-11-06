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

// Package web is web runtime
package web

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/stream"
	"functioncore/pkg/common/urnutils"
	commUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionenv"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/startup"
	"functioncore/pkg/worker/utils"
)

const (
	memoryGap                  = 5 // MB
	memoryFile                 = "memory.stat"
	memoryPath                 = "/runtime/memory"
	functionType               = "event"
	formatIntBase              = 10
	formatInt64                = 64
	megabyte                   = 1024 * 1024
	checkReadyInterval         = time.Duration(10) * time.Millisecond
	defaultInitTimeoutDuration = time.Duration(60) * time.Second
	processStatusCheckPeriod   = 500 * time.Millisecond
	readMemoryInternal         = 2 * time.Millisecond
	apigDecodeErrContent       = "illegal base64 data at input byte"
	apigUnmarshalErrContent    = "invalid character"
	healthCheckRouter          = "/health_check"
)

var (
	podName = os.Getenv(constants.PodName)
)

type creator struct {
}

// RegisterRuntime -
func RegisterRuntime() {
	runtime.Registry.Register(worker.WebRuntimeLanguage, &creator{})
}

// NewRuntime -
func (c *creator) NewRuntime(configuration *config.Configuration, handlers runtime.Handlers,
) (runtime.Runtime, error) {
	newAbstractRuntime, err := runtime.NewAbstractRuntime(configuration)
	if err != nil {
		return nil, fmt.Errorf("new abstract runtime error: %s", err.Error())
	}
	webConfig := configuration.RMConfig.RuntimeConfigs[worker.WebRuntimeLanguage]
	webRT := &web{
		port:            webConfig.Port,
		readyChan:       make(chan struct{}),
		oomChan:         make(chan struct{}),
		cancelChan:      make(chan struct{}),
		AbstractRuntime: *newAbstractRuntime,
		initTimeout:     getInitTimeoutDuration(configuration.RuntimeConfig.Initializer.InitializerTimeout),
		handlers:        handlers,
		customDiskMgr:   &diskusage.EvictionManager{},
		streamResponse:  configuration.StreamResponse,
	}
	if configuration.RuntimeConfig.Language == worker.CustomContainerLanguage {
		oom.StartOOMDetection()
		webRT.customDiskMgr, err = startMonitorCustomDisk(configuration)
		if err != nil {
			return nil, err
		}
	}

	if size, err := strconv.ParseInt(webConfig.Addition["maxResponseBodySize"],
		formatIntBase, formatInt64); err == nil {
		webRT.maxResponseBodyLength = size * megabyte
	}
	webRT.start()
	return runtime.FromLagacy(webRT, handlers), nil
}

func startMonitorCustomDisk(configuration *config.Configuration) (*diskusage.EvictionManager, error) {
	localMountPrefix, err := getLocalMountPrefix(configuration)
	if err != nil {
		return nil, err
	}
	var localMountPath []string
	for i := range configuration.RuntimeConfig.FuncMountConfig.FuncMounts {
		mountPath := configuration.RuntimeConfig.FuncMountConfig.FuncMounts[i].LocalMountPath
		mountPath = filepath.Join(localMountPrefix, mountPath)
		localMountPath = append(localMountPath, mountPath)
	}
	isUserDatasExpansion := configuration.RuntimeConfig.ResourceMetaData.EnableTmpExpansion
	customDiskMgr, err := diskusage.MonitorDiskOfCustomImage(configuration.RuntimeContainerID,
		localMountPath, isUserDatasExpansion, runtime.ProcessExitSignal, runtime.RunMetrics.UpdateStorageUsed)
	if err != nil {
		return nil, fmt.Errorf("monitor custom image disk, error: %s", err.Error())
	}
	return customDiskMgr, nil
}

func getLocalMountPrefix(configuration *config.Configuration) (string, error) {
	cli, err := engine.NewContainerClient()
	if err != nil {
		log.GetLogger().Errorf("failed to create container client: %s", err.Error())
		return "", err
	}
	defer cli.Close()
	containerInfo, err := cli.Info(context.TODO(), configuration.RuntimeContainerID)
	if err != nil {
		log.GetLogger().Errorf("failed to get container info, err: %s", err.Error())
		return "", err
	}
	return containerInfo.RootFsPath, nil
}

func getInitTimeoutDuration(timeout int64) time.Duration {
	if timeout > 0 {
		return time.Duration(timeout) * time.Second
	}
	return defaultInitTimeoutDuration
}

type web struct {
	logger         *RuntimeContainerLogger
	httpClient     *http.Client
	http2Client    *http.Client
	initTimer      *time.Timer
	startTime      time.Time
	serverInitTime time.Time

	initTimeout time.Duration
	once        sync.Once
	http2Once   sync.Once
	port        int32
	readyChan   chan struct{}
	oomChan     chan struct{}

	cancelChanOnce sync.Once
	cancelChan     chan struct{}
	subscribeOnce  sync.Once

	maxResponseBodyLength int64
	invokeTimeout         time.Duration
	runtime.AbstractRuntime
	handlers       runtime.Handlers
	customDiskMgr  *diskusage.EvictionManager
	streamResponse bool
}

// RunProcess run user web server process
func RunProcess(runProcessParams *runtimemanagerrequest.RunProcessParams, conf *config.Configuration,
	logReporter func([]byte)) (*os.Process, error) {
	uid := runProcessParams.UID
	gid := runProcessParams.GID

	deployPath, err := filepath.Abs(os.Getenv("DEPLOY_DIR"))
	if err != nil {
		log.GetLogger().Warnf("failed to get absolute path: %s", err.Error())
		return nil, err
	}
	bootstrap := deployPath + "/bootstrap"
	log.GetLogger().Infof("bootstrap path is %s", bootstrap)

	_, err = os.Stat(bootstrap)
	if err != nil && os.IsNotExist(err) {
		return nil, snerror.New(snerror.RuntimeBootstrapNotFound, snerror.ErrText(snerror.RuntimeBootstrapNotFound))
	}

	args := []string{"/bin/bash", bootstrap}
	log.GetLogger().Infof("running web runtime: %v", args)

	cmd := exec.Command(args[0], args[1:]...)
	if env, err := functionenv.FetchEnvFromConfig(conf, runProcessParams); err != nil {
		log.GetLogger().Errorf("failed to fetch environment from the configuration, error: %s",
			err.Error())
		cmd.Env = make([]string, 0, 0)
	} else {
		// the RuntimeEnvs is not include encrypted user environment variant
		// encrypted user environment variant need to get in context
		cmd.Env = env.RuntimeEnvs
		for idx := range cmd.Env {
			cmd.Env[idx] = strings.TrimPrefix(cmd.Env[idx], worker.RuntimeEnvPrefix)
		}
	}

	cmd.Dir = conf.RMConfig.RuntimeConfigs[worker.WebRuntimeLanguage].WorkerDir
	stdReader := functionlog.CreateSTDLogger()
	stdReader.RegisterLogCallback(logReporter)
	cmd.Stdout, cmd.Stderr = stdReader, stdReader

	if err = utils.SetCmdCredByID(cmd, uid, gid); err != nil {
		log.GetLogger().Errorf("set cmd cred by id error: %s", err)
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		log.GetLogger().Errorf("start the web server error: %s", err.Error())
		return nil, snerror.NewSNError(snerror.RuntimeProcessExit, err.Error())
	}

	return cmd.Process, nil
}

// ProcessCancelEvent -
func (w *web) ProcessCancelEvent(message *runtime.CancelRequest) *runtime.CancelResponse {
	if w.Configuration.Language == worker.CustomContainerLanguage {
		runtime.CloseExitChannel()
		w.cancelChanOnce.Do(func() { close(w.cancelChan) })
		return &runtime.CancelResponse{
			InvokeID: message.InvokeID,
		}
	}

	response := &runtime.CancelResponse{
		Error: &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"cancel is not supported"),
		},
	}
	return response
}

// HotLoad load runtime
func (w *web) HotLoad(ctx context.Context, configuration *config.Configuration) error {
	w.resetMaxBodySize(configuration)
	w.streamResponse = configuration.StreamResponse
	var runtimeError *runtime.Error
	if w.Configuration.RuntimeConfig.Language == worker.CustomContainerLanguage {
		w.subscribeOnce.Do(func() {
			go w.subscribeWorkerErr()
		})
		go w.watchingRuntimeMemoryUsedLoop(ctx)
	} else {
		if err := runtime.InitExtensionProcess(w.initTimeout, w.Configuration, w.FuncLogger); err != nil {
			return err
		}
	}
	abstractRuntime, err := runtime.NewAbstractRuntime(configuration)
	if err != nil {
		return err
	}
	w.AbstractRuntime = *abstractRuntime
	w.invokeTimeout = time.Duration(w.Configuration.Timeout) * time.Second

	traceID := commUtils.UniqueID()
	log.GetLogger().Infof("web hotload, traceID: %s", traceID)
	summarySpan := runtime.NewSummarySpan(constants.InitializeStage, traceID, w.Configuration, w.initTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	defer invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	logRecorder := w.FuncLogger.NewLogRecorder(ic, traceID, constants.InitializeStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	if runtime.PodApplier == constants.WorkerManagerApplier {
		if w.Configuration.RuntimeConfig.Language == worker.CustomContainerLanguage {
			logRecorder.FinishHeader(ic)
			defer logRecorder.FinishFooter(ic)
		} else {
			defer logRecorder.Finish(ic)
		}
	} else {
		defer func() {
			if w.Configuration.RuntimeConfig.Language == worker.CustomContainerLanguage && runtimeError != nil {
				logRecorder.Finish(ic)
				return
			}
			logRecorder.FinishCopy(ic)
		}()
	}

	if w.Configuration.RuntimeConfig.Language == worker.CustomContainerLanguage {
		w.logger = collectRuntimeContainerLog(configuration)
	}

	runtimeError = w.awaitReady(ctx)
	if runtimeError == nil &&
		w.Configuration.FunctionType == functionType && w.Configuration.Initializer.InitializerHandler != "" {
		runtimeError = w.processInitializer(ctx)
	}

	if w.Configuration.RuntimeConfig.Language == worker.CustomContainerLanguage {
		w.logger.syncTo(time.Now())
	}

	summarySpan.Finish(runtimeError)
	// record init error and response when requesting
	if runtimeError != nil {
		if runtime.PodApplier == constants.WorkerManagerApplier {
			runtime.ConResource.SetInitSummary(summarySpan)
		}
		runtime.InitializerDuration = summarySpan.SubDuration()
		return snerror.New(int(runtimeError.Code),
			startup.GenerateErrorString(logRecorder.MarshalAll(), summarySpan.SummaryInfo, runtimeError))
	}
	runtime.ConResource.SetInitSummary(summarySpan)
	runtime.InitializerDuration = summarySpan.SubDuration()
	return nil
}

func (w *web) resetMaxBodySize(configuration *config.Configuration) {
	if configuration.MaxRequestBodySize != "" {
		if size, err := strconv.ParseInt(configuration.MaxRequestBodySize,
			formatIntBase, formatInt64); err == nil {
			w.maxResponseBodyLength = size * megabyte
			log.GetLogger().Infof("succeed to reset the max request body size: %d", w.maxResponseBodyLength)
		}
	}
}

// subscribeWorkerErr -
func (w *web) subscribeWorkerErr() {
	for {
		log.GetLogger().Infof("wait for worker err")
		select {
		case errMsg, ok := <-runtime.GetWorkerErrChan():
			if !ok {
				log.GetLogger().Errorf("workerErr channel is closed")
				return
			}
			log.GetLogger().Warnf("receive WorkerErr")
			runtime.ProcessExitError = snerror.New(int(errMsg.Code), errMsg.Message)
			runtime.CloseExitChannel()
		case <-runtime.ProcessExitSignal:
			log.GetLogger().Infof("stop subscribeWorkerErr")
			return
		}
	}
}

// processInitializer process event of initializer
func (w *web) processInitializer(ctx context.Context) *runtime.Error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan *runtime.Error, 1)
	go func() {
		errCh <- w.doInitializeRequest(ctx)
	}()

	select {
	case err := <-errCh:
		return err
	case <-w.oomChan:
		log.GetLogger().Warnf("oom in process initializer")
		return &runtime.Error{
			Code:    snerror.RuntimeMemoryExceedLimit,
			Message: snerror.ErrText(snerror.RuntimeMemoryExceedLimit),
		}
	case <-w.cancelChan:
		log.GetLogger().Warnf("canceled in process initializer")
		return &runtime.Error{
			Code:    snerror.RequestCanceled,
			Message: snerror.ErrText(snerror.RequestCanceled),
		}
	}
}

func (w *web) doInitializeRequest(ctx context.Context) *runtime.Error {
	requestBody := []byte("{}")
	baseURLPath := fmt.Sprintf("http://127.0.0.1:%d", w.port)
	requestURI := baseURLPath + "/init"
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURI, bytes.NewBuffer(requestBody))
	if err != nil {
		log.GetLogger().Errorf("failed to construct HTTP request, error: %s", err.Error())
		return runtime.NewRuntimeError(snerror.FunctionExceptionCode, fmt.Sprintf("failed to construct HTTP "+
			"request, error: %s", err.Error()))
	}
	queries := httpRequest.URL.Query()
	httpRequest.URL.RawQuery = queries.Encode()
	httpRequest.Header.Add("Content-Type", "application/json")
	w.addFunctionHeader("", httpRequest.Header, w.Configuration.Initializer.Extra)
	w.serverInitTime = time.Now()
	_, err = w.getHTTPClient("init").Do(httpRequest)
	if err != nil {
		log.GetLogger().Errorf("failed to request to user event server, error: %s", err.Error())
		if strings.Contains(err.Error(), "context deadline exceeded") {
			return &runtime.Error{
				Code:    snerror.RuntimeInitTimeoutCode,
				Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInitTimeoutCode), w.initTimeout.String()),
			}
		}
		return runtime.NewRuntimeError(snerror.FunctionExceptionCode, "failed to request to user event server")
	}
	return nil
}

func (w *web) doHealthCheckRequest(path string, timeout int) (*http.Response, error) {
	resp := &http.Response{}
	baseURLPath := fmt.Sprintf("http://127.0.0.1:%d", w.port)
	requestURI := baseURLPath + path
	httpRequest, err := http.NewRequest(http.MethodGet, requestURI, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to construct HTTP request, error: %s", err.Error())
		return nil, err
	}
	queries := httpRequest.URL.Query()
	httpRequest.URL.RawQuery = queries.Encode()
	httpRequest.Header.Add("Content-Type", "application/json")
	httpClient := w.getHTTPClient(healthCheckRouter)
	httpClient.Timeout = time.Duration(timeout) * time.Second
	resp, err = httpClient.Do(httpRequest)
	if err != nil {
		log.GetLogger().Errorf("failed to check health of http runtime, error: %s", err.Error())
		return resp, err
	}
	return resp, nil
}

// Healthz -
func (w *web) Healthz(path string, timeout int) (bool, error) {
	response, err := w.doHealthCheckRequest(path, timeout)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()

	if !(response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusBadRequest) {
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.GetLogger().Errorf("failed to parse health check response body")
			return false, err
		}
		return false, errors.New(string(body))
	}
	log.GetLogger().Debugf("succeed to check health of runtime")
	return true, nil
}

// StartupHealthz return rpc server startup health
func (w *web) StartupHealthz(path string, timeout int) (bool, error) {
	return true, nil
}

// ReportCustomMetrics -
func (w *web) ReportCustomMetrics(timeout int) ([]byte, error) {
	return []byte{}, nil
}

func (w *web) checkReady() {
	checkTimes := w.initTimeout / checkReadyInterval
	for checkTimes >= 0 {
		conn, err := net.Dial("tcp", "127.0.0.1:"+strconv.Itoa(int(w.port)))
		if err == nil {
			err = conn.Close()
			if err != nil {
				log.GetLogger().Warnf("failed to close connection for checking, err: %s", err.Error())
			}
			break
		}
		if !commUtils.IsConnRefusedErr(err) {
			break
		}
		time.Sleep(checkReadyInterval)
		checkTimes = checkTimes - 1
	}
	if checkTimes >= 0 {
		close(w.readyChan)
	}
}

func (w *web) start() {
	go w.checkReady()
}

func (w *web) awaitReady(ctx context.Context) *runtime.Error {
	w.initTimer = time.NewTimer(w.initTimeout)
	w.startTime = time.Now()
	select {
	case <-w.initTimer.C:
		log.GetLogger().Errorf("wait web server ready timeout")
		return &runtime.Error{
			Code:    snerror.RuntimeInitTimeoutCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInitTimeoutCode), w.initTimeout.String()),
		}
	case <-w.readyChan:
		log.GetLogger().Infof("web server is ready")
		return nil
	case <-ctx.Done():
		log.GetLogger().Warnf("context canceled")
		if w.customDiskMgr != nil && w.customDiskMgr.LimitFlag {
			log.GetLogger().Warnf("The disk usage exceeds the threshold, runtime will be killed")
			return runtime.NewRuntimeError(snerror.RuntimeProcessExit, snerror.ErrText(snerror.DiskUsageExceed))
		}
		if w.handlers.GetExceptionHandler != nil {
			if runtimeException := w.handlers.GetExceptionHandler(); runtimeException != nil {
				log.GetLogger().Warnf("runtime error: %v", runtimeException)
				return runtimeException
			}
		}
		return runtime.NewRuntimeError(snerror.RuntimeProcessExit, "")
	case <-w.oomChan:
		log.GetLogger().Warnf("oom in await ready")
		return &runtime.Error{
			Code:    snerror.RuntimeMemoryExceedLimit,
			Message: snerror.ErrText(snerror.RuntimeMemoryExceedLimit),
		}
	case <-w.cancelChan:
		log.GetLogger().Warnf("canceled in await ready")
		return &runtime.Error{
			Code:    snerror.RequestCanceled,
			Message: snerror.ErrText(snerror.RequestCanceled),
		}
	}
}

func (w *web) getHTTP2Client() *http.Client {
	w.http2Once.Do(func() {
		w.http2Client = &http.Client{
			// Skip TLS dial
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			},
		}
	})
	w.http2Client.Timeout = w.invokeTimeout
	return w.http2Client
}

func (w *web) getHTTPClient(router string) *http.Client {
	w.once.Do(func() {
		defaultTransport := http.DefaultTransport
		tr, ok := defaultTransport.(*http.Transport)
		if !ok {
			log.GetLogger().Warnf("failed to assert")
			tr = &http.Transport{}
		}
		// The server can close a previous "keep-alive" TCP connection (mainly because of a read timeout) at any time.
		// If we send a request while the connection is closing, the HTTP client will return an EOF error. As a
		// result, we disable keep-alive completely.
		tr.DisableKeepAlives = true
		w.httpClient = &http.Client{
			Transport: tr,
		}
	})
	if router == "init" {
		w.httpClient.Timeout = w.initTimeout - w.serverInitTime.Sub(w.startTime)
		return w.httpClient
	}
	w.httpClient.Timeout = w.invokeTimeout
	// Turn off redirects
	if router == "invoke" {
		w.httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	return w.httpClient
}

func (w *web) addFunctionHeader(requestID string, header http.Header, extra map[string]string) {
	funcURN := &urnutils.BaseURN{}
	if err := funcURN.ParseFrom(w.Configuration.URN); err != nil {
		log.GetLogger().Warnf("failed to parse function urn, requestID:", requestID)
		return
	}

	header.Set("X-CFF-Request-Id", requestID)
	header.Set("X-CFF-Memory", strconv.FormatInt(w.Configuration.ResourceMetaData.Mem, formatIntBase))
	header.Set("X-CFF-Timeout", strconv.FormatInt(w.Configuration.Timeout, formatIntBase))
	header.Set("X-CFF-Func-Version", funcURN.Version)
	header.Set("X-CFF-Func-Name", funcURN.Name)
	header.Set("X-CFF-Project-Id", funcURN.ProductID)
	serviceID, _, _ := urnutils.GetFunctionNameAndServiceName(funcURN.Name)
	header.Set("X-CFF-Package", serviceID)
	header.Set("X-CFF-Region", funcURN.RegionID)
	if v, ok := extra[constants.HeaderWorkflowID]; ok {
		header.Set("X-CFF-Workflow-Id", v)
	}
	if v, ok := extra[constants.HeaderOriginVersionTag]; ok {
		header.Set("X-CFF-Origin-Version-Tag", v)
	}
	if v, ok := extra[constants.HeaderWorkflowRunID]; ok {
		header.Set("X-CFF-Workflow-Run-Id", v)
	}
	if v, ok := extra[constants.HeaderWorkflowStateID]; ok {
		header.Set("X-CFF-Workflow-State-Id", v)
	}
	if !w.Configuration.EnableAuthInHeader {
		return
	}

	if v, ok := extra[constants.HeaderAccessKey]; ok {
		header.Set("X-CFF-Access-Key", v)
	}
	if v, ok := extra[constants.HeaderSecretKey]; ok {
		header.Set("X-CFF-Secret-Key", v)
	}
	if v, ok := extra[constants.HeaderSecurityAccessKey]; ok {
		header.Set("X-CFF-Security-Access-Key", v)
	}
	if v, ok := extra[constants.HeaderSecuritySecretKey]; ok {
		header.Set("X-CFF-Security-Secret-Key", v)
	}
	if v, ok := extra[constants.HeaderAuthToken]; ok {
		header.Set("X-CFF-Auth-Token", v)
	}
	if v, ok := extra[constants.HeaderSecurityToken]; ok {
		header.Set("X-CFF-Security-Token", v)
	}
}

func (w *web) readResponseBody(response *http.Response) (*bytes.Buffer, *runtime.Error) {
	isStream := false
	// support stream truncated, read maxResponseBodyLength at most
	if response.Header.Get("Transfer-Encoding") == "chunked" {
		isStream = true
	}

	const base = 1024
	body := &bytes.Buffer{}
	buf := make([]byte, base)
	defer response.Body.Close()
	for {
		readLength, err := response.Body.Read(buf)
		if err != nil && err != io.EOF {
			log.GetLogger().Errorf("failed to read response body, error: %s", err.Error())
			if strings.Contains(err.Error(), "context deadline exceeded") {
				return nil, &runtime.Error{
					Code: snerror.RuntimeInvokeTimeoutCode,
					Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode),
						w.invokeTimeout.String()),
				}
			}
			return nil, &runtime.Error{
				Code:    snerror.WorkerInternalErrorCode,
				Message: fmt.Sprintf("failed to read http response body: %s", err.Error()),
			}
		}
		if readLength > 0 {
			body.Write(buf[:readLength])
		} else {
			break
		}
		if int64(body.Len()) > w.maxResponseBodyLength {
			if !isStream {
				errorMessage :=
					fmt.Sprintf(snerror.ErrText(snerror.IllegalArgumentErrorCode), body.Len(), w.maxResponseBodyLength)
				return nil, &runtime.Error{
					Code:    snerror.IllegalArgumentErrorCode,
					Message: errorMessage,
				}
			}
			break
		}
	}
	return body, nil
}

func constructHTTPRequest(ctx context.Context, baseURLPath string, event []byte,
) (*http.Request, *runtime.Error) {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return nil, &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode), "failed to get config"),
		}
	}

	var httpRequest *http.Request
	isUnMarshalErr := false
	if conf.FunctionType == functionType {
		httpRequest, err = eventToHTTPRequest(ctx, baseURLPath, event)
	} else {
		httpRequest, isUnMarshalErr, err = parseAPIGEvent(ctx, event, baseURLPath)
	}
	if err != nil {
		// The APIG request body is a non-Base64-encoded character string. An error is returned to the tenant.
		if strings.Contains(err.Error(), apigDecodeErrContent) ||
			strings.Contains(err.Error(), apigUnmarshalErrContent) || isUnMarshalErr {
			return nil, &runtime.Error{
				Code:    snerror.FunctionExceptionCode,
				Message: fmt.Sprintf(snerror.ErrText(snerror.FunctionExceptionCode) + ", error: " + err.Error()),
			}
		}
		return nil, &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"parse APIG event error: "+err.Error()),
		}
	}
	return httpRequest, nil
}

func constructHTTPResponse(resp *http.Response, body *bytes.Buffer) ([]byte, *runtime.Error) {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return nil, &runtime.Error{
			Code:    snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode), "failed to get config"),
		}
	}

	if conf.FunctionType == functionType {
		return body.Bytes(), nil
	}

	b, err := constructAPIGResponse(resp, body)
	if err != nil {
		log.GetLogger().Errorf("failed to construct apig response: %s", err.Error())
		return nil, &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"construct APIG response error: "+err.Error()),
		}
	}
	return b, nil
}

func (w *web) doHTTPRequest(ctx context.Context, requestID string, event []byte,
	extra map[string]string) ([]byte, *runtime.Error) {
	baseURLPath := fmt.Sprintf("http://127.0.0.1:%d", w.port)
	log.GetLogger().Debugf("prepare to request base url: %s", baseURLPath)
	httpRequest, runtimeErr := constructHTTPRequest(ctx, baseURLPath, event)
	if runtimeErr != nil {
		return nil, runtimeErr
	}
	w.addFunctionHeader(requestID, httpRequest.Header, extra)
	client := w.getHTTPClient("invoke")
	if httpRequest.Proto == "HTTP/2" {
		client = w.getHTTP2Client()
	}
	resp, err := client.Do(httpRequest)
	if err != nil {
		return w.processRuntimeError(err, requestID)
	}

	log.GetLogger().Infof("reqUserWebServer | %d | %s | %s", resp.StatusCode, resp.Header.Get("content-type"),
		requestID)
	if w.streamResponse {
		return processStreamResponse(httpRequest.Context(), resp, extra)
	}
	body, runtimeErr := w.readResponseBody(resp)
	if runtimeErr != nil {
		return nil, runtimeErr
	}
	return constructHTTPResponse(resp, body)
}

func (w *web) processRuntimeError(err error, requestID string) ([]byte, *runtime.Error) {
	log.GetLogger().Errorf("failed to request to user web server, error: %s, requestID: %s", err, requestID)
	if strings.Contains(err.Error(), "broken pipe") || strings.Contains(err.Error(), "connection reset by peer") {
		return nil, runtime.NewRuntimeError(snerror.FunctionExceptionCode, err.Error())
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		if utils.CheckInstanceTimeoutExitWhiteList() {
			log.GetLogger().Warnf("check instance timeout exit for tenant: %s", utils.LocalFuncURN.TenantID)
			runtime.CloseExitChannel()
		}
		return nil, &runtime.Error{
			Code:    snerror.RuntimeInvokeTimeoutCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), w.invokeTimeout.String()),
		}
	}
	if w.Configuration.Language == worker.CustomContainerLanguage && oom.IsRuntimeOOMKilled() {
		runtime.CloseExitChannel()
		return nil, &runtime.Error{
			Code:    snerror.RuntimeMemoryExceedLimit,
			Message: snerror.ErrText(snerror.RuntimeMemoryExceedLimit),
		}
	}
	if w.handlers.GetExceptionHandler != nil {
		if runtimeErr := w.handlers.GetExceptionHandler(); runtimeErr != nil {
			log.GetLogger().Errorf("invoke runtime error: %v", runtimeErr)
			return nil, runtimeErr
		}
	}
	if w.customDiskMgr != nil && w.customDiskMgr.LimitFlag {
		log.GetLogger().Error("The disk usage exceeds the threshold, runtime will be killed")
		return nil, runtime.NewRuntimeError(snerror.DiskUsageExceed, snerror.ErrText(snerror.DiskUsageExceed))
	}
	return nil, runtime.NewRuntimeError(snerror.FunctionExceptionCode, err.Error())
}

func processStreamResponse(ctx context.Context,
	response *http.Response, extra map[string]string) ([]byte, *runtime.Error) {
	dialCtx, cancel := context.WithTimeout(context.Background(), stream.DialTimeout)
	defer cancel()
	option := &stream.ClientOption{
		Token:             extra[constants.HeaderStreamToken],
		ProjectID:         utils.LocalFuncURN.TenantID,
		RequestID:         extra[constants.HeaderStreamRunID],
		StreamHandlerAddr: extra[constants.HeaderStreamAddr],
		StreamTransPort:   constants.StreamTransPort,
		DialCtx:           dialCtx,
		CliCtx:            ctx,
	}
	if utils.GetIsMcpFunction() && response != nil {
		response.Header.Set(constants.HeaderSSEPodName, podName)
	}
	code, msg := stream.SendWebStream(option, response)
	if code != 0 {
		return nil, &runtime.Error{
			Code:    code,
			Message: msg,
		}
	}
	return nil, nil
}

// eventToHTTPRequest construct a request of event invoke
func eventToHTTPRequest(ctx context.Context, baseURLPath string, body []byte) (*http.Request, error) {
	requestURI := baseURLPath + "/invoke"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURI, bytes.NewBuffer(body))
	if err != nil {
		log.GetLogger().Errorf("failed to construct HTTP request, error: %s", err.Error())
		return nil, err
	}
	queries := request.URL.Query()
	request.URL.RawQuery = queries.Encode()
	request.Header.Add("Content-Type", "application/json")
	return request, nil
}

// ProcessEvent process invoke event
func (w *web) ProcessEvent(ic invokectx.InvokeCtx, request *runtime.InvokeRequest) *runtime.InvokeResponse {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	respCh := make(chan *runtime.InvokeResponse, 1)
	if !ic.IsInnerCtxEmpty() {
		invokectx.GetTimeTraceContext(ic).Start(worker.TimeTraceRuntime, time.Now())
	}
	go func() {
		response := &runtime.InvokeResponse{}
		response.Result, response.Error = w.doHTTPRequest(ctx, request.TraceID, request.Event, request.Extra)
		respCh <- response
	}()

	select {
	case resp := <-respCh:
		if w.Configuration.RuntimeConfig.Language == worker.CustomContainerLanguage {
			w.logger.syncTo(time.Now())
			log.GetLogger().Infof("syncLog | %s", request.TraceID)
		}
		return resp
	case <-w.oomChan:
		log.GetLogger().Warnf("oom in process event")
		return &runtime.InvokeResponse{
			Error: &runtime.Error{
				Code:    snerror.RuntimeMemoryExceedLimit,
				Message: snerror.ErrText(snerror.RuntimeMemoryExceedLimit),
			},
		}
	case <-w.cancelChan:
		log.GetLogger().Warnf("canceled in process event")
		return &runtime.InvokeResponse{
			Error: &runtime.Error{
				Code:    snerror.RequestCanceled,
				Message: snerror.ErrText(snerror.RequestCanceled),
			},
		}
	}
}

func (w *web) watchingRuntimeMemoryUsedLoop(ctx context.Context) {
	lastMemoryUsed := 0.0
	_, podMemory := utils.GetCPUMemory()
	limit := float64(podMemory - memoryGap)
	path := filepath.Join(memoryPath, w.Configuration.RuntimeContainerID, memoryFile)
	parser, err := monitor.NewCGroupMemoryParserWithPath(path)
	if err != nil {
		log.GetLogger().Warnf("failed to create cgroup memory parser: %s", err.Error())
		return
	}
	defer parser.Close()
	for {
		select {
		case <-ctx.Done():
			log.GetLogger().Warnf("context canceled")
			return
		default:
			flag := w.checkRuntimeMemory(&lastMemoryUsed, w.Configuration.RuntimeContainerID, limit, parser)
			if flag {
				runtime.CloseExitChannel()
				log.GetLogger().Warnf("the runtime exited")
				return
			}
			time.Sleep(readMemoryInternal)
		}
	}
}

func (w *web) checkRuntimeMemory(lastMemoryUsed *float64, runtimeContainerID string, limit float64,
	parser *monitor.Parser) bool {
	memoryUsedBytes, err := parser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to parse memory used, err: %s", err.Error())
		return false
	}
	memoryVal, ok := memoryUsedBytes.(uint64)
	if !ok {
		log.GetLogger().Warnf("failed to assert memory used, memoryUsedBytes: %+v", memoryUsedBytes)
		return false
	}
	tmpMemoryUsed := float64(memoryVal) / megabyte
	if tmpMemoryUsed != *lastMemoryUsed {
		runtime.RunMetrics.UpdateMemoryUsed(tmpMemoryUsed)
	}
	*lastMemoryUsed = tmpMemoryUsed
	if tmpMemoryUsed > limit {
		close(w.oomChan)
		return true
	}
	return false
}

func (w *web) PreStop() error {
	return nil
}

// GetException get runtime exception error info
func (w *web) GetException(shouldWait bool) *runtime.Error {
	return nil
}
func (w *web) RequestUpdateAgency() error {
	return nil
}
