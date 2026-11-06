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

// Package custom is custom runtime
package custom

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
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
	customConfig "functioncore/pkg/worker/runtime/custom/config"
	"functioncore/pkg/worker/runtime/rpc/pb"
	"functioncore/pkg/worker/startup"
	"functioncore/pkg/worker/utils"
)

const (
	loadTimeout = 30 * time.Second

	base = 10

	funcNameLen     = 3
	servicePosition = 1

	envMapSize  = 10
	decimalBase = 10

	processStatusCheckPeriod = 500 * time.Millisecond

	chanBufferCapacity   = 1
	invokeReqNotifyChNum = 100
)

var once sync.Once

type creator struct {
}

// recvMsgCall is used to notify the goroutine that receives the invoke request to start working.
type recvMsgCall struct {
	rspChan  chan *runtime.InvokeResponse
	ic       *invokectx.InvokeCtx
	traceID  string
	fromTask bool
}

// RegisterRuntime -
func RegisterRuntime() {
	runtime.Registry.Register("custom", &creator{})
}

// NewRuntime -
func (c *creator) NewRuntime(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
	newAbstractRuntime, err := runtime.NewAbstractRuntime(conf)
	if err != nil {
		return nil, fmt.Errorf("new abstract runtime error: %s", err.Error())
	}
	customRuntime := &Custom{
		AbstractRuntime: *newAbstractRuntime,
		stdReader:       functionlog.CreateSTDLogger(),
		handlers:        handlers,
		invokeChan:      make(chan *recvMsgCall, invokeReqNotifyChNum),
	}

	address := getServerAddress(conf.RMConfig.RuntimeConfigs[worker.CustomRuntimeLanguage].Port)
	customRuntime.server, err = getServer(address)
	if err != nil {
		log.GetLogger().Errorf("new custom runtime server error %s", err.Error())
		return nil, err
	}

	// custom runtime always new a process when load actually
	if customRuntime.Process, err = customRuntime.RunWrapper(); err != nil {
		return nil, err
	}

	go customRuntime.InspectProcess()
	return customRuntime, nil
}

// Custom implement Runtime interface
type Custom struct {
	*server
	Process   *os.Process
	stdReader *functionlog.STDLogger
	runtime.AbstractRuntime

	initTimer     *time.Timer
	InvokeTimeout time.Duration

	handlers   runtime.Handlers
	invokeChan chan *recvMsgCall
}

// InitTimer init custom runtime timer
func (c *Custom) InitTimer() {
	// Enable runtime initialization countdown
	c.initTimer = time.NewTimer(loadTimeout)
}

// InitTimerByInvokeTimeout -
func (c *Custom) InitTimerByInvokeTimeout() {
	// Enable runtime initialization countdown
	c.initTimer = time.NewTimer(c.InvokeTimeout)
}

// EnableStdLogCollection collect std log and call back
func (c *Custom) EnableStdLogCollection(cb func([]byte)) {
	if c.stdReader == nil {
		c.stdReader = functionlog.CreateSTDLogger()
	}
	c.stdReader.RegisterLogCallback(cb)
}

// RunWrapperByID run wrapper by uid and gid
func (c *Custom) RunWrapperByID(uid, gid int) (*os.Process, error) {
	deployPath, err := filepath.Abs(os.Getenv("DEPLOY_DIR"))
	if err != nil {
		log.GetLogger().Warnf("failed to get absolute path: %s", err.Error())
		return nil, err
	}
	deployFuncPath := deployPath
	cmd, err := c.buildCmd(deployFuncPath)
	if err != nil {
		log.GetLogger().Errorf("build cmd error: %s", err)
		return nil, err
	}
	if err = utils.SetCmdCredByID(cmd, uid, gid); err != nil {
		log.GetLogger().Errorf("set cmd cred by id error: %s", err)
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		log.GetLogger().Errorf("started the custom runtime error: %s", err.Error())
		return nil, snerror.NewSNError(snerror.RuntimeProcessExit, err.Error())
	}
	c.InitTimer()

	return cmd.Process, nil
}

// RunWrapper -
func (c *Custom) RunWrapper() (*os.Process, error) {
	deployPath, err := filepath.Abs(c.Configuration.DeployDir)
	if err != nil {
		log.GetLogger().Warnf("failed to get absolute path: %s", err.Error())
		return nil, err
	}
	deployFuncPath := deployPath + "/layer/func"
	cmd, err := c.buildCmd(deployFuncPath)
	if err != nil {
		log.GetLogger().Errorf("build cmd error: %s", err)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		log.GetLogger().Errorf("started the custom runtime error: %s", err.Error())
	}
	c.InitTimer()

	return cmd.Process, nil
}

func (c *Custom) buildCmd(deployFuncPath string) (*exec.Cmd, error) {

	bootstrap := deployFuncPath + "/bootstrap"
	log.GetLogger().Infof("bootstrap path is %s", bootstrap)

	_, err := os.Stat(bootstrap)
	if err != nil && os.IsNotExist(err) {
		return nil, snerror.New(snerror.RuntimeBootstrapNotFound, snerror.ErrText(snerror.RuntimeBootstrapNotFound))
	}

	args := []string{bootstrap}
	log.GetLogger().Infof("running custom runtime: %v", args)
	env, err := functionenv.FetchEnvFromConfigWithHandlerErr(c.Configuration)
	if err != nil {
		log.GetLogger().Warnf("failed to fetch environment from the configuration, error: %s", err.Error())
	}
	c.FuncLogger.SetLogLevel(c.Configuration.FuncLogLevel)

	env.RuntimeEnvs = c.appendEnv(env.RuntimeEnvs)
	env.RuntimeEnvs = c.appendCustomRuntimeEnv(env.RuntimeEnvs)
	// the RuntimeEnvs is not include encrypted user environment variant
	// encrypted user environment variant need to get in context
	cmd := buildRunCmd(args, env.RuntimeEnvs,
		c.Configuration.RMConfig.RuntimeConfigs[worker.CustomRuntimeLanguage].WorkerDir)
	cmd.Stdout = c.stdReader
	cmd.Stderr = c.stdReader

	return cmd, nil
}

func (c *Custom) appendEnv(env []string) []string {

	env = append(env, fmt.Sprintf("PATH=%s", fmt.Sprintf("%s:%s",
		validEnvValue("PATH"), c.Configuration.DeployDir)))
	// Contain java class path
	env = append(env, fmt.Sprintf("JAVA_HOME=%s", validEnvValue("JAVA_HOME")))
	env = append(env, fmt.Sprintf("CLASSPATH=%s", fmt.Sprintf("%s:%s",
		validEnvValue("CLASSPATH"), c.Configuration.DeployDir)))
	// Contain python load path
	env = append(env, fmt.Sprintf("PYTHONPATH=%s", fmt.Sprintf("%s:%s",
		validEnvValue("PYTHONPATH"), c.Configuration.DeployDir)))

	env = append(env, fmt.Sprintf("SN_RUNTIME_LOG_DIR=%s", worker.RuntimeLogPath))
	env = append(env, fmt.Sprintf("_HANDLER=%s", c.Configuration.HandlerName))

	return env
}

func validEnvValue(envKey string) string {
	envValue := os.Getenv(envKey)
	if err := commUtils.ValidEnvValuePath(envValue); err != nil {
		log.GetLogger().Warnf("failed to valid env %s, err: %s", envKey, err.Error())
		envValue = ""
	}
	return envValue
}

func (c *Custom) appendCustomRuntimeEnv(envs []string) []string {
	serverConfig, err := customConfig.GetServerConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get serverConfig, err: %s", err.Error())
		return envs
	}
	envMap := make(map[string]string, envMapSize)
	runConfig := c.Configuration.RuntimeConfig
	funcURN := &urnutils.BaseURN{}
	envKeys := serverConfig.ServerEnvKey
	if err := funcURN.ParseFrom(c.Configuration.URN); err == nil {
		envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeProject, funcURN.ProductID)
		envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeFuncName, funcURN.Name)
		envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeFuncVersion, funcURN.Version)
		envMap = setMapValIfKeyExist(envMap, envKeys.RuntimePackage, getServiceID(funcURN.Name))
	}
	envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeHandler, runConfig.HandlerName)
	envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeTime, strconv.FormatInt(runConfig.Timeout, decimalBase))
	if runConfig.ResourceMetaData.CPU != 0 && envKeys.RuntimeCPU != "" {
		envMap[envKeys.RuntimeCPU] = strconv.FormatInt(runConfig.ResourceMetaData.CPU, base)
	}
	if runConfig.ResourceMetaData.Mem != 0 && envKeys.RuntimeMemory != "" {
		envMap[envKeys.RuntimeMemory] = strconv.FormatInt(runConfig.ResourceMetaData.Mem, base)
	}
	envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeCodeRoot, c.Configuration.DeployDir)
	localServerPort := c.Configuration.RMConfig.RuntimeConfigs[worker.CustomRuntimeLanguage].Port
	envMap = setMapValIfKeyExist(envMap, envKeys.RuntimeApiAddr,
		fmt.Sprintf("%s:%d", worker.LocalServerIP, localServerPort))
	for envName, envValue := range envMap {
		envs = append(envs, fmt.Sprintf("%s=%s", envName, envValue))
		log.GetLogger().Debugf("%s=%s", envName, envValue)
	}
	return envs
}

func setMapValIfKeyExist(envMap map[string]string, key string, value string) map[string]string {
	if envMap == nil {
		return envMap
	}
	if key == "" {
		return envMap
	}
	envMap[key] = value
	return envMap
}

func getServiceID(funcName string) string {
	if !strings.Contains(funcName, urnutils.Separator) {
		return ""
	}
	items := strings.SplitN(funcName, urnutils.Separator, funcNameLen)
	if len(items) != funcNameLen {
		return ""
	}
	return items[servicePosition]
}

// InspectProcess inspect the custom runtime process
func (c *Custom) InspectProcess() {
	if c.Process == nil {
		log.GetLogger().Errorf("custom runtime Process nil")
		close(runtime.ProcessExitSignal)
		return
	}
	state, err := c.Process.Wait()
	if err != nil {
		log.GetLogger().Warnf("failed to inspect custom process, error: %s", err.Error())
	}
	if state != nil {
		log.GetLogger().Infof("custom process status: %s, exit code: %d", state.String(), state.ExitCode())
	}
	log.GetLogger().Infof("finished inspecting the custom process")
	close(runtime.ProcessExitSignal)
}

// HotLoad load custom runtime
func (c *Custom) HotLoad(ctx context.Context, conf *config.Configuration) error {
	traceID := commUtils.UniqueID()
	log.GetLogger().Infof("custom hotload, traceID: %s", traceID)
	if err := runtime.InitExtensionProcess(c.InvokeTimeout, c.Configuration, c.FuncLogger); err != nil {
		return err
	}
	if conf.MaxRequestBodySize != "" {
		num, err := strconv.ParseFloat(conf.MaxRequestBodySize, bitSize)
		if err != nil {
			log.GetLogger().Warnf("failed to strconv maxRequestBodySize %s", conf.MaxRequestBodySize)
		} else {
			c.maxRequestBodySize = int(num * megabytes)
			log.GetLogger().Infof("succeed to reset the max request body size: %d", c.maxRequestBodySize)
		}
	}
	if c.server != nil {
		c.streamResponse = conf.StreamResponse
	}
	go c.recvInvokeMsg(ctx)

	c.InvokeTimeout = time.Duration(conf.Timeout) * time.Second
	summarySpan := runtime.NewSummarySpan(constants.InitializeStage, traceID, c.Configuration, c.InvokeTimeout)
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	defer invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	logRecorder := c.FuncLogger.NewLogRecorder(ic, traceID, constants.InitializeStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail())
	if runtime.PodApplier == constants.WorkerManagerApplier {
		logRecorder.FinishHeader(ic)
		defer logRecorder.FinishFooter(ic)
	} else {
		defer logRecorder.FinishCopy(ic)
	}

	// Custom runtime startup time cannot exceed the invoking timeout.
	c.InitTimerByInvokeTimeout()
	select {
	case <-c.initTimer.C:
		log.GetLogger().Errorf("inited custom runtime timeout")
		handleErrorTimed(summarySpan, runtime.Error{})
		err := snerror.New(snerror.RuntimeInitTimeoutCode,
			fmt.Sprintf(snerror.ErrText(snerror.RuntimeInitTimeoutCode), c.InvokeTimeout))
		return c.startupError(summarySpan, logRecorder, err)
	case <-clientReadyChan:
		summarySpan.Finish(nil)
		runtime.StartupDuration = summarySpan.SubDuration()
		// The timeWindows can be added only after the initialization is successful.
		runtime.ConResource.SetInitSummary(summarySpan)
		return nil
	case <-ctx.Done():
		log.GetLogger().Errorf("the runtime exited before ready")
		err := snerror.NewSNError(snerror.RuntimeProcessExit, "the runtime exited before ready")
		handleErrorTimed(summarySpan, runtime.Error{})
		return c.startupError(summarySpan, logRecorder, err)
	}
}

func handleErrorTimed(summarySpan *runtime.Summary, err runtime.Error) {
	summarySpan.Finish(&err)
	runtime.StartupDuration = summarySpan.SubDuration()
	if runtime.PodApplier == constants.WorkerManagerApplier {
		runtime.ConResource.SetInitSummary(summarySpan)
	}
}

func (c *Custom) startupError(summary *runtime.Summary, logRecorder *functionlog.LogRecorder,
	err snerror.SNError) error {
	runtimeError := &runtime.Error{
		Code:    int32(err.Code()),
		Message: err.Error(),
	}
	summary.Finish(runtimeError)
	return snerror.New(int(runtimeError.Code),
		startup.GenerateErrorString(logRecorder.MarshalAll(), summary.SummaryInfo, runtimeError))
}

// Healthz check runtime health
func (c *Custom) Healthz(path string, timeout int) (bool, error) {
	return c.healthy, nil
}

// StartupHealthz return rpc server startup health
func (c *Custom) StartupHealthz(path string, timeout int) (bool, error) {
	return true, nil
}

// ReportCustomMetrics report custom metrics
func (c *Custom) ReportCustomMetrics(timeout int) ([]byte, error) {
	return []byte{}, nil
}

// SendProcessEvent put event to local server
func (c *Custom) SendProcessEvent(ic invokectx.InvokeCtx, request runtime.InvokeRequest, conf *config.Configuration) {
	newEvent, fromTask := parseStatefulEvent(request.Event)
	request.Event = newEvent
	rspChan := make(chan *runtime.InvokeResponse, chanBufferCapacity)

	if c.streamResponse {
		option := &stream.ClientOption{
			Token:             request.Extra[constants.HeaderStreamToken],
			ProjectID:         utils.LocalFuncURN.TenantID,
			RequestID:         request.Extra[constants.HeaderStreamRunID],
			StreamHandlerAddr: request.Extra[constants.HeaderStreamAddr],
			StreamTransPort:   constants.StreamTransPort,
		}
		c.eventManager.waitMap.Lock()
		c.eventManager.waitMap.streamOptions[request.TraceID] = option
		c.eventManager.waitMap.Unlock()
	}
	err := c.eventManager.PutRequest(ic, &request, rspChan)

	if err != nil {
		log.GetLogger().Errorf("put request error: %s", err)
		rspChan <- &runtime.InvokeResponse{
			Error: &runtime.Error{
				Code: snerror.WorkerInternalErrorCode,
				Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
					"event manager put request error"),
			},
		}
	}

	c.invokeChan <- &recvMsgCall{
		rspChan:  rspChan,
		ic:       &ic,
		traceID:  request.TraceID,
		fromTask: fromTask,
	}
	return
}

func (c *Custom) recvInvokeMsg(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msgCall, ok := <-c.invokeChan:
			if !ok {
				log.GetLogger().Errorf("custom invokeChan closed")
				return
			}
			go c.recvInvoke(msgCall)
		}
	}
}

func (c *Custom) recvInvoke(call *recvMsgCall) {
	response := c.watchResponse(call.rspChan, call.traceID, call.fromTask)
	invokectx.GetTimeTraceContext(*call.ic).End(worker.TimeTraceRuntime, time.Now())
	c.eventManager.Release(call.traceID)
	c.handlers.RecvProcessEventHandler(*call.ic, *response)
}

// ProcessCancelEvent process cancel event, not support custom runtime now
func (c *Custom) ProcessCancelEvent(request *runtime.CancelRequest) *runtime.CancelResponse {
	response := &runtime.CancelResponse{
		Error: &runtime.Error{
			Code: snerror.WorkerInternalErrorCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
				"cancel is not supported"),
		},
	}
	return response
}

func (c *Custom) watchResponse(rspChan <-chan *runtime.InvokeResponse, traceID string,
	fromTask bool) *runtime.InvokeResponse {
	response := &runtime.InvokeResponse{}
	if rspChan == nil {
		log.GetLogger().Errorf("watchResponse rspChan is nil")
		return response
	}
	timer := c.newInvokeTimer()
	defer timer.Stop()

	c.eventManager.waitMap.Lock()
	c.eventManager.waitMap.timers[traceID] = timer
	c.eventManager.waitMap.Unlock()

	select {
	case <-timer.C:
		// Invoking timeout
		log.GetLogger().Errorf("invoked timeout after %s, traceID %s",
			c.InvokeTimeout.String(), traceID)
		response.Error = &runtime.Error{
			Code:    snerror.RuntimeInvokeTimeoutCode,
			Message: fmt.Sprintf(snerror.ErrText(snerror.RuntimeInvokeTimeoutCode), c.InvokeTimeout),
		}
		return response
	case rsp, ok := <-rspChan:
		if !ok {
			log.GetLogger().Errorf("response channel closed, traceID %s", traceID)
			response.Error = &runtime.Error{
				Code: snerror.WorkerInternalErrorCode,
				Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
					"watch response error: response channel closed"),
			}
			return response
		}

		if fromTask {
			// Package response into the struct that "functionTask" can recognized
			rsp.Result = newStatefulResult(rsp.Result, nil, "")
		}
		return rsp
	}
}

func buildRunCmd(args, envs []string, workerDir string) *exec.Cmd {
	cmd := exec.Command(args[0], args[1:]...)
	for idx := range envs {
		if strings.HasPrefix(envs[idx], worker.RuntimeEnvPrefix) {
			envs[idx] = envs[idx][len(worker.RuntimeEnvPrefix):]
			if strings.HasPrefix(envs[idx], worker.LibPathPrefix) {
				envs[idx] = envs[idx] + ":/usr/local/nvidia/lib64"
			}
		}
	}
	cmd.Dir = workerDir
	cmd.Env, cmd.Stdout, cmd.Stderr = envs, os.Stdout, os.Stdout
	return cmd
}

// @return newEvent, isFromTask
func parseStatefulEvent(event []byte) ([]byte, bool) {
	var statefulEvent pb.StatefulEvent
	if err := proto.Unmarshal(event, &statefulEvent); err != nil {
		// Event is not from module "functionTask"
		return event, false
	}
	return statefulEvent.Event, true
}

func newStatefulResult(event []byte, state *pb.State, property string) []byte {
	newEvent := &pb.StatefulEvent{
		Event:    event,
		State:    state,
		Property: property,
	}
	statefulEvent, err := proto.Marshal(newEvent)
	if err != nil {
		log.GetLogger().Errorf("new Stateful result error %s", err.Error())
		return event
	}
	return statefulEvent
}

// WatchRuntimeProcessStatus watch runtime process status
func (c *Custom) WatchRuntimeProcessStatus(ctx context.Context, handlers runtime.Handlers) {
	processCheckTicker := time.NewTicker(processStatusCheckPeriod)
	log.GetLogger().Infof("starting to watch customRuntime process status")
	for {
		select {
		case <-processCheckTicker.C:
			c.handleRuntimeError(handlers.CheckRuntimeStatusHandler())
		case <-runtime.ProcessExitSignal:
			processCheckTicker.Stop()
			return
		case <-ctx.Done():
			processCheckTicker.Stop()
			log.GetLogger().Infof("old custom runtime handlers context canceled")
			c.handleRuntimeError(handlers.GetExceptionHandler())
			return
		}
	}
}

func (c *Custom) handleRuntimeError(runtimeErr *runtime.Error) {
	if runtimeErr == nil {
		return
	}

	log.GetLogger().Warnf("runtime has some error. %v", runtimeErr)

	c.eventManager.waitMap.Lock()
	for traceID, value := range c.eventManager.waitMap.waiters {
		if value == nil {
			continue
		}

		select {
		case value <- &runtime.InvokeResponse{Error: runtimeErr}:
		default:
			log.GetLogger().Warnf("failed to put error %v for traceID: %s", runtimeErr, traceID)
		}
	}
	c.eventManager.waitMap.Unlock()
}

func (c *Custom) newInvokeTimer() *time.Timer {
	timer := time.NewTimer(c.InvokeTimeout)
	if runtime.StartupDuration > 0 {
		once.Do(func() {
			// Use the remaining time as the invoke timeout.
			timer = time.NewTimer(c.InvokeTimeout - runtime.StartupDuration)
		})
	}
	return timer
}

// PreStop preStop user function
func (c *Custom) PreStop() error {
	return nil
}

// GetException get runtime exception error info
func (c *Custom) GetException(shouldWait bool) *runtime.Error {
	return nil
}

// RequestUpdateAgency -
func (c *Custom) RequestUpdateAgency() error {
	return nil
}
