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

// Package runtime define runtime of worker
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/functionbus/bus/tcpbustool"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/utils"
)

const (
	freezerPath = "/runtime/freezer"
	// frozen is used to froze runtime process
	frozen = "frozen"
	// thawed is used to recover runtime process
	thawed                         = "thawed"
	changeStateIntervalTime        = 1 * time.Millisecond
	runtimeRequestCountCheckPeriod = 100 * time.Millisecond
	defaultIdleTime                = 3 * time.Second
	defaultRetryTimes              = 3
	defaultMaxTimeout              = 72 * time.Hour
)

var (
	processExitSignalOnce sync.Once
	// ProcessExitSignal closed when worker should exit
	ProcessExitSignal = make(chan struct{})
	// ProcessExitError process exit error info
	ProcessExitError snerror.SNError
	// workerErrChan -
	workerErrChan = make(chan Error, 1)

	recycleSignal = make(chan struct{})

	// RunMetrics runtime metrics
	RunMetrics = &Metrics{
		tracer:                  newFutureTracer(),
		isFunctionLoaded:        false,
		customReportMetrics:     make(tcpbustool.CustomReportMetrics, 0),
		customReportMetricsLock: sync.Mutex{},
	}

	// RuntimeManagerClient runtime manager socket client
	RuntimeManagerClient *tcpmsg.SocketConn
)

var (
	// BackgroundProcessLimit background process limit
	BackgroundProcessLimit = &ProcessLimit{
		Status: newRuntimeProcessStatus(),
	}
	loadLimitConfigOnce sync.Once
	// PodApplier WorkerManagerApplier mark the instance is created by minInstance
	PodApplier = ""
	// isIdleMode 0 means idle mode is false, 1 means idle mode is true
	isIdleMode = uint32(0)

	asyncRequestCheckInterval = 1 * time.Minute

	asyncRequestMaximumTime = 72 * time.Hour

	// CurrentIdleMode indicates the current pod mode.
	CurrentIdleMode = constants.NonReservedInstanceMode

	// MetricsSourceName indicates the name of the indicator reported to the BUS.
	MetricsSourceName = constants.MetricsUsedResourceTimeWindow

	businessType = ""
)

// GetRecycleSignal -
func GetRecycleSignal() <-chan struct{} {
	return recycleSignal
}

// LegacyRuntime receives an event from worker and passes it to a specific language function
//
// Deprecated: in favor of Runtime
type LegacyRuntime interface {
	// HotLoad let the runtime load function
	HotLoad(ctx context.Context, conf *config.Configuration) error

	// Healthz check health of runtime
	Healthz(path string, timeout int) (bool, error)

	// StartupHealthz check startup health of runtime
	StartupHealthz(path string, timeout int) (bool, error)

	// ProcessEvent process invoke event
	ProcessEvent(ic invokectx.InvokeCtx, message *InvokeRequest) *InvokeResponse

	// ProcessCancelEvent process cancel event
	ProcessCancelEvent(message *CancelRequest) *CancelResponse

	// PreStop let the runtime load function
	PreStop() error

	ReportCustomMetrics(timeout int) ([]byte, error)

	// GetException get runtime exception error info
	GetException(shouldWait bool) *Error

	// RequestUpdateAgency update agency info to runtime
	RequestUpdateAgency() error
}

// Runtime receives an event from worker and passes it to a specific language function
type Runtime interface {
	// HotLoad let the runtime load function
	HotLoad(ctx context.Context, conf *config.Configuration) error

	// Healthz check health of runtime
	Healthz(path string, timeout int) (bool, error)

	// StartupHealthz check startup health of runtime
	StartupHealthz(path string, timeout int) (bool, error)

	// SendProcessEvent process invoke event
	SendProcessEvent(ic invokectx.InvokeCtx, message InvokeRequest, conf *config.Configuration)

	// ProcessCancelEvent process cancel event
	ProcessCancelEvent(message *CancelRequest) *CancelResponse

	// PreStop let the runtime load function
	PreStop() error

	ReportCustomMetrics(timeout int) ([]byte, error)

	// GetException get runtime exception error info
	GetException(shouldWait bool) *Error

	// RequestUpdateAgency update agency info to runtime
	RequestUpdateAgency() error
}

type runtimeWrapper struct {
	LegacyRuntime
	handlers Handlers
}

// TraceIDFunctionLogger -
type TraceIDFunctionLogger struct {
	LogRecorder *functionlog.LogRecorder
	Ic          invokectx.InvokeCtx
	Info        *functionlog.InfoRecoder
}

// SendProcessEvent implements Runtime.SendProcessEvent
func (rt *runtimeWrapper) SendProcessEvent(ic invokectx.InvokeCtx, message InvokeRequest, conf *config.Configuration) {
	go func() {
		resp := rt.ProcessEvent(ic, &message)
		if !ic.IsInnerCtxEmpty() {
			invokectx.GetTimeTraceContext(ic).Start(worker.TimeTraceRuntime, time.Now())
		}
		if resp == nil {
			resp = &InvokeResponse{}
			resp.Error = &Error{
				Code: snerror.WorkerInternalErrorCode,
				Message: fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
					"invoke response is nil"),
			}
		}
		rt.handlers.RecvProcessEventHandler(ic, *resp)
	}()
}

// FromLagacy provides a convenient way to update the old Runtime to Runtime
func FromLagacy(rt LegacyRuntime, handlers Handlers) Runtime {
	return &runtimeWrapper{
		LegacyRuntime: rt,
		handlers:      handlers,
	}
}

// Handlers -
type Handlers struct {
	RecvProcessEventHandler   RecvProcessEventHandler
	RequestTimeoutHandler     RequestTimeoutHandler
	CallHandler               CallHandler
	CancelHandler             CancelHandler
	GetExceptionHandler       GetExceptionHandler
	CheckRuntimeStatusHandler CheckRuntimeStatusHandler
	ReportHandler             ReportHandler
	ReportErrMsgHandler       ReportErrMsgHandler
}

// RequestTimeoutHandler -
type RequestTimeoutHandler func(ic invokectx.InvokeCtx, conf *config.Configuration, code int32) bool

// RecvProcessEventHandler -
type RecvProcessEventHandler func(invokectx.InvokeCtx, InvokeResponse)

// CallHandler handle call message from runtime
type CallHandler func(ctx context.Context, request *CallRequest) *CallResponse

// ReportHandler handle report message from runtime
type ReportHandler func(ctx context.Context, info *connecttypes.WorkerReportInfo) error

// CancelHandler handle cancel request from runtime
type CancelHandler func(ctx context.Context, request *CancelRequest) *CancelResponse

// ReportErrMsgHandler handle report err message to bus
type ReportErrMsgHandler func(ctx context.Context, URNWithVersion, errMsg string, properties map[string]string) error

// GetExceptionHandler -
type GetExceptionHandler func() *Error

// CheckRuntimeStatusHandler -
type CheckRuntimeStatusHandler func() *Error

// AbstractRuntime -
type AbstractRuntime struct {
	FuncLogger    *functionlog.FunctionLogger
	Configuration *config.Configuration
}

// InvokeRequest is request of invoking runtime
type InvokeRequest struct {
	Event           []byte
	LogOption       string
	TraceID         string
	Extra           map[string]string
	FutureID        string
	Concurrency     uint32
	RetryFlag       bool
	PersistResponse bool
	DirectRequest   bool
}

// InvokeResponse is response of invoking runtime
type InvokeResponse struct {
	Error           *Error
	Summary         snerror.SummaryInfo
	Result          []byte
	Logs            string
	CffHTTPResponse bool
}

// CancelRequest is request of canceling runtime
type CancelRequest struct {
	InvokeID  string
	TraceID   string
	FutureID  string
	Force     bool
	Recursive bool
}

// CancelResponse is response of canceling runtime
type CancelResponse struct {
	Error    *Error
	InvokeID string
}

// ProcessLimit background process limit
type ProcessLimit struct {
	Status     *ProcessStatus
	client     *tcpmsg.SocketConn
	enable     bool
	idleTime   time.Duration
	retryTimes int
}

// Error of message
type Error struct {
	Code        int32  `json:"code"`
	Message     string `json:"message"`
	DcCallStack []byte `json:"DcCallStack,omitempty"`
}

// CallRequest is request of calling other function
type CallRequest struct {
	utils.FunctionInfo
	Event    []byte
	Timeout  int32
	LogType  string
	CallID   string
	InvokeID string
	TraceID  string
	FutureID string
}

// CallResponse is response of calling other function
type CallResponse struct {
	Error  *Error
	Result []byte
	Logs   string
}

// NewAbstractRuntime creates a new abstract runtime
func NewAbstractRuntime(conf *config.Configuration) (*AbstractRuntime, error) {
	functionLogger, err := functionlog.GetFunctionLogger(conf)
	if err != nil {
		return nil, err
	}
	newAbstractRuntime := &AbstractRuntime{
		FuncLogger:    functionLogger,
		Configuration: conf,
	}
	return newAbstractRuntime, nil
}

// CloseExitChannel closeExitChannel
func CloseExitChannel() {
	processExitSignalOnce.Do(func() {
		close(ProcessExitSignal)
	})
}

// WatchRuntimeRequestCount watch runtime request count
func (p *ProcessLimit) WatchRuntimeRequestCount(client *tcpmsg.SocketConn) {
	p.Status.Set(tcpmsg.RuntimeProcessStatusRunning)
	workerConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Warnf("config loading error: %s", err.Error())
		return
	}
	p.initProcessLimit(client)
	if !p.enable {
		return
	}
	processCheckTicker := time.NewTicker(runtimeRequestCountCheckPeriod)
	log.GetLogger().Infof("starting to watch runtime request count")
	for {
		select {
		case <-processCheckTicker.C:
			p.processCheck(workerConfig, client)
		case _, ok := <-ProcessExitSignal:
			if !ok {
				log.GetLogger().Infof("the runtime processed the exit signal")
			}
			processCheckTicker.Stop()
			log.GetLogger().Infof("stop watching runtime request count")
			return
		}
	}
}

func (p *ProcessLimit) processCheck(workerConfig *config.Configuration, client *tcpmsg.SocketConn) {
	if utils.IsDirectFunc(workerConfig.Language) {
		return
	}
	if PodApplier == constants.WorkerManagerApplier && !GetIdleMode() || PodApplier == constants.PredictionApplier ||
		PodApplier == constants.FaasSchedulerApplier {
		status := p.Status.Get()
		if status != tcpmsg.RuntimeProcessStatusRecover &&
			status != tcpmsg.RuntimeProcessStatusRunning {
			p.recoverRuntime(workerConfig, client)
		}
		return
	}
	if workerConfig.RuntimeConfig.Language != worker.CustomContainerLanguage && client == nil {
		return
	}
	status := p.Status.Get()
	if RunMetrics.GetRequestCount() == 0 && status != tcpmsg.RuntimeProcessStatusPaused &&
		status != tcpmsg.RuntimeProcessStatusReconnect && status != tcpmsg.RuntimeProcessStatusFinalize &&
		status != tcpmsg.RuntimeProcessStatusReconnectFailed {
		p.pauseRuntime(workerConfig, client)
		return
	}
	if RunMetrics.GetRequestCount() != 0 && status != tcpmsg.RuntimeProcessStatusRecover &&
		status != tcpmsg.RuntimeProcessStatusRunning {
		p.recoverRuntime(workerConfig, client)
		return
	}
}

func (p *ProcessLimit) pauseRuntime(workerConfig *config.Configuration, client *tcpmsg.SocketConn) {
	if workerConfig.RuntimeConfig.Language == worker.CustomContainerLanguage {
		p.PauseRuntimeProcess(workerConfig.RuntimeContainerID)
		return
	}
	p.ProcessPauseEvent(client)
}

// PauseRuntime pause runtime
func (p *ProcessLimit) PauseRuntime(workerConfig *config.Configuration, client *tcpmsg.SocketConn) {
	status := p.Status.Get()
	if status != tcpmsg.RuntimeProcessStatusPaused &&
		status != tcpmsg.RuntimeProcessStatusReconnect && status != tcpmsg.RuntimeProcessStatusFinalize &&
		status != tcpmsg.RuntimeProcessStatusReconnectFailed {
		p.pauseRuntime(workerConfig, client)
	}
}

// RecoverRuntime -
func (p *ProcessLimit) RecoverRuntime(workerConfig *config.Configuration, client *tcpmsg.SocketConn) {
	status := p.Status.Get()
	if status != tcpmsg.RuntimeProcessStatusRecover && status != tcpmsg.RuntimeProcessStatusRunning {
		p.recoverRuntime(workerConfig, client)
		return
	}
	return
}

func (p *ProcessLimit) recoverRuntime(workerConfig *config.Configuration, client *tcpmsg.SocketConn) {
	if workerConfig.RuntimeConfig.Language == worker.CustomContainerLanguage {
		p.RecoverRuntimeProcess(workerConfig.RuntimeContainerID, false)
		return
	}
	p.ProcessRecoverEvent(client)
}

func retryingWriteFile(path string, data []byte, mode os.FileMode) error {
	err := ioutil.WriteFile(path, data, mode)
	if err != nil {
		return err
	}
	return nil
}

func fileState(path string) (string, error) {
	current, err := ioutil.ReadFile(filepath.Join(path, "freezer.state"))
	if err != nil {
		return "", err
	}
	return strings.ToLower(strings.TrimSpace(string(current))), nil
}

func waitState(path string, state string) error {
	for {
		if err := retryingWriteFile(
			filepath.Join(path, "freezer.state"),
			[]byte(strings.ToUpper(state)),
			os.FileMode(0)); err != nil {
			return err
		}
		current, err := fileState(path)
		if err != nil {
			return err
		}
		if current == state {
			return nil
		}
		time.Sleep(changeStateIntervalTime)
	}
}

// PauseRuntimeProcess pause runtime process in custom-image situation
func (p *ProcessLimit) PauseRuntimeProcess(runtimeContainerID string) {
	path := filepath.Join(freezerPath, runtimeContainerID)
	var err error
	for i := 0; i < p.retryTimes+1; i++ {
		if time.Now().Sub(RunMetrics.GetRequestFinishTime()) < p.idleTime {
			log.GetLogger().Debugf("not reach idle time")
			return
		}
		err = waitState(path, frozen)
		if err == nil {
			p.Status.Set(tcpmsg.RuntimeProcessStatusPaused)
			log.GetLogger().Infof("pauseRuntime")
			return
		}
		log.GetLogger().Warnf("failed to process pause event, retry %d times, err: %s",
			i+1, err.Error())
	}
	p.Status.Set(tcpmsg.RuntimeProcessStatusException)
	CloseExitChannel()
}

// ProcessPauseEvent pause runtime process
func (p *ProcessLimit) ProcessPauseEvent(client *tcpmsg.SocketConn) {
	reqMsg := tcpmsg.BuildMessage(tcpmsg.PauseRuntimeProcess, nil)
	for i := 0; i < p.retryTimes+1; i++ {
		if time.Now().Sub(RunMetrics.GetRequestFinishTime()) < p.idleTime {
			log.GetLogger().Debugf("not reach idle time")
			return
		}
		runtimeManagerRsp := client.SendMessage(reqMsg)
		if runtimeManagerRsp.Err == "" {
			log.GetLogger().Infof("pauseRuntime")
			p.Status.Set(tcpmsg.RuntimeProcessStatusPaused)
			return
		}
		log.GetLogger().Warnf("failed to process pause event, retry %d times, err: %s",
			i+1, runtimeManagerRsp.Err)
	}
	p.Status.Set(tcpmsg.RuntimeProcessStatusException)
	CloseExitChannel()
}

// RecoverRuntimeProcess recover runtime process in custom-image situation
func (p *ProcessLimit) RecoverRuntimeProcess(runtimeContainerID string, isRecoverForExit bool) {
	path := filepath.Join(freezerPath, runtimeContainerID)
	err := waitState(path, thawed)
	if err == nil {
		if isRecoverForExit {
			p.setFinalizeStatus()
		} else {
			p.setRecoverStatus()
		}
		return
	}
	p.Status.Set(tcpmsg.RuntimeProcessStatusException)
	log.GetLogger().Errorf("failed to change runtime process: %s", err.Error())
	for i := 0; i < p.retryTimes; i++ {
		err := waitState(path, thawed)
		if err == nil {
			p.setRecoverStatus()
			return
		}
		log.GetLogger().Warnf("failed to process recover event, retry %d times, err: %s",
			i+1, err.Error())
	}
	CloseExitChannel()
}

// ProcessRecoverEvent recover runtime process
func (p *ProcessLimit) ProcessRecoverEvent(client *tcpmsg.SocketConn) {
	reqMsg := tcpmsg.BuildMessage(tcpmsg.RecoverRuntimeProcess, nil)
	runtimeManagerRsp := client.SendMessage(reqMsg)
	if runtimeManagerRsp.Err == "" {
		/*
		 when heartbeat is unHealthy, runtime-manager kill runtime, runtime-manager will restart runtime and runtime
		 status is set to tcpmsg.RuntimeProcessStatusReconnect. when request comes, if runtime status is seted to
		 tcpmsg.RuntimeProcessStatusRecover, heartbeat check will be trigged, but actually runtime is reconnecting
		 and not ready, so we do not set runtime status to tcpmsg.RuntimeProcessStatusRecover when runtime is in
		 tcpmsg.RuntimeProcessStatusReconnect status
		*/
		p.setRecoverStatus()
		return
	}
	p.Status.Set(tcpmsg.RuntimeProcessStatusException)
	log.GetLogger().Warnf("failed to process recover event and will retry. err: %s", runtimeManagerRsp.Err)
	for i := 0; i < p.retryTimes; i++ {
		runtimeManagerRsp := client.SendMessage(reqMsg)
		if runtimeManagerRsp.Err == "" {
			p.setRecoverStatus()
			return
		}
		log.GetLogger().Warnf("failed to process recover event, retry %d times, err: %s",
			i+1, runtimeManagerRsp.Err)
	}
	CloseExitChannel()
}

func (p *ProcessLimit) setRecoverStatus() {
	for {
		status := p.Status.Get()
		if status == tcpmsg.RuntimeProcessStatusReconnect || status == tcpmsg.RuntimeProcessStatusReconnectFailed {
			break
		}
		if p.Status.CAS(status, tcpmsg.RuntimeProcessStatusRecover) {
			log.GetLogger().Infof("recoverRuntime")
			break
		}
	}
}

func (p *ProcessLimit) setFinalizeStatus() {
	for {
		status := p.Status.Get()
		if status == tcpmsg.RuntimeProcessStatusReconnect || status == tcpmsg.RuntimeProcessStatusReconnectFailed {
			break
		}
		if p.Status.CAS(status, tcpmsg.RuntimeProcessStatusFinalize) {
			log.GetLogger().Infof("succeed to recover and finalize the runtime process")
			break
		}
	}
}

// initProcessLimit init process limit
func (p *ProcessLimit) initProcessLimit(client *tcpmsg.SocketConn) {
	loadLimitConfigOnce.Do(func() {
		workerConfig, err := config.GetConfig()
		if err != nil {
			log.GetLogger().Warnf("config loading error: %s", err.Error())
			CloseExitChannel()
			return
		}
		BackgroundProcessLimit.enable = workerConfig.LimitEnable
		BackgroundProcessLimit.idleTime = defaultIdleTime
		if workerConfig.IdleTime > 0 {
			BackgroundProcessLimit.idleTime = time.Duration(workerConfig.IdleTime) * time.Second
		}
		BackgroundProcessLimit.retryTimes = defaultRetryTimes
		if workerConfig.RetryTimes > 0 {
			BackgroundProcessLimit.retryTimes = workerConfig.RetryTimes
		}
		BackgroundProcessLimit.client = client
	})
}

// ProcessStatus -
type ProcessStatus struct {
	status int64
}

func newRuntimeProcessStatus() *ProcessStatus {
	s := ProcessStatus{}
	s.Set(tcpmsg.RuntimeProcessStatusInit)
	return &s
}

// Set -
func (s *ProcessStatus) Set(status tcpmsg.RuntimeProcessStatus) {
	atomic.StoreInt64(&s.status, int64(status))
}

// Get -
func (s *ProcessStatus) Get() tcpmsg.RuntimeProcessStatus {
	return tcpmsg.RuntimeProcessStatus(atomic.LoadInt64(&s.status))
}

// CAS -
func (s *ProcessStatus) CAS(old tcpmsg.RuntimeProcessStatus, new tcpmsg.RuntimeProcessStatus) bool {
	return atomic.CompareAndSwapInt64(&s.status, int64(old), int64(new))
}

// CheckAsyncRequest periodically checks whether there is an asynchronous requests executing
func CheckAsyncRequest() {
	executingReq := RunMetrics.GetRequestCount()
	if executingReq != 0 {
		log.GetLogger().Infof("process residual asynchronous requests")
		checkRequestCount(executingReq)
	}
}

func checkRequestCount(executingReq int) {
	ticker := time.NewTicker(asyncRequestCheckInterval)
	timer := time.NewTimer(asyncRequestMaximumTime)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			executingReq = RunMetrics.GetRequestCount()
			if executingReq == 0 {
				log.GetLogger().Infof("the async request is done")
				return
			}
			log.GetLogger().Infof("the async request is executing, num: %d", executingReq)
		case <-timer.C:
			log.GetLogger().Warnf("the async request reaches maximum duration")
			return
		}
	}
}

// SetIdleMode set idle mode
func SetIdleMode(idleMode bool) {
	if idleMode {
		log.GetLogger().Infof("current SetIdleMode is true")
		atomic.StoreUint32(&isIdleMode, 1)
	} else {
		log.GetLogger().Infof("current SetIdleMode is false")
		atomic.StoreUint32(&isIdleMode, 0)
	}
}

// GetIdleMode get idle mode
func GetIdleMode() bool {
	return atomic.LoadUint32(&isIdleMode) == 1
}

// SetReservedInstanceModeInCurrentTimeWindow -
// Set the mode of the charging period. The mode is set only at the beginning of each check.
func SetReservedInstanceModeInCurrentTimeWindow() {
	if PodApplier == constants.WorkerManagerApplier {
		if GetIdleMode() {
			CurrentIdleMode = constants.ReservedInstanceIdleMode
		} else {
			CurrentIdleMode = constants.ReservedInstanceNonIdleMode
		}
	} else {
		CurrentIdleMode = constants.NonReservedInstanceMode
	}
	log.GetLogger().Infof("setCurMode %d", CurrentIdleMode)
	return
}

// GetReservedInstanceModeInCurrentTimeWindow -
func GetReservedInstanceModeInCurrentTimeWindow() int {
	log.GetLogger().Infof("get current mode, mode: %d", CurrentIdleMode)
	return CurrentIdleMode
}

// NewRuntimeError -
func NewRuntimeError(code int, msg string) *Error {
	codeValue := int32(code)
	errMsg := snerror.ErrText(code)
	if msg != "" {
		errMsg += ", error: " + msg
	}
	return &Error{
		Code:    codeValue,
		Message: errMsg,
	}
}

// IsUserError true if a user error occurs
func IsUserError(err Error) bool {
	// The user error is a four-digit integer.
	if err.Code < snerror.UserErrorMax {
		return true
	}
	return false
}

func changeErrorStructToError(err *Error) error {
	if err != nil {
		return snerror.New((int)(err.Code), err.Message)
	}
	return nil
}

// SetBusinessType -
func SetBusinessType(busType string) {
	businessType = busType
}

// CreateRuntimeLoggerCtx -
func CreateRuntimeLoggerCtx(conf *config.Configuration, funcLog *functionlog.FunctionLog, isReversed bool) error {
	if funcLog.Message == nil || conf == nil {
		return errors.New("funcLog.Message or conf is nil")
	}
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	CPUInfo := RunMetrics.GetCurrentCPU()
	functionlog.RecodeIcWithTraceID(funcLog.TraceID, ic.ID, CPUInfo.CPUUsedNs, CPUInfo.TotalTimestampNs)

	funcLogger := functionlog.GetRuntimeFunctionLogger()
	if funcLogger == nil {
		log.GetLogger().Errorf("failed to GetRuntimeFunctionLogger")
		return errors.New("failed to GetRuntimeFunctionLogger")
	}
	summarySpan := NewSummarySpan(funcLog.Stage, funcLog.TraceID, conf, defaultMaxTimeout)
	RecodeSummary(funcLog.TraceID, summarySpan)
	logRecorder := funcLogger.NewLogRecorder(ic, funcLog.TraceID, funcLog.Stage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
		functionlog.WithLivedataID(funcLog.LiveDataTraceID))
	logRecorder.SetHeader(funcLog)
	if funcLog.Stage == "invoke" || isReversed {
		logRecorder.FinishHeader(ic)
	}
	return nil
}

// DestroyRuntimeLoggerCtx -
func DestroyRuntimeLoggerCtx(funcLog *functionlog.FunctionLog) error {
	DeleteSummary(funcLog.TraceID)
	info := functionlog.GetRuntimeInfo(funcLog.TraceID)
	if info == nil || info.ID == 0 {
		log.GetLogger().Errorf("failed to destroy RuntimeLoggerCtx")
		return errors.New("failed to destroy RuntimeLoggerCtx")
	}
	ic, ok := invokectx.DefaultMetrics.GetInvokeCtx(info.ID)
	if !ok {
		log.GetLogger().Errorf("failed to GetInvokeCtx")
		return errors.New("failed to GetInvokeCtx")
	}
	invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	functionlog.ClearIcWithTraceID(funcLog.TraceID)
	return nil
}

// GetRuntimeLogRecorder -
func GetRuntimeLogRecorder(traceID string) (TraceIDFunctionLogger, error) {
	traceLogger := TraceIDFunctionLogger{}
	funcLogger, info, isUsed := functionlog.UpdateIsUsedFlag(traceID)
	if info == nil || info.ID == 0 || isUsed || funcLogger == nil {
		log.GetLogger().Errorf("failed to get RuntimeLoggerCtx")
		return traceLogger, errors.New("failed to get RuntimeLoggerCtx")
	}
	ic, ok := invokectx.DefaultMetrics.GetInvokeCtx(info.ID)
	if !ok {
		log.GetLogger().Errorf("failed to GetInvokeCtx")
		return traceLogger, errors.New("failed to get InvokeCtx")
	}
	logRecorder := funcLogger.GetLogRecorder(ic)
	if logRecorder == nil {
		return traceLogger, errors.New("failed to get LogRecorder")
	}
	traceLogger.LogRecorder = logRecorder
	traceLogger.Ic = ic
	traceLogger.Info = info
	return traceLogger, nil
}

// WriteLoadInitLog -
func WriteLoadInitLog(traceID string, livedataID string) *Error {
	returnErr := &Error{}
	if len(functionlog.LogRecorderBox) == 0 {
		return returnErr
	}
	for i := 0; i < len(functionlog.LogRecorderBox); i++ {
		functionlog.LogRecorderBox[i].ReplaceTraceID(traceID, livedataID)
		functionlog.LogRecorderBox[i].FinishSave()
		summarySpan, ok := functionlog.LogRecorderBox[i].Generator().(*Summary)
		if !ok {
			break
		}
		if summarySpan.GetErr() != nil {
			returnErr = summarySpan.GetErr()
		}
	}
	functionlog.LogRecorderBox = functionlog.LogRecorderBox[:0]
	return returnErr
}

// GetWorkerErrChan get workerErrChan
func GetWorkerErrChan() chan Error {
	return workerErrChan
}

// SendWorkerErr send runtimeErr to fusion runtime or web runtime
func SendWorkerErr(runtimeErr Error) {
	workerErrChan <- runtimeErr
}
