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

// Package invoke interacts with bus and invoke runtime
package invoke

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/contentsignaturecheck"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient/podevent"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/connecttypes"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	bustypes "functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	busutils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/deploy"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/network"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/fusion"
	"functioncore/pkg/worker/snapshot"
	"functioncore/pkg/worker/startup"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/telemetry"
	"functioncore/pkg/worker/utils"
)

var sysLog map[string]struct{} = map[string]struct{}{
	worker.CustomRuntimeLanguage:   struct{}{},
	worker.GOV1RuntimeLanguage:     struct{}{},
	worker.WebRuntimeLanguage:      struct{}{},
	worker.CustomContainerLanguage: struct{}{},
}

var (
	dnCache       domainNameCache
	curPodName    = os.Getenv("PodName")
	curNodeIP     = os.Getenv("HOST_IP")
	curClusterID  = os.Getenv("CLUSTER_ID")
	coldStartOnce sync.Once
)

const (
	base                        = 10
	delay                       = time.Duration(1) * time.Second
	sendRetryMaximumSeconds     = 15 * 60
	labelsNum                   = 3
	coldStartIndex              = 2
	retryTime                   = 5
	retryInterval               = 20 * time.Second
	pythonLanguage              = "python"
	checkHealthTimeout          = 3
	defaultDirectReqConcurrency = 1000
	defaultTermTimeout          = 170 * time.Second
)

type domainNameCache struct {
	m                  *sync.Mutex
	domainNameValue    string
	preDomainNameValue string
}

func init() {
	dnCache = domainNameCache{
		preDomainNameValue: "",
		domainNameValue:    "",
		m:                  &sync.Mutex{},
	}
}

func (c *domainNameCache) needToSendMessage(domainName string) bool {
	return c.loadAndStore(domainName)
}

func (c *domainNameCache) loadAndStore(domainName string) bool {
	c.m.Lock()
	defer c.m.Unlock()
	if domainName == c.domainNameValue {
		log.GetLogger().Debugf("handle same domainName")
		return false
	}
	log.GetLogger().Infof("handle different domainName with new: %s and old: %s", domainName, c.domainNameValue)
	c.preDomainNameValue = c.domainNameValue
	c.domainNameValue = domainName
	return true
}

func (c *domainNameCache) resetDomainName() {
	c.m.Lock()
	defer c.m.Unlock()
	c.domainNameValue = c.preDomainNameValue
}

type eventCtx struct {
	summarySpan   *runtime.Summary
	dst           common.FunctionProperty
	extra         map[string]string
	busRequestID  uint64
	traceID       string
	concurrency   uint32
	retryFlag     bool
	futureID      string
	directRequest bool

	longTimeStopFn func() bool // only used in long time request
}

// Reset implements invokectx.Poolable
func (ec *eventCtx) Reset() {
	ec.longTimeStopFn = nil
}

var getter = invokectx.DefaultMetrics.Register(func() invokectx.Poolable { return &eventCtx{} })

func getEventCtx(ic invokectx.InvokeCtx) *eventCtx {
	eventCtxIf := getter(ic)
	evenetCtx, ok := eventCtxIf.(*eventCtx)
	if !ok {
		log.GetLogger().Errorf("failed to assert eventCtx")
		return nil
	}
	return evenetCtx
}

// Server represents invoke server
type Server struct {
	conf                            *config.Configuration
	functionLogger                  *functionlog.FunctionLogger
	billingLog                      *functionlog.BillingLogger
	bus                             *tcpbus.TCPBus
	initBus                         *tcpbus.TCPBus
	rt                              runtime.Runtime
	invokeTimeout                   time.Duration
	isExiting                       bool
	client                          *tcpbus.Connection
	funcUrn                         urnutils.BaseURN
	NetworkController               types.NetworkController
	IsVPCFunc                       bool
	readyChan                       chan struct{}
	DeployCodeFinishedChan          chan struct{}
	closeDeployCodeFinishedChanOnce sync.Once
	readyChanCloseOnce              sync.Once
	initBusClientOnce               sync.Once
}

// HandleRequest handle request from clientIP
func (s *Server) HandleRequest(payload []byte, clientIP string, client *tcpbus.Connection) bustypes.Result {
	return bustypes.Result{}
}

// SendWithRetryCallBack when worker send response to bus fails, it will retry for one hour
func (s *Server) SendWithRetryCallBack(msg *protocol.Message) {
	var err error
	startTime := time.Now()
	defer msg.ResetPayload()
	for {
		client := s.client
		if isTimeout(startTime) {
			if err != nil {
				log.GetLogger().Errorf("failed to send response %s to %s: %s", msg.Header.PacketID,
					client.PeerIP, err)
			}
			break
		}
		if client == nil {
			time.Sleep(delay)
			continue
		}

		if err = client.SendWithoutRetry(msg); err != nil {
			time.Sleep(delay)
			continue
		}
		break
	}
}

func isTimeout(startTime time.Time) bool {
	return time.Since(startTime).Seconds() > sendRetryMaximumSeconds
}

// RegisterService register client to server
func (s *Server) RegisterService(client *tcpbus.Connection) error {
	tcpBusOpt := &tcpbus.Option{
		CreateClient: false,
		Client:       client,
		Remote:       true,
		SingleMode:   true,
	}
	tcpBus, err := tcpbus.NewTCPBus(common.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		log.GetLogger().Errorf("create bus for worker error: %s", err.Error())
		return err
	}
	client.RegisterSendCallBack(s.SendWithRetryCallBack)
	check := os.Getenv("INIT_AUTH_CHECK")
	if check == "CHECK" {
		curTimeTamp := strconv.FormatInt(timeutil.NowUnixMillisecond(), base)
		signKey := "appid=" + "worker" + "=timestamp=" + curTimeTamp
		key, err := localauth.Encrypt(signKey)
		commonUtils.ClearStringMemory(signKey)
		if err != nil {
			log.GetLogger().Errorf("failed to encrypt: %s", err.Error())
			return err
		}
		checkResult := tcpBus.CheckAuth(key, constants.WorkKeyTask)
		if !checkResult {
			log.GetLogger().Error("failed to init bus instance and check auth")
			return errors.New("failed to init bus instance and check auth")
		}
	}
	tcpBus.RegisterAsyncCallback(common.WrapInvokeAdaptor(s.Adaptor))
	tcpBus.RegisterCancel(s.Cancel)
	tcpBus.RegisterRecover(s.Recover)
	tcpBus.RegisterPauseRuntime(s.PauseRuntime)
	tcpBus.RegisterRecoverRuntime(s.RecoverRuntime)
	tcpBus.RegisterCheckRuntime(s.RuntimeCheck)

	if s.isExiting {
		err = tcpBus.WaitRequestsFinished(true, nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// InitServer initializes a new server connected to busProxy
func InitServer(conf *config.Configuration, serverAddr string) (*Server, error) {
	server := &Server{
		conf:                   conf,
		readyChan:              make(chan struct{}),
		DeployCodeFinishedChan: make(chan struct{}),
	}
	serverOpt := tcpbus.ServerOption{
		Receiver:   server,
		ListenAddr: serverAddr,
		IsServer:   true,
	}

	if err := startBusServer(conf, serverOpt); err != nil {
		log.GetLogger().Errorf("failed to start bus server, error: %s", err.Error())
		return nil, err
	}
	return server, nil
}

func startBusServer(conf *config.Configuration, opt tcpbus.ServerOption) error {
	if conf.InvokeServer.Protocol == "" {
		conf.InvokeServer.Protocol = "tcp"
	}
	if conf.InvokeServer.Protocol != "tcp" {
		return fmt.Errorf("unsupported protocol: %s", "to be implemented")
	}

	if conf.HTTPSConfig.HTTPSEnable {
		if err := tcpbus.StartServerWithTLS(opt); err != nil {
			log.GetLogger().Errorf("failed to create socket server: %s", err.Error())
			return err
		}
	} else {
		if err := tcpbus.StartServer(opt); err != nil {
			log.GetLogger().Errorf("failed to create socket server: %s", err.Error())
			return err
		}
	}
	return nil
}

func (s *Server) stopFusionRuntimeProcess(conf *config.Configuration) error {
	if conf.Type == "fusion" {
		start := time.Now()
		rsp := fusion.StopRuntimeProcess(conf)
		if rsp.Err != "" {
			log.GetLogger().Errorf("error when stopping runtime processes: %s", rsp.Err)
			if rsp.Code != 0 {
				return snerror.New(rsp.Code, rsp.Err)
			}
			return errors.New(rsp.Err)
		}
		log.GetLogger().Debugf("stopRuntimeProcess cost: %v", time.Now().Sub(start))
	}
	return nil
}

// LoadAndServe load function and serve
func (s *Server) LoadAndServe(ctx context.Context, conf *config.Configuration,
	deployMessageChan <-chan *deploy.Message, netWorkCh chan error) (*deploy.DurationInfo, error) {
	var err error
	s.functionLogger, err = functionlog.GetFunctionLogger(conf)
	if err != nil {
		log.GetLogger().Warnf("failed to get functionLogger: %s", err.Error())
		return nil, err
	}
	go s.functionLogger.RefreshFileModTime(runtime.ProcessExitSignal)

	s.billingLog, err = functionlog.NewBillingLogger(s.conf)
	if err != nil {
		log.GetLogger().Warnf("failed to new billingLogger: %s", err.Error())
		return nil, err
	}

	if runtime.RuntimeManagerClient != nil && !runtime.RuntimeManagerClient.IsConnected() {
		ok := runtime.RuntimeManagerClient.Reconnect()
		if !ok {
			return nil, snerror.New(snerror.RuntimeManagerProcessExited,
				snerror.ErrText(snerror.RuntimeManagerProcessExited))
		}
	}
	funcUrn := urnutils.BaseURN{}
	err = funcUrn.ParseFrom(conf.URN)
	if err != nil {
		log.GetLogger().Errorf("failed to parse urn %s err %s", conf.URN, err.Error())
	}
	s.funcUrn = funcUrn
	runtime.RunMetrics.SetBaseURN(funcUrn)
	deployMessage := &deploy.Message{}
	conf.RuntimeName = conf.Language
	conf.Language = commonUtils.GetRuntimeLanguage(conf.Language)
	if err = s.stopFusionRuntimeProcess(conf); err != nil {
		return nil, err
	}
	if deployMessageChan != nil {
		select {
		case deployMessage, _ = <-deployMessageChan:
			if deployMessage.Err != nil {
				return nil, deployMessage.Err
			}
		}
	}
	s.closeDeployCodeFinishedChanOnce.Do(func() {
		close(s.DeployCodeFinishedChan)
	})
	if netWorkCh != nil {
		select {
		case networkErr, _ := <-netWorkCh:
			if networkErr != nil {
				return nil, networkErr
			}
		}
	}

	if !s.conf.IsLoaded && s.NetworkController.DisablePublicNetwork && !s.IsVPCFunc {
		log.GetLogger().Infof("start to close public network access")
		cmdline := "ip route del default"
		_, result, err := network.ExecuteShellCommandLineWithCombinedOutput(cmdline)
		if err != nil {
			log.GetLogger().Errorf("failed to execute command line '%s' error: %s, %s", cmdline, err.Error(), result)
			return nil, err
		}
		err = addDefaultDestination()
		if err != nil {
			log.GetLogger().Errorf("failed to addDefaultDestination error: %s", err.Error())
			return nil, err
		}
	}

	s.rt, err = runtime.Registry.NewRuntime(conf.Type, conf, runtime.Handlers{
		RecvProcessEventHandler: s.RecvProcessEvent,
		RequestTimeoutHandler:   s.PythonTimeoutJudger,
		CallHandler:             s.CallByBus,
		CancelHandler:           s.CancelByBus,
		ReportHandler:           s.ReportSummaryLogsToBus,
		ReportErrMsgHandler:     s.ReportErrMsgToBus,
		SetExitHandler:          s.SetExit,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to new runtime, error: %s", err.Error())
		return deployMessage.Duration, err
	}

	var ch chan int
	if utils.IsDirectFunc(conf.Language, conf.RMConfig.RuntimeConfigs[conf.Language].EnableBusClient) {
		ch = make(chan int)
		runtime.InitRuntimeMetrics(s.conf, ch)
	}
	if err = s.rt.HotLoad(ctx, conf); err != nil {
		log.GetLogger().Errorf("failed to load the function, error: %s", err.Error())
		return deployMessage.Duration, err
	}
	// if custom-runtime-rpc with enableBusClient=true, with cpp runtime is not DirectRuntime, stop reportRuntimeMetrics
	if !utils.IsDirectRuntime && ch != nil {
		log.GetLogger().Info("direct runtime is false, quit reportRuntimeMetrics")
		close(ch)
	}
	runtimeConfig := s.conf.RuntimeConfig
	functionLoadErr := cache.GetLoadErr()
	if utils.IsHeartbeatEnable(runtimeConfig.Language) && commonUtils.IsCAEFunc(utils.GetBusinessType()) &&
		utils.IsHealthCheckEnable(runtimeConfig.StartupHeartbeat) {
		log.GetLogger().Info("cae startup health check has configured, wait startup health check ready")
		ok := fusion.WaitStartHealthCheckOK()
		if !ok {
			errMsg := "cae startup health check failed"
			if functionLoadErr != nil {
				if snErr, ok := functionLoadErr.(snerror.SNError); ok {
					errMsg = fmt.Sprintf("%s, %s", errMsg, snerror.ErrEventMessageText(snErr.Code()))
				} else {
					errMsg = fmt.Sprintf("%s, %s", errMsg, functionLoadErr.Error())
				}
			}
			return deployMessage.Duration, errors.New(errMsg)
		}
	} else {
		// return no init error except InnerRuntimeInitTimeoutCode
		if functionLoadErr != nil {
			if snErr, ok := functionLoadErr.(snerror.SNError); ok {
				if snErr.Code() != snerror.InnerRuntimeInitTimeoutCode {
					cache.SetLoadErr(nil)
				}
			}
		}
	}
	if utils.IsHeartbeatEnable(runtimeConfig.Language) && commonUtils.IsCAEFunc(utils.GetBusinessType()) &&
		conf.RunningMode != constants.RunningModeCheckpoint && utils.IsHealthCheckEnable(runtimeConfig.Heartbeat) {
		log.GetLogger().Info("do alive heartbeat check for CAE")
		if err = fusion.AliveHeartbeatCheck(s.rt, s.conf.Heartbeat); err != nil {
			return deployMessage.Duration, err
		}
	} else if utils.IsM2SHeartbeatEnable(runtimeConfig.Language, runtimeConfig.Heartbeat.HealthEnable,
		runtimeConfig.Heartbeat.Handler) {
		if err = s.afterLoadHealthCheck(); err != nil {
			return deployMessage.Duration, err
		}
	}

	go runtime.BackgroundProcessLimit.WatchRuntimeRequestCount(runtime.RuntimeManagerClient)
	// clean secret on disk
	config.CleanWorkerSecret()

	s.invokeTimeout = time.Duration(conf.RuntimeConfig.Timeout) * time.Second

	go s.startReportTimeWindowsTask(conf.ReportTimeWindowsPeriod)
	return deployMessage.Duration, nil
}

func addDefaultDestination() error {
	funcContainerIP := os.ExpandEnv("${POD_IP}")
	_, containerGateway, err := network.GetIntfnameAndGatewayByIPAddr(funcContainerIP)
	if err != nil {
		return fmt.Errorf("failed to get interface and gateway of container ip '%s' "+
			"when addDefaultDestination, err: %s", funcContainerIP, err.Error())
	}
	addDefaultRoute := fmt.Sprintf("ip route add 100.64.0.0/10 via %s dev eth0", containerGateway)
	_, result, err := network.ExecuteShellCommandLineWithCombinedOutput(addDefaultRoute)
	if err != nil {
		return fmt.Errorf("failed to execute command line '%s' when addDefaultDestination"+
			" error: %s, %s", addDefaultRoute, err.Error(), result)
	}
	return nil
}

func (s *Server) afterLoadHealthCheck() error {
	heartbeatFailureThreshold := s.conf.FailureThreshold
	checkPeriodSeconds := s.conf.CheckPeriodSeconds
	log.GetLogger().Infof("start heartbeat check m2s function for language %s, status: %d",
		s.conf.RuntimeConfig.Language, runtime.BackgroundProcessLimit.Status.Get())
	var err error
	var isHealthy bool
	for i := 0; i < heartbeatFailureThreshold; i++ {
		isHealthy, err = s.rt.Healthz("", checkHealthTimeout)
		if err == nil && isHealthy {
			log.GetLogger().Infof("succeed to afterLoadHealthCheck")
			break
		}
		time.Sleep(time.Duration(checkPeriodSeconds) * time.Second)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to afterLoadHealthCheck error: %s", err)
		return fmt.Errorf("health check failed after load function: %s", err)
	}
	return nil
}

// CheckpointRuntimeProcess -
func (s *Server) CheckpointRuntimeProcess(deployer deploy.Deployer, traceID string) {
	// 1. check function health make sure microservice have been started
	failureFunc := func(errMsg string) {
		snapshot.SetCheckpointError(errMsg)
		status.Set(status.CheckpointFailed)
	}
	if s == nil {
		return
	}

	// 2. checkpoint
	err := fusion.SendCheckpointCommand()
	if err != nil {
		log.GetLogger().Errorf("%s | fail to checkpoint function, err: %s", traceID, err.Error())
		failureFunc(err.Error())
		return
	}

	funcBucketID, snapshotObjectID := s.conf.SnapshotBucketID, s.conf.SnapshotObjectID
	if len(funcBucketID) == 0 || len(snapshotObjectID) == 0 {
		log.GetLogger().Error("function deploy info is empty")
		failureFunc("function deploy info is empty")
		return
	}

	// 3. delete history unused snapshot version
	err = deployer.DeleteSnapshot(s.conf, funcBucketID, snapshotObjectID, s.conf.NeedDelObjectIDs)
	if err != nil {
		log.GetLogger().Errorf("%s | fail to delete history version snapshot, err: %s", traceID, err.Error())
		failureFunc(err.Error())
		return
	}

	// 4. upload to obs/data-system
	fileSize, fileHash, uploadErr := deployer.UploadSnapshot(s.conf, funcBucketID, snapshotObjectID)
	if uploadErr != nil {
		log.GetLogger().Errorf("%s | fail to upload snapshot, err: %s", traceID, uploadErr.Error())
		failureFunc(uploadErr.Error())
		return
	}

	snapshotInfo := &types.SnapshotDeploymentSpec{
		SnapshotSize:   fileSize,
		SnapshotSha256: fileHash,
		BucketID:       funcBucketID,
		ObjectID:       snapshotObjectID,
	}
	snapshot.SetSnapshotInfo(snapshotInfo)
	status.Set(status.CheckpointReady)
}

// CheckHealth return runtime's status
func (s *Server) CheckHealth() bool {
	if s.bus == nil || !s.bus.CheckHealth() {
		log.GetLogger().Errorf("bus unhealthy")
		return false
	}
	return true
}

// GracefulShutdown close httpServer gracefully
func (s *Server) GracefulShutdown() error {
	if s.bus == nil && !commonUtils.IsCAEFunc(utils.GetBusinessType()) {
		log.GetLogger().Infof("the bus is not on, exited directly")
		return nil
	}
	status.SetExitState()
	if commonUtils.IsCAEFunc(utils.GetBusinessType()) {
		runtime.RunMetrics.IncreaseRequestCount("termCAE")
		defer runtime.RunMetrics.DecreaseRequestCount("termCAE")
	}

	// The concurrentResourceTimeWindows should be reported after all requests finished and
	// before the bus stops.
	functionURN := utils.LocalFuncURN.String()

	s.checkTermCaeProcessResp(functionURN)

	if s.bus == nil {
		log.GetLogger().Infof("the bus is not on, CAE function terminated, return")
		return nil
	}

	s.isExiting = true
	if err := s.bus.WaitRequestsFinished(true, runtime.GetRecycleSignal()); err != nil {
		log.GetLogger().Errorf("failed to wait requests finished, %s", err.Error())
		return err
	}
	if !commonUtils.IsCAEFunc(utils.GetBusinessType()) {
		runtime.CheckAsyncRequest()
	}

	podName := os.Getenv("PodName")
	if err := s.ReportConcurrentResourceTimeWindows(functionURN, podName); err != nil {
		log.GetLogger().Errorf("failed to reportConcurrentResourceTimeWindows "+
			"before gracefully shutdown : %s", err.Error())
		return err
	}
	if s.conf.PreStop.Handler != "" {
		log.GetLogger().Info("start to preStop")
		runtime.BackgroundProcessLimit.RecoverRuntime(s.conf, runtime.RuntimeManagerClient)
		runtime.RunMetrics.IncreaseRequestCount("preStop")
		s.rt.PreStop()
		runtime.RunMetrics.DecreaseRequestCount("preStop")

		//  The preStopInvokeTimeWindows should be reported after preStop invoke finished and
		// before the bus stops.
		if err := s.ReportPreStopInvokeTimeWindows(functionURN, podName); err != nil {
			log.GetLogger().Errorf("failed to reportPreStopInvokeTimeWindows "+
				"before gracefully shutdown : %s", err.Error())
			return err
		}
	}
	s.bus.Stop()
	return nil
}

func (s *Server) checkTermCaeProcessResp(functionURN string) {
	if commonUtils.IsCAEFunc(utils.GetBusinessType()) {
		log.GetLogger().Infof("start to send term signal to runtime-manager")
		resp := runtime.RuntimeManagerClient.SendMessageWithTimeout(
			tcpmsg.BuildMessage(tcpmsg.TermCAEProcess, nil), defaultTermTimeout)
		log.GetLogger().Infof("send term signal to runtime-manager get resp, code: %d, Err: %s", resp.Code, resp.Err)
		switch resp.Code {
		case tcpmsg.MessageWithSuccessResponse:
			log.GetLogger().Infof("succeed to send term message to runtime-manager")
		case tcpmsg.MessageWithTermError:
			log.GetLogger().Warnf("failed to send term message to runtime-manager, err: %s", resp.Err)
		case tcpmsg.MessageWithTermTimeout:
			log.GetLogger().Warnf("failed to send term message to runtime-manager, reason: time out")
			ctx, cancel := context.WithTimeout(context.Background(), worker.DefaultReportErrMsgContextTimeout)
			if err := s.ReportErrMsgToBus(ctx, functionURN, resp.Err, podevent.EventErrorType,
				runtime.GetProperties()); err != nil {
				// if report err, just log enough
				log.GetLogger().Warnf("failed to report err msg to bus when "+
					"send term message to runtime-manager, err: %s", err.Error())
			}
			cancel()
		default:
			log.GetLogger().Warnf("failed to send term message with unknown resp, code: %d", resp.Code)
		}
	}
}

// HandleUpdateAgencyToRuntime -
func (s *Server) HandleUpdateAgencyToRuntime() {
	err := s.rt.RequestUpdateAgency()
	if err != nil {
		log.GetLogger().Errorf("failed to update agency to runtime,error: %s", err.Error())
	}
}

// HandleRouterReady -
func (s *Server) HandleRouterReady() {
	if s.rt == nil {
		return
	}
	err := s.rt.NotifyRouterReady()
	if err != nil {
		log.GetLogger().Warnf("failed to notify router ready,error: %s", err.Error())
	}
}

func (s *Server) cancelRecursive(callees []string, traceID string, dst *common.CancelProperty) {
	ctx := context.Background()
	for _, callee := range callees {
		res := s.CancelByBus(ctx, &runtime.CancelRequest{
			TraceID:   traceID,
			FutureID:  callee,
			Force:     dst.Force,
			Recursive: dst.Recursive,
		})
		if res.Error != nil && res.Error.Code != 0 {
			log.GetLogger().Warnf("failed to cancel, caller: %s, callee: %s, code: %d, msg: %s, traceID: %s",
				dst.FutureID, callee, res.Error.Code, res.Error.Message, traceID)
		}
	}
}

// Cancel call the runtime to cancel futureID
func (s *Server) Cancel(ctx context.Context, dst *common.CancelProperty, req *types.TCPRequest) error {
	traceID := busutils.ContextValueString(ctx, constants.HeaderTraceID)
	callees, canceled := runtime.RunMetrics.Cancel(dst.FutureID)
	if canceled {
		log.GetLogger().Infof("duplicate cancel from bus, invokeID: %s, traceID: %s",
			utils.BusID2InvokeID(req.RequestID), traceID)
		result := &types.TCPResponse{
			RequestID: req.RequestID,
		}
		if err := s.bus.SetCancelResult(result); err != nil {
			log.GetLogger().Errorf("set cancel result error, err: %s", err.Error())
			return err
		}
		return nil
	}

	log.GetLogger().Infof("cancel from bus, invokeID: %s, traceID: %s", utils.BusID2InvokeID(req.RequestID), traceID)

	if dst.Recursive {
		s.cancelRecursive(callees, traceID, dst)
	}

	result := &types.TCPResponse{
		RequestID: req.RequestID,
	}
	response := s.rt.ProcessCancelEvent(&runtime.CancelRequest{
		FutureID:  dst.FutureID,
		Force:     dst.Force,
		Recursive: dst.Recursive,
		TraceID:   busutils.ContextValueString(ctx, constants.HeaderTraceID),
		InvokeID:  utils.BusID2InvokeID(req.RequestID),
	})
	if response == nil {
		result.ErrorCode = uint32(snerror.WorkerInternalErrorCode)
		result.ErrorMessage = fmt.Sprintf(snerror.ErrText(snerror.WorkerInternalErrorCode),
			"process cancel event, but receive no response")
	} else if response.Error != nil {
		result.ErrorCode = uint32(response.Error.Code)
		result.ErrorMessage = response.Error.Message
	}

	if err := s.bus.SetCancelResult(result); err != nil {
		log.GetLogger().Errorf("set cancel result error, err: %s", err.Error())
		return err
	}
	return nil
}

// RuntimeCheck get runtime error info
func (s *Server) RuntimeCheck(traceID string, shouldWait bool) ([]byte, error) {
	log.GetLogger().Infof("RuntimeCheck traceID %s, shouldWait %t", traceID, shouldWait)
	reportInfo, err := s.generateLogAndSummary(traceID, shouldWait)
	if err != nil {
		log.GetLogger().Errorf("set generate summary info error, traceID: %s, err: %s", traceID, err.Error())
		return nil, err
	}
	msg := &pb.Message_WorkerReport{
		TraceID:        reportInfo.TraceID,
		RuntimeLog:     reportInfo.RuntimeLog,
		RuntimeSummary: reportInfo.RuntimeSummary,
		ErrorCode:      (uint32)(reportInfo.ErrorCode),
		ErrorMessage:   reportInfo.ErrorMessage,
	}
	data, err := proto.Marshal(&pb.Message{
		Type:         pb.Message_WORKER_REPORT,
		WorkerReport: msg,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to proto.Marshal, %s", err.Error())
		return nil, err
	}
	return data, nil
}

// PauseRuntime pause runtime
func (s *Server) PauseRuntime() error {
	runtime.BackgroundProcessLimit.PauseRuntime(s.conf, runtime.RuntimeManagerClient)
	return nil
}

// RecoverRuntime recover runtime
func (s *Server) RecoverRuntime(restartRuntime bool) error {
	log.GetLogger().Infof("recover runtime restartRuntime %t", restartRuntime)
	if restartRuntime {
		if err := runtime.GetDirectRuntimeState().WaitUnblock(restartRuntime); err != nil {
			log.GetLogger().Errorf("failed to recover runtime, err: %s", err.Error())
			return err
		}
		log.GetLogger().Infof("succeed to recover runtime restartRuntime %t", restartRuntime)
		return nil
	}
	runtime.BackgroundProcessLimit.RecoverRuntime(s.conf, runtime.RuntimeManagerClient)
	log.GetLogger().Infof("succeed to recover runtime restartRuntime %t", restartRuntime)
	return nil
}

// Recover recover current request count
func (s *Server) Recover(req *types.TCPRequest, commonBus interface{}) error {
	log.GetLogger().Infof("recoverFromBus | invokeID %s", utils.BusID2InvokeID(req.RequestID))
	recoverProperty := runtime.RunMetrics.GetRecoverProperty()
	recoverProperty.PodName = os.Getenv(constants.PodName)
	b, err := json.Marshal(recoverProperty)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal recover property: %s", err.Error())
		return err
	}
	if req.IsInitialConcurrent {
		runtime.ConResource.AppendLoadAndInitTimeWindows(defaultDirectReqConcurrency, true)
	}
	result := &types.TCPResponse{
		RequestID: req.RequestID,
		RawData:   b,
	}
	tcpBus, ok := commonBus.(*tcpbus.TCPBus)
	if !ok {
		return errors.New("receive common bus is not tcp bus")
	}

	// if WorkerID match podName replace client and bus
	if strings.Contains(req.WorkerID, recoverProperty.PodName) {
		s.client = tcpBus.GetClient()
		s.bus = tcpBus
		if s.readyChan != nil {
			s.readyChanCloseOnce.Do(func() {
				close(s.readyChan)
			})
		}
		log.GetLogger().Infof("worker id: %s match pod name, replace client and bus", req.WorkerID)
	} else {
		log.GetLogger().Warnf("worker id: %s not match pod name: %s", req.WorkerID, recoverProperty.PodName)
	}
	// if WorkerID not match podName, return WorkerID and bus judge resp's WorkerID not equal req's WorkerID,
	// bus deal with this correctly.
	if err := tcpBus.SetRecoverResult(result); err != nil {
		log.GetLogger().Errorf("set recover result error, err: %s", err.Error())
		return err
	}
	return nil
}

func (s *Server) isFuncInfoConsistent(dst *common.FunctionProperty) bool {
	if s.funcUrn.Name != dst.FunctionName || s.funcUrn.Version != dst.FunctionVersion ||
		s.funcUrn.TenantID != dst.TenantID {
		return false
	}
	return true
}

// Adaptor is the entry point when worker receives an invoke request.
func (s *Server) Adaptor(ctx context.Context, dst *common.FunctionProperty, req *types.TCPRequest) {
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	invokectx.GetTimeTraceContext(ic).Start(worker.TimeTraceTotal, time.Now())
	request := buildInvokeRequest(ctx, req, dst.FutureID)
	runtime.BackgroundProcessLimit.RecoverRuntime(s.conf, runtime.RuntimeManagerClient)
	runtime.RunMetrics.IncreaseRequestCount(request.FutureID)
	runtime.RunMetrics.IncreaseTotalReqCount()
	preUsed := runtime.RunMetrics.GetCurrentCPU()
	if !s.isFuncInfoConsistent(dst) {
		errMsg := fmt.Sprintf("The function information does not match, funcName %s, version: %s, tenantID: %s",
			dst.FunctionName, dst.FunctionVersion, dst.TenantID)
		log.GetLogger().Errorf("current pod function info is not Consistent, traceID: %s, err: %s",
			request.TraceID, errMsg)
		err := snerror.New(snerror.WorkerInternalErrorCode, errMsg)
		s.setErrorResult(req.RequestID, request, dst, err, preUsed)
		s.handleErrorResult(ctx, ic, request, isPrintUserErrorLog(true, nil))
		runtime.CloseExitChannel()
		return
	}
	if err := cache.GetMessageQueue().WaitUnblock(request.Concurrency, request.DirectRequest); err != nil {
		s.setErrorResult(req.RequestID, request, dst, err, preUsed)
		s.handleErrorResult(ctx, ic, request, isPrintUserErrorLog(false, err))
		return
	}
	if runtime.PodApplier != constants.WorkerManagerApplier {
		runtime.WriteLoadInitLog(busutils.ContextValueString(ctx, constants.HeaderTraceID),
			request.Extra[constants.HeaderLiveDataTraceID])
	}
	if s.conf.Initializer.Extra[constants.HeaderIAMToken] != "" {
		request.Extra[constants.HeaderIAMToken] = s.conf.Initializer.Extra[constants.HeaderIAMToken]
	}

	check := os.Getenv("CONTENT_SIGNATURE_CHECK")
	if check == "CHECK" && !checkContentSignature(request, dst.FunctionName) {
		err := snerror.New(snerror.ContentInconsistentErrorCode, "worker content is inconsistent")
		s.setErrorResult(req.RequestID, request, dst, err, preUsed)
		s.handleErrorResult(ctx, ic, request, isPrintUserErrorLog(false, nil))
		return
	}

	s.sendRequest(ic, req.RequestID, request, *dst)
}

func isPrintUserErrorLog(isFuncInfoConsistent bool, blockErr error) bool {
	if isFuncInfoConsistent {
		return false
	}
	if blockErr != nil && blockErr.Error() == snerror.ErrText(snerror.INodeUsageExceed) {
		return false
	}
	return true
}

func buildInvokeRequest(ctx context.Context, req *types.TCPRequest, futureID string) runtime.InvokeRequest {
	return runtime.InvokeRequest{
		Event:           req.RawData,
		LogOption:       req.LogType,
		TraceID:         busutils.ContextValueString(ctx, constants.HeaderTraceID),
		Extra:           getExtra(req),
		FutureID:        futureID,
		Concurrency:     req.Concurrency,
		RetryFlag:       req.RetryFlag,
		PersistResponse: req.PersistResponse,
		DirectRequest:   req.DirectRequest,
	}
}

func checkContentSignature(request runtime.InvokeRequest, funcName string) bool {
	startTime := time.Now()
	if !request.DirectRequest {
		return true
	}
	headerSignature := request.Extra[constants.HeaderContentSignature]
	contentSignature := contentsignaturecheck.GetContentSignature(request.Event)
	if headerSignature != contentSignature {
		log.GetLogger().Errorf("worker content is inconsistent, funcName: %s, header signature: %s, content signature: %s, "+
			"traceID: %s, cost: %d ms", funcName, headerSignature, contentSignature, request.TraceID,
			time.Since(startTime).Milliseconds())
		return false
	}
	return true
}

func (s *Server) handleErrorResult(ctx context.Context, ic invokectx.InvokeCtx, request runtime.InvokeRequest,
	isPrintUserErrorLog bool) {
	loadInitErr := &runtime.Error{}
	if runtime.PodApplier != constants.WorkerManagerApplier {
		loadInitErr = runtime.WriteLoadInitLog(busutils.ContextValueString(ctx, constants.HeaderTraceID),
			request.Extra[constants.HeaderLiveDataTraceID])
	}
	if isPrintUserErrorLog {
		s.setDefaultErrorLog(ic, request, loadInitErr)
	}
	runtime.RunMetrics.DecreaseRequestCount(request.FutureID)
	invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
}

func getExtra(req *types.TCPRequest) map[string]string {
	extra := req.Extra
	if extra == nil {
		extra = make(map[string]string, 1)
	}
	if req.Concurrency > 1 {
		extra["concurrent"] = "true"
	}
	return extra
}

func (s *Server) setDefaultErrorLog(ic invokectx.InvokeCtx, request runtime.InvokeRequest, err *runtime.Error) {
	summarySpan := runtime.NewSummarySpan(constants.InvokeStage, request.TraceID, s.conf, s.invokeTimeout)
	summarySpan.SetErr(err)
	logRecorder := s.functionLogger.NewLogRecorder(ic, request.TraceID, constants.InvokeStage,
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLogOptionTail(),
	)
	summarySpan.Finish(err)
	logRecorder.FinishHeader(ic)
	logRecorder.FinishFooter(ic)
}

// PythonTimeoutJudger -
func (s *Server) PythonTimeoutJudger(ic invokectx.InvokeCtx, conf *config.Configuration, code int32) bool {
	eventCtx := getEventCtx(ic)
	if eventCtx == nil {
		return false
	}
	if conf == nil {
		return false
	}
	return eventCtx.concurrency == 1 && code == snerror.RuntimeInvokeTimeoutCode &&
		strings.Contains(conf.Language, pythonLanguage)
}

// RecvProcessEvent returns runtime's invoke reponse back to proxy.
func (s *Server) RecvProcessEvent(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
	eventCtx := getEventCtx(ic)
	if eventCtx.longTimeStopFn != nil {
		eventCtx.longTimeStopFn()
	}

	concurrentStartTime := runtime.ConResource.StopOccupation(eventCtx.concurrency,
		eventCtx.retryFlag, eventCtx.directRequest)
	eventCtx.summarySpan.Finish(resp.Error)
	runtime.ConResource.AppendTimeWindows(concurrentStartTime, eventCtx.summarySpan.EndTime(), constants.InvokeStage)

	resp.Summary = eventCtx.summarySpan.SummaryInfo
	buildRuntimeDoneAndReportEvent(constants.TelemetryRuntimeDone, constants.TelemetryReport,
		eventCtx.summarySpan, eventCtx.traceID)

	logRecorder := s.functionLogger.GetLogRecorder(ic)
	s.setResponseLogs(&resp, logRecorder)

	s.writeResponse(ic, resp)

	logRecorder.FinishFooter(ic)
	runtime.RunMetrics.DecreaseRequestCount(eventCtx.futureID)

	invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
}

func (s *Server) setErrorResult(requestID uint64, request runtime.InvokeRequest, dst *common.FunctionProperty,
	err error, cpuUsed runtime.CPUUsed) {
	runtime.RunMetrics.IncreaseTotalFailedCount()
	result := types.TCPResponse{
		RequestID: requestID,
	}
	s.generateErrorResult(&result, request, dst, err, cpuUsed)
	if err := s.bus.SetResult(&result); err != nil {
		log.GetLogger().Warnf("set result to the bus error, invokeID: %s, traceID: %s",
			requestID, request.TraceID)
		return
	}
	log.GetLogger().Infof("write response with error code: %d, invokeID: %s, traceID: %s, error message: %s",
		result.ErrorCode, requestID, request.TraceID, result.ErrorMessage)
}

func (s *Server) generateErrorResult(result *types.TCPResponse, request runtime.InvokeRequest,
	dst *common.FunctionProperty, err error, cpuUsed runtime.CPUUsed) {

	var errMsg string
	if snErr, ok := err.(snerror.SNError); ok {
		result.ErrorCode = uint32(snErr.Code())
		errMsg = snErr.Error()
	} else {
		result.ErrorCode = snerror.WorkerInternalErrorCode
		errMsg = err.Error()
	}
	duration := runtime.StartupDuration + runtime.InitializerDuration +
		runtime.RestoreDuration + runtime.ExtensionDuration
	tmpErr := startup.DecodeErrorString(errMsg)
	result.ErrorMessage = tmpErr.Message
	info := snerror.SummaryInfo{
		MemorySize:      int(s.conf.RuntimeConfig.ResourceMetaData.Mem),
		Duration:        float64(duration/time.Microsecond) / runtime.MicroToMilliSecond,
		BillingDuration: int(duration/time.Microsecond) / runtime.MicroToMilliSecond,
		MemoryUsed:      runtime.RunMetrics.GetMemoryUsed(err),
		PodName:         curPodName,
		NodeIP:          curNodeIP,
		ClusterID:       curClusterID,
	}
	avgCPUUsed := runtime.RunMetrics.CalAvgCPUUsed(cpuUsed)
	if avgCPUUsed != 0 {
		info.CPUUsed = avgCPUUsed
	}
	info.StorageUsed = runtime.RunMetrics.GetStorageUsed()

	result.Logs = tmpErr.Log
	if len(s.conf.RuntimeConfig.CodeSha512) > runtime.FuncDigestLength {
		info.FuncDigest = s.conf.RuntimeConfig.CodeSha512[0 : runtime.FuncDigestLength-1]
	} else {
		info.FuncDigest = s.conf.RuntimeConfig.CodeSha512
	}
	result.Summary = info.MarshalJSONString()
	generateResultInfos(result, dst, request.Extra, request.Concurrency, info.BillingDuration)
}

func (s *Server) writeResponse(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
	eventCtx := getEventCtx(ic)
	result := types.TCPResponse{
		RequestID: eventCtx.busRequestID,
	}
	s.generateResultBody(&result, resp)
	generateResultInfos(&result, &eventCtx.dst, eventCtx.extra, eventCtx.concurrency,
		eventCtx.summarySpan.SummaryInfo.BillingDuration)

	for i := 0; i < retryTime; i++ {
		err := s.bus.SetResult(&result)
		if err == nil {
			break
		}
		log.GetLogger().Warnf("set result to the bus error, invokeID: %s, traceID: %s, err: %s",
			eventCtx.busRequestID, eventCtx.traceID, err.Error())
		if i != retryTime-1 {
			time.Sleep(retryInterval)
		}
	}

	tc := invokectx.GetTimeTraceContext(ic)
	tc.End(worker.TimeTraceTotal, time.Now())

	totalCost := tc.GetElapsed(worker.TimeTraceTotal)
	runtimeCost := tc.GetElapsed(worker.TimeTraceRuntime)
	workerCost := totalCost - runtimeCost
	if len(result.ErrorMessage) != 0 {
		runtime.RunMetrics.IncreaseTotalFailedCount()
		invokeErr := &runtime.Error{Code: int32(result.ErrorCode), Message: result.ErrorMessage}
		// funcName|tenantID|traceID|cost|workerCost:runtimeCost
		log.GetLogger().Infof("%s | %s | %s | %.3f | %.3f:%.3f | %.3f | %.3f | %.3f | invokeEndWorkerErr | %v | "+
			"invokeID %d", urnutils.ExtractFuncName(s.funcUrn.Name), urnutils.Anonymize(s.funcUrn.TenantID),
			eventCtx.traceID, commonUtils.ConvertSecondToMillisecond(totalCost.Seconds()),
			commonUtils.ConvertSecondToMillisecond(workerCost.Seconds()),
			commonUtils.ConvertSecondToMillisecond(runtimeCost.Seconds()),
			eventCtx.summarySpan.SummaryInfo.CPUUsed, eventCtx.summarySpan.SummaryInfo.MemoryUsed,
			eventCtx.summarySpan.SummaryInfo.StorageUsed, invokeErr, eventCtx.busRequestID)
	} else {
		// funcName|tenantID|traceID|cost|workerCost:runtimeCost
		log.GetLogger().Infof("%s | %s | %s | %.3f | %.3f:%.3f | %.3f | %.3f | %.3f | invokeEndWorker | invokeID %d",
			urnutils.ExtractFuncName(s.funcUrn.Name), urnutils.Anonymize(s.funcUrn.TenantID), eventCtx.traceID,
			commonUtils.ConvertSecondToMillisecond(totalCost.Seconds()),
			commonUtils.ConvertSecondToMillisecond(workerCost.Seconds()),
			commonUtils.ConvertSecondToMillisecond(runtimeCost.Seconds()),
			eventCtx.summarySpan.SummaryInfo.CPUUsed, eventCtx.summarySpan.SummaryInfo.MemoryUsed,
			eventCtx.summarySpan.SummaryInfo.StorageUsed, eventCtx.busRequestID)
	}
	go func() {
		runtime.RunMetrics.RecordTimeCost(workerCost.Seconds())
	}()
}

func (s *Server) sendRequest(ic invokectx.InvokeCtx, busRequestID uint64, request runtime.InvokeRequest,
	dst common.FunctionProperty) {

	stage := constants.InvokeStage
	coldStartOnce.Do(func() {
		if s.conf.RunningMode == constants.RunningModeRestore {
			stage = constants.RestoreStage
		} else {
			if commonUtils.IsCAEFunc(utils.GetBusinessType()) {
				if runtime.CAEPodApplier != constants.WorkerManagerApplier {
					s.conf.DurationInfo.ColdStartRequestID = busRequestID
				}
			} else {
				if runtime.PodApplier != constants.WorkerManagerApplier {
					stage = constants.ColdStartStage
					s.conf.DurationInfo.ColdStartRequestID = busRequestID
				}
			}
		}
	})
	summarySpan := runtime.NewSummarySpan(constants.InvokeStage, request.TraceID, s.conf, s.invokeTimeout)
	opts := []functionlog.LogRecorderOption{
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithLivedataID(request.Extra[constants.HeaderLiveDataTraceID]),
		functionlog.WithLogOption(request.LogOption),
	}
	if isSysLog(s.conf.Language) {
		opts = append(opts, functionlog.WithoutLineBreak())
	}

	logRecorder := s.functionLogger.NewLogRecorder(ic, request.TraceID, stage, opts...)
	logRecorder.FinishHeader(ic)
	runtime.ConResource.StartOccupation(request.Concurrency, summarySpan.StartTime(),
		request.RetryFlag, request.DirectRequest)
	handleDomainName(request.Extra, s.conf.Language == worker.CustomContainerLanguage)
	eventCtx := getEventCtx(ic)
	eventCtx.summarySpan = summarySpan
	eventCtx.dst = dst
	eventCtx.extra = request.Extra
	eventCtx.busRequestID = busRequestID
	eventCtx.traceID = request.TraceID
	eventCtx.concurrency = request.Concurrency
	eventCtx.retryFlag = request.RetryFlag
	eventCtx.futureID = request.FutureID
	eventCtx.directRequest = request.DirectRequest

	if s.invokeTimeout > time.Duration(s.conf.SyncRequestTimeout)*time.Second {
		// If a request takes longer than SyncRequestTimeout, we return a special RequestAccepted response and
		// continue waiting...
		timer := time.AfterFunc(time.Duration(s.conf.SyncRequestTimeout)*time.Second, func() {
			resp := runtime.InvokeResponse{
				Error: &runtime.Error{
					Code:    snerror.RequestAcceptedCode,
					Message: "request is accepted",
				},
			}
			s.writeResponse(ic, resp)
		})
		eventCtx.longTimeStopFn = timer.Stop
	}
	if slice.ContainsString(telemetry.Config.Types, constants.TelemetryPlatformType, nil) {
		startEvent := telemetry.Event{
			Type:      constants.TelemetryPlatformType,
			Name:      constants.TelemetryStart,
			StartTime: time.Now().UTC().Format(functionlog.LogTimeFormat),
			TraceID:   eventCtx.traceID,
		}
		telemetry.SendEvent(startEvent)
	}

	runtime.RunMetrics.SetFunctionLoaded()
	s.rt.SendProcessEvent(ic, request, s.conf)
}

func handleDomainName(extra map[string]string, isCustomContainer bool) {
	domainName := extra[constants.HeaderDomainName]
	if domainName != "" {
		if runtime.RuntimeManagerClient != nil {
			needToSend := dnCache.needToSendMessage(domainName)
			if !needToSend {
				return
			}
			body, err := json.Marshal(extra)
			if err != nil {
				return
			}
			resp := runtime.RuntimeManagerClient.SendMessage(tcpmsg.BuildMessage(tcpmsg.ResetDomainName, body))
			if resp.Err != "" {
				log.GetLogger().Errorf("failed to reset domain names: %+v, error: %s", domainName, resp.Err)
				dnCache.resetDomainName()
				return
			}
			log.GetLogger().Infof("succeed to reset domain names: %+v", domainName)
			return
		}
		log.GetLogger().Warnf("receive domainName while runtimeManagerClient is nil")
		if isCustomContainer {
			domainNames := map[string]string{}
			err := json.Unmarshal([]byte(domainName), &domainNames)
			if err != nil {
				log.GetLogger().Errorf("failed to unmarshal domain names: %s", err.Error())
				return
			}
			err = commonUtils.ReplaceByDNS("/etc/hosts", domainNames)
			if err != nil {
				log.GetLogger().Errorf("failed to replace DNS for /etc/hosts file with error: %s", err.Error())
				return
			}
			log.GetLogger().Infof("succeed to reset domain names: %+v", domainNames)
		}
	}
	return
}

func isSysLog(language string) bool {
	_, ok := sysLog[language]
	return ok
}

func (s *Server) setResponseLogs(resp *runtime.InvokeResponse, logRecorder *functionlog.LogRecorder) {
	// Get logs from log recorder
	if isSysLog(s.conf.Language) && runtime.RuntimeManagerClient != nil {
		logRecorder.StartSync()
		// Send RequestFinished to runtime manager and
		// wait response to insure that log collection is complete
		err := runtime.RuntimeManagerClient.SendMessageWithNoResp(
			tcpmsg.BuildMessage(tcpmsg.RequestFinished, logRecorder.InvokeID()))
		if err != nil {
			log.GetLogger().Errorf("failed to send RequestFinished to runtime manager, err: %s", err.Error())
			logRecorder.ClearSync()
		}
	}

	if logRecorder.LogOption() == constants.RuntimeLogOptTail {
		resp.Logs = logRecorder.MarshalAll()
	}
}

func (s *Server) generateResultBody(result *types.TCPResponse, response runtime.InvokeResponse) {
	result.Logs = response.Logs
	if runtime.ProcessExitError != nil {
		result.ErrorCode = uint32(runtime.ProcessExitError.Code())
		result.ErrorMessage = runtime.ProcessExitError.Error()
	} else if response.Error != nil {
		result.ErrorCode = uint32(response.Error.Code)
		result.ErrorMessage = response.Error.Message
		result.DcCallStack = response.Error.DcCallStack
		if response.Error.Code > snerror.UserErrorMax {
			response.Summary.Duration = 0
		}
	} else {
		result.RawData = response.Result
	}
	response.Summary.PodName = curPodName
	response.Summary.NodeIP = curNodeIP
	response.Summary.ClusterID = curClusterID
	result.CffHTTPResponse = response.CffHTTPResponse
	if result.ErrorCode == 0 {
		s.setDurationInfo(&response, result.RequestID)
	}
	result.Summary = response.Summary.MarshalJSONString()
}

func (s *Server) setDurationInfo(response *runtime.InvokeResponse, requestID uint64) {
	if s.conf == nil {
		log.GetLogger().Warnf("s.conf is nil")
		return
	}
	if requestID != s.conf.DurationInfo.ColdStartRequestID {
		return
	}
	response.Summary.SpecifyPodDuration = s.conf.DurationInfo.SpecifyPodDuration
	response.Summary.LoadFuncDuration = s.conf.DurationInfo.LoadFuncDuration
	response.Summary.InitFuncDuration = s.conf.DurationInfo.InitFuncDuration
	response.Summary.GetCodeAndDecompressDuration = s.conf.DurationInfo.GetCodeAndDecompressDuration
}

func generateResultInfos(result *types.TCPResponse, dst *common.FunctionProperty, extra map[string]string,
	concurrency uint32, billingDuration int) {

	result.FunctionKey = buildFunctionKey(dst)
	result.InstanceID = dst.InstanceID
	result.Labels = make(map[string]string, labelsNum)
	result.Labels[constants.HeaderInvokeSource] = extra[constants.HeaderInvokeSource]
	result.Labels[constants.HeaderRequestID] = extra[constants.HeaderRequestID]
	result.Labels[constants.HeaderWorkflowID] = extra[constants.HeaderWorkflowID]
	result.Labels[constants.HeaderWorkflowRunID] = extra[constants.HeaderWorkflowRunID]
	result.Labels[constants.HeaderWorkflowStateID] = extra[constants.HeaderWorkflowStateID]
	result.Labels[constants.HeaderOriginVersionTag] = extra[constants.HeaderOriginVersionTag]
	// For concurrent function calls, calculate billing duration from use-resource-windows
	if concurrency == 1 && runtime.PodApplier != constants.WorkerManagerApplier {
		result.Labels[constants.HeaderBillingDuration] = strconv.Itoa(billingDuration)
	}
}

func buildFunctionKey(dst *common.FunctionProperty) string {
	return strings.Join([]string{dst.TenantID, dst.FunctionName, dst.FunctionVersion}, "/")
}

func buildRuntimeDoneAndReportEvent(runtimeDoneEvent string, reportEvent string,
	summarySpan *runtime.Summary, traceID string) {
	if !slice.ContainsString(telemetry.Config.Types, constants.TelemetryPlatformType, nil) {
		return
	}
	now := time.Now()
	runtimeDoneAndReportEvent := telemetry.Event{
		Type:      constants.TelemetryPlatformType,
		Name:      runtimeDoneEvent,
		StartTime: now.UTC().Format(functionlog.LogTimeFormat),
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
	runtimeDoneAndReportEvent.StartTime = time.UnixMilli(now.UnixMilli() -
		int64(metrics.BillingDurationMS)).UTC().Format(functionlog.LogTimeFormat)
	runtimeDoneAndReportEvent.EndTime = now.UTC().Format(functionlog.LogTimeFormat)
	telemetry.SendEvent(runtimeDoneAndReportEvent)
}

// ReportResourceUsage report resource usage
func (s *Server) ReportResourceUsage(resourceUsage *common.ResourceUsage, reportTimeout time.Duration) error {
	// Get logs from log recorder
	if !s.CheckHealth() {
		return errors.New("can't get bus client, so can't report resource usage")
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	return s.bus.ReportResourceUsage(ctx, resourceUsage)
}

// ReportCaeMetrics report cae metrics
func (s *Server) ReportCaeMetrics(caeMetrics *common.CaeMetrics, reportTimeout time.Duration) error {
	// Get logs from log recorder
	if !s.CheckHealth() {
		return errors.New("can't get bus client, so can't report resource usage")
	}

	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()

	return s.bus.ReportCaeMetrics(ctx, caeMetrics)
}

// ReportBridgeFuncMetrics report bridgeFunc metrics
func (s *Server) ReportBridgeFuncMetrics(bridgeFuncMetrics *common.BridgeFuncMetrics,
	reportTimeout time.Duration) error {
	// Get logs from log recorder
	if !s.CheckHealth() {
		return errors.New("can't get bus client, so can't report bridgeFunc resource usage")
	}
	ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
	defer cancel()
	return s.bus.ReportBridgeFuncMetrics(ctx, bridgeFuncMetrics)
}

func (s *Server) generateLogAndSummary(traceID string, shouldWait bool) (*connecttypes.WorkerReportInfo,
	error) {
	traceLogger, err := runtime.GetRuntimeLogRecorder(traceID)
	if err != nil && strings.Contains(err.Error(), "failed to get RuntimeLoggerCtx") {
		// traceID is not exist,generate new loggerRecoder
		timeout := time.Duration(s.conf.RuntimeConfig.Timeout) * time.Second
		summarySpan := runtime.NewSummarySpan(constants.InvokeStage, traceID, s.conf, timeout)
		funcLog := &functionlog.FunctionLog{
			TraceID: traceID,
			Stage:   constants.InvokeStage,
			Message: new(bytes.Buffer),
		}
		isReservedIns := runtime.PodApplier == constants.WorkerManagerApplier
		if err := runtime.CreateRuntimeLoggerCtx(s.conf, funcLog, isReservedIns); err != nil {
			log.GetLogger().Errorf("failed to create runtimeLoggerCtx, err: %s", err.Error())
			return nil, err
		}
		curTraceLogger, err := runtime.GetRuntimeLogRecorder(traceID)
		if err != nil || curTraceLogger.LogRecorder == nil || curTraceLogger.Info == nil {
			log.GetLogger().Errorf("failed to getRuntimeLogRecorder")
			return nil, errors.New("failed to get RuntimeLogRecorder")
		}
		workerInfo, err := s.getSummaryInfo(curTraceLogger, summarySpan, traceID, shouldWait)
		if err != nil {
			log.GetLogger().Errorf("failed to get start and finish summaryInfo, traceID:%s, err:%s",
				traceID, err.Error())
			return nil, errors.New("failed to get RuntimeLogRecorder")
		}
		return workerInfo, nil
	}
	summarySpan := runtime.GetSummary(traceID)
	if summarySpan == nil {
		log.GetLogger().Errorf("failed to get summary, traceID: %s", traceID)
		return nil, errors.New("failed to get summary")
	}
	workerInfo, err := s.getSummaryInfo(traceLogger, summarySpan, traceID, shouldWait)
	if err != nil {
		log.GetLogger().Errorf("failed to get finish summaryInfo, err: %s", err.Error())
		return nil, errors.New("failed to get RuntimeLogRecorder")
	}
	return workerInfo, nil
}

func (s *Server) getSummaryInfo(traceIDLogger runtime.TraceIDFunctionLogger, summarySpan *runtime.Summary,
	traceID string, shouldWait bool) (*connecttypes.WorkerReportInfo, error) {
	var errorCode int32
	var errorMsg string
	if summarySpan == nil {
		log.GetLogger().Errorf("summarySpan is nil")
		return nil, errors.New("summarySpan is nil")
	}
	runtimeErr := s.rt.GetException(shouldWait)
	if runtimeErr != nil {
		errorCode = runtimeErr.Code
		errorMsg = runtimeErr.Message
		snErr := snerror.New(int(errorCode), errorMsg)
		summarySpan.Finish(runtimeErr)
		summarySpan.SummaryInfo = runtime.GenerateSummaryInfo(s.conf, traceIDLogger.Info.CPUUsedNs,
			traceIDLogger.Info.TotalTimestampNs, snErr)
	} else {
		summarySpan.Finish(nil)
		summarySpan.SummaryInfo = runtime.GenerateSummaryInfo(s.conf, traceIDLogger.Info.CPUUsedNs,
			traceIDLogger.Info.TotalTimestampNs, nil)
	}
	finishLog := &functionlog.FunctionLog{Message: new(bytes.Buffer), TraceID: traceID}
	traceIDLogger.LogRecorder.SetFooter(finishLog, summarySpan)
	traceIDLogger.LogRecorder.FinishFooter(traceIDLogger.Ic)
	logs := traceIDLogger.LogRecorder.MarshalAll()
	log.GetLogger().Infof("traceID %s, check runtime logs %s", traceID, logs)
	err := runtime.DestroyRuntimeLoggerCtx(finishLog)
	if err != nil {
		log.GetLogger().Errorf("failed to destroyRuntimeLogRecorder, err: %s", err.Error())
		return nil, err
	}
	summaryStr, err := json.Marshal(summarySpan.SummaryInfo)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal summaryStr, %s", err.Error())
		return nil, errors.New("failed to marshal summaryStr")
	}
	workerInfo := &connecttypes.WorkerReportInfo{
		TraceID:        traceID,
		ErrorCode:      errorCode,
		ErrorMessage:   errorMsg,
		RuntimeSummary: string(summaryStr),
		RuntimeLog:     logs,
	}
	log.GetLogger().Infof("report to bus %+v", workerInfo)
	return workerInfo, nil
}
