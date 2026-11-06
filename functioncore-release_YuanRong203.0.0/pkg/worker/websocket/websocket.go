/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package websocket -
package websocket

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/snerror"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/network"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/utils"
)

const (
	// same with defaultRuntimeTermTimeout
	wsConnGracefulShutdownTimeout = 150 * time.Second
	defaultMaxTimeout             = 72 * time.Hour
)

var (
	globalWSRT *WSRT
)

// WSRT short for websocket runtime
type WSRT struct {
	Configuration *config.Configuration
	CustomDiskMgr *diskusage.EvictionManager
	Handlers      runtime.Handlers
}

// SetGlobalWSRT set global websocket runtime
func SetGlobalWSRT(wsRT *WSRT) {
	globalWSRT = wsRT
}

// GetWebRuntimeException -
func GetWebRuntimeException() *runtime.Error {
	if globalWSRT == nil {
		return nil
	}
	if globalWSRT.Configuration.Language == worker.CustomContainerLanguage && oom.IsRuntimeOOMKilled() {
		return &runtime.Error{
			Code:    snerror.RuntimeMemoryExceedLimit,
			Message: snerror.ErrText(snerror.RuntimeMemoryExceedLimit),
		}
	}
	if globalWSRT.CustomDiskMgr != nil && globalWSRT.CustomDiskMgr.LimitFlag {
		return runtime.NewRuntimeError(snerror.RuntimeProcessExit, snerror.ErrText(snerror.DiskUsageExceed))
	}
	if globalWSRT.Handlers.GetLastExceptionHandler != nil {
		if runtimeException := globalWSRT.Handlers.GetLastExceptionHandler(); runtimeException != nil {
			return runtimeException
		}
	}
	return nil
}

// GetWebSocketRuntimeConfiguration -
func GetWebSocketRuntimeConfiguration() *config.Configuration {
	if globalWSRT == nil {
		log.GetLogger().Warnf("got websocket runtime configuration is nil")
		return &config.Configuration{}
	}
	return globalWSRT.Configuration
}

// WssConnectionPair websocket connection pair
type WssConnectionPair struct {
	TraceID     string
	Ctx         invokectx.InvokeCtx
	Summary     *runtime.Summary
	Upstream    *commonwebsocket.WssConnection
	Downstream  *commonwebsocket.WssConnection
	releaseOnce sync.Once
}

// WssManager manage all websocket connection
type WssManager struct {
	wssCache         map[string]*WssConnectionPair // key是traceID，value是对应的连接对
	wssCacheLock     sync.RWMutex
	wssIptablesCache map[string]bool
	wssIptablesLock  sync.RWMutex
	functionLogger   *functionlog.FunctionLogger
}

// WssMessageWrapper -
type WssMessageWrapper struct {
	Data    []byte `json:"data" valid:",optional"`
	TraceID string `json:"traceID" valid:",optional"`
}

const (
	dialTimeout          = 20 * time.Second
	checkReleaseInterval = 100 * time.Millisecond
	wsPath               = "websocket"
	success              = 0
	downStreamFail       = 1
	upStreamFail         = 2
	twoWayCommunication  = 2
)

var (
	wssOnce           sync.Once
	manager           *WssManager
	podName           = os.Getenv("PodName")
	enableRemoteDebug = os.Getenv("EnableRemoteDebug") == "true"
)

// Instance get websocket manager instance
func Instance() *WssManager {
	wssOnce.Do(func() {
		logger, err := functionlog.GetFunctionLogger(GetWebSocketRuntimeConfiguration())
		if err != nil {
			log.GetLogger().Warnf("failed to get logger: %v", err)
		}
		manager = &WssManager{
			wssCache:         make(map[string]*WssConnectionPair),
			wssCacheLock:     sync.RWMutex{},
			wssIptablesCache: make(map[string]bool),
			wssIptablesLock:  sync.RWMutex{},
			functionLogger:   logger,
		}
	})
	return manager
}

// BuildStreamOption build create stream option
func BuildStreamOption(traceID string, req *schedulerapi.CreateWebSocketConnectionReq,
	conf *config.Configuration) *commonwebsocket.StreamOption {
	var downstreamURL string
	if enableRemoteDebug {
		log.GetLogger().Infof("enable debug agent")
		downstreamURL = fmt.Sprintf("ws://%s:%s/%s", worker.LocalServerIP, commonwebsocket.DebugAgentWSPort, wsPath)
	} else {
		downstreamURL = fmt.Sprintf("ws://%s:%s%s",
			worker.LocalServerIP, commonwebsocket.RuntimeWSPort, req.WebSocketConfig.Path)
	}
	var websocketTag string
	if conf.HTTPSEnable {
		websocketTag = "wss"
	} else {
		websocketTag = "ws"
	}
	var upstreamURL string
	connInternalUpstream := runtime.GetWebSocketEnable()
	if connInternalUpstream {
		// connect to frontend
		upstreamURL = fmt.Sprintf("%s://%s:%s/serverless/v1/functions/%s/%s/%s/websocket", websocketTag,
			req.WebSocketConfig.TargetIP, req.WebSocketConfig.TargetPort, podName, traceID, req.FunctionKey)
	} else {
		// connect to rdispatcher
		upstreamURL = fmt.Sprintf("%s://%s:%s/v2/fgs/i/worker/%s/%s/%s/websocket", websocketTag,
			req.WebSocketConfig.TargetIP, req.WebSocketConfig.TargetPort, podName, traceID, req.FunctionKey)
	}
	return &commonwebsocket.StreamOption{
		UpstreamOption: &commonwebsocket.SingleStreamOption{
			URL:         upstreamURL,
			HTTPSEnable: conf.HTTPSEnable,
			Auth:        conf.AuthConfig,
			Internal:    connInternalUpstream,
		},
		DownstreamOption: &commonwebsocket.SingleStreamOption{
			URL:         downstreamURL,
			HTTPSEnable: false,
		},
	}
}

// GetWssConnPairsCount get websocket connection counts
func (m *WssManager) GetWssConnPairsCount() int {
	m.wssCacheLock.RLock()
	defer m.wssCacheLock.RUnlock()
	return len(m.wssCache)
}

// CreateWssConnPair create websocket connection pair
func (m *WssManager) CreateWssConnPair(traceID string, opt *commonwebsocket.StreamOption) (*WssConnectionPair, error) {
	pair := &WssConnectionPair{
		TraceID: traceID,
	}
	finished := make(chan int, 1)
	go pair.createStreams(opt, finished)
	select {
	case i, ok := <-finished:
		if ok && i == upStreamFail {
			return nil, snerror.New(snerror.WebSocketUpStreamConnError,
				snerror.ErrText(snerror.WebSocketUpStreamConnError))
		}
		if ok && i == downStreamFail {
			return nil, snerror.New(snerror.WebSocketDownStreamConnError,
				snerror.ErrText(snerror.WebSocketDownStreamConnError))
		}
	case <-time.After(dialTimeout):
		return nil, errors.New("create up and down stream timeout")
	}
	m.wssCacheLock.Lock()
	m.wssCache[traceID] = pair
	m.wssCacheLock.Unlock()
	ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
	summarySpan := runtime.NewSummarySpan(constants.InvokeStage, traceID,
		GetWebSocketRuntimeConfiguration(), defaultMaxTimeout)
	opts := []functionlog.LogRecorderOption{
		functionlog.WithLogHeaderFooterGenerator(summarySpan),
		functionlog.WithoutLineBreak(),
	}

	logRecorder := m.functionLogger.NewLogRecorder(ic, traceID, constants.InvokeStage, opts...)
	logRecorder.FinishHeader(ic)
	pair.Ctx = ic
	pair.Summary = summarySpan
	runtime.RunMetrics.IncreaseRequestCount(traceID)
	runtime.RunMetrics.IncrWsConnCount()
	runtime.ConResource.StartOccupation(twoWayCommunication, time.Now().UTC(), false, false)
	log.GetLogger().Infof("finish create websocket pair for traceID: %s, up: %s, down: %s",
		traceID, opt.UpstreamOption.URL, opt.DownstreamOption.URL)
	return pair, nil
}

// GetWssConnPair get websocket connection pair
func (m *WssManager) GetWssConnPair(traceID string) *WssConnectionPair {
	m.wssCacheLock.RLock()
	defer m.wssCacheLock.RUnlock()
	pair, ok := m.wssCache[traceID]
	if !ok {
		return nil
	}
	return pair
}

// ReleaseWssConnPair release websocket connection pair
func (m *WssManager) ReleaseWssConnPair(traceID string, isCloseNormal bool) {
	m.wssCacheLock.Lock()
	cache := m.wssCache[traceID]
	delete(m.wssCache, traceID)
	m.wssCacheLock.Unlock()
	actualEndTime := time.Now().UTC()
	concurrentStartTime := runtime.ConResource.StopOccupation(twoWayCommunication, false, false)
	runtime.ConResource.AppendTimeWindows(concurrentStartTime, actualEndTime, constants.InvokeStage)
	if cache != nil {
		var runtimeErr *runtime.Error
		if !isCloseNormal {
			runtimeErr = GetWebRuntimeException()
		}
		if runtimeErr != nil {
			log.GetLogger().Infof("ws connection released and got some runtime exception, "+
				"traceID %s, code %d runtimeErr: %s", traceID, runtimeErr.Code, runtimeErr.Message)
		}
		cache.Summary.FinishWithEndTime(runtimeErr, actualEndTime)
		logRecorder := m.functionLogger.GetLogRecorder(cache.Ctx)
		logRecorder.FinishFooter(cache.Ctx)
		invokectx.DefaultMetrics.ReleaseInvokeCtx(cache.Ctx)
	}
	runtime.RunMetrics.DecreaseRequestCount(traceID)
	runtime.RunMetrics.DecrWsConnCount()
}

// GracefulShutdown shutdown websocket connection gracefully
func (m *WssManager) GracefulShutdown() {
	done := make(chan struct{})
	timer := time.NewTimer(wsConnGracefulShutdownTimeout)
	defer timer.Stop()
	go func() {
		m.wssCacheLock.RLock()
		if len(m.wssCache) == 0 {
			close(done)
			m.wssCacheLock.RUnlock()
			return
		}
		m.wssCacheLock.RUnlock()
		ticker := time.NewTicker(checkReleaseInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.wssCacheLock.RLock()
				if len(m.wssCache) == 0 {
					close(done)
					m.wssCacheLock.RUnlock()
					return
				}
				m.wssCacheLock.RUnlock()
			case <-timer.C:
				log.GetLogger().Warn("websocket conn graceful shutdown timeout")
				close(done)
				return
			}
		}
	}()
	select {
	case _, ok := <-done:
		if !ok {
			log.GetLogger().Infof("websocket all connection pairs has been released")
			return
		}
	}
}

func (p *WssConnectionPair) createStreams(opt *commonwebsocket.StreamOption, finished chan<- int) {
	downstream, err := commonwebsocket.Connect(opt.DownstreamOption)
	if err != nil {
		log.GetLogger().Errorf("downstream websocket connect failed, error: %s", err.Error())
		finished <- downStreamFail
		return
	}
	p.Downstream = downstream
	upstream, err := commonwebsocket.Connect(opt.UpstreamOption)
	if err != nil {
		log.GetLogger().Errorf("upstream websocket connect failed, error: %s", err.Error())
		if p.Downstream != nil {
			p.Downstream.Close()
		}
		finished <- upStreamFail
		return
	}
	p.Upstream = upstream
	finished <- success
}

func (p *WssConnectionPair) release(err error) {
	p.releaseOnce.Do(func() {
		log.GetLogger().Infof("release websocket connection pair for %s", p.TraceID)
		p.Upstream.Close()
		p.Downstream.Close()
		wsmanager := Instance()
		wsmanager.ReleaseWssConnPair(p.TraceID, checkWsConnIsCloseNormal(err))
	})
}

func checkWsConnIsCloseNormal(err error) bool {
	if err == nil {
		return true
	}
	return websocket.IsCloseError(err, websocket.CloseNormalClosure)
}

// SendMsgToRDispatcher -
func SendMsgToRDispatcher(msg string) {
	wsManager := Instance()
	if wsManager == nil {
		log.GetLogger().Warnf("it will not send message to dispatcher because websocket manager is nil")
		return
	}
	var connPair *WssConnectionPair
	wsManager.wssCacheLock.RLock()
	if len(wsManager.wssCache) == 0 {
		wsManager.wssCacheLock.RUnlock()
		log.GetLogger().Warnf("it will not send message to dispatcher because websocket cache length is 0")
		return
	}
	for _, pair := range wsManager.wssCache {
		if pair != nil {
			// it is only one connection pair when open remote debug
			connPair = pair
			break
		}
	}
	wsManager.wssCacheLock.RUnlock()
	if connPair != nil {
		connPair.sendMsgToRDispatcher(msg)
	}
}

func (p *WssConnectionPair) sendMsgToRDispatcher(msg string) {
	if p.Upstream == nil {
		log.GetLogger().Warnf("websocket connection upstream is nil")
		return
	}
	msgTraceID, wrappedMsgBytes, err := commonwebsocket.WrapSystemInfo([]byte(msg))
	if err != nil {
		log.GetLogger().Warnf("websocket connection upstream wrong message, error: %s", err.Error())
		return
	}
	// send message to downstream.ForwardChan, it will send message to upstream(rdispatcher)
	err = p.Upstream.TrySendMessage(commonwebsocket.WssMessage{
		MessageType: websocket.TextMessage,
		Data:        wrappedMsgBytes,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to send message to dispatcher, remote: %s, websocketTraceID: %s, "+
			"msgTraceID: %s, error: %s", p.Upstream.Endpoint, p.TraceID, msgTraceID, err.Error())
		return
	}
	log.GetLogger().Debugf("succeed to send message to dispatcher, remote: %s, websocketTraceID: %s, msgTraceID: %s",
		p.Upstream.Endpoint, p.TraceID, msgTraceID)
}

// HandleMessage begin handle two-way websocket message
func (p *WssConnectionPair) HandleMessage() {
	log.GetLogger().Infof("Begin handle websocket message")
	// upstream is linked to rdispatcher, downstream is linked to runtime or debug-agent
	// worker -> rdispatcher
	p.forwardMessage(p.Downstream, p.Upstream, commonwebsocket.WrapSystemInfo)
	// rdispatcher -> worker
	p.forwardMessage(p.Upstream, p.Downstream, commonwebsocket.UnwrapSystemInfo)
}

func (p *WssConnectionPair) forwardMessage(from *commonwebsocket.WssConnection, to *commonwebsocket.WssConnection,
	msgHandler func(data []byte) (string, []byte, error)) {
	go func() {
		for {
			select {
			case _, ok := <-from.CloseChan:
				if !ok {
					log.GetLogger().Infof("received wss termination signal, stop read message from %s", from.Endpoint)
					return
				}
			default:
				messageType, data, err := from.Connection.ReadMessage()
				if err != nil {
					log.GetLogger().Warnf("read message failed, remote: %s, err: %s", from.Endpoint, err.Error())
					p.release(err)
					return
				}
				msgTraceID, data, err := msgHandler(data)
				if err != nil {
					log.GetLogger().Errorf("websocket connection traceID: %s | failed to get traceID from "+
						"websocket message, remote: %s, err: %s", p.TraceID, from.Endpoint, err.Error())
					p.release(err)
					return
				}
				log.GetLogger().Infof("websocket connection traceID: %s | websocket message traceID: %s | "+
					"received wss message stop read message from %s", p.TraceID, msgTraceID, from.Endpoint)
				to.ForwardChan <- commonwebsocket.WssMessage{MessageType: messageType, Data: data}
			}
		}
	}()
	go func() {
		for {
			select {
			case message := <-to.ForwardChan:
				if err := to.TrySendMessage(message); err != nil {
					log.GetLogger().Errorf("send message failed, remote: %s, err: %s",
						to.Endpoint, err.Error())
					p.release(err)
					return
				}
			case _, ok := <-to.CloseChan:
				if !ok {
					log.GetLogger().Infof("received wss termination signal, stop send message to %s",
						to.Endpoint)
					return
				}
			}
		}
	}()
}

// ChangeWebSocketIptables -
func (m *WssManager) ChangeWebSocketIptables(connReq *schedulerapi.CreateWebSocketConnectionReq) error {
	if net.ParseIP(connReq.WebSocketConfig.TargetIP) == nil {
		log.GetLogger().Errorf("failed to get target ip from webSocketConfig")
		return errors.New("failed to get target ip from webSocketConfig")
	}
	targetPort, err := strconv.Atoi(connReq.WebSocketConfig.TargetPort)
	if err != nil {
		log.GetLogger().Errorf("failed to get target port from webSocketConfig")
		return err
	}
	if targetPort < utils.MinPort || targetPort > utils.MaxPort {
		log.GetLogger().Errorf("failed to get target port from webSocketConfig")
		return errors.New("failed to get target port from webSocketConfig")
	}
	m.wssIptablesLock.Lock()
	defer m.wssIptablesLock.Unlock()
	isIptableExist, ok := m.wssIptablesCache[connReq.WebSocketConfig.TargetIP+connReq.WebSocketConfig.TargetPort]
	if ok && isIptableExist { // Avoid setting duplicate iptable rules.
		return nil
	}
	err = executeWebSocketIptablesCommand(connReq)
	if err != nil {
		return err
	}

	m.wssIptablesCache[connReq.WebSocketConfig.TargetIP+connReq.WebSocketConfig.TargetPort] = true

	return nil
}

func executeWebSocketIptablesCommand(connReq *schedulerapi.CreateWebSocketConnectionReq) error {
	startTime := time.Now()
	cmdLine := fmt.Sprintf("iptables -I OUTPUT -p tcp -m tcp --dport %s -d %s -j ACCEPT",
		connReq.WebSocketConfig.TargetPort, connReq.WebSocketConfig.TargetIP)

	ret, result, err := network.ExecuteShellCommandLineWithCombinedOutput(cmdLine)

	if err != nil {
		log.GetLogger().Errorf("failed to add iptables rule for ip and port %s:%s",
			connReq.WebSocketConfig.TargetIP, connReq.WebSocketConfig.TargetPort)
		return err
	}
	if ret == utils.IptablesCmdSucceed ||
		(ret == utils.IptablesCmdFailed && strings.Contains(result, utils.IptablesExists)) {
		log.GetLogger().Infof("execute command '%s' ok, cost: %+v, ret: %d, result=%s.",
			cmdLine, time.Since(startTime), ret, result)
		return nil
	}
	log.GetLogger().Errorf("execute command '%s' not ok, cost: %+v, ret: %d, result=%s.",
		cmdLine, time.Since(startTime), ret, result)
	return errors.New("unexpected returned value")
}
