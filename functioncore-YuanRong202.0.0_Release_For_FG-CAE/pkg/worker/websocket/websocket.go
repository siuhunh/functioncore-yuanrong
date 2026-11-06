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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
)

const (
	defaultHandshakeTimeout = 5 * time.Second
)

// WssConnectionPair websocket connection pair
type WssConnectionPair struct {
	TraceID     string
	Upstream    *commonwebsocket.WssConnection
	Downstream  *commonwebsocket.WssConnection
	releaseOnce sync.Once
}

// WssManager manage all websocket connection
type WssManager struct {
	wssCache     map[string]*WssConnectionPair // key是traceID，value是对应的连接对
	wssCacheLock sync.RWMutex
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
	headerTimestamp      = "X-CFF-Timestamp-Auth"
	headerAuthorization  = "X-CFF-Authorization"
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
		manager = &WssManager{
			wssCache:     make(map[string]*WssConnectionPair),
			wssCacheLock: sync.RWMutex{},
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
	upstreamURL := fmt.Sprintf("%s://%s:%s/v2/fgs/i/worker/%s/%s/%s/websocket", websocketTag,
		req.WebSocketConfig.TargetIP, req.WebSocketConfig.TargetPort, podName, traceID, req.FunctionKey)
	return &commonwebsocket.StreamOption{
		UpstreamOption: &commonwebsocket.SingleStreamOption{
			URL:         upstreamURL,
			HTTPSEnable: conf.HTTPSEnable,
			Auth:        conf.AuthConfig,
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
	runtime.RunMetrics.IncreaseRequestCount(traceID)
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
func (m *WssManager) ReleaseWssConnPair(traceID string) {
	m.wssCacheLock.Lock()
	delete(m.wssCache, traceID)
	if enableRemoteDebug && len(m.wssCache) == 0 {
		log.GetLogger().Infof("remote debug, connection pair is released, start to exit")
		runtime.CloseExitChannel()
	}
	m.wssCacheLock.Unlock()
	runtime.RunMetrics.DecreaseRequestCount(traceID)
	concurrentStartTime := runtime.ConResource.StopOccupation(twoWayCommunication, false, false)
	runtime.ConResource.AppendTimeWindows(concurrentStartTime, time.Now().UTC(), constants.InvokeStage)
}

// GracefulShutdown shutdown websocket connection gracefully
func (m *WssManager) GracefulShutdown() {
	done := make(chan struct{})
	ticker := time.NewTicker(checkReleaseInterval)
	defer ticker.Stop()
	go func() {
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
	downstream, err := wssConnect(opt.DownstreamOption)
	if err != nil {
		log.GetLogger().Errorf("downstream websocket connect failed, error: %s", err.Error())
		finished <- downStreamFail
		return
	}
	p.Downstream = downstream
	upstream, err := wssConnect(opt.UpstreamOption)
	if err != nil {
		log.GetLogger().Errorf("upstream websocket connect failed, error: %s", err.Error())
		finished <- upStreamFail
		return
	}
	p.Upstream = upstream
	finished <- success
}

func (p *WssConnectionPair) release() {
	p.releaseOnce.Do(func() {
		log.GetLogger().Infof("release websocket connection pair for %s", p.TraceID)
		p.Upstream.Close()
		p.Downstream.Close()
		wsmanager := Instance()
		wsmanager.ReleaseWssConnPair(p.TraceID)
	})
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
	msgTraceID, wrappedMsgBytes, err := wrapSystemInfo([]byte(msg))
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

func wrapSystemInfo(data []byte) (string, []byte, error) {
	traceID := uuid.New().String()
	dataWithTraceID := WssMessageWrapper{
		Data:    data,
		TraceID: traceID,
	}
	dataWithTraceIDEncode, err := json.Marshal(dataWithTraceID)
	return traceID, dataWithTraceIDEncode, err
}

func unwrapSystemInfo(data []byte) (string, []byte, error) {
	var dataWithTraceIDDecode WssMessageWrapper
	err := json.Unmarshal(data, &dataWithTraceIDDecode)
	return dataWithTraceIDDecode.TraceID, dataWithTraceIDDecode.Data, err
}

// HandleMessage begin handle two-way websocket message
func (p *WssConnectionPair) HandleMessage() {
	log.GetLogger().Infof("Begin handle websocket message")
	// upstream is linked to rdispatcher, downstream is linked to runtime or debug-agent
	// worker -> rdispatcher
	p.forwardMessage(p.Downstream, p.Upstream, wrapSystemInfo)
	// rdispatcher -> worker
	p.forwardMessage(p.Upstream, p.Downstream, unwrapSystemInfo)
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
					p.release()
					return
				}
				msgTraceID, data, err := msgHandler(data)
				if err != nil {
					log.GetLogger().Errorf("websocket connection traceID: %s | failed to get traceID from "+
						"websocket message, remote: %s, err: %s", p.TraceID, from.Endpoint, err.Error())
					p.release()
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
					p.release()
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

func createWssConnection(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
	dial := websocket.DefaultDialer
	dial.HandshakeTimeout = defaultHandshakeTimeout
	header := make(http.Header)
	if opt.HTTPSEnable {
		dial.TLSClientConfig = tls.GetClientTLSConfig()
		authorization, timestamp := localauth.CreateAuthorization(opt.Auth.AKey, opt.Auth.SKey, "",
			"", nil)
		header.Set(headerTimestamp, timestamp)
		header.Set(headerAuthorization, authorization)
	}
	conn, _, err := dial.Dial(opt.URL, header)
	if err != nil {
		return nil, err
	}
	wssConn := &commonwebsocket.WssConnection{
		Endpoint:    opt.URL,
		Connection:  conn,
		ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		CloseChan:   make(chan struct{}),
		IsClosed:    uint32(0),
	}
	return wssConn, nil
}

func wssConnect(opt *commonwebsocket.SingleStreamOption) (*commonwebsocket.WssConnection, error) {
	for i := 0; i < commonwebsocket.MaxSendRetryTimes; i++ {
		conn, err := createWssConnection(opt)
		if err == nil {
			return conn, nil
		}
		log.GetLogger().Errorf("create wss connection for %s failed, err: %s", opt.URL, err.Error())
		time.Sleep(commonwebsocket.DialInterval)
	}
	return nil, fmt.Errorf("after %d attempts, failed to connect to %s",
		commonwebsocket.MaxSendRetryTimes, opt.URL)
}
