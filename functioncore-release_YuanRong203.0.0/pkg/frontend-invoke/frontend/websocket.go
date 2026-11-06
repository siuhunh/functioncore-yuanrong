/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package frontend deal with websocket
package frontend

import (
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/websocket"
	"functioncore/pkg/frontend-invoke/constant"
)

const (
	frontendWSPort = "8889"
	podNameParam   = "podName"
	requestIDParam = "requestId"
	projectIDParam = "projectId"
	versionParam   = "version"
)

var (
	frontendPodIP = os.Getenv("POD_IP")
	socketOnce    sync.Once
	manager       *WebsocketManager
)

// WssMsgHandlerFunc -
type WssMsgHandlerFunc func(data []byte) (string, []byte, error)

// WebSocketWatcher -
type WebSocketWatcher struct {
	TraceID string
	// projectID/0@default@funcName/version
	FuncKey      string
	Upstream     *websocket.WssConnection
	Downstream   *websocket.WssConnection
	upstreamIP   string
	upstreamPort string
	releaseOnce  sync.Once
}

// WebsocketManager -
type WebsocketManager struct {
	socketManager map[string]*WebSocketWatcher // key is TraceID
	socketLock    sync.RWMutex
}

type websocketConnectInfo struct {
	podName   string
	traceID   string
	projectID string
	funcName  string
	version   string
	funcKey   string
}

// WebSocketWorkerConnect -
func WebSocketWorkerConnect(c *gin.Context) {
	reqInfo, err := websocketConnectFilter(c)
	if err != nil {
		log.GetLogger().Errorf("failed to process the request %s, err %s", reqInfo.traceID, err.Error())
		c.Writer.WriteHeader(http.StatusBadRequest)
		return
	}
	log.GetLogger().Infof("receive pod: %s websocket connect request: %s func %s",
		reqInfo.podName, reqInfo.traceID, reqInfo.funcName)
	if err := authCheck(c.Request); err != nil {
		log.GetLogger().Errorf("failed to auth the pod: %s websocket conn request: %s, err %s", reqInfo.podName,
			reqInfo.traceID, err.Error())
		c.Writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	watcher := Instance().GetWebsocketWatcher(reqInfo.traceID)
	if watcher == nil {
		log.GetLogger().Errorf("not found the watcher of the worker websocket request: %s", reqInfo.traceID)
		writeErrResponse(c.Writer, http.StatusBadRequest, "watcher not found")
		return
	}
	if err := validWorkerWebSocketParam(watcher, reqInfo.funcKey); err != nil {
		log.GetLogger().Errorf("failed to valid worker websocket param for request: %s func: %s and pod: %s",
			reqInfo.traceID, reqInfo.funcName, reqInfo.podName)
		writeErrResponse(c.Writer, http.StatusForbidden, err.Error())
		return
	}
	upConn, err := createUpstreamWssConn(watcher.upstreamIP, watcher.upstreamPort, reqInfo.podName,
		reqInfo.traceID, reqInfo.funcKey)
	if err != nil {
		log.GetLogger().Errorf("failed to create the request: %s to %s upstream websocket connection, err: %s",
			reqInfo.traceID, watcher.upstreamIP+":"+watcher.upstreamPort, err.Error())
		writeErrResponse(c.Writer, http.StatusInternalServerError, err.Error())
		return
	}
	watcher.Upstream = upConn
	downConn, err := websocket.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to upgrade websocket request: %s of func: %s and pod: %s, err %s ",
			reqInfo.traceID, reqInfo.funcName, reqInfo.podName, err.Error())
		writeErrResponse(c.Writer, http.StatusInternalServerError, err.Error())
		return
	}
	watcher.Downstream = websocket.NewWssConnection(c.Request.RemoteAddr, downConn)
	watcher.Downstream.ClientName = reqInfo.podName
	log.GetLogger().Infof("succeed to upgrade the request: %s from %s of the func: %s and pod: %s",
		reqInfo.traceID, watcher.Downstream.ToString(), reqInfo.funcName, reqInfo.podName)
	defer watcher.release()
	// forward message worker -> rdispatcher
	go watcher.ForwardMessageLoop(watcher.Downstream, watcher.Upstream, websocket.UnwrapSystemInfo)
	// forward message rdispatcher -> worker
	watcher.ForwardMessageLoop(watcher.Upstream, watcher.Downstream, websocket.UnwrapSystemInfo)
}

func websocketConnectFilter(c *gin.Context) (websocketConnectInfo, error) {
	reqInfo := websocketConnectInfo{
		podName:   c.Param(podNameParam),
		traceID:   c.Param(requestIDParam),
		projectID: c.Param(projectIDParam),
		funcName:  c.Param(functionURNParam),
		version:   c.Param(versionParam),
	}
	if reqInfo.podName == "" {
		return reqInfo, fmt.Errorf("url param podName is nil")
	}
	if reqInfo.traceID == "" {
		return reqInfo, fmt.Errorf("url param requestId is nil")
	}
	if reqInfo.projectID == "" {
		return reqInfo, fmt.Errorf("url param projectID is nil")
	}
	if reqInfo.funcName == "" {
		return reqInfo, fmt.Errorf("url param funcName is nil")
	}
	if reqInfo.version == "" {
		return reqInfo, fmt.Errorf("url param version is nil")
	}
	serviceID, functionName, err := urnutils.GetFunctionNameAndServiceName(reqInfo.funcName)
	if err != nil {
		return reqInfo, err
	}
	if err := urnutils.CheckFunctionName(functionName); err != nil {
		return reqInfo, err
	}
	if err := urnutils.CheckServiceID(serviceID); err != nil {
		return reqInfo, err
	}
	reqInfo.funcKey = urnutils.GetFuncKey(reqInfo.projectID, reqInfo.funcName, reqInfo.version)
	return reqInfo, nil
}

func writeErrResponse(writer gin.ResponseWriter, statusCode int, errMsg string) {
	writer.WriteHeader(statusCode)
	_, err := writer.Write([]byte(errMsg))
	if err != nil {
		log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
	}
}

func createUpstreamWssConn(targetIP, targetPort, podName, traceID,
	functionKey string) (*websocket.WssConnection, error) {
	var websocketTag string
	if constant.FConfig.HTTPSConfig.HTTPSEnable {
		websocketTag = "wss"
	} else {
		websocketTag = "ws"
	}
	upstreamURL := fmt.Sprintf("%s://%s:%s/v2/fgs/i/worker/%s/%s/%s/websocket", websocketTag,
		targetIP, targetPort, podName, traceID, functionKey)
	UpstreamOption := &websocket.SingleStreamOption{
		URL:         upstreamURL,
		HTTPSEnable: constant.FConfig.HTTPSConfig.HTTPSEnable,
		Auth:        *constant.FConfig.LocalAuth,
	}
	conn, err := websocket.Connect(UpstreamOption)
	if err != nil {
		return nil, err
	}
	conn.ClientName = podName
	return conn, nil
}

func validWorkerWebSocketParam(watcher *WebSocketWatcher, funcKey string) error {
	if watcher.FuncKey != funcKey {
		return fmt.Errorf("function key is not match, original is %s, but request is %s",
			watcher.FuncKey, funcKey)
	}
	return nil
}

// Instance -
func Instance() *WebsocketManager {
	socketOnce.Do(func() {
		manager = &WebsocketManager{
			socketManager: make(map[string]*WebSocketWatcher),
			socketLock:    sync.RWMutex{},
		}
	})
	return manager
}

// GetOrAddWebsocketWatcher -
func (m *WebsocketManager) GetOrAddWebsocketWatcher(traceID string) *WebSocketWatcher {
	m.socketLock.RLock()
	watcher, ok := m.socketManager[traceID]
	m.socketLock.RUnlock()
	if !ok {
		m.socketLock.Lock()
		defer m.socketLock.Unlock()
		watcher = &WebSocketWatcher{
			TraceID: traceID,
		}
		watcherGet, ok := m.socketManager[traceID]
		if ok && watcherGet != nil {
			return watcherGet
		}
		m.socketManager[traceID] = watcher
	}
	return watcher
}

// GetWebsocketWatcher -
func (m *WebsocketManager) GetWebsocketWatcher(traceID string) *WebSocketWatcher {
	m.socketLock.RLock()
	defer m.socketLock.RUnlock()
	watcher, ok := m.socketManager[traceID]
	if !ok {
		return nil
	}
	return watcher
}

// ReleaseWebsocketWatcher -
func (m *WebsocketManager) ReleaseWebsocketWatcher(traceID string) {
	m.socketLock.Lock()
	defer m.socketLock.Unlock()
	delete(m.socketManager, traceID)
}

// Shutdown -
func (m *WebsocketManager) Shutdown() {
	m.socketLock.RLock()
	defer m.socketLock.RUnlock()
	for _, watcher := range m.socketManager {
		if watcher.Upstream != nil {
			watcher.Upstream.Close()
		}
		if watcher.Downstream != nil {
			watcher.Downstream.Close()
		}
	}
}

// ForwardMessageLoop -
func (w *WebSocketWatcher) ForwardMessageLoop(from *websocket.WssConnection,
	to *websocket.WssConnection, msgHandler WssMsgHandlerFunc) {
	go w.forwardReceivedMessages(from, to, msgHandler)
	w.sendForwardMessage(to)
}

func (w *WebSocketWatcher) release() {
	w.releaseOnce.Do(func() {
		log.GetLogger().Infof("release websocket connection for %s", w.TraceID)
		if w.Upstream != nil {
			w.Upstream.Close()
		}
		if w.Downstream != nil {
			w.Downstream.Close()
		}
		Instance().ReleaseWebsocketWatcher(w.TraceID)
	})
}

func (w *WebSocketWatcher) forwardReceivedMessages(from, to *websocket.WssConnection, handler WssMsgHandlerFunc) {
	for {
		select {
		case _, ok := <-from.CloseChan:
			if !ok {
				log.GetLogger().Infof("received wss termination signal, stop read message from %s", from.Endpoint)
				return
			}
		default:
			msg, err := from.ReceiveMessage()
			if err != nil {
				log.GetLogger().Warnf("read message failed, remote: %s, err: %s", from.Endpoint, err.Error())
				w.release()
				return
			}
			msgTraceID, _, err := handler(msg.Data)
			if err != nil {
				log.GetLogger().Errorf("websocket connection traceID: %s | failed to get TraceID from "+
					"websocket message, remote: %s, err: %s", w.TraceID, from.Endpoint, err.Error())
				w.release()
				return
			}
			log.GetLogger().Debugf("websocket connection traceID: %s | websocket message traceID: %s | "+
				"received wss message stop read message from %s", w.TraceID, msgTraceID, from.Endpoint)
			to.ForwardChan <- msg
		}
	}
}

func (w *WebSocketWatcher) sendForwardMessage(conn *websocket.WssConnection) {
	for {
		select {
		case message := <-conn.ForwardChan:
			if err := conn.TrySendMessage(message); err != nil {
				log.GetLogger().Errorf("send message failed, remote: %s, err: %s",
					conn.Endpoint, err.Error())
				w.release()
				return
			}
		case _, ok := <-conn.CloseChan:
			if !ok {
				log.GetLogger().Infof("received wss termination signal, stop send message conn %s",
					conn.Endpoint)
				return
			}
		}
	}
}

// ShutDownAllWebsocketConnection All WebSocket connections need to be closed when frontend http server shutdown
func ShutDownAllWebsocketConnection() {
	log.GetLogger().Warnf("start to close all WebSocket connections when frontend http server shutdown")
	Instance().Shutdown()
	log.GetLogger().Warnf("finish to close all WebSocket connections when frontend http server shutdown")
}
