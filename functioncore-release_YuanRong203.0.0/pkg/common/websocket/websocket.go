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

// Package websocket operates websocket
package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"functioncore/pkg/common/cffclient"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/uuid"
)

// WssConnection single websocket connection
type WssConnection struct {
	Endpoint    string
	Connection  *websocket.Conn
	ForwardChan chan WssMessage `json:"-"` // message to be forwarded
	CloseChan   chan struct{}
	IsClosed    uint32
	ClientName  string
}

// WssMessage websocket message
type WssMessage struct {
	MessageType int
	Data        []byte
}

// StreamOption stream option
type StreamOption struct {
	UpstreamOption   *SingleStreamOption
	DownstreamOption *SingleStreamOption
}

// SingleStreamOption single stream option
type SingleStreamOption struct {
	URL         string
	HTTPSEnable bool
	Auth        localauth.AuthConfig
	Internal    bool
}

// WssMessageWrapper -
type WssMessageWrapper struct {
	Data    []byte `json:"data" valid:",optional"`
	TraceID string `json:"traceID" valid:",optional"`
}

const (
	// RuntimeWSPort is runtime websocket port
	RuntimeWSPort = "8000"
	// DebugRuntimeWSPort debug-agent runtime websocket port
	DebugRuntimeWSPort = "8800"
	// DebugAgentWSPort is debug-agent websocket port
	DebugAgentWSPort = "8801"
	// ForwardChanSize is forward channel size
	ForwardChanSize = 10
	// MaxSendRetryTimes is max send retry times
	MaxSendRetryTimes = 3
	// DialInterval is dial interval
	DialInterval = 1 * time.Second

	defaultHandshakeTimeout = 5 * time.Second
)

var (
	upgrade = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

// IsConnClosed check the conn is closed
func (c *WssConnection) IsConnClosed() bool {
	return atomic.LoadUint32(&c.IsClosed) == 1
}

// TrySendMessage send message
func (c *WssConnection) TrySendMessage(message WssMessage) error {
	if c.IsConnClosed() {
		return errors.New("connection is closed")
	}
	var err error
	for i := 0; i < MaxSendRetryTimes; i++ {
		err = c.Connection.WriteMessage(message.MessageType, message.Data)
		if err == nil {
			return nil
		}
	}
	return err
}

// Close shutdown the websocket connection
func (c *WssConnection) Close() {
	if !atomic.CompareAndSwapUint32(&c.IsClosed, 0, 1) {
		log.GetLogger().Infof("%s websocket connection has been closed", c.Endpoint)
		return
	}
	if c.CloseChan != nil {
		close(c.CloseChan)
	}
	err := c.Connection.Close()
	if err != nil {
		log.GetLogger().Errorf("close %s websocket connection failed, err: %s", c.Endpoint, err.Error())
	}
	return
}

// ReceiveMessage -
func (c *WssConnection) ReceiveMessage() (WssMessage, error) {
	if c.Connection == nil {
		return WssMessage{}, fmt.Errorf("connection not established")
	}

	messageType, data, err := c.Connection.ReadMessage()
	return WssMessage{MessageType: messageType, Data: data}, err
}

// ToString -
func (c *WssConnection) ToString() string {
	return fmt.Sprintf("websocket connection: hostip[%s], clientname[%s], clientip[%s]",
		c.Connection.LocalAddr().String(), c.ClientName, c.Connection.RemoteAddr().String())
}

func createWssConnection(opt *SingleStreamOption) (*WssConnection, error) {
	dial := websocket.DefaultDialer
	dial.HandshakeTimeout = defaultHandshakeTimeout
	header := make(http.Header)
	if opt.HTTPSEnable {
		dial.TLSClientConfig = tls.GetClientTLSConfig()
		if opt.Internal {
			authorization, timestamp := localauth.CreateAuthorization(opt.Auth.AKey, opt.Auth.SKey, "",
				"internal", nil)
			header.Set(constants.HeaderAuthTimestamp, timestamp)
			header.Set(constants.HeaderAuthorization, authorization)
		} else {
			authorization, timestamp := localauth.CreateAuthorization(opt.Auth.AKey, opt.Auth.SKey, "",
				"", nil)
			header.Set(cffclient.HeaderAuthTimestamp, timestamp)
			header.Set(cffclient.HeaderAuthorization, authorization)
		}
	}
	conn, _, err := dial.Dial(opt.URL, header)
	if err != nil {
		return nil, err
	}
	return NewWssConnection(opt.URL, conn), nil
}

// Connect -
func Connect(opt *SingleStreamOption) (*WssConnection, error) {
	for i := 0; i < MaxSendRetryTimes; i++ {
		conn, err := createWssConnection(opt)
		if err == nil {
			return conn, nil
		}
		log.GetLogger().Warnf("create wss connection for %s failed, err: %s", opt.URL, err.Error())
		time.Sleep(DialInterval)
	}
	return nil, fmt.Errorf("after %d attempts, failed to connect to %s", MaxSendRetryTimes, opt.URL)
}

// Upgrade -
func Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*websocket.Conn, error) {
	r.Method = http.MethodGet
	conn, err := upgrade.Upgrade(w, r, responseHeader)
	if err != nil {
		log.GetLogger().Errorf("failed to upgrade websocket, err %s", err.Error())
		return nil, err
	}
	return conn, nil
}

// NewWssConnection -
func NewWssConnection(url string, conn *websocket.Conn) *WssConnection {
	return &WssConnection{
		Endpoint:    url,
		Connection:  conn,
		ForwardChan: make(chan WssMessage, ForwardChanSize),
		CloseChan:   make(chan struct{}),
		IsClosed:    uint32(0),
	}
}

// UnwrapSystemInfo -
func UnwrapSystemInfo(data []byte) (string, []byte, error) {
	var dataWithTraceIDDecode WssMessageWrapper
	if err := json.Unmarshal(data, &dataWithTraceIDDecode); err != nil {
		return "", nil, err
	}
	return dataWithTraceIDDecode.TraceID, dataWithTraceIDDecode.Data, nil
}

// WrapSystemInfo -
func WrapSystemInfo(data []byte) (string, []byte, error) {
	traceID := uuid.New().String()
	dataWithTraceID := WssMessageWrapper{
		Data:    data,
		TraceID: traceID,
	}
	dataWithTraceIDEncode, err := json.Marshal(dataWithTraceID)
	return traceID, dataWithTraceIDEncode, err
}

// WaitUtilInterrupt wait until interrupt
func WaitUtilInterrupt(stopCh <-chan struct{}, server *http.Server) {
	if stopCh == nil {
		log.GetLogger().Errorf("waitUtilInterrupt's stopCh is nil")
		return
	}
	select {
	case _, ok := <-stopCh:
		if !ok {
			log.GetLogger().Warnf("stopCh closed")
		}
		// We received an interrupt signal, shutdown main goroutine.
		if err := server.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.GetLogger().Errorf("http server shutdown error: %v", err)
		}
	}
}
