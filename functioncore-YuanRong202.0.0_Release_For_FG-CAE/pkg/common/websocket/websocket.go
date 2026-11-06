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
	"errors"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
)

// WssConnection single websocket connection
type WssConnection struct {
	Endpoint    string
	Connection  *websocket.Conn
	ForwardChan chan WssMessage `json:"-"` // message to be forwarded
	CloseChan   chan struct{}
	IsClosed    uint32
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
)

func (c *WssConnection) isConnClosed() bool {
	return atomic.LoadUint32(&c.IsClosed) == 1
}

// TrySendMessage send message
func (c *WssConnection) TrySendMessage(message WssMessage) error {
	if c.isConnClosed() {
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
	close(c.CloseChan)
	err := c.Connection.Close()
	if err != nil {
		log.GetLogger().Errorf("close %s websocket connection failed, err: %s", c.Endpoint, err.Error())
	}
	return
}
