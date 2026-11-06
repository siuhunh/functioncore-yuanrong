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

// Package webshell define function to exec webshell
package webshell

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/web-terminal/config"
)

// SafeWsConn -
type SafeWsConn struct {
	*websocket.Conn
	sync.RWMutex
}

func newSafeWsConn(conn *websocket.Conn) *SafeWsConn {
	safeWsConn := &SafeWsConn{
		Conn: conn,
	}
	safeWsConn.setUp()
	return safeWsConn
}

// SetDeadline -
func (s *SafeWsConn) SetDeadline(t time.Time) error {
	err := s.SetReadDeadline(t)
	if err != nil {
		log.GetLogger().Errorf("failed to SetDeadline, err : %s", err.Error())
		return err
	}
	return s.SetWriteDeadline(t)
}

// WriteControlMessage -
func (s *SafeWsConn) WriteControlMessage(messageType int) error {
	s.Lock()
	defer s.Unlock()
	return s.Conn.WriteControl(messageType, []byte(""), time.Now().Add(time.Second))
}

// WriteMessage -
func (s *SafeWsConn) WriteMessage(messageType int, data []byte) error {
	s.Lock()
	defer s.Unlock()
	return s.Conn.WriteMessage(messageType, data)
}

// setUp -
func (s *SafeWsConn) setUp() {
	pingPongEnable, pingPongInterval := config.GetPingPongConfig()
	if !pingPongEnable {
		return
	}
	err := s.SetDeadline(time.Now().Add(time.Duration(pingPongInterval) * time.Second))
	if err != nil {
		log.GetLogger().Errorf("failed to SetDeadline, err : %s", err.Error())
		return
	}
	s.SetPongHandler(func(appData string) error {
		return s.SetDeadline(time.Now().Add(time.Duration(pingPongInterval) * time.Second))
	})
	s.SetPingHandler(func(appData string) error {
		err := s.WriteMessage(websocket.PongMessage, []byte{})
		if err != nil {
			log.GetLogger().Errorf("failed to write pong message, err: %s", err.Error())
		}
		return s.SetDeadline(time.Now().Add(time.Duration(pingPongInterval) * time.Second))
	})
}
