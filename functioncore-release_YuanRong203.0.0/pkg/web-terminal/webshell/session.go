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
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"functioncore/pkg/common/logger/log"
	commonwss "functioncore/pkg/common/websocket"
	"functioncore/pkg/web-terminal/config"
	"functioncore/pkg/web-terminal/constant"
)

const (
	pingWriteInterval = 5 * time.Second
	maxFailPingCnt    = 10
	maxSendRetryTimes = 3
	readTimeout       = 15 * time.Minute
)

var (
	once              = sync.Once{}
	sessionMgr        *SessionManager
	sessionKeyCounter = rand.New(rand.NewSource(int64(time.Now().Nanosecond()))).Int63()
)

type maxPingErrCallBack func(failPingCount int)

// SessionManager -
type SessionManager struct {
	mapAccessLock sync.RWMutex
	sessions      map[string]*SessionPair // key is traceID
}

// Session represent the connection of websocket
type Session struct {
	targetID   string // clusterID_namespace_podName
	sessionID  int64  // global only
	updateTime time.Time
	*SafeWsConn
	pingCancel  context.CancelFunc
	isClient    bool
	forwardChan chan commonwss.WssMessage // message to be forwarded
	closeChan   chan struct{}
	isClosed    uint32
}

// SessionPair represent the connections upStream is between CAE and web-terminal
// downStream is between web-terminal and K8S apiserver
type SessionPair struct {
	traceID     string
	upStream    *Session
	downStream  *Session
	releaseOnce sync.Once
}

// GetSessionMgrInstance get singleton session manager
func GetSessionMgrInstance() *SessionManager {
	once.Do(func() {
		sessionMgr = &SessionManager{
			mapAccessLock: sync.RWMutex{},
			sessions:      make(map[string]*SessionPair),
		}
	})
	return sessionMgr
}

func (sp *SessionPair) release() {
	sp.releaseOnce.Do(func() {
		log.GetLogger().Infof("release websocket connection pair for %s", sp.traceID)
		sp.upStream.Close()
		sp.downStream.Close()
		GetSessionMgrInstance().Remove(sp.traceID)
	})
}

func (sp *SessionPair) startPings(cb maxPingErrCallBack) {
	ctx, cancel := context.WithCancel(context.Background())
	sp.downStream.pingCancel = cancel
	go func() {
		t := time.NewTicker(pingWriteInterval)
		defer t.Stop()
		failPingCnt := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				err := sp.downStream.SafeWsConn.WriteControlMessage(websocket.PingMessage)
				if err != nil {
					log.GetLogger().Errorf("Error writing ping, err is %s, targetID: %s",
						err.Error(), sp.downStream.targetID)
					failPingCnt++
				}
				if failPingCnt >= maxFailPingCnt {
					log.GetLogger().Errorf("Error writing ping beyond %d count in history, must remove this session, targetID: %s",
						maxFailPingCnt, sp.downStream.targetID)
					sp.downStream.pingCancel = nil
					cb(failPingCnt)
					return
				}
			}
		}
	}()
}

func codecData(data []byte, isDecode bool) ([]byte, error) {
	codec := NewSubProtocolCodec()
	if isDecode {
		_, res, err := codec.Decode(data)
		if err != nil {
			return nil, err
		}
		return res, nil
	}
	return codec.Encode(data, STDIN), nil
}

// HandleMessage handle message
func (sp *SessionPair) HandleMessage() {
	log.GetLogger().Infof("begin to handle webshell message, traceID: %s", sp.traceID)
	sp.startPings(func(failPingCount int) {
		log.GetLogger().Errorf("startPing failPingCount %d in history, will close Underlying Conn, "+
			"traceID: %s", failPingCount, sp.traceID)
		sp.release()
	})
	sp.forwardMessage(sp.upStream, sp.downStream, codecData)
	sp.forwardMessage(sp.downStream, sp.upStream, codecData)
}

func (sp *SessionPair) forwardMessage(from *Session, to *Session,
	msgHandler func(data []byte, isDecode bool) ([]byte, error)) {
	go func() {
		for {
			select {
			case _, ok := <-from.closeChan:
				if !ok {
					log.GetLogger().Infof("received wss termination signal, stop read message from %s", from.sessionID)
					return
				}
			default:
				log.GetLogger().Debugf("begin to read msg from remote: %s", from.sessionID)
				err := from.SetReadDeadline(time.Now().Add(readTimeout))
				if err != nil {
					log.GetLogger().Warnf("failed to set read deadline for remote: %s, err: %s", from.sessionID, err.Error())
				}
				messageType, data, err := from.ReadMessage()
				if err != nil {
					log.GetLogger().Warnf("read message failed, remote: %s, err: %s", from.sessionID, err.Error())
					sp.release()
					return
				}
				log.GetLogger().Debugf("read msg from remote: %s success", from.sessionID)
				data, err = msgHandler(data, from.isClient)
				if err != nil {
					log.GetLogger().Errorf("websocket connection traceID: %s | failed to deal with "+
						"websocket message, remote: %s, err: %s", sp.traceID, from.sessionID, err.Error())
					sp.release()
					return
				}
				to.forwardChan <- commonwss.WssMessage{MessageType: messageType, Data: data}
			}
		}
	}()
	go func() {
		for {
			select {
			case message := <-to.forwardChan:
				if err := to.TrySendMessage(message); err != nil {
					log.GetLogger().Errorf("send message failed, remote: %s, err: %s",
						to.sessionID, err.Error())
					sp.release()
					return
				}
			case _, ok := <-to.closeChan:
				if !ok {
					log.GetLogger().Infof("received wss termination signal, stop send message to %s",
						to.sessionID)
					return
				}
			}
		}
	}()
}

func (s *Session) isConnClosed() bool {
	return atomic.LoadUint32(&s.isClosed) == 1
}

// TrySendMessage try to send message
func (s *Session) TrySendMessage(message commonwss.WssMessage) error {
	if s.isConnClosed() {
		return errors.New("connection is closed")
	}
	var err error
	for i := 0; i < maxSendRetryTimes; i++ {
		err = s.WriteMessage(message.MessageType, message.Data)
		if err == nil {
			return nil
		}
	}
	return err
}

// Close define close session
func (s *Session) Close() {
	log.GetLogger().Infof("close session, targetID: %s", s.targetID)
	if !atomic.CompareAndSwapUint32(&s.isClosed, 0, 1) {
		log.GetLogger().Infof("%s_%s websocket connection has been closed, isClient: %s",
			s.targetID, s.sessionID, s.isClient)
		return
	}
	s.stopPings()
	close(s.closeChan)
	_ = s.SafeWsConn.Close()
}

func (s *Session) stopPings() {
	if s.pingCancel == nil {
		return
	}
	s.pingCancel()
}

func nextSessionKey() int64 {
	return atomic.AddInt64(&sessionKeyCounter, 1)
}

// CreateSessionPair define create session pair with cae and k8s apiserver
func (m *SessionManager) CreateSessionPair(traceID string, targetID string,
	upConn *websocket.Conn, downConn *websocket.Conn) *SessionPair {
	m.Add(traceID, targetID, upConn, false)
	m.Add(traceID, targetID, downConn, true)
	return m.Get(traceID)
}

// Add define add session to SessionManager
func (m *SessionManager) Add(traceID string, targetID string, conn *websocket.Conn, isClient bool) {
	newSession := &Session{
		targetID:    targetID,
		sessionID:   nextSessionKey(),
		SafeWsConn:  newSafeWsConn(conn),
		isClient:    isClient,
		forwardChan: make(chan commonwss.WssMessage, commonwss.ForwardChanSize),
		closeChan:   make(chan struct{}),
		isClosed:    uint32(0),
	}

	m.mapAccessLock.Lock()
	defer m.mapAccessLock.Unlock()

	if m.sessions == nil {
		m.sessions = make(map[string]*SessionPair)
	}
	sessionPair := m.sessions[traceID]
	if sessionPair == nil {
		sessionPair = &SessionPair{
			traceID: traceID,
		}
	}
	if isClient {
		sessionPair.downStream = newSession
	} else {
		sessionPair.upStream = newSession
	}
	m.sessions[traceID] = sessionPair
	log.GetLogger().Infof("add session to sessionManager, traceID: %s, targetID: %s, sessionID: %s, isClient: %v",
		traceID, newSession.targetID, newSession.sessionID, isClient)
}

// Get define get session from SessionManager
func (m *SessionManager) Get(traceID string) *SessionPair {
	m.mapAccessLock.RLock()
	defer m.mapAccessLock.RUnlock()
	sessionPair, ok := m.sessions[traceID]
	if !ok {
		return nil
	}
	return sessionPair
}

// Remove define remove session from SessionManager
func (m *SessionManager) Remove(traceID string) {
	m.mapAccessLock.Lock()
	defer m.mapAccessLock.Unlock()
	delete(m.sessions, traceID)
}

// IsReachMaxConnection check whether reach max connections
func (m *SessionManager) IsReachMaxConnection() bool {
	maxSessions := constant.DefaultMaxSessions
	cfg, err := config.GetConfig()
	if err == nil && cfg.MaxSessions != 0 {
		maxSessions = cfg.MaxSessions
	}
	m.mapAccessLock.RLock()
	defer m.mapAccessLock.RUnlock()
	return len(m.sessions) >= maxSessions
}
