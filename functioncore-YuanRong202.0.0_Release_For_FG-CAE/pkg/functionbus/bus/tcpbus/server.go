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

// Package tcpbus contains interface of connection and basic codes
package tcpbus

import (
	"crypto/tls"
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	errcode "functioncore/pkg/functionbus/proxyerrors"
)

var (
	clientsMap = &sync.Map{}
	mutex      sync.Mutex
	// InitTimeout init timeout
	InitTimeout int64
	// BusinessType business type
	BusinessType string
)

// AuthCheckInfo define key and resource from worker
type AuthCheckInfo struct {
	// Key is sign key
	Key string
	// Source define key source
	Source string
}

// StartServerWithTLS build a socket tls client connection by ServerOption
func StartServerWithTLS(opt ServerOption) error {
	config := commonTLS.GetClientTLSConfig()
	serverNet, err := tls.Listen("tcp", opt.ListenAddr, config)
	if err != nil {
		log.GetLogger().Errorf("failed to the listen connect on %s: %s", opt.ListenAddr, err.Error())
		return err
	}
	log.GetLogger().Infof("waiting for client to connect(%s)...", opt.ListenAddr)
	clientsMap = &sync.Map{}
	go func(TlsListen net.Listener) {
		defer TlsListen.Close()
		for {
			conn, acceptErr := TlsListen.Accept()
			if acceptErr != nil {
				log.GetLogger().Errorf("failed to accept connection: %s", acceptErr)
				break
			}
			log.GetLogger().Debugf("accept a new connection %s", getConnInfo(conn))
			go ProcessNewConn(opt, conn)
		}
	}(serverNet)
	return err
}

// StartServer build a socket client connection by ServerOption
func StartServer(opt ServerOption) error {
	tcpAddr, err := net.ResolveTCPAddr("tcp", opt.ListenAddr)
	if err != nil {
		log.GetLogger().Errorf("failed to resolve tcp addr %s", err.Error())
		return err
	}
	serverNet, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.GetLogger().Errorf("failed to the listen connection on %s: %s", opt.ListenAddr, err)
		return nil
	}
	log.GetLogger().Infof("waiting for client to connect(%s)...", opt.ListenAddr)
	clientsMap = &sync.Map{}
	go func(netListen net.TCPListener) {
		defer netListen.Close()
		for {
			conn, err := netListen.Accept()
			if err == nil {
				log.GetLogger().Debugf("accept a new connection %s", getConnInfo(conn))
				go ProcessNewConn(opt, conn)
				continue
			}
			log.GetLogger().Errorf("failed to accept connection: %s", err)
			break
		}
	}(*serverNet)
	log.GetLogger().Infof("server start to listen tcp addr %s", opt.ListenAddr)
	return nil
}

// ProcessNewConn process new connection
func ProcessNewConn(opt ServerOption, conn net.Conn) {
	connection := &Connection{}
	peerIP := conn.RemoteAddr().String()
	if clientsMap == nil {
		return
	}
	mutex.Lock()
	clientIf, ok := clientsMap.Load(peerIP)
	if !ok {
		handleNewClient(opt, conn)
		mutex.Unlock()
		return
	}
	mutex.Unlock()
	if connection, ok = clientIf.(*Connection); !ok {
		log.GetLogger().Errorf("failed to assert connection type" + conn.RemoteAddr().String())
		return
	}
	if !connection.isConnect {
		log.GetLogger().Infof("connection of %s is closed", connection.PeerIP)
		if err := conn.Close(); err != nil {
			log.GetLogger().Warnf("failed to close socket: %s", err)
			return
		}
	}
	if err := connection.netPool.Put(conn); err != nil {
		log.GetLogger().Warnf("failed to put socket in pool: %s", err)
		if conn.Close() != nil {
			log.GetLogger().Warnf("failed to close socket: %s", err)
		}
		return
	}
}

func handleNewClient(opt ServerOption, conn net.Conn) {
	peerIP := getPeerIP(conn.RemoteAddr().String())
	receiveChan := make(chan interface{})
	connWG := &sync.WaitGroup{}
	isMsgChanOpen := int32(1)
	poolOpt := pool.Options{
		ConnHandler:      handleConn,
		MaxConns:         defaultConns,
		MsgChan:          receiveChan,
		ConnWG:           connWG,
		IsMsgChanOpen:    &isMsgChanOpen,
		IsServer:         opt.IsServer,
		DisableHeartbeat: opt.DisableHeartbeat,
	}
	p, err := pool.NewConnPool(poolOpt)
	if err != nil {
		log.GetLogger().Errorf("server failed to create connection for client %s", peerIP)
		return
	}
	if err = p.Put(conn); err != nil {
		log.GetLogger().Errorf("failed to put conn %s to pool: %s", getConnInfo(conn), err.Error())
		return
	}
	connection := &Connection{
		pending:         &sync.Map{},
		wg:              &sync.WaitGroup{},
		msgChan:         receiveChan,
		mux:             sync.RWMutex{},
		netPool:         p,
		isConnect:       true,
		connectionType:  serverConnection,
		isTrustedClient: !common.FConfig.ConnectionAuthCheck,
		PeerIP:          peerIP,
		DisabledSignal:  make(chan struct{}, 1),
	}
	connection.reqHandler = func(payload []byte, peerIP string, w func(types.Result)) {
		w(opt.Receiver.HandleRequest(payload, peerIP, connection))
	}
	if err = opt.Receiver.RegisterService(connection); err != nil {
		log.GetLogger().Errorf("failed to registry client %s: %s", peerIP, err)
		return
	}
	if clientsMap == nil {
		clientsMap = &sync.Map{}
	}
	clientsMap.Store(peerIP, connection)
	go connection.watchClientDisable(connWG, &isMsgChanOpen)
	go connection.serverListenMsg()
	log.GetLogger().Debugf("server received a new socket and created connection %s", getConnInfo(conn))
}

func (c *Connection) serverListenMsg() {
	t := time.NewTimer(authCheckTimeout)
	for {
		select {
		case <-t.C:
			if !c.isTrustedClient {
				log.GetLogger().Warnf("the client %s authentication is not performed after 5s,"+
					"remove it", c.PeerIP)
				c.forceStop()
				return
			}
		case msgIf, ok := <-c.msgChan:
			if !ok {
				log.GetLogger().Warnf("received chan of client has been closed %s", c.PeerIP)
				c.clearPendingReq()
				return
			}
			msg, ok := msgIf.(*protocol.Message)
			if !ok {
				log.GetLogger().Warnf("failed to assert received message type")
				continue
			}
			if !c.isTrustedClient && msg.Header.MsgType != protocol.AuthCheckMsg {
				log.GetLogger().Warnf("connection authentication %s is not performed and business "+
					"requests are not processed", c.PeerIP)
				continue
			}
			c.processMsg(msg)
		}
	}
}

func (c *Connection) processMsg(msg *protocol.Message) {
	switch msg.Header.MsgType {
	case protocol.CallReqMsg: // received post message
		c.wg.Add(1)
		if c.callback != nil {
			c.processCallReqMsg(msg)
		} else {
			go c.processCallReqMsg(msg)
		}
	case protocol.CallRspMsg: // received normal response message
		go c.processCallRspMsg(msg)
	case protocol.AuthCheckMsg:
		go c.processAuthCheckMsg(msg)
	case protocol.CtrlRuntimeMsg:
		c.wg.Add(1)
		go c.processCallReqMsg(msg)
	default:
		log.GetLogger().Warnf("the server receive other message type %d from client %s, skip",
			c.PeerIP, msg.Header.MsgType)
	}
}

// watchClientDisable server watch client is weather disabled
func (c *Connection) watchClientDisable(connWG *sync.WaitGroup, isMsgChanOpen *int32) {
	c.netPool.DisableInformer()
	log.GetLogger().Infof("the server watched client of %s is disabled", c.PeerIP)
	c.mux.Lock()
	c.isConnect = false

	setSignal(c.DisabledSignal)
	if clientsMap != nil {
		clientsMap.Delete(c.PeerIP)
	}
	c.mux.Unlock()

	atomic.StoreInt32(isMsgChanOpen, 0)
	connWG.Wait()
	log.GetLogger().Debugf("all connections exited %s", c.PeerIP)
	close(c.msgChan)
}

func (c *Connection) processAuthCheckMsg(msg *protocol.Message) {
	msg.Header.MsgType = protocol.CallRspMsg
	defer func() {
		err := c.send(msg)
		if err != nil {
			log.GetLogger().Errorf("failed to send auth check result to client %s", c.PeerIP)
		}
		mp.Put(msg)
	}()
	auth := &AuthCheckInfo{}
	err := json.Unmarshal(msg.Payload, auth)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal auth check info: %s", err.Error())
		msg.Header.ErrCode = errcode.AuthErrCode
		c.forceStop()
		return
	}
	key, err := localauth.Decrypt(auth.Key)
	if err != nil {
		log.GetLogger().Errorf("failed to decry auth check info: %s", err.Error())
		msg.Header.ErrCode = errcode.AuthErrCode
		c.forceStop()
		return
	}
	keyString := string(key)
	utils.ClearByteMemory(key)
	utils.ClearStringMemory(keyString)
	c.isTrustedClient = true
	log.GetLogger().Infof("check %s's auth successfully", c.PeerIP)
	return
}
