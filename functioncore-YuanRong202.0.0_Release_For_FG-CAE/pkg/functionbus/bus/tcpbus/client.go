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
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/common/logger/log"
	commonTLS "functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/pool"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/bus/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/proxyview"
)

// ClientOption defines client properties
type ClientOption struct {
	Config *tls.Config
	// RemoteAddr the remote server address
	ServerAddr string
	// true: tls client, false: tcp client.
	TLSEnable  bool
	NeedVerify bool
	// HINT: Exiting of worker server with podIP is irreversible, retrying on the old ip
	// which may be distributed to another pod for a short time will cause unexpected problem.
	DisableWhenServerExit bool
}

type podName struct {
	PodName string `json:"podName"`
}

const (
	reconnectInterval = 1 * time.Second
	recoverTTL        = 5 * time.Second
)

// BuildClientConnection build a socket server connection by ClientOption
func BuildClientConnection(opt ClientOption) (*Connection, error) {
	receiveChan := make(chan interface{}, defaultChanCap)
	connWG := &sync.WaitGroup{}
	isMsgChanOpen := int32(1)
	poolOpt := initPoolOptions(opt, receiveChan, connWG, isMsgChanOpen)
	p, err := pool.NewConnPool(poolOpt)
	if err != nil {
		log.GetLogger().Errorf("failed to create client's connection to server %s: %s", opt.ServerAddr, err)
		return nil, err
	}
	connection := &Connection{
		pending:               &sync.Map{},
		wg:                    &sync.WaitGroup{},
		msgChan:               receiveChan,
		mux:                   sync.RWMutex{},
		netPool:               p,
		isConnect:             true,
		connectionType:        clientConnection,
		PeerIP:                getPeerIP(opt.ServerAddr),
		DisabledSignal:        make(chan struct{}, 1),
		disableWhenServerExit: opt.DisableWhenServerExit,
	}
	go connection.watchServerDisable(connWG, &isMsgChanOpen)
	go connection.clientListenMsg()
	log.GetLogger().Infof("client succeed to create connection to server %s", opt.ServerAddr)
	return connection, nil
}

func initPoolOptions(opt ClientOption, receiveChan chan interface{}, connWG *sync.WaitGroup,
	isMsgChanOpen int32) pool.Options {
	return pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			if opt.TLSEnable && opt.NeedVerify {
				// need verify cert
				conf := newVerifyTLSConfig(opt)
				dialer := new(net.Dialer)
				if common.FConfig.DisableDialTimeout == 0 {
					log.GetLogger().Debugf("enable to set dial timeout")
					dialer.Timeout = common.DefaultDialTimeout
				}
				conn, err := tls.DialWithDialer(dialer, "tcp", opt.ServerAddr, &conf)
				if err != nil {
					log.GetLogger().Warnf("failed to dialWithDialer to server %s", opt.ServerAddr)
				}
				return conn, err
			}
			conn, err := net.DialTimeout("tcp", opt.ServerAddr, common.DefaultDialTimeout)
			if err != nil {
				log.GetLogger().Warnf("failed to dial to server %s", opt.ServerAddr)
				return conn, err
			}
			if !opt.TLSEnable {
				return conn, err
			}
			tlsConn := tls.Client(conn, opt.Config)
			return tlsConn, err
		},
		ConnHandler:   handleConn,
		MaxConns:      defaultConns,
		MsgChan:       receiveChan,
		ConnWG:        connWG,
		IsMsgChanOpen: &isMsgChanOpen,
	}
}

func newVerifyTLSConfig(opt ClientOption) tls.Config {
	if opt.Config == nil {
		return tls.Config{}
	}
	return tls.Config{
		ClientCAs:                opt.Config.ClientCAs,
		Certificates:             opt.Config.Certificates,
		CipherSuites:             opt.Config.CipherSuites,
		PreferServerCipherSuites: opt.Config.PreferServerCipherSuites,
		ClientAuth:               opt.Config.ClientAuth,
		InsecureSkipVerify:       opt.Config.InsecureSkipVerify,
		MinVersion:               opt.Config.MinVersion,
		MaxVersion:               opt.Config.MaxVersion,
		Renegotiation:            opt.Config.Renegotiation,
		VerifyPeerCertificate:    commonTLS.VerifyCert,
	}
}

func (c *Connection) clientListenMsg() {
	for {
		select {
		case msgIf, ok := <-c.msgChan:
			if !ok {
				log.GetLogger().Warnf("client received chan of %s has been closed", c.PeerIP)
				return
			}
			msg, ok := msgIf.(*protocol.Message)
			if !ok {
				log.GetLogger().Warnf("failed to assert received message type")
				continue
			}
			switch msg.Header.MsgType {
			case protocol.CallReqMsg: // received post message
				c.wg.Add(1)
				go c.processCallReqMsg(msg)
			case protocol.CallRspMsg: // received normal response message
				c.processCallRspMsg(msg)
			case protocol.ReportRspMsg: // received report response message
				c.processCallReportMsg(msg)
			default:
				log.GetLogger().Warnf("client receive other message type %d from %s, skip",
					msg.Header.MsgType, c.PeerIP)
			}
		}
	}
}

// watchServerDisable client watch server is weather disabled
func (c *Connection) watchServerDisable(connWG *sync.WaitGroup, isMsgChanOpen *int32) {
	c.netPool.DisableInformer()
	log.GetLogger().Debugf("watched server of %s is disabled", c.PeerIP)
	if c.isConnNormalExit(connWG, isMsgChanOpen) {
		log.GetLogger().Infof("server of %s connection close normally", c.PeerIP)
		return
	}
	c.mux.Lock()
	c.isConnect = false
	// client should reconnect to the server
	for i := 0; i < reconnectionTimes; i++ {
		_, err := c.netPool.Get()
		log.GetLogger().Debugf("try to reconnect server %s, workerID %s, retry count: %d",
			c.PeerIP, c.workerID, i)
		if err == nil {
			c.isConnect = true
			if checkErr := c.getAndCheckWorkerID(); checkErr != nil {
				c.isConnect = false
				log.GetLogger().Warnf("failed to reconnect server %s for workerID %s "+
					"mismatch worker pod name, err: %s",
					c.PeerIP, c.workerID, checkErr.Error())
				break
			}
			log.GetLogger().Infof("succeed to reconnect server %s ...", c.PeerIP)
			c.mux.Unlock()
			proxyview.UpdateProxyAvailableStatus(c.PeerIP, true)
			c.watchServerDisable(connWG, isMsgChanOpen)
			return
		}
		if c.disableWhenServerExit && utils.ContainsConnRefusedErr(err) {
			log.GetLogger().Warnf("server %s exited, disable the client and no reconnect anymore", c.PeerIP)
			break
		}
		time.Sleep(reconnectInterval)
	}
	log.GetLogger().Warnf("failed to reconnect server %s", c.PeerIP)
	c.clearPendingReq()
	clientsMap.Delete(c.PeerIP)
	c.mux.Unlock()

	atomic.StoreInt32(isMsgChanOpen, 0)
	connWG.Wait()
	log.GetLogger().Debugf("all connections exited %s", c.PeerIP)
	setSignal(c.DisabledSignal)
	close(c.msgChan)
}

// checkAuth check auth from worker
func (c *Connection) checkAuth(key string, source string) bool {
	msgHeader := protocol.MsgHeader{MsgType: protocol.AuthCheckMsg, PacketID: uuid.New().String()}
	payLoad, err := json.Marshal(&AuthCheckInfo{
		Key:    key,
		Source: source,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal auth check info :%s", err.Error())
		return false
	}

	msg := &protocol.Message{Header: msgHeader, Payload: payLoad}
	ctx, cancel := context.WithTimeout(context.Background(), common.DefaultDialTimeout)
	defer cancel()
	resp := c.sendRequest(ctx, msg, false)
	if resp.ErrorCode != 0 {
		log.GetLogger().Errorf("failed to check auth in server %s", c.PeerIP)
		c.forceStop()
		return false
	}
	return true
}

// registerCallBack client register callBack to process req which is from server
func (c *Connection) registerCallBack(receiver func(payload []byte, peerIP string, w func(types.Result))) bool {
	if receiver == nil {
		return false
	}
	c.reqHandler = receiver
	return true
}

func (c *Connection) getAndCheckWorkerID() error {
	if c.workerID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.TODO(), recoverTTL)
	res, err := c.SendRecoverRequest(ctx, &commontypes.TCPRequest{WorkerID: c.workerID}, "", "")
	cancel()
	if err != nil {
		return err
	}
	if res.ErrorCode != 0 {
		return errors.New("failed to send recover request: " + res.ErrorMessage)
	}
	if !c.checkWorkerID(res.RawData) {
		return errors.New("failed to check workerID: " + c.workerID)
	}
	return nil
}

func (c *Connection) checkWorkerID(data []byte) bool {
	if len(data) == 0 {
		log.GetLogger().Warnf("workerID: %s return nil data", c.workerID)
		return true
	}
	var pn podName
	err := json.Unmarshal(data, &pn)
	if err != nil || !strings.Contains(c.workerID, pn.PodName) {
		log.GetLogger().Errorf("failed to check workerID: %s, err: %v, worker pod name:%s",
			c.workerID, err, pn.PodName)
		return false
	}
	log.GetLogger().Debugf("succeed to check workerID: %s, worker pod name", c.workerID, pn.PodName)
	return true
}
