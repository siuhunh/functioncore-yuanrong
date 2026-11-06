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

// Package websocket operates websocket server and socket client
package websocket

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"

	"functioncore/pkg/common/logger/log"
	commonwebsocket "functioncore/pkg/common/websocket"
	"functioncore/pkg/debug-agent/config"
)

type socketConnection struct {
	endpoint    string
	connection  net.Conn
	forwardChan chan commonwebsocket.WssMessage `json:"-"` // message to be forwarded
	closeChan   chan struct{}
	isClosed    uint32
}

type connectionPair struct {
	upstream    *commonwebsocket.WssConnection
	downstream  *socketConnection
	releaseOnce sync.Once
	receivedFirstMsg
}

const (
	localIP              = "127.0.0.1"
	jdwpHandshakeMessage = "JDWP-Handshake"
	maxSendRetryTimes    = 3
	bufferSize           = 1024
	tcp                  = "tcp"
	websocketPath        = "/websocket"
)

type receivedFirstMsg struct {
	sync.Once
	wg sync.WaitGroup
}

// BuildStreamOption builds stream option of websocket
func BuildStreamOption(conf *config.DebugAgentConfig) *commonwebsocket.StreamOption {
	downstreamURL := fmt.Sprintf("%s:%s", localIP, commonwebsocket.DebugRuntimeWSPort) // connect runtime
	upstreamURL := fmt.Sprintf("%s:%s", localIP, commonwebsocket.DebugAgentWSPort)     // connect worker
	return &commonwebsocket.StreamOption{
		UpstreamOption: &commonwebsocket.SingleStreamOption{
			URL:         upstreamURL,
			HTTPSEnable: conf.HTTPSConfig.HTTPSEnable,
			Auth:        conf.AuthConfig,
		},
		DownstreamOption: &commonwebsocket.SingleStreamOption{
			URL:         downstreamURL,
			HTTPSEnable: false,
		},
	}
}

// CreateConnPair creates websocket and socket connection pair
func CreateConnPair(opt *commonwebsocket.StreamOption, stopCh <-chan struct{}) error {
	if err := startWssServer(opt.UpstreamOption, stopCh); err != nil {
		log.GetLogger().Errorf("failed to create upstream websocket server, err: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to create upstream websocket server")
	return nil
}

func (p *connectionPair) release() {
	p.releaseOnce.Do(func() {
		log.GetLogger().Infof("release websocket socket connection pair")
		p.upstream.Close()
		p.downstream.close()
	})
}

func (p *connectionPair) handleMessage() {
	log.GetLogger().Infof("begin to handle websocket socket message")
	p.wg.Add(1)
	p.forwardMessageFromUpToDown()
	p.wg.Wait()
	p.forwardMessageFromDownToUp()
}

func (p *connectionPair) forwardMessageFromDownToUp() {
	go func() {
		for {
			select {
			case _, ok := <-p.downstream.closeChan:
				if !ok {
					log.GetLogger().Infof("received socket termination signal, stop read message from %s",
						p.downstream.endpoint)
					return
				}
			default:
				if err := p.readMessageFromSocket(); err != nil {
					p.release()
					return
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case message := <-p.upstream.ForwardChan:
				if err := p.upstream.TrySendMessage(message); err != nil {
					log.GetLogger().Errorf("failed to send message to websocket, remote: %s, err: %s",
						p.upstream.Endpoint, err.Error())
					p.release()
					return
				}
			case _, ok := <-p.upstream.CloseChan:
				if !ok {
					log.GetLogger().Infof("received wss termination signal, stop send message to %s",
						p.upstream.Endpoint)
					return
				}
			}
		}
	}()
}

func (p *connectionPair) readMessageFromSocket() error {
	buffer := make([]byte, bufferSize)
	n, err := p.downstream.connection.Read(buffer)
	if err != nil {
		log.GetLogger().Errorf("failed to read message from socket connection %s, err: %s",
			p.downstream.endpoint, err.Error())
		return err
	}
	data := buffer[:n]
	p.upstream.ForwardChan <- commonwebsocket.WssMessage{MessageType: websocket.BinaryMessage, Data: data}
	return nil
}

func (p *connectionPair) forwardMessageFromUpToDown() {
	go func() {
		for {
			select {
			case _, ok := <-p.upstream.CloseChan:
				if !ok {
					log.GetLogger().Infof("received wss termination signal, stop read message from %s",
						p.upstream.Endpoint)
					return
				}
			default:
				if err := p.readMessageFromWebsocket(); err != nil {
					p.release()
					return
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case message := <-p.downstream.forwardChan:
				if err := p.downstream.trySendSocketMessage(message.Data); err != nil {
					log.GetLogger().Errorf("failed to send message to socket, remote: %s, err: %s",
						p.downstream.endpoint, err.Error())
					p.release()
					return
				}
				if string(message.Data) != jdwpHandshakeMessage {
					p.receivedFirstMsg.Do(func() {
						p.receivedFirstMsg.wg.Done()
					})
				}
			case _, ok := <-p.downstream.closeChan:
				if !ok {
					log.GetLogger().Infof("received socket termination signal, stop send message to %s",
						p.downstream.endpoint)
					return
				}
			}
		}
	}()
}

func (p *connectionPair) readMessageFromWebsocket() error {
	messageType, data, err := p.upstream.Connection.ReadMessage()
	if err != nil {
		log.GetLogger().Errorf("failed to read message from websocket connection %s, err: %s",
			p.upstream.Endpoint, err.Error())
		return err
	}
	if messageType == websocket.BinaryMessage {
		if string(data) == jdwpHandshakeMessage {
			log.GetLogger().Infof("receive handshake message from websocket")
			conn, err := socketConnect(p.downstream.endpoint)
			if err != nil {
				return err
			}
			if err := handshakeWithRuntime(conn); err != nil {
				return err
			}
			log.GetLogger().Infof("succeed to handshake with runtime")
			p.downstream.connection = conn

			sendErr := p.upstream.TrySendMessage(commonwebsocket.WssMessage{
				MessageType: websocket.BinaryMessage, Data: []byte(jdwpHandshakeMessage)})
			if sendErr != nil {
				log.GetLogger().Errorf("failed to send jdwp handshake message to websocket, err: %s", sendErr.Error())
				return sendErr
			}
		} else {
			p.downstream.forwardChan <- commonwebsocket.WssMessage{MessageType: messageType, Data: data}
		}
	}
	return nil
}

func handshakeWithRuntime(conn net.Conn) error {
	_, err := conn.Write([]byte(jdwpHandshakeMessage))
	if err != nil {
		log.GetLogger().Errorf("failed to send jdwp handshake message to runtime, err: %s", err.Error())
		return err
	}
	buffer := make([]byte, bufferSize)
	n, err := conn.Read(buffer)
	if err != nil {
		log.GetLogger().Errorf("failed to read jdwp handshake message from runtime, err: %s", err.Error())
		return err
	}
	data := buffer[:n]
	if err = checkJDWPHandshakeMessage(data); err != nil {
		return err
	}
	return nil
}

func (c *socketConnection) isConnClosed() bool {
	return atomic.LoadUint32(&c.isClosed) == 1
}

func (c *socketConnection) trySendSocketMessage(message []byte) error {
	if c.isConnClosed() {
		return errors.New("connection is closed")
	}
	var err error
	for i := 0; i < maxSendRetryTimes; i++ {
		_, err = c.connection.Write(message)
		if err == nil {
			return nil
		}
	}
	return err
}

func (c *socketConnection) close() {
	if !atomic.CompareAndSwapUint32(&c.isClosed, 0, 1) {
		log.GetLogger().Infof("%s socket connection has been closed", c.endpoint)
		return
	}
	close(c.closeChan)
	if err := c.connection.Close(); err != nil {
		log.GetLogger().Errorf("failed to close socket connection %s, err: %s", c.endpoint, err.Error())
	}
	return
}

func startWssServer(opt *commonwebsocket.SingleStreamOption, stopCh <-chan struct{}) error {
	server := &http.Server{
		Addr:    opt.URL,
		Handler: buildHandler(),
	}
	log.GetLogger().Infof("websocket server is running at address: %s", opt.URL)

	go commonwebsocket.WaitUtilInterrupt(stopCh, server)
	if err := server.ListenAndServe(); err != nil {
		log.GetLogger().Errorf("failed to start websocket server, err: %s", err.Error())
		return err
	}

	return nil
}

func buildHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET(websocketPath, func(ctx *gin.Context) {
		handleWebsocket(ctx.Writer, ctx.Request)
	})
	return router
}

func handleWebsocket(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to upgrade to websocket, err: %s", err.Error())
		return
	}

	pair := &connectionPair{}
	downstream := &socketConnection{
		endpoint:    fmt.Sprintf("%s:%s", localIP, commonwebsocket.DebugRuntimeWSPort),
		forwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		closeChan:   make(chan struct{}),
		isClosed:    uint32(0),
	}
	pair.downstream = downstream

	upstream := &commonwebsocket.WssConnection{
		Endpoint:    fmt.Sprintf("%s:%s", localIP, commonwebsocket.DebugAgentWSPort),
		Connection:  conn,
		ForwardChan: make(chan commonwebsocket.WssMessage, commonwebsocket.ForwardChanSize),
		CloseChan:   make(chan struct{}),
		IsClosed:    uint32(0),
	}
	pair.upstream = upstream
	pair.handleMessage()
}

func socketConnect(url string) (net.Conn, error) {
	for i := 0; i < maxSendRetryTimes; i++ {
		conn, err := createSocketConnection(url)
		if err == nil {
			return conn, nil
		}
		log.GetLogger().Warnf("failed to create socket connection for %s, err: %s", url, err.Error())
		time.Sleep(commonwebsocket.DialInterval)
	}
	errMsg := fmt.Sprintf("after %d attempts, failed to connect to socket %s", maxSendRetryTimes, url)
	log.GetLogger().Errorf(errMsg)
	return nil, errors.New(errMsg)
}

func createSocketConnection(url string) (net.Conn, error) {
	conn, err := net.Dial(tcp, url)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Infof("succeed to create socket client")
	return conn, nil
}

func checkJDWPHandshakeMessage(data []byte) error {
	if string(data) != jdwpHandshakeMessage {
		errMsg := fmt.Sprintf("message received from runtime is not jdwp handshake message, received message: %s",
			string(data))
		log.GetLogger().Errorf(errMsg)
		return errors.New(errMsg)
	}
	return nil
}
