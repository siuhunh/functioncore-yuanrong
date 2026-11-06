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

// Package tcpmsg is tcp msg struct and protocol
package tcpmsg

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/strutil"
	"functioncore/pkg/common/uuid"
)

// set timeout for resource recycling, request timeout is promised by users
var defaultWaitResponseTimeout = 30 * time.Second

var (
	incNumber      uint64
	packetIDPrefix = newPacketIDPrefix()
)

const (
	// RuntimeUDS default unix domain socket between worker and runtime-manager
	RuntimeUDS = "/home/snuser/socket/runtime.sock"
)

const (
	socketTypeTCP  = "tcp"
	socketTypeUnix = "unix"
)

// SocketConn bi-direction socket connection
type SocketConn struct {
	handlerFunc func(*Message) (int, RuntimeRSP, error)
	// ConnectionErr closed when error occurred
	ConnectionErr chan struct{}
	conn          net.Conn
	packetMap     sync.Map
	isConnected   bool
	addr          string
	once          sync.Once
}

func getSocketType(addr string) string {
	if strings.HasPrefix(addr, "/") {
		return socketTypeUnix
	}
	return socketTypeTCP
}

// ListenSocketServer listen socket server
func ListenSocketServer(address string, handlerFunc func(*Message) (int, RuntimeRSP, error),
	newConnFunc func(conn *SocketConn)) error {
	listener, err := net.Listen(getSocketType(address), address)
	if err != nil {
		log.GetLogger().Errorf("failed to listen tcp with error: %s", err)
		return err
	}
	go func() {
		// server has single client
		defer listener.Close()
		log.GetLogger().Infof("listening on %s", address)
		conn, err := listener.Accept()
		if err != nil {
			log.GetLogger().Errorf("failed to accept message with error: %s", err)
			newConnFunc(nil)
			return
		}
		// logs an incoming message
		log.GetLogger().Infof("received message %s -> %s", conn.RemoteAddr(), conn.LocalAddr())
		newConnFunc(createSocketConn(conn, "", handlerFunc))
	}()
	return nil
}

// CreateSocketClient create socket client
func CreateSocketClient(address string, handlerFunc func(*Message) (int, RuntimeRSP, error)) (*SocketConn, error) {
	conn, err := net.Dial(getSocketType(address), address)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Infof("sent message %s -> %s", conn.LocalAddr(), conn.RemoteAddr())
	return createSocketConn(conn, address, handlerFunc), nil
}

func createSocketConn(conn net.Conn, addr string, handlerFunc func(*Message) (int, RuntimeRSP, error)) *SocketConn {
	socketConn := &SocketConn{
		ConnectionErr: make(chan struct{}),
		conn:          conn,
		addr:          addr,
		handlerFunc:   handlerFunc,
		isConnected:   true,
	}
	go socketConn.ReceiveMessage()
	return socketConn
}

// IsConnected judge socket connected is ok or not
func (c *SocketConn) IsConnected() bool {
	return c.isConnected
}

// Reconnect deal with received message
func (c *SocketConn) Reconnect() bool {
	if c.isConnected {
		log.GetLogger().Warnf("socket connect is ok, don't need to reconnect")
		return true
	}
	if c.addr == "" {
		log.GetLogger().Errorf("addr is blank, can't do reconnect")
		return false
	}
	conn, err := net.Dial(getSocketType(c.addr), c.addr)
	if err != nil {
		log.GetLogger().Errorf("reconnect %s failed:%s", c.addr, err.Error())
		return false
	}
	c.conn = conn
	c.isConnected = true
	c.ConnectionErr = make(chan struct{})
	c.once = sync.Once{}
	go c.ReceiveMessage()
	log.GetLogger().Infof("reconnect %s success", c.addr)
	return true
}

// ReceiveMessage deal with received message
func (c *SocketConn) ReceiveMessage() {
	reader := bufio.NewReader(c.conn)
	for {
		msg, err := c.read(reader)
		if err != nil {
			log.GetLogger().Warnf("read message error: %s", err)
			c.isConnected = false
			break
		}

		if !c.isResponseMessage(msg) {
			if err = c.processRequest(msg); err != nil {
				break
			}
		} else {
			if err = c.processResponse(msg); err != nil {
				break
			}
		}
	}
	c.CloseConnectionErr()
	if err := c.conn.Close(); err != nil {
		log.GetLogger().Errorf("close socket connection error: %s", err)
		return
	}
}

// CloseConnectionErr close ConnectionErr
func (c *SocketConn) CloseConnectionErr() {
	c.once.Do(func() {
		close(c.ConnectionErr)
	})
}

func (c *SocketConn) processRequest(requestMsg *Message) error {
	if handlerMsg, resp, err := c.handlerFunc(requestMsg); handlerMsg != MessageWithNoResponse {
		// write response to worker
		respMsg, err := buildResp(requestMsg, err, handlerMsg, resp)
		if err != nil {
			log.GetLogger().Errorf("build response error: %s", err)
			return err
		}
		if err = c.write(respMsg); err != nil {
			log.GetLogger().Warnf("write message error: %s", err)
			return err
		}
	}
	return nil
}

func (c *SocketConn) processResponse(msg *Message) error {
	respChanItf, exist := c.packetMap.Load(msg.Header.PacketID)
	if !exist {
		return nil
	}
	if respChan, ok := respChanItf.(chan *Message); ok && respChan != nil {
		respChan <- msg
		close(respChan)
	}
	c.packetMap.Delete(msg.Header.PacketID)
	return nil
}

func (c *SocketConn) isResponseMessage(msg *Message) bool {
	_, exist := c.packetMap.Load(msg.Header.PacketID)
	return exist
}

func newPacketIDPrefix() []byte {
	res := make([]byte, uuidLen, uuidLen)
	u1 := uuid.New()
	copy(res, u1[:])
	return res
}

func newPacketID() string {
	id := make([]byte, packetIDLen, packetIDLen)
	copy(id[:uuidLen], packetIDPrefix)
	binary.BigEndian.PutUint64(id[uuidLen:], atomic.AddUint64(&incNumber, 1))
	return strutil.BytesToString(id)
}

// BuildMessage build message with message type and body
func BuildMessage(messageType MessageType, body []byte) *Message {
	return &Message{
		Header: Header{
			MsgType:  messageType,
			PacketID: newPacketID(),
		},
		Body: body,
	}
}

// SendMessageWithNoResp send message and no response
func (c *SocketConn) SendMessageWithNoResp(msg *Message) error {
	if err := c.write(msg); err != nil {
		log.GetLogger().Warnf("write message error: %s", err)
		return err
	}
	return nil
}

// SendMessage send message and receive response
func (c *SocketConn) SendMessage(msg *Message) RuntimeManagerRSP {
	return c.SendMessageWithTimeout(msg, defaultWaitResponseTimeout)
}

// SendMessageWithTimeout send message and receive response
func (c *SocketConn) SendMessageWithTimeout(msg *Message, timeout time.Duration) RuntimeManagerRSP {
	m := RuntimeManagerRSP{}
	if !c.isConnected {
		m.Code = snerror.RuntimeManagerProcessExited
		m.Err = snerror.ErrText(snerror.RuntimeManagerProcessExited)
		log.GetLogger().Warnf("connection between worker and runtime-manager is closed")
		return m
	}
	respChan := make(chan *Message, 1)
	c.packetMap.Store(msg.Header.PacketID, respChan)
	defer c.packetMap.Delete(msg.Header.PacketID)

	if err := c.write(msg); err != nil {
		log.GetLogger().Warnf("write message error: %s", err)
		m.Err = err.Error()
		return m
	}

	ctx, cancel := context.WithTimeout(context.TODO(), timeout)
	defer cancel()
	select {
	case _, ok := <-ctx.Done():
		if !ok {
			m.Err = "read response timeout"
			return m
		}
	case response, ok := <-respChan:
		if !ok {
			c.packetMap.Delete(msg.Header.PacketID)
			m.Err = "read response chan error"
			return m
		}
		if err := json.Unmarshal(response.Body, &m); err != nil {
			m.Err = err.Error()
			return m
		}
	}
	return m
}

func buildResp(reqMsg *Message, msgErr error, handlerMsg int, resp RuntimeRSP) (*Message, error) {
	var errMsg = ""
	if msgErr != nil {
		errMsg = msgErr.Error()
	}
	body := &RuntimeManagerRSP{
		Err:      errMsg,
		Code:     handlerMsg,
		Response: resp,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	msg := &Message{
		Header: Header{
			MsgType:  reqMsg.Header.MsgType,
			PacketID: reqMsg.Header.PacketID,
		},
		Body: bodyBytes,
	}
	return msg, nil
}

func (c *SocketConn) write(msg *Message) error {
	msgBytes, err := Encode(msg)
	if err != nil {
		return err
	}
	_, err = c.conn.Write(msgBytes)
	if err != nil {
		return err
	}
	return nil
}

func (c *SocketConn) read(reader io.Reader) (*Message, error) {
	msg, err := Decode(reader)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
