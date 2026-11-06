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

// Package pool contains interface of bus and tcp pool codes
package pool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"functioncore/pkg/common/connection"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functionbus/common"
)

const (
	defaultIdleConns = 1
)

// ErrConnectionPoolEmpty error of connection pool empty
var ErrConnectionPoolEmpty = errors.New("failed to get connection: the connection in the connection pool is empty")

// Pool interface of connection pool
type Pool interface {
	Get() (net.Conn, error)
	Put(conn net.Conn) error
	Delete(conn net.Conn)
	Close()
	DisableInformer()
}

// Options used to create connection pool
type Options struct {
	SyncCond          *sync.Cond
	ConnHandler       func(conn net.Conn, option Options)
	Dialer            func(ctx context.Context) (net.Conn, error)
	MsgChan           chan interface{}
	MaxConns          int
	IsMsgChanOpen     *int32
	IsConnectListened *int32
	IsServer          bool
	DisableHeartbeat  bool
}

// ConnPool pool of tcp connection
type ConnPool struct {
	poolMu       *sync.Mutex
	disabledSign chan struct{}
	conns        []net.Conn
	options      Options
	curr         int
}

// NewConnPool creates connection pool with options
func NewConnPool(opt Options) (*ConnPool, error) {
	if opt.MaxConns <= 0 {
		opt.MaxConns = defaultIdleConns
	}
	p := &ConnPool{
		options:      opt,
		poolMu:       new(sync.Mutex),
		disabledSign: make(chan struct{}, 1),
	}
	if opt.Dialer != nil {
		_, err := p.Get()
		if err != nil {
			return nil, err
		}

	}
	return p, nil
}

// Get get a connection from the pool
func (p *ConnPool) Get() (net.Conn, error) {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()
	var dialErr error
	if len(p.conns) < p.options.MaxConns && p.options.Dialer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), common.DefaultDialTimeout)
		defer cancel()
		var conn net.Conn
		conn, dialErr = p.options.Dialer(ctx)
		if dialErr == nil {
			go p.listenConn(conn)
			p.conns = append(p.conns, conn)
			return conn, nil
		}
		log.GetLogger().Warnf("createNewConnFail | %s", dialErr.Error())
	}
	if len(p.conns) == 0 {
		if dialErr != nil {
			return nil, dialErr
		}
		return nil, ErrConnectionPoolEmpty
	}
	p.curr++
	if p.curr >= len(p.conns) {
		p.curr = 0
	}
	conn := p.conns[p.curr]
	return conn, nil
}

func (p *ConnPool) listenConn(conn net.Conn) {
	p.options.ConnHandler(conn, p.options)
	log.GetLogger().Infof("finishListenConn | %s", conn.RemoteAddr().String())
	p.Delete(conn)
}

// Put a connection into the pool
func (p *ConnPool) Put(conn net.Conn) error {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()
	if len(p.conns) > p.options.MaxConns {
		return fmt.Errorf("failed to put connection %s to pool: the number of"+
			"connections in the connection pool is exceeded", conn.RemoteAddr().String())
	}
	conn = connection.NewBufferedConn(conn)
	go p.listenConn(conn)
	p.conns = append(p.conns, conn)
	return nil
}

// Delete a pool from the pool
func (p *ConnPool) Delete(conn net.Conn) {
	p.poolMu.Lock()
	for i := 0; i < len(p.conns); i++ {
		if p.conns[i].RemoteAddr() == conn.RemoteAddr() && p.conns[i].LocalAddr() == conn.LocalAddr() {
			if p.conns[i].Close() != nil {
				log.GetLogger().Debug("conn has been closed")
			}
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	if len(p.conns) == 0 {
		p.disabledSign <- struct{}{}
	}
	p.poolMu.Unlock()
}

// Close all connections in the pool
func (p *ConnPool) Close() {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()
	for i := 0; i < len(p.conns); i++ {
		if p.conns[i].Close() != nil {
			log.GetLogger().Debug("conn has been closed")
		}
	}
	p.conns = p.conns[0:0]
}

// DisableInformer informer of that the pool is disabled
func (p *ConnPool) DisableInformer() {
	<-p.disabledSign
}
