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

package tcpbus

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/functionbus/bus/pool"
)

type mockListener struct{}

func (m *mockListener) Accept() (net.Conn, error) {
	return nil, fmt.Errorf("failed to accept")
}

func (m *mockListener) Close() error {
	return nil
}
func (m *mockListener) Addr() net.Addr {
	return nil
}

func TestStartServerWithTLS(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(tls.Listen,
			func(network, addr string, config *tls.Config) (net.Listener, error) {
				return &mockListener{}, nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()

	if err := StartServerWithTLS(ServerOption{}); err != nil {
		t.Errorf("failed to start server with tls: %s", err.Error())
	}

	patches = [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(tls.Listen,
			func(network, addr string, config *tls.Config) (net.Listener, error) {
				return nil, fmt.Errorf("error")
			}),
	}
	if err := StartServerWithTLS(ServerOption{}); err == nil {
		t.Errorf("failed to test tls listen error")
	}
}

func TestStartServer(t *testing.T) {
	if err := StartServer(ServerOption{ListenAddr: "-1"}); err == nil {
		t.Errorf("failed to test invalid listen address")
	}

	if err := StartServer(ServerOption{ListenAddr: "127.127.127.127"}); err == nil {
		t.Errorf("failed to test listen TCP failed")
	}
}

func TestProcessNewConn(t *testing.T) {
	var err = errors.New("not execute ConnHandler")
	patch1 := gomonkey.ApplyMethod(reflect.TypeOf(clientsMap), "Load", func(*sync.Map, interface{}) (value interface{}, ok bool) {
		connPool, _ := pool.NewConnPool(pool.Options{
			ConnHandler: func(conn net.Conn, opt pool.Options) {
				err = nil
				return
			},
		})
		return &Connection{netPool: connPool}, true
	})
	defer patch1.Reset()

	conn := &mockConn{}
	opt := ServerOption{}
	ProcessNewConn(opt, conn)
	time.Sleep(1 * time.Second)

	if err != nil {
		t.Error(err.Error())
	}
}
