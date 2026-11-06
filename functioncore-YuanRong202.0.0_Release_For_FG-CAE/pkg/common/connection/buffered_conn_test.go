/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package connection

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type fakeConn struct {
	readBuf *bytes.Buffer
}

func (f fakeConn) Read(b []byte) (n int, err error) { return f.readBuf.Read(b) }

func (fakeConn) Write(b []byte) (n int, err error) { return 0, nil }

func (fakeConn) Close() error { return nil }

func (fakeConn) LocalAddr() net.Addr { return &net.IPAddr{} }

func (fakeConn) RemoteAddr() net.Addr { return &net.IPAddr{} }

func (fakeConn) SetDeadline(t time.Time) error { return nil }

func (fakeConn) SetReadDeadline(t time.Time) error { return nil }

func (fakeConn) SetWriteDeadline(t time.Time) error { return nil }

func TestBufferdConn(t *testing.T) {
	expected := []byte("abc")
	c := NewBufferedConn(fakeConn{readBuf: bytes.NewBuffer(expected)})
	b := make([]byte, 3)
	n, err := c.Read(b)
	assert.Nil(t, err)
	assert.Equal(t, len(expected), n)
	assert.Equal(t, expected, b)

	_, err = c.Write(expected)
	assert.Nil(t, err)

	err = c.Close()
	assert.Nil(t, err)
}
