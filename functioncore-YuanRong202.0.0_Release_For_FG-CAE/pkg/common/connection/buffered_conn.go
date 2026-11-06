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

// Package connection provides utilities when handling network connections.
package connection

import (
	"bufio"
	"net"
)

const (
	defaultReaderSize = 1 << 12
)

// BufferedConn wraps a net.Conn and add read and writer buffers to it.
type BufferedConn struct {
	net.Conn
	reader *bufio.Reader
	writer *BufferedWriter
}

// NewBufferedConn -
func NewBufferedConn(conn net.Conn) *BufferedConn {
	c := &BufferedConn{
		Conn:   conn,
		reader: bufio.NewReaderSize(conn, defaultReaderSize),
		writer: NewBufferedWriter(conn),
	}
	return c
}

// Read overrides net.Conn.Read
func (c *BufferedConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

// Write overrides net.Conn.Write
func (c *BufferedConn) Write(b []byte) (int, error) {
	return c.writer.Write(b)
}

// Close overrides net.Conn.Close
func (c *BufferedConn) Close() error {
	c.writer.Close()
	return c.Conn.Close()
}
