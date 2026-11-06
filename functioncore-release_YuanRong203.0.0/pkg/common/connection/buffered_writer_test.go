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
	"bufio"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runServer(wf func(net.Conn) io.Writer, addr string) string {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			go func() {
				r := bufio.NewReader(conn)
				w := wf(conn)
				for {
					if _, err := io.Copy(w, r); err != nil {
						panic(err)
					}
				}
			}()
		}
	}()

	return listener.Addr().String()
}

func TestBufferedWriter(t *testing.T) {
	addr := runServer(func(c net.Conn) io.Writer {
		return c
	}, "")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		panic(err)
	}
	r := bufio.NewReader(conn)
	w := NewBufferedWriterWithSize(conn, 1<<10)

	size := 250
	expectedDataSet := make([][]byte, size)
	for i := range expectedDataSet {
		expectedDataSet[i] = make([]byte, 100)
		for j := range expectedDataSet[i] {
			expectedDataSet[i][j] = byte(i)
		}
	}

	for i := 0; i < 1000; i++ {
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			for i := 0; i < size/2; i++ {
				_, err = w.Write(expectedDataSet[i])
				require.NoError(t, err)
			}
		}()
		go func() {
			defer wg.Done()
			for i := size / 2; i < size; i++ {
				_, err = w.Write(expectedDataSet[i])
				require.NoError(t, err)
			}
		}()

		go func() {
			defer wg.Done()
			data := make([]byte, 100)
			notSeen := make(map[int]struct{})
			for i := 0; i < size; i++ {
				notSeen[i] = struct{}{}
			}
			for j := 0; j < size; j++ {
				_, err = io.ReadFull(r, data)
				require.NoError(t, err)
				i := int(data[0])
				assert.Equal(t, expectedDataSet[i], data)
				delete(notSeen, i)
			}
			assert.Equal(t, 0, len(notSeen))
		}()

		wg.Wait()
	}
	w.Close()
	w.Close() // second close should not panic

	// when the connection is closed, no panic occurs and only an error is reported.
	_, err = w.Write([]byte("test"))
	assert.NotNil(t, err)
}
