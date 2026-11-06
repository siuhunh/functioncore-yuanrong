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

package async

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockWriter struct {
	buf   []byte
	delay time.Duration
	sync.Mutex
}

func (m *mockWriter) Write(data []byte) (int, error) {
	m.Lock()
	m.buf = data
	if m.delay != 0 {
		time.Sleep(m.delay)
	}
	m.Unlock()
	return len(data), nil
}

func (m *mockWriter) Clear() []byte {
	m.Lock()
	ret := m.buf
	m.buf = nil
	m.Unlock()
	return ret
}

func (m *mockWriter) SetWriteDelay(delay time.Duration) {
	m.delay = delay
}

func TestWriter_Write(t *testing.T) {
	w := &mockWriter{}

	asyncWriter := NewAsyncWriteSyncer(w)

	data := []byte("hello world")

	// write small data, will be cached in inner buffer
	asyncWriter.Write(data)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(w.Clear()))

	// small data will be written after flush time
	time.Sleep(diskFlushTime)
	assert.Equal(t, data, w.Clear())

	// big data will be flushed immediately
	asyncWriter.Write(make([]byte, diskFlushSize+1))
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, diskFlushSize+1, len(w.Clear()))

	// Sync() will flush buffer immediately
	asyncWriter.Write(data)
	assert.Equal(t, 0, len(w.Clear()))
	asyncWriter.Sync()
	assert.Equal(t, len(data), len(w.Clear()))

	for i := 0; i < 100; i++ {
		go asyncWriter.Sync()
	}
	time.Sleep(10 * time.Millisecond)
	asyncWriter.Sync()
}

func TestCachedLimit(t *testing.T) {
	w := &mockWriter{}
	w.SetWriteDelay(150 * time.Millisecond)

	asyncWriter := NewAsyncWriteSyncer(w, WithCachedLimit(diskFlushSize*4)) // softLimit = 512kb * 4 * 0.8 = 1.6mb

	size := float64(diskFlushSize)*2*softLimitFactor + 1
	data := make([]byte, int(size))

	// big data will be flushed immediately and triggers the mockWriter's write
	asyncWriter.Write(make([]byte, diskFlushSize+1))

	// logConsumer is blocked in mockWriter's write
	asyncWriter.Write(data)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(asyncWriter.lines))

	// this write should hit the soft limit
	asyncWriter.Write(data)
	time.Sleep(100 * time.Millisecond) // mockWriter's write finishes
	assert.Equal(t, 1, len(asyncWriter.lines))

	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 0, len(asyncWriter.lines))
}

func BenchmarkWrite(b *testing.B) {
	asyncWriter := NewAsyncWriteSyncer(io.Discard, WithCachedLimit(diskFlushSize*4))
	data := []byte("hello world")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			asyncWriter.Write(data)
		}
	})
}
