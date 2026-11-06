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
	"bytes"
	"errors"
	"net"
	"runtime"
	"sync"

	"functioncore/pkg/common/logger/log"
)

// DefaultBufferedWriterSize -
const DefaultBufferedWriterSize = 1 << 16

// BufferedWriter -
type BufferedWriter struct {
	flushBuf     *bytes.Buffer
	dirtyBuf     *bytes.Buffer
	dst          net.Conn
	mux          sync.Mutex
	dirtyBufMux  sync.Mutex
	err          error
	flushPending bool
	notifyFlush  chan struct{}
	closeOnce    sync.Once
	closeFlag    bool
	bufSize      int

	needWaitFlush bool
	waitFlush     chan struct{}
}

// NewBufferedWriter -
func NewBufferedWriter(dst net.Conn) *BufferedWriter {
	return NewBufferedWriterWithSize(dst, DefaultBufferedWriterSize)
}

// NewBufferedWriterWithSize -
func NewBufferedWriterWithSize(dst net.Conn, bufSize int) *BufferedWriter {
	w := &BufferedWriter{
		flushBuf:    new(bytes.Buffer),
		dirtyBuf:    new(bytes.Buffer),
		dst:         dst,
		notifyFlush: make(chan struct{}, 1),
		bufSize:     bufSize,
		waitFlush:   make(chan struct{}, 1),
	}
	go w.flush()
	return w
}

// Write implements io.Writer
func (w *BufferedWriter) Write(p []byte) (int, error) {
	w.mux.Lock()

	w.dirtyBufMux.Lock()
	n, err := w.writeToDirtyBuf(p)
	if err == nil && !w.flushPending {
		w.flushPending = true
		w.dirtyBuf, w.flushBuf = w.flushBuf, w.dirtyBuf
		w.dirtyBufMux.Unlock()
		if w.closeFlag {
			w.mux.Unlock()
			log.GetLogger().Error("notifyFlush has been closed")
			return 0, errors.New("notifyFlush has been closed")
		}
		w.notifyFlush <- struct{}{}
	} else {
		w.dirtyBufMux.Unlock()
	}

	w.mux.Unlock()
	return n, err
}

func (w *BufferedWriter) writeToDirtyBuf(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	if w.dirtyBuf.Len() != 0 && w.dirtyBuf.Len()+len(p) > w.bufSize {
		w.needWaitFlush = true
		w.dirtyBufMux.Unlock()
		<-w.waitFlush
		w.dirtyBufMux.Lock()
		if w.err != nil {
			return 0, w.err
		}
	}

	return w.dirtyBuf.Write(p)
}

// Close release resources created by BufferdWriter
func (w *BufferedWriter) Close() {
	w.closeOnce.Do(func() {
		w.mux.Lock()
		w.closeFlag = true
		close(w.notifyFlush)
		w.mux.Unlock()
	})
}

func (w *BufferedWriter) flush() {
	var err error
	for range w.notifyFlush {
		for {
			_, err = w.flushBuf.WriteTo(w.dst)
			if err != nil {
				w.dirtyBufMux.Lock()
				w.err = err
				close(w.waitFlush)
				w.dirtyBufMux.Unlock()
				return
			}
			w.dirtyBufMux.Lock()
			w.dirtyBuf, w.flushBuf = w.flushBuf, w.dirtyBuf
			if w.needWaitFlush {
				w.needWaitFlush = false
				w.waitFlush <- struct{}{}
			}
			if w.flushBuf.Len() == 0 {
				w.flushPending = false
				w.dirtyBufMux.Unlock()
				break
			}
			w.dirtyBufMux.Unlock()
		}

		// allow other goroutines to run
		runtime.Gosched()
	}
}
