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

package fusion

import (
	"sync"
	"time"

	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/runtime"
)

const defaultGetExceptionTimeout = 20 * time.Second

type exceptionWaiter struct {
	timeout   time.Duration
	finishCh  chan struct{}
	waitCh    chan struct{}
	exception *runtime.Error
}

func newExceptionWaiter(timeout time.Duration) *exceptionWaiter {
	return &exceptionWaiter{
		timeout:  timeout,
		finishCh: make(chan struct{}),
		waitCh:   make(chan struct{}),
	}
}

func (w *exceptionWaiter) start() {
	timer := time.NewTimer(w.timeout)
	defer timer.Stop()

	select {
	case <-w.finishCh:
		close(w.waitCh)
		return
	case <-timer.C:
		close(w.waitCh)
		return
	}
}

func (w *exceptionWaiter) finish() {
	close(w.finishCh)
}

func (w *exceptionWaiter) waitTimeoutOrFinish() {
	<-w.waitCh
}

type exceptionHandler struct {
	exception *runtime.Error
	waiter    *exceptionWaiter
	mux       sync.RWMutex
}

func newExceptionHandler() *exceptionHandler {
	return &exceptionHandler{}
}

func (h *exceptionHandler) setException(e *runtime.Error) {
	h.mux.Lock()
	h.exception = e
	if h.waiter != nil {
		h.waiter.exception = e
		h.waiter.finish()
		h.waiter = nil
	}
	h.mux.Unlock()
}

func (h *exceptionHandler) getException() *runtime.Error {
	cache.GetMessageQueue().Block()

	if err := h.checkException(); err != nil {
		return err
	}

	return h.getExceptionSlow()
}

func (h *exceptionHandler) getExceptionSlow() *runtime.Error {
	h.mux.Lock()
	if h.exception != nil {
		h.mux.Unlock()
		return h.exception
	}

	if h.waiter == nil {
		waiter := newExceptionWaiter(defaultGetExceptionTimeout)
		go waiter.start()
		h.waiter = waiter
	}
	getter := h.waiter
	h.mux.Unlock()

	getter.waitTimeoutOrFinish()
	return getter.exception
}

func (h *exceptionHandler) checkException() *runtime.Error {
	h.mux.RLock()
	e := h.exception
	h.mux.RUnlock()
	return e
}

func (h *exceptionHandler) clearException() {
	h.mux.Lock()
	h.exception = nil
	h.waiter = nil
	h.mux.Unlock()
}
