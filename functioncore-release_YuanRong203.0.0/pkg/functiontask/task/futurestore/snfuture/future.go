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

// Package snfuture implements the future.
package snfuture

import (
	"sync/atomic"
	"time"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/future"
)

// ID represents the future id.
type ID = string

// Future type.
const (
	// created by `invoke` API in runtime.
	TypeInvoke = iota
	// created by triggerbykey of a function.
	TypeTrigger
	// created by `waitForExternalEvent` API in runtime.
	TypeExternalEvent
)

// Future represents a promise which will be resolved in the future.
type Future struct {
	respFuture      *future.Future
	DeleteFromStore func(*Future)
	Metadata

	typ      int
	refCount int32

	SetTime time.Time

	// external event.
	eventName string
}

// Metadata represents metadata for future.
type Metadata struct {
	ID ID

	// trigger by key or invoke.
	ServiceID string
	FuncName  string
	Version   string
	StateID   string
	TraceID   common.ReqID     // trigger by key
	ReqIDList common.ReqIDList // invoke
}

// NewInvoke returns a new future whose type is `TypeInvoke`.
func NewInvoke(metadata Metadata) *Future {
	if metadata.ID == "" {
		metadata.ID = "invoke$" + metadata.ServiceID + "$"
		if len(metadata.ReqIDList) > 0 {
			metadata.ID += metadata.ReqIDList[len(metadata.ReqIDList)-1]
		}
	}
	f := &Future{
		Metadata:   metadata,
		typ:        TypeInvoke,
		respFuture: future.New(),
		refCount:   1,
	}
	return f
}

// NewTrigger returns a new future whose type is `TypeTrigger`.
func NewTrigger(funcName string, stateID string, traceID common.ReqID) *Future {
	f := &Future{
		Metadata: Metadata{
			ID:       "trigger$" + traceID,
			FuncName: funcName,
			StateID:  stateID,
			TraceID:  traceID,
		},
		typ:        TypeTrigger,
		respFuture: future.New(),
		refCount:   1,
	}
	return f
}

// NewExternalEvent returns a new future whose type is `TypeExternalEvent`.
func NewExternalEvent(eventName string) *Future {
	f := &Future{
		Metadata: Metadata{
			ID: "event$" + eventName,
		},
		typ:        TypeExternalEvent,
		eventName:  eventName,
		respFuture: future.New(),
		refCount:   1,
	}
	return f
}

// GetResponseWithTimeout return the value of the future if it is obtained within `t`,
// otherwise return nil.
func (r *Future) GetResponseWithTimeout(t time.Duration) *common.Response {
	return r.respFuture.GetUntil(t)
}

// GetResponse blocking get the value of the future.
func (r *Future) GetResponse() *common.Response {
	return r.respFuture.Get()
}

// PeekResponse peeks the value of the future.
func (r *Future) PeekResponse() *common.Response {
	return r.respFuture.Peek()
}

// SetResponse sets the value of the future.
func (r *Future) SetResponse(result *common.Response) {
	r.respFuture.Set(result)
	r.SetTime = time.Now()
}

// ClearResponse clear the value of the future.
func (r *Future) ClearResponse() {
	r.respFuture.Clear()
}

// GetRefCount returns reference counting of the future
func (r *Future) GetRefCount() int32 {
	return atomic.LoadInt32(&r.refCount)
}

// AddRefCount adds reference counting of the future by 1
func (r *Future) AddRefCount() int32 {
	return atomic.AddInt32(&r.refCount, 1)
}

// ReduceRefCount reduces reference counting of the future by 1
func (r *Future) ReduceRefCount() int32 {
	return atomic.AddInt32(&r.refCount, -1)
}

// GetType get Future type.
func (r *Future) GetType() int {
	return r.typ
}
