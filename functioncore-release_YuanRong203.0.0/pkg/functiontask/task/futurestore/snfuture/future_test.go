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
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/future"
)

// TestNewInvoke checks a new invoke.
func TestNewInvoke(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func() {
		f := NewInvoke(Metadata{
			FuncName:  funcName,
			StateID:   stateID,
			ReqIDList: reqIDList,
		})
		So(f, ShouldNotBeNil)
		So(f.FuncName, ShouldEqual, funcName)
		So(f.StateID, ShouldEqual, stateID)
		So(f.ReqIDList, ShouldResemble, reqIDList)
		So(f.typ, ShouldEqual, TypeInvoke)
	})
}

// TestNewTrigger checks a new triggerbykey.
func TestNewTrigger(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	traceID := common.NewReqID(nil, "", "", 0)
	Convey("create triggerbykey future", t, func() {
		f := NewTrigger(funcName, stateID, traceID)
		So(f, ShouldNotBeNil)
		So(f.FuncName, ShouldEqual, funcName)
		So(f.StateID, ShouldEqual, stateID)
		So(f.TraceID, ShouldResemble, traceID)
		So(f.typ, ShouldEqual, TypeTrigger)
	})
}

// TestNewExternalEvent checks a new external event.
func TestNewExternalEvent(t *testing.T) {
	funcName := "event-name"
	Convey("create external event future", t, func() {
		f := NewExternalEvent(funcName)
		So(f, ShouldNotBeNil)
		So(f.eventName, ShouldEqual, funcName)
		So(f.typ, ShouldEqual, TypeExternalEvent)
	})
}

// TestResponse checks filling the future.
func TestResponse(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func() {
		f := NewInvoke(Metadata{
			FuncName:  funcName,
			StateID:   stateID,
			ReqIDList: reqIDList,
		})
		resp := &common.Response{
			Code:    0,
			Message: "12",
			Result:  "",
			StateID: "325",
			State:   "",
		}
		f.SetResponse(resp)
		So(f.GetResponse(), ShouldResemble, resp)
		So(f.GetResponseWithTimeout(time.Millisecond), ShouldResemble, resp)
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestGetResponseConcurrently(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func(c C) {
		f := NewInvoke(Metadata{
			FuncName:  funcName,
			StateID:   stateID,
			ReqIDList: reqIDList,
		})
		resp := &common.Response{Code: 100}
		f.SetResponse(resp)
		ch := make(chan bool, 100)
		for i := 0; i < 100; i++ {
			go func(c C) {
				c.So(f.GetResponse(), ShouldEqual, resp)
				ch <- true
			}(c)
		}
		for i := 0; i < 100; i++ {
			<-ch
		}
		So(f.GetResponse(), ShouldNotBeNil)
	})
}

// TestResponseTimeout checks filling the future with timeout.
func TestResponseTimeout(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func() {
		f := NewInvoke(Metadata{
			FuncName:  funcName,
			StateID:   stateID,
			ReqIDList: reqIDList,
		})
		So(f.GetResponseWithTimeout(time.Millisecond), ShouldBeNil)
	})
}

// TestResponseWithTimeout1 checks filling the future with timeout.
func TestResponseWithTimeout1(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func(c C) {
		f := NewInvoke(Metadata{
			FuncName:  funcName,
			StateID:   stateID,
			ReqIDList: reqIDList,
		})
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func(c C) {
			time.Sleep(time.Second)
			resp := &common.Response{
				Code:    0,
				Message: "123",
				Result:  "",
				StateID: "3225",
				State:   "",
			}
			f.SetResponse(resp)
			c.So(f.GetResponse(), ShouldResemble, resp)
			wg.Done()
		}(c)
		So(f.GetResponseWithTimeout(time.Millisecond), ShouldBeNil)
		wg.Wait()
	})
}

// TestResponseWithTimeout2 checks filling the future with timeout.
func TestResponseWithTimeout2(t *testing.T) {
	funcName := "test-func"
	stateID := "test"
	reqIDList := []common.ReqID{common.NewReqID(nil, "", "", 0)}
	Convey("create invoke future", t, func(c C) {
		f := NewInvoke(Metadata{
			FuncName:  funcName,
			StateID:   stateID,
			ReqIDList: reqIDList,
		})
		resp := &common.Response{
			Code:    0,
			Message: "143652",
			Result:  "",
			StateID: "365425",
			State:   "",
		}
		go func(c C) {
			time.Sleep(time.Millisecond)
			f.SetResponse(resp)
			c.So(f.GetResponse(), ShouldResemble, resp)
		}(c)
		So(f.GetResponseWithTimeout(time.Second), ShouldResemble, resp)
	})
}

// TestAddRefCountConcurrently checks add reference count concurrently.
func TestAddRefCountConcurrently(t *testing.T) {
	future := Future{}
	times := 1000000
	Convey("reference counter must equal with 1000000", t, func() {
		wg := sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				future.AddRefCount()
				wg.Done()
			}()
		}
		wg.Wait()
		So(future.refCount, ShouldEqual, times)
	})
}

// TestReduceRefCountConcurrently checks reduce reference count concurrently.
func TestReduceRefCountConcurrently(t *testing.T) {
	times := 1000000
	future := Future{
		refCount: int32(times),
	}
	Convey("reference counter must equal with 0", t, func() {
		wg := sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				future.ReduceRefCount()
				wg.Done()
			}()
		}
		wg.Wait()
		So(future.refCount, ShouldEqual, 0)
	})
}

func TestFuture_ClearResponse(t *testing.T) {
	type fields struct {
		respFuture      *future.Future
		DeleteFromStore func(*Future)
		Metadata        Metadata
		typ             int
		refCount        int32
		eventName       string
	}
	var f fields
	respFuture := &future.Future{}
	f.respFuture = respFuture
	tests := []struct {
		name   string
		fields fields
	}{
		{"case1", f},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Future{
				respFuture:      tt.fields.respFuture,
				DeleteFromStore: tt.fields.DeleteFromStore,
				Metadata:        tt.fields.Metadata,
				typ:             tt.fields.typ,
				refCount:        tt.fields.refCount,
				eventName:       tt.fields.eventName,
			}
			r.ClearResponse()
		})
	}
}

func TestFuture_GetRefCount(t *testing.T) {
	type fields struct {
		respFuture      *future.Future
		DeleteFromStore func(*Future)
		Metadata        Metadata
		typ             int
		refCount        int32
		eventName       string
	}
	var f fields
	f.refCount = 1
	tests := []struct {
		name   string
		fields fields
		want   int32
	}{
		{"case1", f, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Future{
				respFuture:      tt.fields.respFuture,
				DeleteFromStore: tt.fields.DeleteFromStore,
				Metadata:        tt.fields.Metadata,
				typ:             tt.fields.typ,
				refCount:        tt.fields.refCount,
				eventName:       tt.fields.eventName,
			}
			if got := r.GetRefCount(); got != tt.want {
				t.Errorf("GetRefCount() = %v, want %v", got, tt.want)
			}
		})
	}
}
